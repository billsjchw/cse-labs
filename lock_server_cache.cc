// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
  mutex = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
  *mutex = PTHREAD_MUTEX_INITIALIZER;
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int &)
{
  lock_protocol::status ret;
  std::string revokeid = "";
  bool revoke = false;
  rpcc *cl;
  int dummy;

  pthread_mutex_lock(mutex);

  if (owner.count(lid) == 0) {
    owner[lid] = "";
    waiting[lid] = std::list<std::string>();
  }

  add_to_waiting(lid, id);

  if (waiting[lid].front() != id) {
    ret = lock_protocol::RETRY;
  } else if (owner[lid] != "" && owner[lid] != id) {
    revokeid = owner[lid];
    ret = lock_protocol::RETRY;
  } else {
    waiting[lid].pop_front();
    owner[lid] = id;
    ret = lock_protocol::OK;
    if (!waiting[lid].empty())
      revoke = true;
  }

  pthread_mutex_unlock(mutex);

  if (revokeid != "") {
    cl = get_rpcc(revokeid);
    cl->bind();
    cl->call(rlock_protocol::revoke, lid, dummy);
  }

  if (revoke) {
    cl = get_rpcc(id);
    cl->bind();
    cl->call(rlock_protocol::revoke, lid, dummy);
  }

  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, int &)
{
  lock_protocol::status ret;
  std::string retryid = "";
  rpcc *cl;
  int dummy;

  pthread_mutex_lock(mutex);

  owner[lid] = "";
  if (!waiting[lid].empty())
    retryid = waiting[lid].front();

  pthread_mutex_unlock(mutex);
  
  if (retryid != "") {
    cl = get_rpcc(retryid);
    cl->bind();
    cl->call(rlock_protocol::retry, lid, dummy);
  }

  ret = lock_protocol::OK;

  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

void
lock_server_cache::add_to_waiting(lock_protocol::lockid_t lid, std::string id)
{
  bool found;
  std::list<std::string>::iterator itr;

  for (itr = waiting[lid].begin(); itr != waiting[lid].end(); ++itr)
    if (*itr == id) {
      found = true;
      break;
    }

  if (!found)
    waiting[lid].push_back(id);
}

rpcc *
lock_server_cache::get_rpcc(std::string hostandport) {
  sockaddr_in dstsock;
  rpcc *ret;

  pthread_mutex_lock(mutex);

  if (cli.count(hostandport) == 0) {
    make_sockaddr(hostandport.c_str(), &dstsock);
    cli[hostandport] = new rpcc(dstsock);
  }

  ret = cli[hostandport];

  pthread_mutex_unlock(mutex);

  return ret;
}
