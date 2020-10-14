// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
  mutex = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
  *mutex = PTHREAD_MUTEX_INITIALIZER;
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret, acquire_ret, dummy;

  pthread_mutex_lock(mutex);

  if (state.count(lid) == 0) {
    state[lid] = NONE;
    revoke[lid] = false;
    waiting[lid] = 0;
    cv[lid] = (pthread_cond_t *) malloc(sizeof(pthread_cond_t));
    *cv[lid] = PTHREAD_COND_INITIALIZER;
  }

  ++waiting[lid];

  while (state[lid] != FREE)
    if (state[lid] == NONE) {
      state[lid] = ACQUIRING;
      pthread_mutex_unlock(mutex);
      acquire_ret = cl->call(lock_protocol::acquire, lid, id, dummy);
      pthread_mutex_lock(mutex);
      if (acquire_ret == lock_protocol::OK)
        break;
    } else {
      pthread_cond_wait(cv[lid], mutex);
    }

  state[lid] = LOCKED;
  --waiting[lid];

  pthread_mutex_unlock(mutex);

  ret = lock_protocol::OK;

  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret, dummy;
  bool release = false;

  pthread_mutex_lock(mutex);

  if (revoke[lid] && waiting[lid] == 0) {
    state[lid] = RELEASING;
    revoke[lid] = false;
    release = true;
  } else {
    state[lid] = FREE;
    pthread_cond_signal(cv[lid]);
  }

  pthread_mutex_unlock(mutex);

  if (release) {
    cl->call(lock_protocol::release, lid, id, dummy);
    pthread_mutex_lock(mutex);
    state[lid] = NONE;
    pthread_cond_signal(cv[lid]);
    pthread_mutex_unlock(mutex);
  }

  ret = lock_protocol::OK;

  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret, dummy;
  bool release = false;

  pthread_mutex_lock(mutex);

  if (state[lid] == FREE && waiting[lid] == 0) {
    release = true;
    state[lid] = RELEASING;
  } else {
    revoke[lid] = true;
  }

  pthread_mutex_unlock(mutex);

  if (release) {
    cl->call(lock_protocol::release, lid, id, dummy);
    pthread_mutex_lock(mutex);
    state[lid] = NONE;
    pthread_cond_signal(cv[lid]);
    pthread_mutex_unlock(mutex);
  }

  ret = rlock_protocol::OK;

  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret, acquire_ret, dummy;
  
  acquire_ret = cl->call(lock_protocol::acquire, lid, id, dummy);

  if (acquire_ret == lock_protocol::OK) {
    pthread_mutex_lock(mutex);
    state[lid] = FREE;
    pthread_cond_signal(cv[lid]);
    pthread_mutex_unlock(mutex);
  }

  ret = rlock_protocol::OK;

  return ret;
}
