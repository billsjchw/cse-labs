// the extent server implementation

#include "extent_server.h"
#include "handle.h"
#include "extent_protocol.h"
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <set>

extent_server::extent_server() 
{
  im = new inode_manager();
  mutex = PTHREAD_MUTEX_INITIALIZER;
}

int extent_server::create(std::string id, uint32_t type, extent_protocol::extentid_t &eid)
{
  // alloc a new inode and return inum
  unsigned new_version = 0;
  std::map<std::string, unsigned> to_invs;

  printf("extent_server: create inode\n");

  pthread_mutex_lock(&mutex);

  eid = im->alloc_inode(type);
  new_version = ++version[eid];
  to_invs = caching[eid];

  pthread_mutex_unlock(&mutex);

  invalidate(eid, new_version, to_invs);

  return extent_protocol::OK;
}

int extent_server::put(std::string id, extent_protocol::extentid_t eid, std::string buf, int &)
{
  unsigned new_version = 0;
  std::map<std::string, unsigned> to_invs;

  eid &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  int size = buf.size();

  pthread_mutex_lock(&mutex);

  im->write_file(eid, cbuf, size);
  new_version = ++version[eid];
  to_invs = caching[eid];
  
  pthread_mutex_unlock(&mutex);

  invalidate(eid, new_version, to_invs);

  return extent_protocol::OK;
}

int extent_server::get(std::string id, extent_protocol::extentid_t eid, std::string &buf)
{
  printf("extent_server: get %lld\n", eid);

  eid &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  pthread_mutex_lock(&mutex);

  im->read_file(eid, &cbuf, &size);
  caching[eid][id] = version[eid];
  
  pthread_mutex_unlock(&mutex);

  if (size == 0) {
    buf = "";
  } else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(std::string id, extent_protocol::extentid_t eid, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", eid);

  eid &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  
  pthread_mutex_lock(&mutex);

  im->getattr(eid, attr);
  
  pthread_mutex_unlock(&mutex);

  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(std::string id, extent_protocol::extentid_t eid, int &)
{
  unsigned new_version = 0;
  std::map<std::string, unsigned> to_invs;

  printf("extent_server: write %lld\n", eid);

  eid &= 0x7fffffff;
  
  pthread_mutex_lock(&mutex);

  im->remove_file(eid);
  new_version = ++version[eid];
  to_invs = caching[eid];

  pthread_mutex_unlock(&mutex);

  invalidate(eid, new_version, to_invs);

  return extent_protocol::OK;
}

void extent_server::invalidate(extent_protocol::extentid_t eid, unsigned new_version,
                               const std::map<std::string, unsigned> &to_invs) {
  std::set<std::string> success_invs;

  for (const auto &p : to_invs) {
    handle h(p.first);
    rpcc *cl = h.safebind();
    int dummy = 0;
    if (cl != NULL && cl->call(iextent_protocol::invalidate, eid, dummy) == iextent_protocol::OK)
      success_invs.insert(p.first);
  }

  pthread_mutex_lock(&mutex);

  for (const auto &id : success_invs)
    if (caching[eid][id] < new_version)
      caching[eid].erase(id);

  pthread_mutex_unlock(&mutex);
}
