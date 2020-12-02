// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() 
{
  im = new inode_manager();
  mutex = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
  *mutex = PTHREAD_MUTEX_INITIALIZER;
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");

  pthread_mutex_lock(mutex);

  id = im->alloc_inode(type);

  pthread_mutex_unlock(mutex);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  int size = buf.size();

  pthread_mutex_lock(mutex);

  im->write_file(id, cbuf, size);
  
  pthread_mutex_unlock(mutex);

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  pthread_mutex_lock(mutex);

  im->read_file(id, &cbuf, &size);
  
  pthread_mutex_unlock(mutex);

  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  
  pthread_mutex_lock(mutex);

  im->getattr(id, attr);
  
  pthread_mutex_unlock(mutex);

  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;
  
  pthread_mutex_lock(mutex);

  im->remove_file(id);

  pthread_mutex_unlock(mutex);

  return extent_protocol::OK;
}
