// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  mutex = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
  *mutex = PTHREAD_MUTEX_INITIALIZER;
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &)
{
  lock_protocol::status ret = lock_protocol::OK;

  pthread_mutex_lock(mutex);

  if (locked.count(lid) == 0) {
    locked[lid] = false;
    cvs[lid] = (pthread_cond_t *) malloc(sizeof(pthread_cond_t));
    *cvs[lid] = PTHREAD_COND_INITIALIZER;
  }
  while (locked[lid])
    pthread_cond_wait(cvs[lid], mutex);
  locked[lid] = true;

  pthread_mutex_unlock(mutex);

  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &)
{
  lock_protocol::status ret = lock_protocol::OK;

  pthread_mutex_lock(mutex);

  if (locked.count(lid) > 0) {
    locked[lid] = false;
    pthread_cond_signal(cvs[lid]);
  }
  
  pthread_mutex_unlock(mutex);

  return ret;
}