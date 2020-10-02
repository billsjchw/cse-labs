#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <map>
#include <list>
#include <pthread.h>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
 private:
  int nacquire;
  pthread_mutex_t *mutex;
  std::map<lock_protocol::lockid_t, std::string> owner;
  std::map<lock_protocol::lockid_t, std::list<std::string> > waiting;
  std::map<std::string, rpcc *> cli;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string, int &);
  int release(lock_protocol::lockid_t, std::string, int &);
 private:
  void add_to_waiting(lock_protocol::lockid_t, std::string);
  rpcc *get_rpcc(std::string hostandport);
};

#endif
