// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include <map>
#include <pthread.h>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"


// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
 private:
  enum lock_state { NONE, FREE, LOCKED, ACQUIRING, RELEASING };
 private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  pthread_mutex_t *mutex;
  std::map<lock_protocol::lockid_t, lock_state> state;
  std::map<lock_protocol::lockid_t, bool> revoke;
  std::map<lock_protocol::lockid_t, int> waiting;
  std::map<lock_protocol::lockid_t, pthread_cond_t *> cv;
 public:
  static int last_port;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                                       int &);
};


#endif
