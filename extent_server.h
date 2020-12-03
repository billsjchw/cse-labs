// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include <set>
#include <pthread.h>
#include "extent_protocol.h"
#include "inode_manager.h"

class extent_server {
 private:
  inode_manager *im;
  pthread_mutex_t mutex;
  std::map<extent_protocol::extentid_t, std::string> owner;
  std::set<extent_protocol::extentid_t> free_eids;
 public:
  extent_server();
  extent_protocol::status acquire(std::string cid, extent_protocol::extentid_t eid, extent_protocol::extent &e);
};

#endif
