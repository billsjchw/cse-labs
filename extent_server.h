// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include <pthread.h>
#include "extent_protocol.h"
#include "inode_manager.h"

class extent_server {
 protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;
  pthread_mutex_t mutex;
  std::map<extent_protocol::extentid_t, std::map<std::string, unsigned>> caching;
  std::map<extent_protocol::extentid_t, unsigned> version;
 public:
  extent_server();
  int create(std::string id, uint32_t type, extent_protocol::extentid_t &eid);
  int put(std::string id, extent_protocol::extentid_t eid, std::string, int &);
  int get(std::string id, extent_protocol::extentid_t eid, std::string &);
  int getattr(std::string id, extent_protocol::extentid_t eid, extent_protocol::attr &);
  int remove(std::string id, extent_protocol::extentid_t eid, int &);
 private:
  void invalidate(extent_protocol::extentid_t eid, unsigned new_version,
                  const std::map<std::string, unsigned> &to_invs);
};

#endif
