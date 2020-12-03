// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include <map>
#include <set>
#include <utility>
#include <pthread.h>
#include "extent_protocol.h"
#include "extent_server.h"

class extent_client {
 private:
  rpcc *cl;
  int rextent_port;
  std::string hostname;
  std::string id;
  pthread_mutex_t mutex;
  std::map<extent_protocol::extentid_t, extent_protocol::extent> cache;
  std::set<extent_protocol::extentid_t> free_eids;
 public:
  static int last_port;
  extent_client(std::string dst);
  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  rextent_protocol::status revoke_handler(extent_protocol::extentid_t eid, extent_protocol::extent &e);
};

#endif
