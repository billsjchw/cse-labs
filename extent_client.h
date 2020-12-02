// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include <map>
#include <utility>
#include <pthread.h>
#include "extent_protocol.h"
#include "extent_server.h"

class extent_client {
 private:
  rpcc *cl;
  int iextent_port;
  std::string hostname;
  std::string id;
  pthread_mutex_t mutex;
  std::map<extent_protocol::extentid_t, std::pair<unsigned, std::string>> cache;
  std::map<extent_protocol::extentid_t, unsigned> version;
 public:
  static int last_port;
  extent_client(std::string dst);
  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  iextent_protocol::status invalidate_handler(extent_protocol::extentid_t, int &);
};

#endif
