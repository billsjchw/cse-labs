// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <utility>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <stdlib.h>

int extent_client::last_port = 0;

extent_client::extent_client(std::string dst): mutex(PTHREAD_MUTEX_INITIALIZER)
{
  sockaddr_in dstsock;
  std::ostringstream host;
  rpcs *iesrpc = NULL;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
  srand(time(NULL) ^ last_port);
  iextent_port = ((rand() % 32000) | (0x1 << 10));
  host << "127.0.0.1:" << iextent_port;
  id = host.str();
  last_port = iextent_port;
  iesrpc = new rpcs(iextent_port);
  iesrpc->reg(iextent_protocol::invalidate, this, &extent_client::invalidate_handler);
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  
  ret = cl->call(extent_protocol::create, id, type, eid);

  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  unsigned new_version = 0;
  
  pthread_mutex_lock(&mutex);

  if (cache.count(eid) && cache[eid].first == version[eid]) {
    buf = cache[eid].second;
    goto release;
  }
  new_version = version[eid];

  pthread_mutex_unlock(&mutex);
  ret = cl->call(extent_protocol::get, id, eid, buf);
  pthread_mutex_lock(&mutex);
  if (ret == extent_protocol::OK)
    cache[eid] = std::make_pair(new_version, buf);

release:
  pthread_mutex_unlock(&mutex);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  
  ret = cl->call(extent_protocol::getattr, id, eid, attr);

  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  int dummy = 0;

  ret = cl->call(extent_protocol::put, id, eid, buf, dummy);

  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  int dummy = 0;
  
  ret = cl->call(extent_protocol::remove, id, eid, dummy);

  return ret;
}

iextent_protocol::status extent_client::invalidate_handler(extent_protocol::extentid_t eid, int &) {
  pthread_mutex_lock(&mutex);

  ++version[eid];

  pthread_mutex_unlock(&mutex);
  return iextent_protocol::OK;
}
