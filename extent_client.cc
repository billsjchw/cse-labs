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

int extent_client::last_port = 12345;

extent_client::extent_client(std::string dst): mutex(PTHREAD_MUTEX_INITIALIZER){
  sockaddr_in dstsock;
  std::ostringstream host;
  rpcs *resrpc = NULL;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0)
    printf("extent_client: bind failed\n");
  srand(time(NULL) ^ last_port);
  rextent_port = ((rand() % 32000) | (0x1 << 10));
  host << "127.0.0.1:" << rextent_port;
  id = host.str();
  last_port = rextent_port;
  resrpc = new rpcs(rextent_port);
  resrpc->reg(rextent_protocol::revoke, this, &extent_client::revoke_handler);
}

extent_protocol::status extent_client::create(uint32_t type, extent_protocol::extentid_t &eid) {
  extent_protocol::status ret = extent_protocol::OK;
  extent_protocol::status server_ret = extent_protocol::OK;
  extent_protocol::extent e;

  // printf("CREATE\n");

  pthread_mutex_lock(&mutex);

  if (!free_eids.empty()) {
    eid = *free_eids.begin();
    free_eids.erase(free_eids.begin());
  } else {
    pthread_mutex_unlock(&mutex);
    server_ret = cl->call(extent_protocol::acquire, id, (extent_protocol::extentid_t) 0, e);
    pthread_mutex_lock(&mutex);
    if (server_ret != extent_protocol::OK || e.eid == 0) {
      ret = extent_protocol::NOENT;
      goto release;
    }
    eid = e.eid;
  }

  cache[eid].eid = eid;
  cache[eid].attr.type = type;
  cache[eid].attr.size = 0;
  cache[eid].attr.atime = cache[eid].attr.ctime = cache[eid].attr.mtime = (unsigned) time(NULL);
  cache[eid].data = "";

release:
  pthread_mutex_unlock(&mutex);
  return ret;
}

extent_protocol::status extent_client::get(extent_protocol::extentid_t eid, std::string &buf) {
  extent_protocol::status ret = extent_protocol::OK;
  extent_protocol::status server_ret = extent_protocol::OK;
  extent_protocol::extent e;

  // printf("GET %llu\n", eid);

  pthread_mutex_lock(&mutex);

  if (!cache.count(eid)) {
    pthread_mutex_unlock(&mutex);
    server_ret = cl->call(extent_protocol::acquire, id, eid, e);
    pthread_mutex_lock(&mutex);
    if (server_ret != extent_protocol::OK) {
      ret = extent_protocol::NOENT;
      goto release;
    }
    cache[eid] = e;
    if (e.attr.type == 0)
      free_eids.insert(eid);
  }

  if (cache[eid].attr.type == 0)
    ret = extent_protocol::NOENT;

  buf = cache[eid].data;
  cache[eid].attr.atime = (unsigned) time(NULL);

release:
  pthread_mutex_unlock(&mutex);
  return ret;
}

extent_protocol::status extent_client::getattr(extent_protocol::extentid_t eid, extent_protocol::attr &a) {
  extent_protocol::status ret = extent_protocol::OK;
  extent_protocol::status server_ret = extent_protocol::OK;
  extent_protocol::extent e;

  // printf("GETATTR %llu\n", eid);

  pthread_mutex_lock(&mutex);

  if (!cache.count(eid)) {
    pthread_mutex_unlock(&mutex);
    server_ret = cl->call(extent_protocol::acquire, id, eid, e);
    pthread_mutex_lock(&mutex);
    if (server_ret != extent_protocol::OK) {
      ret = extent_protocol::NOENT;
      goto release;
    }
    cache[eid] = e;
    if (e.attr.type == 0)
      free_eids.insert(eid);
  }

  if (cache[eid].attr.type == 0) {
    ret = extent_protocol::NOENT;
    goto release;
  }

  a = cache[eid].attr;

release:
  pthread_mutex_unlock(&mutex);
  return ret;
}

extent_protocol::status extent_client::put(extent_protocol::extentid_t eid, const std::string &buf) {
  extent_protocol::status ret = extent_protocol::OK;
  extent_protocol::status server_ret = extent_protocol::OK;
  extent_protocol::extent e;

  // printf("PUT %llu\n", eid);

  pthread_mutex_lock(&mutex);

  if (!cache.count(eid)) {
    pthread_mutex_unlock(&mutex);
    server_ret = cl->call(extent_protocol::acquire, id, eid, e);
    pthread_mutex_lock(&mutex);
    if (server_ret != extent_protocol::OK) {
      ret = extent_protocol::NOENT;
      goto release;
    }
    cache[eid] = e;
    if (e.attr.type == 0)
      free_eids.insert(eid);
  }

  if (cache[eid].attr.type == 0) {
    ret = extent_protocol::NOENT;
    goto release;
  }

  cache[eid].data = buf;
  cache[eid].attr.size = buf.size();
  cache[eid].attr.atime = cache[eid].attr.ctime = cache[eid].attr.mtime = (unsigned) time(NULL);

release:
  pthread_mutex_unlock(&mutex);
  return ret;
}

extent_protocol::status extent_client::remove(extent_protocol::extentid_t eid) {
  extent_protocol::status ret = extent_protocol::OK;
  extent_protocol::status server_ret = extent_protocol::OK;
  extent_protocol::extent e;

  // printf("REMOVE %llu\n", eid);

  pthread_mutex_lock(&mutex);

  if (!cache.count(eid)) {
    pthread_mutex_unlock(&mutex);
    server_ret = cl->call(extent_protocol::acquire, id, eid, e);
    pthread_mutex_lock(&mutex);
    if (server_ret != extent_protocol::OK) {
      ret = extent_protocol::NOENT;
      goto release;
    }
    cache[eid] = e;
    if (e.attr.type == 0)
      free_eids.insert(eid);
  }

  cache[eid].attr.type = 0;
  cache[eid].attr.size = 0;
  cache[eid].data = "";
  
  free_eids.insert(eid);

release:
  pthread_mutex_unlock(&mutex);
  return ret;
}

rextent_protocol::status extent_client::revoke_handler(extent_protocol::extentid_t eid, extent_protocol::extent &e) {
  rextent_protocol::status ret = rextent_protocol::OK;

  // printf("REVOKE_HANDLER %llu\n", eid);

  pthread_mutex_lock(&mutex);

  if (!cache.count(eid)) {
    ret = rextent_protocol::NOENT;
    goto release;
  }

  e = cache[eid];
  cache.erase(eid);
  free_eids.erase(eid);

release:
  pthread_mutex_unlock(&mutex);
  return ret;
}
