// the extent server implementation

#include "extent_server.h"
#include "handle.h"
#include "rpc.h"
#include "extent_protocol.h"
#include "inode_manager.h"
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>

extent_server::extent_server(): im(new inode_manager), mutex(PTHREAD_MUTEX_INITIALIZER) {
  for (extent_protocol::extentid_t eid = 2; eid < INODE_NUM; ++eid)
    free_eids.insert(eid);
}

extent_protocol::status extent_server::acquire(std::string cid, extent_protocol::extentid_t eid,
                                               extent_protocol::extent &e) {
  extent_protocol::status ret = extent_protocol::OK;
  rextent_protocol::status client_ret = rextent_protocol::OK;
  char *buf = NULL;
  int size = 0;

  pthread_mutex_lock(&mutex);

  if (eid == 0) {
    if (free_eids.empty()) {
      e.eid = 0;
    } else {
      e.eid = *free_eids.begin();
      owner[e.eid] = cid;
      free_eids.erase(free_eids.begin());
    }
    e.attr.type = 0;
    e.attr.size = 0;
    e.data = "";
    goto release;
  }

  if (owner.count(eid)) {
    pthread_mutex_unlock(&mutex);
    handle h(owner[eid]);
    rpcc *cl = h.safebind();
    client_ret = cl->call(rextent_protocol::revoke, eid, e);
    pthread_mutex_lock(&mutex);
    if (client_ret != rextent_protocol::OK) {
      ret = extent_protocol::NOENT;
      goto release;
    }
  } else {
    e.eid = eid;
    im->getattr(eid, e.attr);
    im->read_file(eid, &buf, &size);
    e.data = std::string(buf, size);
  }

  owner[eid] = cid;
  free_eids.erase(eid);

release:
  pthread_mutex_unlock(&mutex);
  return ret;
}
