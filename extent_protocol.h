// extent wire protocol

#ifndef extent_protocol_h
#define extent_protocol_h

#include "rpc.h"

class extent_protocol {
 public:
  typedef int status;
  typedef unsigned long long extentid_t;
  enum xxstatus { OK, NOENT };
  enum rpc_number {
    acquire = 0x6001,
  };

  enum types {
    T_DIR = 1,
    T_FILE,
    T_SYMLINK,
  };

  struct attr {
    uint32_t type;
    unsigned int atime;
    unsigned int mtime;
    unsigned int ctime;
    unsigned int size;
  };

  struct extent {
    extentid_t eid;
    struct attr attr;
    std::string data;
  };
};

class rextent_protocol {
public:
  enum xxstatus { OK, NOENT };
  typedef int status;
  enum rpc_numbers {
    revoke = 0x5001
  };
};

inline unmarshall &
operator>>(unmarshall &u, extent_protocol::attr &a)
{
  u >> a.type;
  u >> a.atime;
  u >> a.mtime;
  u >> a.ctime;
  u >> a.size;
  return u;
}

inline marshall &
operator<<(marshall &m, extent_protocol::attr a)
{
  m << a.type;
  m << a.atime;
  m << a.mtime;
  m << a.ctime;
  m << a.size;
  return m;
}

inline unmarshall &
operator>>(unmarshall &u, extent_protocol::extent &e)
{
  u >> e.eid;
  u >> e.attr;
  u >> e.data;
  return u;
}

inline marshall &
operator<<(marshall &m, extent_protocol::extent e)
{
  m << e.eid;
  m << e.attr;
  m << e.data;
  return m;
}

#endif 
