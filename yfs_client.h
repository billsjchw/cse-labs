#ifndef yfs_client_h
#define yfs_client_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


class yfs_client {
  extent_client *ec;
  lock_client *lc;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct symlinkinfo {
    unsigned long long size;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

 private:
  int mk(inum, const char *, mode_t, extent_protocol::types, inum &);

 public:
  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);
  bool issymlink(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  int getsymlink(inum, symlinkinfo &);

  int setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum,const char *);
  int mkdir(inum, const char *, mode_t, inum &);
  int symlink(const char *, inum, const char *, inum &);
  int readlink(inum, std::string &);
};

#endif 
