// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
    // Lab2: Use lock_client_cache when you test lock_cache
    lc = new lock_client(lock_dst);
    // lc = new lock_client_cache(lock_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
{
    return !isfile(inum) && !issymlink(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    size_t n;
    std::string buf;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    if (ec->get(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    n = buf.size();
    if (n < size)
        buf += std::string(size - n, '\0');
    else
        buf = buf.substr(0, size);

    if (ec->put(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

release:
    return r;
}

#define ALIGN(size) (((size) + 0x3) & ~0x3)

int yfs_client::mk(inum parent, const char *name, mode_t mode,
        extent_protocol::types type, inum &ino_out) {
        int r = OK;
    uint32_t ino;
    uint16_t rec_len, name_len;
    std::string buf;
    extent_protocol::extentid_t eid;
    bool found;

    if ((r = lookup(parent, name, found, ino_out)) != OK)
        goto release;
    if (found) {
        r = EXIST;
        goto release;
    }

    if (ec->get(parent, buf) != OK) {
        r = IOERR;
        goto release;
    }

    if (ec->create(type, eid) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    ino = eid;
    name_len = strlen(name);
    rec_len = ALIGN(8 + name_len);
    buf += std::string((char *) &ino, 4);
    buf += std::string((char *) &rec_len, 2);
    buf += std::string((char *) &name_len, 2);
    buf += std::string(name);
    buf += std::string(rec_len - name_len - 8, '\0');
    if (ec->put(parent, buf) != extent_protocol::OK) {
        ec->remove(eid);
        r = IOERR;
        goto release;
    }

    ino_out = eid;

release:
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    return mk(parent, name, mode, extent_protocol::T_FILE, ino_out);
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    return mk(parent, name, mode, extent_protocol::T_DIR, ino_out);
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    std::list<dirent> list;
    std::list<dirent>::iterator itr;

    if ((r = readdir(parent, list)) != OK)
        goto release;

    found = false;
    for (itr = list.begin(); itr != list.end(); ++itr)
        if (itr->name.compare(name) == 0) {
            found = true;
            ino_out = itr->inum;
            break;
        }

release:
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    const char *ptr;
    uint32_t ino;
    uint16_t rec_len, name_len;
    std::string buf;
    dirent den;
    size_t left;
    int r = OK;

    if (ec->get(dir, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    left = buf.size();
    ptr = buf.data();
    while (left > 0) {
        memcpy(&ino, ptr, 4);
        memcpy(&rec_len, ptr + 4, 2);
        memcpy(&name_len, ptr + 6, 2);
        den.inum = ino;
        den.name = std::string(ptr + 8, name_len);
        list.push_back(den);
        ptr += rec_len;
        left -= rec_len;
    }

release:
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    std::string buf;

    if (ec->get(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    data = buf.substr(off, size);
    
release:
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    size_t n;
    std::string buf;

    if (ec->get(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    n = buf.size();
    if (off < (int) n) {
        buf = buf.substr(0, off) + std::string(data, size) +
                buf.substr(off + size, n - off - size);
        bytes_written = size;
    } else {
        buf = buf + std::string(off - n, '\0') + std::string(data, size);
        bytes_written = off - n + size;
    }

    if (ec->put(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

release:
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    std::string buf;
    inum ino_found;
    uint32_t ino;
    uint16_t rec_len, name_len;
    size_t n, left;
    const char *ptr;

    if (ec->get(parent, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    n = buf.size();

    ino_found = 0;
    left = n;
    ptr = buf.data();
    while (left > 0) {
        memcpy(&ino, ptr, 4);
        memcpy(&rec_len, ptr + 4, 2);
        memcpy(&name_len, ptr + 6, 2);
        if (std::string(ptr + 8, name_len).compare(name) == 0) {
            ino_found = ino;
            buf = buf.substr(0, n - left) +
                    buf.substr(n - left + rec_len, left - rec_len);
            break;
        }
        ptr += rec_len;
        left -= rec_len;
    }

    if (ino_found == 0) {
        r = NOENT;
        goto release;
    }

    if (ec->remove(ino_found) != extent_protocol::OK) {
        r = IOERR;
        goto release;    
    }

    if (ec->put(parent, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

release:
    return r;
}


bool
yfs_client::issymlink(inum ino)
{
    extent_protocol::attr a;

    if (ec->getattr(ino, a) != extent_protocol::OK)
        return false;

    if (a.type == extent_protocol::T_SYMLINK)
        return true;

    return false;
}

int
yfs_client::getsymlink(inum inum, symlinkinfo &sin)
{
    int r = OK;

    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    sin.size = a.size;

release:
    return r;
}

int
yfs_client::symlink(const char *link, inum parent,
        const char *name, inum &ino_out)
{
    int r = OK;
    size_t bytes_written;

    if ((r = mk(parent, name, 0, extent_protocol::T_SYMLINK, ino_out)) != OK)
        goto release;

    if ((r = write(ino_out, strlen(link), 0, link, bytes_written)) != OK)
        goto release;

release:
    return r;
}

int
yfs_client::readlink(inum ino, std::string &link)
{
    int r = OK;

    if (ec->get(ino, link) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

release:
    return r;
}
