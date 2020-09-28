#include "inode_manager.h"
#include <cstring>
#include <ctime>

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

#define FIRST_DATA_BLOCK (IBLOCK(INODE_NUM - 1, BLOCK_NUM) + 1)

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  int mask;
  char buf[BLOCK_SIZE];
  blockid_t id, ret;

  ret = 0;
  for (id = FIRST_DATA_BLOCK; id < BLOCK_NUM && ret == 0; ++id) {
    mask = 1 << (id % BPB % 8);
    read_block(BBLOCK(id), buf);
    if ((buf[id % BPB / 8] & mask) == 0) {
      buf[id % BPB / 8] |= mask;
      write_block(BBLOCK(id), buf);
      ret = id;
    }
  }

  return ret;
}

void
block_manager::free_block(uint32_t id)
{
  int mask;
  char buf[BLOCK_SIZE];

  mask = 1 << (id % BPB % 8);
  read_block(BBLOCK(id), buf);
  buf[id % BPB / 8] &= ~mask;
  write_block(BBLOCK(id), buf);

  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  uint32_t inum, ret;
  inode_t *ino;

  ret = 0;
  for (inum = 1; inum < INODE_NUM && ret == 0; ++inum) {
    ino = get_inode(inum);
    if (ino == NULL) {
      ino = (inode_t *) malloc(sizeof(inode_t));
      ino->type = type;
      ino->size = 0;
      ino->atime = ino->mtime = ino->ctime = (unsigned) time(NULL);
      put_inode(inum, ino);
      ret = inum;
    }
    free(ino);
  }
  
  return ret;
}

void
inode_manager::free_inode(uint32_t inum)
{
  inode_t *ino;

  ino = get_inode(inum);
  if (ino == NULL)
    return;

  ino->type = 0;
  put_inode(inum, ino);

  free(ino);

  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))
#define FILE_BLOCK_NUM(size) ((size) / BLOCK_SIZE + ((size) % BLOCK_SIZE != 0 ? 1 : 0))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  char *ptr;
  inode_t *ino;
  blockid_t id;
  unsigned i, n;
  char buf[BLOCK_SIZE];
  blockid_t ids[NINDIRECT];

  ino = get_inode(inum);
  if (ino == NULL) {
    *buf_out = NULL;
    *size = 0;
    return;
  }

  n = FILE_BLOCK_NUM(ino->size);

  *size = ino->size;
  *buf_out = ptr = (char *) malloc(n * BLOCK_SIZE);

  if (n > NDIRECT)
    bm->read_block(ino->blocks[NDIRECT], (char *) ids);
  for (i = 0; i < n; ++i) {
    id = i < NDIRECT ? ino->blocks[i] : ids[i - NDIRECT];
    bm->read_block(id, buf);
    memcpy(ptr, buf, BLOCK_SIZE);
    ptr += BLOCK_SIZE;
  }

  ino->atime = (unsigned) time(NULL);
  put_inode(inum, ino);

  free(ino);
  
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  char *buf_ext;
  const char *ptr;
  inode_t *ino;
  unsigned i, j, n, m, total_alloc;
  blockid_t id, ids[NINDIRECT], alloc_ids[MAXFILE];

  ino = get_inode(inum);
  if (ino == NULL)
    return;

  n = FILE_BLOCK_NUM(ino->size);
  m = FILE_BLOCK_NUM(size);
  if (n > NDIRECT)
    bm->read_block(ino->blocks[NDIRECT], (char *) ids);

  total_alloc = 0;
  if (m > n)
    total_alloc += m - n;
  if (n <= NDIRECT && m > NDIRECT)
    ++total_alloc;
  for (i = 0; i < total_alloc; ++i) {
    id = bm->alloc_block();
    if (id == 0) {
      for (j = 0; j < i; ++j)
        bm->free_block(alloc_ids[j]);
      return;
    }
    alloc_ids[i] = id;
  }

  j = 0;
  if (n <= NDIRECT && m > NDIRECT)
    ino->blocks[NDIRECT] = alloc_ids[j++];
  for (i = n; i < m; ++i)
    if (i < NDIRECT)
      ino->blocks[i] = alloc_ids[j++];
    else
      ids[i - NDIRECT] = alloc_ids[j++];

  buf_ext = (char *) malloc(m * BLOCK_SIZE);
  memcpy(buf_ext, buf, size);
  ptr = buf_ext;
  for (i = 0; i < m; ++i) {
    id = i < NDIRECT ? ino->blocks[i] : ids[i - NDIRECT];
    bm->write_block(id, ptr);
    ptr += BLOCK_SIZE;
  }
  free(buf_ext);
  for (i = m; i < n; ++i) {
    id = i < NDIRECT ? ino->blocks[i] : ids[i - NDIRECT];
    bm->free_block(id);
  }

  ino->size = size;
  ino->atime = ino->ctime = ino->mtime = (unsigned) time(NULL);
  put_inode(inum, ino);
  if (m > NDIRECT)
    bm->write_block(ino->blocks[NDIRECT], (char *) ids);
  else if (n > NDIRECT)
    bm->free_block(ino->blocks[NDIRECT]);
  
  free(ino);

  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  inode_t * ino;

  ino = get_inode(inum);
  if (ino != NULL) {
    a.type = ino->type;
    a.size = ino->size;
    a.atime = ino->atime;
    a.mtime = ino->mtime;
    a.ctime = ino->ctime;
    free(ino);
  }

  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  inode_t *ino;
  unsigned i, n;
  blockid_t id, ids[NINDIRECT];

  ino = get_inode(inum);
  if (ino == NULL)
    return;

  n = FILE_BLOCK_NUM(ino->size);
  if (n > NDIRECT)
    bm->read_block(ino->blocks[NDIRECT], (char *) ids);

  for (i = 0; i < n; ++i) {
    id = i < NDIRECT ? ino->blocks[i] : ids[i - NDIRECT];
    bm->free_block(id);
  }

  if (n > NDIRECT)
    bm->free_block(ino->blocks[NDIRECT]);
  free_inode(inum);

  free(ino);

  return;
}
