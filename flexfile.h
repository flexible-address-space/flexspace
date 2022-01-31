#ifndef _FLEXFILE_H
#define _FLEXFILE_H

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "c/ctypes.h"
#include "c/lib.h"
#include "flextree.h"
#include "generic.h"

// the address space flexfile manages
#ifdef FLEXFILE_MAX_OFFSET_OVERRIDE
#define FLEXFILE_MAX_OFFSET FLEXFILE_MAX_OFFSET_OVERRIDE
#else
#define FLEXFILE_MAX_OFFSET (800lu << 30) // 800 GB
#endif
static_assert(FLEXFILE_MAX_OFFSET >= (4lu << 30), "dont manage small space");

// block config
#define FLEXFILE_BLOCK_BITS (22u) // 4M
#define FLEXFILE_BLOCK_SIZE (1u << FLEXFILE_BLOCK_BITS)
#define FLEXFILE_BLOCK_COUNT (FLEXFILE_MAX_OFFSET >> FLEXFILE_BLOCK_BITS)
#define FLEXFILE_MAX_EXTENT_BIT (5u)
#define FLEXFILE_MAX_EXTENT_SIZE (FLEXFILE_BLOCK_SIZE >> FLEXFILE_MAX_EXTENT_BIT) // 128K (1/32)
static_assert(FLEXFILE_BLOCK_BITS < 32, "one u32 to track one block");
static_assert(FLEXFILE_BLOCK_COUNT < (1lu<<48), "no more than 2^48 blocks");

// logical logging
#define FLEXFILE_LOG_MEM_CAP (8u << 20) // buffered log size 8M
#define FLEXFILE_LOG_MAX_SIZE (2u << 30) // max on-disk log size 2G

#define FLEXFILE_IO_URING
// block manager
#ifdef FLEXFILE_IO_URING
#include <liburing.h>
#define FLEXFILE_BM_DEPTH (16) // uring
#define FLEXFILE_BM_BATCH_SIZE (FLEXFILE_BM_DEPTH >> 2) // uring
static_assert(FLEXFILE_BM_DEPTH <= 512, "no more than 512 blocks in a ring");
#endif
#define FLEXFILE_BM_BLKDIST_BITS (16) // stat
#define FLEXFILE_BM_BLKDIST_SIZE ((FLEXFILE_BLOCK_SIZE >> FLEXFILE_BM_BLKDIST_BITS) + 1) // stat

// garbage collector
#define FLEXFILE_GC_QUEUE_DEPTH (8192)
#define FLEXFILE_GC_THRESHOLD (64)
static_assert(FLEXFILE_GC_THRESHOLD >= (1u << FLEXFILE_MAX_EXTENT_BIT), "need some blocks to do the final gc");

#ifdef FLEXFILE_IO_URING
#define FLEXFILE_RRING_DEPTH (32)
#define FLEXFILE_RRING_BATCH_SIZE (FLEXFILE_RRING_DEPTH >> 1)
#endif

/*
 * Note:
 * The current implementation of io engine that wraps the flextree structure
 * does not allow holes in file, which simplified the usage by applications
 *
 * One should always call flexfile_sync() manually to guarantee the metadata
 * consistency, if one only calls flexfile_write() without a flexfile_sync(),
 * the data could be written but it is not logged in the flextree. The space in
 * the log can be rewritten.
 */

struct flexfile_bm; // block manager
struct flexfile_log_entry;
#ifdef FLEXFILE_IO_URING
struct flexfile_rring;
#endif

struct flexfile {
    char *path;
    struct flextree *flextree;
    int fd;     // data file fd
    int log_fd; // logical log fd
    u8 *log_buf;
    u32 log_buf_size;
    u32 log_total_size;
    struct flexfile_bm* bm;
    struct {
        u64 loff;
        struct {
            struct flextree_node *node;
            u64 poff;
            u32 len;
            u32 idx;
            u8 *buf;
        } *queue;
        u32 count;
        u8 write_between_stages;
    } gc_ctx;
};

struct flexfile_handler {
    const struct flexfile *file;
    struct flextree_pos fp; // cache the flextree position
};

/*
 * General stateful APIs
 */

extern struct flexfile *flexfile_open(const char *const path);

extern void flexfile_close(struct flexfile *const ff);

extern void flexfile_sync(struct flexfile *const ff);

extern ssize_t flexfile_read(struct flexfile *const ff, void *const buf, const u64 loff, const u64 len);

extern ssize_t flexfile_insert(struct flexfile *const ff, const void *const buf, const u64 loff, const u64 len);

extern int flexfile_collapse(struct flexfile *const ff, const u64 loff, const u64 len);

extern ssize_t flexfile_write(struct flexfile *const ff, const void *const buf, const u64 loff, const u64 len);

extern int flexfile_set_tag(struct flexfile *const ff, const u64 loff, const u16 tag);

extern int flexfile_get_tag(const struct flexfile *const ff, const u64 loff, u16 *const tag);

extern ssize_t flexfile_update(
        struct flexfile *const ff, const void *const buf, const u64 loff, const u64 len, const u64 olen);

extern ssize_t flexfile_read_fragmentation(
        struct flexfile *const ff, void *const buf, const u64 loff, const u64 len, u64 *const frag);

extern int flexfile_defrag(struct flexfile *const ff, const void *const buf, const u64 loff, const u64 len);

extern unsigned long flexfile_size(const struct flexfile *const ff);

extern void flexfile_gc(struct flexfile *const ff);

extern int flexfile_fallocate(struct flexfile *const ff, const u64 loff, const u64 size);

extern int flexfile_ftruncate(struct flexfile *const ff, const u64 size);

#ifdef FLEXFILE_IO_URING
extern struct flexfile_rring *flexfile_rring_create(struct flexfile *const ff, void *const buf, const u64 bufsz);

extern void flexfile_rring_destroy(struct flexfile_rring *const rring);

extern ssize_t flexfile_read_rring(
        const struct flexfile *const ff, void *const buf, const u64 loff, const u64 len,
        struct flexfile_rring *const rring);

extern ssize_t flexfile_read_fragmentation_rring(
        const struct flexfile *const ff, void *const buf, const u64 loff, const u64 len, u64 *const frag,
        struct flexfile_rring *const rring);
#endif

// extern ssize_t flexfile_read_extent(
//         struct flexfile *const ff, void *const buf, const u64 loff, const u64 max_len, u64 *const rloff);

/*
 * Handler APIs
 */

// read-only flexfile handler

extern struct flexfile_handler flexfile_get_handler(const struct flexfile *const ff, const u64 loff);

extern ssize_t flexfile_handler_read(const struct flexfile_handler *const fh, void *const buf, const u64 len);

extern unsigned long flexfile_handler_get_loff(const struct flexfile_handler *const fh);

extern unsigned long flexfile_handler_get_poff(const struct flexfile_handler *const fh);

extern void flexfile_handler_forward(struct flexfile_handler *const fh, const u64 step);

extern void flexfile_handler_forward_extent(struct flexfile_handler *const fh);

extern void flexfile_handler_backward(struct flexfile_handler *const fh, const u64 step);

extern int flexfile_handler_valid(const struct flexfile_handler *const fh);

extern int flexfile_handler_get_tag(const struct flexfile_handler *const fh, u16 *const tag);

#endif
