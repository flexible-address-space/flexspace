#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "flexfile.h"

// {{{ log

struct flexfile_log_entry {
    enum flexfile_op {
        FLEXFILE_OP_TREE_INSERT = 0,
        FLEXFILE_OP_TREE_COLLAPSE_N = 1,
        FLEXFILE_OP_GC = 2,
        FLEXFILE_OP_SET_TAG = 3,
    } op:2;
    u64 p1:48;
    u64 p2:48;
    u64 p3:30;
} __attribute__((packed));

#define FLEXFILE_LOG_ENTRY_MASK_48 0xffffffffffff
#define FLEXFILE_LOG_ENTRY_MASK_30 0x3fffffff

static_assert(sizeof(struct flexfile_log_entry) == 16 , "16 bytes per logent");

static void flexfile_log_truncate(const struct flexfile *const ff)
{
    // the ff seems to be changed.. but it's still const
    const off_t r = generic_lseek(ff->log_fd, 0, SEEK_SET);
    debug_assert(r == 0);
    const int r2 = generic_ftruncate(ff->log_fd, 0);
    debug_assert(r2 == 0);
    (void)r;
    (void)r2;
}

static inline u8 flexfile_log_full(const struct flexfile *const ff)
{
    return (ff->log_buf_size >= FLEXFILE_LOG_MEM_CAP);
}

// caller guarantees log not full
static void flexfile_log_write(
        struct flexfile *const ff, const enum flexfile_op op, const u64 p1, const u64 p2, const u64 p3)
{
    const struct flexfile_log_entry ffle =
        {.op = op, .p1 = p1 & FLEXFILE_LOG_ENTRY_MASK_48,
                   .p2 = p2 & FLEXFILE_LOG_ENTRY_MASK_48,
                   .p3 = p3 & FLEXFILE_LOG_ENTRY_MASK_30};
    memcpy(ff->log_buf + ff->log_buf_size, &ffle, sizeof(ffle));
    ff->log_buf_size += sizeof(ffle);
}

static void flexfile_log_sync(struct flexfile *const ff)
{
    if (ff->log_buf_size == 0) {
        return;
    }
    const ssize_t r = generic_pwrite(ff->log_fd, ff->log_buf, ff->log_buf_size, ff->log_total_size);
    debug_assert(r == (ssize_t)ff->log_buf_size);
    (void)r;
    ff->log_total_size += ff->log_buf_size;
    ff->log_buf_size = 0;
    // barrier
    generic_fdatasync(ff->log_fd);
}

static void flexfile_log_redo(const struct flexfile *const ff)
{
    struct flexfile_log_entry ffle;
    u32 i = 0;
    struct flextree_node *node = ff->flextree->leaf_head; // for gc log redo
    u32 idx = 0;
    const u32 version_offset = sizeof(ff->flextree->version);
    while (1) {
        const ssize_t r = generic_pread(ff->log_fd, &ffle, sizeof(ffle), (off_t)((i++)*sizeof(ffle) + version_offset));
        if (r != sizeof(ffle)) {
            break;
        }
        if (ffle.op == FLEXFILE_OP_TREE_INSERT) {
            flextree_insert(ff->flextree, (u64)ffle.p1, (u64)ffle.p2, (u32)ffle.p3);
        }
        else if (ffle.op == FLEXFILE_OP_TREE_COLLAPSE_N) {
            flextree_delete(ff->flextree, (u64)ffle.p1, (u64)ffle.p2);
        }
        else if (ffle.op == FLEXFILE_OP_GC) {
            if (idx == node->count) {
                generic_printf("invalid GC log, abort\n");
                generic_exit(1);
            }
            while (node->leaf_entry.extents[idx].poff != (u64)ffle.p1) {
                if (idx >= node->count-1) {
                    if (node->leaf_entry.next) node = node->leaf_entry.next;
                    else node = ff->flextree->leaf_head;
                    idx = 0;
                } else {
                    idx++;
                }
            }
            if (node->leaf_entry.extents[idx].len != ffle.p3) {
                generic_printf("flexfile gc log inconsistenct\n");
                generic_exit(1);
            }
            node->leaf_entry.extents[idx].poff = ffle.p2;
        }
        else if (ffle.op == FLEXFILE_OP_SET_TAG) {
            flextree_set_tag(ff->flextree, (u64)ffle.p1, (u16)ffle.p2);
        } else {
            generic_printf("flexfile log corrupted\n");
            generic_exit(1);
        }
    }
    flextree_sync(ff->flextree);
}

// }}}

// {{{ block

struct flexfile_bm {
    struct flexfile *file; // the reverse pointer to the file
    u64 blkid; // current block in use
    u64 blkoff; // offset in current block
#ifdef FLEXFILE_IO_URING
    u16 bufidx; // current buffer idx
    u16 bufstat[FLEXFILE_BLOCK_COUNT]; // buffer index (1 valid bit)
    void *mem; // just the start of the buffer
    u64 head; // free list head, -1lu means NULL
    u32 active; // active uring reqs (submitted)
    u32 pending; // pending uring reqs (batched)
    u8 fixed_buf;
    u8 fixed_file;
    struct io_uring ring; // uring
#else
    u8 *buf;
#endif
    u32 blkusage[FLEXFILE_BLOCK_COUNT]; // block usage
    u64 blkdist[FLEXFILE_BM_BLKDIST_SIZE];
    u64 free_blocks;
};

// all about io_uring
#ifdef FLEXFILE_IO_URING
static inline void flexfile_bm_bufstat_set_idx(u16 *const s, const u16 idx)
{
    *s = idx + (1u << 15); // implicit validate
}

static inline u16 flexfile_bm_bufstat_get_idx(const u16 s)
{
    return (s & ((1u << 15) - 1));
}

static inline void flexfile_bm_bufstat_invalidate(u16 *const s)
{
    *s &= ((1u << 15) - 1);
}

static inline u8 flexfile_bm_bufstat_valid(const u16 s)
{
    return (s & (1u << 15)) ? 1 : 0;
}

static inline u64 flexfile_bm_data_create(const u16 bufidx, const u64 blkid)
{
    return ((u64)bufidx << 48) + blkid;
}

static inline u16 flexfile_bm_data_get_bufidx(const u64 data)
{
    return (u16)(data >> 48);
}

static inline u64 flexfile_bm_data_get_blkid(const u64 data)
{
    return (data & ((1lu << 48) - 1));
}

static inline void *flexfile_bm_get_current_buf(const struct flexfile_bm *const fb)
{
    return (fb->mem + fb->bufidx * FLEXFILE_BLOCK_SIZE);
}

static u16 flexfile_bm_wait_buf(struct flexfile_bm *const fb)
{
    struct io_uring_cqe *cqe = NULL;
    const int ret = io_uring_wait_cqe(&fb->ring, &cqe);
    debug_assert(ret == 0);
    (void)ret;
    // data: high 16 bit for buf id, low 48 bit for blk id
    const u64 data = (u64)io_uring_cqe_get_data(cqe);
    const u16 bufidx = flexfile_bm_data_get_bufidx(data);
    const u64 blkid = flexfile_bm_data_get_blkid(data);
    flexfile_bm_bufstat_invalidate(&fb->bufstat[blkid]);
    io_uring_cqe_seen(&fb->ring, cqe);
    fb->active--;
    return bufidx;
}

static u16 flexfile_bm_acquire_buf(struct flexfile_bm *const fb)
{
    u64 idx = ~(u64)0;
    if (fb->head == idx) {
        idx = flexfile_bm_wait_buf(fb);
    } else {
        idx = fb->head;
        const void *const ptr = fb->mem + idx * FLEXFILE_BLOCK_SIZE;
        fb->head = *(u64*)ptr;
    }
    return (u16)idx;
}

static void flexfile_bm_submit_buf(struct flexfile_bm *const fb)
{
    const int n = io_uring_submit(&fb->ring);
    debug_assert(n > 0 && (u32)n <= fb->pending);
    fb->pending -= (u32)n;
    fb->active += (u32)n;
}
#endif

static inline u32 flexfile_bm_get_blkusage(const struct flexfile_bm *const fb, const u64 blkid)
{
    return fb->blkusage[blkid];
}

static inline u64 flexfile_bm_get_blkid(const struct flexfile_bm *const fb)
{
    return fb->blkid;
}

static inline u64 flexfile_bm_offset(const struct flexfile_bm *fb)
{
    return fb->blkid * FLEXFILE_BLOCK_SIZE + fb->blkoff;
}

static inline u32 flexfile_bm_update_blkusage(struct flexfile_bm *const fb, const u64 blkid, const s32 delta)
{
    const u32 oidx = fb->blkusage[blkid] >> FLEXFILE_BM_BLKDIST_BITS;
    fb->blkdist[oidx]--;
    if (fb->blkusage[blkid] == 0) {
        fb->free_blocks--;
    }

    fb->blkusage[blkid] = (u32)(((s32)fb->blkusage[blkid]) + delta);

    const u32 nidx = fb->blkusage[blkid] >> FLEXFILE_BM_BLKDIST_BITS;
    fb->blkdist[nidx]++;
    if (fb->blkusage[blkid] == 0) {
        fb->free_blocks++;
    }

    return fb->blkusage[blkid];
}

static u64 flexfile_bm_find_empty_block(const struct flexfile_bm *const fb, const u64 blkid, const u8 gc)
{
    // fb seems to be changed.. but still const
    if (!gc) flexfile_gc(fb->file);
    u64 ret = -1lu;
    // phase 1: look forward
    for (u64 i=blkid; i<FLEXFILE_BLOCK_COUNT; i++) {
        if (flexfile_bm_get_blkusage(fb, i) == 0) {
            ret = i;
            break;
        }
    }
    if (ret != -1lu) {
        return ret;
    }
    // phase 2: see if any recycled block
    for (u64 i=0; i<blkid; i++) {
        if (flexfile_bm_get_blkusage(fb, i) == 0) {
            ret = i;
            break;
        }
    }
    // after gc, still no empty blocks, error
    if (ret == -1lu) {
        generic_printf("cannot find any more empty blocks to write, exit\n");
        generic_exit(1);
    }
    return ret;
}

// here we go.. apis
static struct flexfile_bm *flexfile_bm_create(struct flexfile *const ff)
{
    struct flexfile_bm *fb= generic_malloc(sizeof(*fb));
    memset(fb, 0, sizeof(*fb));
    fb->file = ff;
#ifdef FLEXFILE_IO_URING
    const u32 buf_size = FLEXFILE_BLOCK_SIZE * FLEXFILE_BM_DEPTH;
    fb->mem = generic_malloc(buf_size);
    fb->head = 0;
    debug_assert(fb->mem);
    for (u32 i=0; i<FLEXFILE_BM_DEPTH-1; i++) { // free list
        *(u64 *)(fb->mem+(FLEXFILE_BLOCK_SIZE*i)) = (u64)(i+1);
    }
    *(u64 *)(fb->mem+(FLEXFILE_BLOCK_SIZE*(FLEXFILE_BM_DEPTH-1))) = -1lu;
    struct io_uring_params p = {};
    if (io_uring_queue_init_params(FLEXFILE_BM_DEPTH, &fb->ring, &p)) {
        generic_free(fb->mem);
        generic_free(fb);
        return NULL;
    }
    struct iovec iov = {.iov_base = fb->mem, .iov_len = buf_size};
    int r = io_uring_register_buffers(&fb->ring, &iov, 1);
    if (r == 0) {
        fb->fixed_buf = 1;
    }
    r = io_uring_register_files(&fb->ring, &ff->fd, 1);
    if (r == 0) {
        fb->fixed_file = 1;
    }
    fb->active = 0;
    fb->pending = 0;
#else
    fb->buf = generic_malloc(FLEXFILE_BLOCK_SIZE);
#endif
    return fb;
}

static void flexfile_bm_init(struct flexfile_bm *const fb, const struct flextree *const tree)
{
    struct flextree_node *node = tree->leaf_head;
    u64 max_blkid = 0;
    fb->blkdist[0] = FLEXFILE_BLOCK_COUNT;
    fb->free_blocks = FLEXFILE_BLOCK_COUNT;
    while (node) {
        const struct flextree_leaf_entry *const le = &node->leaf_entry;
        for (u32 i=0; i<node->count; i++) {
            u64 poff = le->extents[i].poff;
            u64 len = le->extents[i].len;
            while (len > 0) {
                const u64 blkid = poff >> FLEXFILE_BLOCK_BITS;
                if (blkid > max_blkid) max_blkid = blkid;
                const u64 remain = FLEXFILE_BLOCK_SIZE - (poff >> FLEXFILE_BLOCK_BITS);
                const u64 tlen = (len > remain) ? remain : len;
                flexfile_bm_update_blkusage(fb, blkid, (s32)tlen);
                poff += tlen;
                len -= tlen;
            }
        }
        node = node->leaf_entry.next;
    }
    fb->blkid = flexfile_bm_find_empty_block(fb, max_blkid, 0);
    fb->blkoff = 0;
#ifdef FLEXFILE_IO_URING
    fb->bufidx = flexfile_bm_acquire_buf(fb);
    flexfile_bm_bufstat_set_idx(&fb->bufstat[fb->blkid], fb->bufidx);
#endif
}

static void flexfile_bm_next_block(struct flexfile_bm *const fb, const u8 gc)
{
    const u64 blkid = fb->blkid;
    const u64 new_blkid = flexfile_bm_find_empty_block(fb, blkid, gc);
    if (blkid == new_blkid) {
        return;
    }
#ifdef FLEXFILE_IO_URING
    // enqueue current blk
    struct io_uring_sqe *const sqe = io_uring_get_sqe(&fb->ring);
    debug_assert(sqe);
    const void *const buf = fb->mem+ FLEXFILE_BLOCK_SIZE * fb->bufidx;
    const off_t off = (off_t)(fb->blkid * FLEXFILE_BLOCK_SIZE);
    if (fb->fixed_buf) {
        io_uring_prep_write_fixed(sqe, 0, buf, FLEXFILE_BLOCK_SIZE, off, 0);
    } else {
        io_uring_prep_write(sqe, 0, buf, FLEXFILE_BLOCK_SIZE, off);
    }
    if (fb->fixed_file) {
        io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
    }
    const u64 data = flexfile_bm_data_create(fb->bufidx, fb->blkid);
    io_uring_sqe_set_data(sqe, (void *)data);
    fb->pending++;
    if (fb->pending >= FLEXFILE_BM_BATCH_SIZE) {
        flexfile_bm_submit_buf(fb);
    }
#else
    const off_t off = (off_t)(fb->blkid * FLEXFILE_BLOCK_SIZE);
    const ssize_t r = generic_pwrite(fb->file->fd, fb->buf, FLEXFILE_BLOCK_SIZE, off);
    debug_assert(r == FLEXFILE_BLOCK_SIZE);
    (void)r;
#endif
    fb->blkid = new_blkid;
    fb->blkoff = 0;
#ifdef FLEXFILE_IO_URING
    fb->bufidx = flexfile_bm_acquire_buf(fb);
    flexfile_bm_bufstat_set_idx(&fb->bufstat[fb->blkid], fb->bufidx);
#endif
}

static inline u64 flexfile_bm_block_fit(const struct flexfile_bm *fb, const u64 len)
{
    const u64 remain = FLEXFILE_BLOCK_SIZE - fb->blkoff;
    u64 olen = len;
    if (olen > FLEXFILE_MAX_EXTENT_SIZE) {
        olen = FLEXFILE_MAX_EXTENT_SIZE;
    }
    return (remain >= len) ? 1 : 0;
}

static u64 flexfile_bm_write(struct flexfile_bm *const fb, const void *const buf, const u64 size, const u8 gc)
{
    const u64 remain = FLEXFILE_BLOCK_SIZE - fb->blkoff;
    u64 osize = size > remain ? remain : size;
    osize = osize > FLEXFILE_MAX_EXTENT_SIZE ? FLEXFILE_MAX_EXTENT_SIZE : osize;
#ifdef FLEXFILE_IO_URING
    void *const blkbuf = flexfile_bm_get_current_buf(fb);
#else
    void *const blkbuf = fb->buf;
#endif
    memcpy(blkbuf+fb->blkoff, buf, osize);
    fb->blkoff += osize;
    flexfile_bm_update_blkusage(fb, fb->blkid, (s32)osize);
    if (fb->blkoff == FLEXFILE_BLOCK_SIZE) {
        flexfile_bm_next_block(fb, gc);
    }
    return osize;
}

static void flexfile_bm_flush(struct flexfile_bm* const fb, const u8 gc)
{
    flexfile_bm_next_block(fb, gc);
#ifdef FLEXFILE_IO_URING
    while (fb->pending) {
        flexfile_bm_submit_buf(fb);
    }
    while (fb->active) {
        const u16 bufidx = flexfile_bm_wait_buf(fb);
        void *const ptr = fb->mem + bufidx * FLEXFILE_BLOCK_SIZE;
        *(u64 *)ptr = (u64)(fb->head);
        fb->head = bufidx;
    }
#else
    generic_fdatasync(fb->file->fd);
#endif
}

static inline u8 flexfile_bm_read_valid(const struct flexfile_bm *const fb, const u64 blkid)
{
#ifdef FLEXFILE_IO_URING
    return flexfile_bm_bufstat_valid(fb->bufstat[blkid]) ? 1 : 0;
#else
	return (fb->blkid == blkid) ? 1 : 0;
#endif
}

static u64 flexfile_bm_read(
        const struct flexfile_bm *const fb, void *const buf, const u64 poff, const u64 size)
{
    const u64 blkid = poff >> FLEXFILE_BLOCK_BITS;
    if (!flexfile_bm_read_valid(fb, blkid)) {
        return 0;
    }
    const u64 blkoff = poff % FLEXFILE_BLOCK_SIZE;
    const u64 remain = FLEXFILE_BLOCK_SIZE - blkoff;
    u64 osize = size > remain ? remain : size;
#ifdef FLEXFILE_IO_URING
    // read from buffer
    const u16 bufidx = flexfile_bm_bufstat_get_idx(fb->bufstat[blkid]);
    const void *const blkbuf = fb->mem + FLEXFILE_BLOCK_SIZE * bufidx;
    memcpy(buf, blkbuf+blkoff, osize);
#else
    memcpy(buf, fb->buf+blkoff, osize);
#endif
    return osize;
}

static void flexfile_bm_destroy(struct flexfile_bm *const fb)
{
    flexfile_bm_flush(fb, 0);
#ifdef FLEXFILE_IO_URING
    generic_free(fb->mem);
    io_uring_queue_exit(&fb->ring);
#else
    generic_free(fb->buf);
#endif
    generic_free(fb);
}

// }}}

// {{{ general

// A few notes:
// 1. each extent is guaranteed not to cross the border of two segments
// 2. an operation either succeeds or fails

static __thread struct flextree_pos seqio_fp = { .node = NULL };
static __thread u64 seqio_epoch = 0;

static volatile u64 global_epoch = 1;

struct flexfile *flexfile_open(const char *const path)
{
    if (!path) {
        generic_printf("null path, exit\n");
        generic_exit(-1);
    }
    struct flexfile *const ff = generic_malloc(sizeof(*ff));
    memset(ff, 0, sizeof(*ff));

    if (access(path, F_OK) == -1) {
        const int r = generic_mkdir(path, 0755);
        if (r != 0) {
            generic_printf("flexfile mkdir failed, exit\n");
            generic_exit(-1);
        }
    }
    // set path
    ff->path = generic_malloc((strlen(path)+1) *sizeof(char));
    strcpy(ff->path, path);

    // set fd
    char *const tpath = generic_malloc((strlen(ff->path)+16) * sizeof(char));
    generic_sprintf(tpath, "%s/%s", ff->path, "DATA");
    ff->fd = generic_open(tpath, O_RDWR | O_CREAT, 0644);
    debug_assert(ff->fd > 0);

    // open flextree
    generic_sprintf(tpath, "%s/%s", ff->path, "FLEXTREE");
    ff->flextree = flextree_open(tpath, FLEXFILE_MAX_EXTENT_SIZE);
    debug_assert(ff->flextree);

    // set log
    generic_sprintf(tpath, "%s/%s", ff->path, "LOG");
    ff->log_fd = generic_open(tpath, O_RDWR | O_CREAT, 0644);
    debug_assert(ff->log_fd > 0);

    const off_t r = generic_lseek(ff->log_fd, 0, SEEK_END);
    if (r > (off_t)sizeof(ff->flextree->version)) { // log not empty..
        u64 version = 0;
        // read version header
        const ssize_t r2 = generic_pread(ff->log_fd, &version, sizeof(ff->flextree->version), 0);
        debug_assert(r2 == sizeof(ff->flextree->version));
        (void)r2;
        // redo log only when the version matches
        if (version == ff->flextree->version) {
            flexfile_log_redo(ff); // implicit flextree_sync()
        }
    }
    flexfile_log_truncate(ff);
    // now all clean
    // write current ft version
    const ssize_t r2 = generic_pwrite(ff->log_fd, &ff->flextree->version, sizeof(ff->flextree->version), 0);
    debug_assert(r2 == sizeof(ff->flextree->version));
    (void)r2;
    flexfile_log_sync(ff);
    // allocate in-memory log buffer
    // give it ample space to avoid overflow on single operation
    ff->log_buf = generic_malloc(FLEXFILE_LOG_MEM_CAP * 8);
    memset(ff->log_buf, 0, FLEXFILE_LOG_MEM_CAP * 8);
    ff->log_buf_size = 0;
    ff->log_total_size = sizeof(ff->flextree->version);

    // init blocks
    ff->bm = flexfile_bm_create(ff);
    flexfile_bm_init(ff->bm, ff->flextree);

    // init gc
    ff->gc_ctx.queue = generic_malloc(FLEXFILE_GC_QUEUE_DEPTH * sizeof(ff->gc_ctx.queue[0]));
    ff->gc_ctx.loff = 0;
    ff->gc_ctx.count = 0;
    ff->gc_ctx.write_between_stages = 0;

    generic_free(tpath);
    return ff;
}

void flexfile_close(struct flexfile *const ff)
{
    flexfile_sync(ff);
    flextree_close(ff->flextree);
    // at this point, the ff log and flextree are all persisted
    flexfile_log_truncate(ff);
    flexfile_bm_destroy(ff->bm);
    int r = generic_close(ff->fd);
    debug_assert(r == 0);
    r = generic_close(ff->log_fd);
    debug_assert(r == 0);
    generic_free(ff->path);
    // free pending gc
    for (u32 i=0; i<ff->gc_ctx.count; i++) {
        generic_free(ff->gc_ctx.queue[i].buf);
    }
    generic_free(ff->gc_ctx.queue);
    generic_free(ff->log_buf);
    generic_free(ff);
    (void)r;
}

static void flexfile_sync_r(struct flexfile *const ff, const u8 gc)
{
    flexfile_bm_flush(ff->bm, gc);
    generic_fdatasync(ff->fd); // data persisted
    flexfile_log_sync(ff);
    // need to update version or not?
    if (ff->log_total_size >= FLEXFILE_LOG_MAX_SIZE) {
        flextree_sync(ff->flextree);
        flexfile_log_truncate(ff);
        // write current ft version
        const ssize_t r = generic_pwrite(ff->log_fd, &ff->flextree->version, sizeof(ff->flextree->version), 0);
        debug_assert(r == sizeof(ff->flextree->version));
        flexfile_log_sync(ff);
        (void)r;
        ff->log_buf_size = 0;
        ff->log_total_size = sizeof(ff->flextree->version);
    }
}

static inline void flexfile_sync_gc(struct flexfile *const ff)
{
    return flexfile_sync_r(ff, 1);
}

inline void flexfile_sync(struct flexfile *const ff)
{
    return flexfile_sync_r(ff, 0);
}

// returns bytes read, or -1 on error
static ssize_t flexfile_read_r(
        struct flexfile *const ff, void *const buf, const u64 loff, const u64 len, u64 *const frag)
{
    if (loff + len > ff->flextree->max_loff) {
        return -1;
    }
    // now we can assume that the range is allocated!
    // because there is no hole in the file
    struct flextree_pos *fp;
    if (global_epoch != seqio_epoch || loff != flextree_pos_get_loff_ll(&seqio_fp)) {
        seqio_epoch = global_epoch;
        seqio_fp = flextree_pos_get_ll(ff->flextree, loff);
    }
    fp = &seqio_fp;
    void *b = buf;
    u64 tlen = len;
    u64 count = 0;
    while (tlen > 0) {
        count++;
        u64 slen = fp->node->leaf_entry.extents[fp->idx].len - fp->diff;
        slen = slen < tlen ? slen : tlen;
        // a segment is either in cache or on disk
        const u64 poff = fp->node->leaf_entry.extents[fp->idx].poff + fp->diff;
        u64 r = flexfile_bm_read(ff->bm, b, poff, slen);
        if (!r) {
            r = (u64)generic_pread(ff->fd, b, slen, (off_t)poff);
        }
        debug_assert(r == slen);
        flextree_pos_forward_ll(fp, slen);

        b += slen;
        tlen -= slen;
    }
    if (frag) {
        *frag = count;
    }

    debug_assert(((u64)b - (u64)buf) == len);
    return (ssize_t)len;
}

inline ssize_t flexfile_read(struct flexfile *const ff, void *const buf, const u64 loff, const u64 len)
{
    return flexfile_read_r(ff, buf, loff, len, NULL);
}

inline ssize_t flexfile_read_fragmentation(
        struct flexfile *const ff, void *const buf, const u64 loff, const u64 len, u64 *const frag)
{
    return flexfile_read_r(ff, buf, loff, len, frag);
}

static ssize_t flexfile_insert_r(
        struct flexfile *const ff, const void *const buf, const u64 loff, const u64 len, u8 commit)
{
    if (loff > ff->flextree->max_loff) {
        return -1; // no hole in file
    }
    ff->gc_ctx.write_between_stages = 1;
    global_epoch++;
    const void *b = buf;
    u64 olen = len;
    u64 oloff = loff;
    if (!flexfile_bm_block_fit(ff->bm, olen)) {
        flexfile_bm_next_block(ff->bm, 0);
    }
    while (olen > 0) {
        const u64 poff = flexfile_bm_offset(ff->bm);
        const u64 tlen = flexfile_bm_write(ff->bm, b, olen, 0);
        const int r = flextree_insert(ff->flextree, oloff, poff, (u32)tlen);
        flexfile_log_write(ff, FLEXFILE_OP_TREE_INSERT, oloff, poff, tlen);
        debug_assert(r == 0);
        (void)r;
        oloff += tlen;
        olen -= tlen;
        b += tlen;
    }
    // Note: a potential issue here is too large insert request
    // may cause the log buffer overflow. This is not a deal now
    // because FlexFile does not serve large file stores
    if (flexfile_log_full(ff) && commit) {
        flexfile_sync(ff);
    }
    return (ssize_t)len;
}

// returns bytes inserted, or -1 on error
inline ssize_t flexfile_insert(struct flexfile *const ff, const void *const buf, const u64 loff, const u64 len)
{
    return flexfile_insert_r(ff, buf, loff, len, 1);
}

// returns 0 on success, or -1 on error
static int flexfile_collapse_r(struct flexfile *const ff, const u64 loff, const u64 len, u8 commit)
{
    if (loff + len > ff->flextree->max_loff) {
        return -1;
    }
    ff->gc_ctx.write_between_stages = 1;
    global_epoch++;
    struct flextree_query_result *const rr = flextree_query(ff->flextree, loff, len);
    debug_assert(rr);
    const int r = flextree_delete(ff->flextree, loff, len);
    debug_assert(r == 0);
    (void)r;
    flexfile_log_write(ff, FLEXFILE_OP_TREE_COLLAPSE_N, loff, len, 0);
    for (u32 i=0; i<rr->count; i++) {
        const u64 blkid = rr->v[i].poff >> FLEXFILE_BLOCK_BITS;
        flexfile_bm_update_blkusage(ff->bm, blkid, -(s32)rr->v[i].len);
    }
    generic_free(rr);
    if (flexfile_log_full(ff) && commit) {
        flexfile_sync(ff);
    }
    return 0;
}

inline int flexfile_collapse(struct flexfile *const ff, const u64 loff, const u64 len)
{
    return flexfile_collapse_r(ff, loff, len, 1);
}

static int flexfile_set_tag_r(struct flexfile *const ff, const u64 loff, const u16 tag, const u8 commit)
{
    ff->gc_ctx.write_between_stages = 1;
    global_epoch++;
    const int r = flextree_set_tag(ff->flextree, loff, tag);
    flexfile_log_write(ff, FLEXFILE_OP_SET_TAG, loff, (u64)tag, 0lu);
    if (flexfile_log_full(ff) && commit) {
        flexfile_sync(ff);
    }
    return r;
}

// returns bytes written, or -1 on error
ssize_t flexfile_update(
        struct flexfile *const ff, const void *const buf, const u64 loff, const u64 len, const u64 olen)
{
    if (loff + olen > ff->flextree->max_loff) {
        return -1;
    }
    // get tags (if any)
    u16 tag = 0;
    flexfile_get_tag(ff, loff, &tag);
    // collapse (no commit)
    const int r = flexfile_collapse_r(ff, loff, olen, 0);
    debug_assert(r == 0);
    (void)r;
    // write (no commit)
    const ssize_t r2 = flexfile_insert_r(ff, buf, loff, len, 0);
    debug_assert(r2 == (ssize_t)len);
    (void)r2;
    // set tag (if any)
    if (tag != 0) {
        flexfile_set_tag_r(ff, loff, tag, 0);
    }
    // try commit buffer
    if (flexfile_log_full(ff)) {
        flexfile_sync(ff);
    }
    return (ssize_t)len;
}

// returns bytes written, or -1 on error
inline ssize_t flexfile_write(struct flexfile *const ff, const void *const buf, const u64 loff, const u64 len)
{
    const u64 size = flexfile_size(ff);
    if (loff > size) {
        return -1;
    }
    else if (loff == size) {
        return flexfile_insert(ff, buf, loff, len);
    }
    // collapse?
    else if (loff + len > size) {
        int r = flexfile_collapse(ff, loff, size - loff);
        if (r != 0) {
            return -1;
        }
        return flexfile_insert(ff, buf, loff, len);
    }
    return flexfile_update(ff, buf, loff, len, len);
}

inline int flexfile_set_tag(struct flexfile *const ff, const u64 loff, const u16 tag)
{
    return flexfile_set_tag_r(ff, loff, tag, 1);
}

inline int flexfile_get_tag(const struct flexfile *const ff, const u64 loff, u16 *const tag)
{
    return flextree_get_tag(ff->flextree, loff, tag);
}

// returns 0 on success, or -1 on error
int flexfile_defrag(struct flexfile *const ff, const void *const buf, const u64 loff, const u64 len)
{
    ssize_t r = flexfile_update(ff, buf, loff, len, len);
    return r != -1 ? 0 : -1;
}

inline unsigned long flexfile_size(const struct flexfile *const ff)
{
    return ff->flextree->max_loff;
}

int flexfile_fallocate(struct flexfile *const ff, const u64 loff, const u64 size) {
    u64 remain = size;
    u64 off = 0;
    u8 *const buf = malloc(FLEXFILE_MAX_EXTENT_SIZE);
    int ret = 0;
    while (remain > 0) {
        u64 tsize = remain > FLEXFILE_MAX_EXTENT_SIZE ? FLEXFILE_MAX_EXTENT_BIT : remain;
        ssize_t r = flexfile_insert(ff, buf, loff + off, tsize);
        off += tsize;
        remain -= tsize;
        if (r != (ssize_t)size) {
            ret = -1;
            break;
        }
    }
    free(buf);
    return ret;
}

int flexfile_ftruncate(struct flexfile *const ff, const u64 size) {
    const u64 fsize = flexfile_size(ff);
    if (fsize <= size) {
        return 0;
    }
    const u64 diff = fsize - size;
    return flexfile_collapse(ff, size, diff);
}

// ssize_t flexfile_read_extent(
//         struct flexfile *const ff, void *const buf, const u64 loff, const u64 max_len, u64 *const rloff)
// {
//     if (loff >= flexfile_size(ff)) {
//         return 0;
//     }
//     const struct flextree_pos fp = flextree_pos_get_ll(ff->flextree, loff);
//     u64 len = fp.node->leaf_entry.extents[fp.idx].len;
//     len = (len > max_len) ? max_len : len;
//     const u64 oloff = loff - fp.diff;
//     *rloff = oloff;
//     return flexfile_read(ff, buf, oloff, len);
// }

// }}}

// {{{ handlers

inline struct flexfile_handler flexfile_get_handler(const struct flexfile *const ff, const u64 loff)
{
    const struct flextree_pos fp = flextree_pos_get_ll(ff->flextree, loff);
    return (struct flexfile_handler){ .file = ff, .fp = fp};
}

ssize_t flexfile_handler_read(const struct flexfile_handler *const fh, void *const buf, const u64 len)
{
    u64 tlen = len;
    u8 *b = buf;
    struct flexfile_handler tfh = *fh;
    while (tlen > 0) {
        u64 slen = tfh.fp.node->leaf_entry.extents[tfh.fp.idx].len - tfh.fp.diff;
        slen = slen < tlen ? slen : tlen;
        const u64 poff = tfh.fp.node->leaf_entry.extents[tfh.fp.idx].poff + tfh.fp.diff;
        const struct flexfile *const ff = tfh.file;
        u64 r = flexfile_bm_read(ff->bm, b, poff, slen);
        if (!r) {
            r = (u64)generic_pread(ff->fd, b, slen, (off_t)poff);
        }
        debug_assert(r == slen);
        b += slen;
        tlen -= slen;
        flexfile_handler_forward(&tfh, slen);
        if (!flexfile_handler_valid(&tfh)) {
            if (tlen != 0) {
                return -1;
            }
        }
    }
    debug_assert(((u64)b - (u64)buf) == len);
    return (ssize_t)len;
}

inline unsigned long flexfile_handler_get_loff(const struct flexfile_handler *const fh)
{
    return fh->fp.loff;
}

inline unsigned long flexfile_handler_get_poff(const struct flexfile_handler *const fh)
{
    return flextree_pos_get_poff_ll(&fh->fp);
}

inline void flexfile_handler_forward(struct flexfile_handler *const fh, const u64 step)
{
    flextree_pos_forward_ll(&fh->fp, step);
}

inline void flexfile_handler_forward_extent(struct flexfile_handler *const fh)
{
    flextree_pos_forward_extent_ll(&fh->fp);
}

inline void flexfile_handler_backward(struct flexfile_handler *const fh, const u64 step)
{
    flextree_pos_backward_ll(&fh->fp, step);
}

inline int flexfile_handler_valid(const struct flexfile_handler *const fh)
{
    return flextree_pos_valid_ll(&fh->fp);
}

inline int flexfile_handler_get_tag(const struct flexfile_handler *const fh, u16 *const tag)
{
    return flextree_pos_get_tag_ll(&fh->fp, tag);
}

// }}} handlers

// {{{ rring

#ifdef FLEXFILE_IO_URING

struct flexfile_rring {
    struct flexfile *file;
    u32 active;
    u32 pending;
    struct io_uring ring;
    u8 fixed_buf;
    u8 fixed_file;
};

static void flexfile_rring_wait(struct flexfile_rring *const rring)
{
    struct io_uring_cqe *cqe;
    int r = io_uring_wait_cqe(&rring->ring, &cqe);
    debug_assert(r == 0);
    (void)r;
    io_uring_cqe_seen(&rring->ring, cqe);
    rring->active--;
}

static void flexfile_rring_submit(struct flexfile_rring *const rring)
{
    if (rring->pending == 0) return;
    while (rring->pending > 0) {
        const int n = io_uring_submit(&rring->ring);
        debug_assert(n > 0 && (u32)n <= rring->pending);
        rring->pending -= (u32)n;
        rring->active += (u32)n;
    }
}

static void flexfile_rring_finish(struct flexfile_rring *const rring)
{
    flexfile_rring_submit(rring);
    while (rring->active > 0) {
        flexfile_rring_wait(rring);
    }
}

static void flexfile_rring_prep(
        struct flexfile_rring *const rring, void *const buf, const u64 size, const u64 poff)
{
    if (rring->active + rring->pending == FLEXFILE_RRING_DEPTH) {
        flexfile_rring_wait(rring); // need to wait sqe
    }
    struct io_uring_sqe *const sqe = io_uring_get_sqe(&rring->ring);
    debug_assert(sqe);

    if (rring->fixed_buf) {
        io_uring_prep_read_fixed(sqe, 0, buf, (u32)size, (off_t)poff, 0);
        // the caller should guarantee that the buf is registered
    } else {
        io_uring_prep_read(sqe, 0, buf, (u32)size, (off_t)poff);
    }
    if (rring->fixed_file) {
        io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
    }
    rring->pending++;
    if (rring->pending >= FLEXFILE_RRING_BATCH_SIZE) {
        flexfile_rring_submit(rring);
    }
}

struct flexfile_rring *flexfile_rring_create(struct flexfile *const ff, void *const buf, const u64 bufsz)
{
    struct flexfile_rring *const rring = malloc(sizeof(*rring));
    memset(rring, 0, sizeof(*rring));
    rring->file = ff;
    rring->active = 0;
    rring->pending = 0;
    struct io_uring_params p = {};
    if (io_uring_queue_init_params(FLEXFILE_RRING_DEPTH, &rring->ring, &p)) {
        generic_free(rring);
        return NULL;
    }
    int r = io_uring_register_files(&rring->ring, &ff->fd, 1);
    if (r == 0) {
        rring->fixed_file = 1;
    }
    debug_assert(r == 0);
    if (buf) {
        struct iovec iov = {.iov_base = buf, .iov_len = bufsz};
        r = io_uring_register_buffers(&rring->ring, &iov, 1);
        if (r == 0) {
            rring->fixed_buf = 1;
        }
    }
    return rring;
}

void flexfile_rring_destroy(struct flexfile_rring *const rring)
{
    flexfile_rring_finish(rring);
    io_uring_queue_exit(&rring->ring);
    free(rring);
}

static ssize_t flexfile_read_rring_r(
        const struct flexfile *const ff, void *const buf, const u64 loff, const u64 len,
        struct flexfile_rring *const rring, u64 *const frag)
{
    if (loff + len > ff->flextree->max_loff || len > FLEXFILE_MAX_EXTENT_SIZE) {
        return -1;
    }
    struct flextree_pos *fp;
    if (global_epoch != seqio_epoch || loff != flextree_pos_get_loff_ll(&seqio_fp)) {
        seqio_epoch = global_epoch;
        seqio_fp = flextree_pos_get_ll(ff->flextree, loff);
    }
    fp = &seqio_fp;
    void *b = buf;
    u64 tlen = len;
    u64 count = 0;
    while (tlen > 0) {
        count++;
        u64 slen = fp->node->leaf_entry.extents[fp->idx].len - fp->diff;
        slen = slen < tlen ? slen : tlen;
        // a segment is either in cache or on disk
        const u64 poff = fp->node->leaf_entry.extents[fp->idx].poff + fp->diff;
        u64 r = flexfile_bm_read(ff->bm, b, poff, slen);
        if (!r) {
            flexfile_rring_prep(rring, b, slen, poff);
        }
        debug_assert(r == slen);
        flextree_pos_forward_ll(fp, slen);

        b += slen;
        tlen -= slen;
    }
    if (frag) {
        *frag = count;
    }

    debug_assert(((u64)b - (u64)buf) == len);
    flexfile_rring_submit(rring);
    flexfile_rring_finish(rring);
    return (ssize_t)len;
}


ssize_t flexfile_read_rring(
        const struct flexfile *const ff, void *const buf, const u64 loff, const u64 len,
        struct flexfile_rring *const rring)
{
    return flexfile_read_rring_r(ff, buf, loff, len, rring, NULL);
}

ssize_t flexfile_read_fragmentation_rring(
        const struct flexfile *const ff, void *const buf, const u64 loff, const u64 len, u64 *const frag,
        struct flexfile_rring *const rring)
{
    debug_assert(rring);
    return flexfile_read_rring_r(ff, buf, loff, len, rring, frag);
}

#endif

// }}} rring

// {{{ gc

// NOTE: gc does not delete or create new extents, it remaps existing extent(s) in-place
//       so there is no need for logging tags

static inline u8 flexfile_gc_needed(const struct flexfile *const ff)
{
    return (ff->bm->free_blocks < FLEXFILE_GC_THRESHOLD);
}

static void flexfile_gc_async_prepare(struct flexfile *const ff, struct bitmap *const bitmap)
{
    if (ff->gc_ctx.count >= FLEXFILE_GC_QUEUE_DEPTH) {
        return;
    }
    if (ff->gc_ctx.loff >= flexfile_size(ff)) {
        ff->gc_ctx.loff = 0;
    }
    // check flags
    if (ff->gc_ctx.write_between_stages && ff->gc_ctx.count) {
        // if write_between_stages marked, invalidate existing gc
        // queue because you should not modify the flextree
        // between two gc preparations
        for (u32 i=0; i<ff->gc_ctx.count; i++) {
            generic_free(ff->gc_ctx.queue[i].buf);
        }
        ff->gc_ctx.count = 0;
        ff->gc_ctx.loff = 0;
    }
    ff->gc_ctx.write_between_stages = 0;
    struct flextree_pos fp = flextree_pos_get_ll(ff->flextree, ff->gc_ctx.loff);
    if (fp.node == NULL) {
        return;
    }
    // rewind the gc.loff to start of the node
    flextree_pos_rewind_ll(&fp);
    ff->gc_ctx.loff = flextree_pos_get_loff_ll(&fp);
    struct flextree_node *node = fp.node;
    const struct flextree_leaf_entry *le = &node->leaf_entry;
    for (u32 i=0; i<node->count; i++) {
        const u64 poff = le->extents[i].poff;
        const u32 len = le->extents[i].len;
        const u64 blkid = poff >> FLEXFILE_BLOCK_BITS;
        // const u64 blkoff = poff % FLEXFILE_BLOCK_SIZE;
        ff->gc_ctx.loff += len;
        if (blkid != (poff + len - 1) >> FLEXFILE_BLOCK_BITS) {
            // if the current segment in the tree crosses two blocks,
            // ... hey this should not happen!
            generic_printf("spot extent crosses two blocks, terminate\n");
            generic_fflush(stdout);
            generic_exit(1);
        }
        if (!bitmap_test(bitmap, blkid)) {
            // no need to gc on this range
            continue;
        }
        const u32 idx = ff->gc_ctx.count++;
        ff->gc_ctx.queue[idx].node = node;
        ff->gc_ctx.queue[idx].poff = poff;
        ff->gc_ctx.queue[idx].len = len;
        ff->gc_ctx.queue[idx].idx = i;
        u8 *const buf = generic_malloc(len);

        u64 r = flexfile_bm_read(ff->bm, buf, poff, len);
        if (!r) {
            r = (u64)generic_pread(ff->fd, buf, len, (off_t)poff);
        }
        debug_assert(r == len);
        ff->gc_ctx.queue[idx].buf = buf;

        if (ff->gc_ctx.count >= FLEXFILE_GC_QUEUE_DEPTH) {
            break;
        }
    }
    if (ff->gc_ctx.loff >= flexfile_size(ff)) {
        ff->gc_ctx.loff = 0; // rewind to start
    }
}

static inline int flexfile_gc_async_queue_full(const struct flexfile *const ff)
{
    return (ff->gc_ctx.count == FLEXFILE_GC_QUEUE_DEPTH) ? 1 : 0;
}

// this operation is in-place..
static int flexfile_gc_async(struct flexfile *const ff, struct bitmap *const hist_bitmap, const u8 commit)
{
    if (ff->gc_ctx.count == 0) {
        return 0;
    }
    // so.. you passed the detection of write between stages
    ff->gc_ctx.write_between_stages = 0;
    int rblocks = 0;
    // you must be the sole writer now...
    for (u64 i=0; i<ff->gc_ctx.count; i++) {
        const u64 opoff = ff->gc_ctx.queue[i].poff;
        const u32 len = ff->gc_ctx.queue[i].len;
        u8 *const buf = ff->gc_ctx.queue[i].buf;
        // write to new position
        if (!flexfile_bm_block_fit(ff->bm, len)) {
            flexfile_bm_next_block(ff->bm, 1);
        }
        const u64 poff = flexfile_bm_offset(ff->bm);
        const u64 blkid = flexfile_bm_get_blkid(ff->bm);
        flexfile_bm_write(ff->bm, buf, len, 1);
        bitmap_set1(hist_bitmap, blkid); // blacklist new gc block in future

        const u64 oblkid = opoff >> FLEXFILE_BLOCK_BITS;
        // reset old status
        const u32 new_usage = flexfile_bm_update_blkusage(ff->bm, oblkid, -(s32)len);
        if (new_usage == 0) {
            rblocks++;
        }

        // in-place modify the flextree node
        flexfile_log_write(
                ff, FLEXFILE_OP_GC, ff->gc_ctx.queue[i].node->leaf_entry. extents[ff->gc_ctx.queue[i].idx].poff, poff, len);
        // now it is logged. safe!
        ff->gc_ctx.queue[i].node->leaf_entry.extents[ff->gc_ctx.queue[i].idx].poff = poff;
        ff->gc_ctx.queue[i].node->dirty = 1;
        generic_free(buf);
    }
    ff->gc_ctx.count = 0;
    // sync just GC'd tree, no matter the log is full or not
    // note: here the "next block" will be a non-gc preserved one
    //       because one round of gc is finished
    if (commit) {
        flexfile_sync_gc(ff);
    }
    return rblocks;
}

static u64 flexfile_gc_find_targets(
        const struct flexfile *const ff, struct bitmap *const bitmap, const struct bitmap *const hist_bitmap,
        const u8 round, const u64 nfblks)
{
    bitmap_set_all0(bitmap);
    const u64 threshold =
        (round == 0) ? (FLEXFILE_BLOCK_SIZE - 2 * FLEXFILE_MAX_EXTENT_SIZE): (FLEXFILE_BLOCK_SIZE >> round);
    u64 onfblks =
        (round == 0) ? (1u << FLEXFILE_MAX_EXTENT_BIT) : ((nfblks * ((1u << round) - 1u)) >> round);
    u64 count = 0;
    for (u64 i=0; i<FLEXFILE_BLOCK_COUNT && count < onfblks; i++) {
        const u64 usage = flexfile_bm_get_blkusage(ff->bm, i);
        if (usage != 0 && usage <= threshold && !bitmap_test(bitmap, i) && !bitmap_test(hist_bitmap, i)) {
            bitmap_set1(bitmap, i);
            count++;
        }
    }
    if (round == 0 && count != onfblks) {
        generic_printf("could not find enough blocks for the last round of gc, found %lu\n", count);
        for (u32 i=0; i<FLEXFILE_BM_BLKDIST_SIZE; i++) {
            generic_printf("%lu ", ff->bm->blkdist[i]);
        }
        generic_printf("free blocks %lu\n", ff->bm->free_blocks);
        generic_fflush(stdout);
        generic_exit(1);
    }
    return count;
}

// synchronous version, just like _prepare + _aync_gc.
void flexfile_gc(struct flexfile *const ff)
{
    if (!flexfile_gc_needed(ff)) {
        return;
    }
    // first, clean up the current pending gc
    for (u32 i=0; i<ff->gc_ctx.count; i++) {
        generic_free(ff->gc_ctx.queue[i].buf);
    }
    ff->gc_ctx.count = 0;
    // no matter what the current cursor is, reset it, cycle the first node
    ff->gc_ctx.loff = 0;

    //generic_printf("gc start empty %lu\n", ff->bm->free_blocks);
    //for (u32 i=0; i<FLEXFILE_BM_BLKDIST_SIZE; i++)
    //    generic_printf("%lu ", ff->bm->blkdist[i]);
    //generic_printf("\n");
    //generic_fflush(stdout);

    struct bitmap *const bitmap = bitmap_create(FLEXFILE_BLOCK_COUNT);
    struct bitmap *const hist_bitmap = bitmap_create(FLEXFILE_BLOCK_COUNT);
    // hist_bitmap cannot be recycled in this round
    bitmap_set_all0(hist_bitmap);
    // three rounds: 3 - 12.5% - 0.875 free, 2 - 25% - 0.75, 1 - 50%, 0.5 free
    for (u8 i=3; i>=0 && flexfile_gc_needed(ff); i--) {
        //if (i == 0) generic_printf("the final round\n");
        //generic_fflush(stdout);
        while (flexfile_gc_needed(ff) &&
               flexfile_gc_find_targets(ff, bitmap, hist_bitmap, i, ff->bm->free_blocks) > 1) {
            flexfile_gc_async_prepare(ff, bitmap);
            while (ff->gc_ctx.loff != 0) {
                flexfile_gc_async_prepare(ff, bitmap);
                if (flexfile_gc_async_queue_full(ff)) {
                    flexfile_gc_async(ff, hist_bitmap, 0);
                }
            }
            flexfile_gc_async(ff, hist_bitmap, 1);
        }
    }
    generic_free(bitmap);
    generic_free(hist_bitmap);

    //generic_printf("gc end empty %lu\n", ff->bm->free_blocks);
    //for (u32 i=0; i<FLEXFILE_BM_BLKDIST_SIZE; i++)
    //    generic_printf("%lu ", ff->bm->blkdist[i]);
    //generic_printf("\n");
    //generic_fflush(stdout);

    if (flexfile_gc_needed(ff)) {
        generic_printf("gc failed to recycle enough blocks, exit\n");
        for (u32 i=0; i<FLEXFILE_BM_BLKDIST_SIZE; i++) {
            generic_printf("%lu ", ff->bm->blkdist[i]);
        }
        generic_printf("free blocks %lu\n", ff->bm->free_blocks);
        generic_fflush(stdout);
        generic_exit(1);

        generic_exit(1);
    }
}

// }}} general
