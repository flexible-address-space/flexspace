#ifndef _FLEXDB_H
#define _FLEXDB_H

#include "c/ctypes.h"
#include "c/lib.h"
#include "c/kv.h"
#include "c/ord.h"
#include "c/wh.h"
#include "flexfile.h"

#define FLEXDB_MAX_KV_SIZE (4u << 10) // max size 4K

#define FLEXDB_TREE_LEAF_CAP (122u)
#define FLEXDB_TREE_INTERNAL_CAP (40u)

// thresholds by both count and size
#define FLEXDB_TREE_SPARSE_INTERVAL_COUNT (16u)
#define FLEXDB_TREE_SPARSE_INTERVAL_SIZE (16u << 10)

#define FLEXDB_MEMTABLE_CAP (1024u << 20) // 1G memtable
#define FLEXDB_MEMTABLE_FLUSH_BATCH (1024u)
#define FLEXDB_MEMTABLE_FLUSH_TIME (5u) // every 5s
#define FLEXDB_MEMTABLE_LOG_BUFFER_CAP (4lu << 20) // 4MB buffer

#define FLEXDB_CACHE_PARTITION_BITS (10u) // 1024
#define FLEXDB_CACHE_PARTITION_COUNT (1u << FLEXDB_CACHE_PARTITION_BITS)
#define FLEXDB_CACHE_PARTITION_MASK (FLEXDB_CACHE_PARTITION_COUNT - 1)
#define FLEXDB_CACHE_ENTRY_CHANCE (2u)
#define FLEXDB_CACHE_ENTRY_CHANCE_WARMUP (3u)
#define FLEXDB_CACHE_ENTRY_LOADING (~(u32)0u)

#define FLEXDB_UNSORTED_WRITE_QUOTA_COUNT (15u)
#define FLEXDB_TREE_SPARSE_INTERVAL (FLEXDB_TREE_SPARSE_INTERVAL_COUNT + FLEXDB_UNSORTED_WRITE_QUOTA_COUNT + 1)

#define FLEXDB_LOCK_SHARDING_BITS (4u)
#define FLEXDB_LOCK_SHARDING_COUNT (1u << FLEXDB_LOCK_SHARDING_BITS)
#define FLEXDB_LOCK_SHARDING_MASK (FLEXDB_LOCK_SHARDING_COUNT - 1)

#define FLEXDB_RECOVERY_WORKER_COUNT (4u)
#define FLEXDB_RECOVERY_SANITY_CHECK (0)

// #define FLEXDB_USE_RRING

static_assert(FLEXDB_MAX_KV_SIZE <= FLEXFILE_MAX_EXTENT_SIZE, "one kv must fit in one extent, no more");
// static_assert(FLEXDB_MAX_KV_SIZE < (16u << 10), "u16 for kv anchor size, so cap it at quarter");
static_assert(FLEXDB_UNSORTED_WRITE_QUOTA_COUNT < 128, "an u7 for blind writes");
static_assert(FLEXDB_TREE_SPARSE_INTERVAL < 255, "sparse interval < 255");
// static_assert(FLEXDB_TREE_SPARSE_INTERVAL_SIZE < ((64u << 10) - FLEXDB_MAX_KV_SIZE), "interval size < 64k");
// static_assert(FLEXDB_TREE_SPARSE_INTERVAL_SIZE + FLEXDB_UNSORTED_WRITE_QUOTA_COUNT * FLEXDB_MAX_KV_SIZE < (1u << 16),
//         "u16 for an anchor's (p)size, so the total size should be smaller");
static_assert(FLEXDB_TREE_SPARSE_INTERVAL % 16 == 0, "make the interval array aligned for AVX/AVX2");

struct flexdb_tree_anchor {
    struct kv *key;
    u32 loff;
    u32 psize;
    u8 unsorted;
    u8 padding1[7];
    struct flexdb_cache_entry *cache_entry; // :64
};

struct flexdb_tree_leaf_entry {
    struct flexdb_tree_anchor *anchors[FLEXDB_TREE_LEAF_CAP];
    struct flexdb_tree_node *prev;
    struct flexdb_tree_node *next;
};

struct flexdb_tree_internal_entry {
    struct kv *pivots[FLEXDB_TREE_INTERNAL_CAP]; // n pivots
    struct {
        struct flexdb_tree_node *node;
        s64 shift;
    } children[FLEXDB_TREE_INTERNAL_CAP+1]; // n+1 children
};

struct flexdb_tree_node {
    u32 parent_id;
    u32 count;
    u8 is_leaf;
    u8 padding1[3];
    struct flexdb_tree *tree;
    struct flexdb_tree_node *parent;
    union {
        struct flexdb_tree_internal_entry internal_entry;
        struct flexdb_tree_leaf_entry leaf_entry;
    };
};

static_assert(sizeof(struct flexdb_tree_node) == 1024, "1k node for flexdb index");

struct flexdb_tree_node_handler {
    struct flexdb_tree_node *node;
    s64 shift;
    u32 idx;
};

struct flexdb_tree {
    struct flexdb *db;
    struct flexdb_tree_node *root;
    struct flexdb_tree_node *leaf_head;
    struct slab *node_slab;
    struct slab *anchor_slab;
};

struct flexdb_cache_entry {
    // 64B
    u16 kv_fps[FLEXDB_TREE_SPARSE_INTERVAL]; // fingerprints for each cached key
    // 24B here to refcnt
    struct flexdb_tree_anchor *anchor;
    u32 size;
    u8 count;
    volatile u8 loading;
    u8 padding1[1];
    // for replacement
    u8 partial; // not enabled

    u16 frag;
    u16 access;
    au32 refcnt;
    struct kv *kv_interval[FLEXDB_TREE_SPARSE_INTERVAL];
    struct flexdb_cache_entry *prev;
    struct flexdb_cache_entry *next;
    u64 padding2[3];
};
static_assert(sizeof(struct flexdb_cache_entry) % 64 == 0, "cacheline aligned for slab");

struct flexdb_cache_partition {
    u64 cap;
    struct slab *entry_slab;
    struct flexdb_cache *cache;
    struct flexdb_cache_entry *tick; // clock pointer
    u64 padding1[7];
    spinlock spinlock;
    u64 padding2[7];
    au64 size;
    u64 padding3[7];
};

struct flexdb_cache {
    struct flexdb *db;
    u64 cap;
    // for replacement
    struct flexdb_cache_partition partitions[FLEXDB_CACHE_PARTITION_COUNT];
};

struct flexdb_memtable {
    struct flexdb *db;
    void *map;
    u8 *log_buffer;
    u32 log_buffer_size;
    int log_fd;
    volatile u8 hidden;
    u64 padding1[7];
    au32 size;
    u64 padding2[7];
    spinlock log_buffer_lock;
    u64 padding3[7];
};

struct flexdb_iterator {
    struct flexdb_ref *dbref;
    void *miter;
    struct kvref kvref;
    struct {
        u8 parked;
        u8 a;
        u8 h1;
        u8 h2;
        u32 mt_lockid;
        u32 ff_lockid;
    } status;
};

struct flexdb {
    char *path;
    struct flexfile *flexfile;
    struct flexdb_tree *tree;
    struct flexdb_memtable memtables[2];
    volatile u32 active_memtable; // active memtable 0 or 1, the other one is read-only
    volatile au32 refcnt;
    struct flexdb_cache *cache;
    struct {
        pthread_t thread;
        volatile u64 immediate_work;
        au64 work;
    } flush_worker;
    u8 *kvbuf1;
    u8 *kvbuf2;
    u8 *itvbuf;
    void *priv; // the rring placeholder
    u64 padding1[4];
    union {
        u64 _[8];
        rwlock lock;
    } rwlock_memtable[FLEXDB_LOCK_SHARDING_COUNT];
    union {
        u64 _[8];
        rwlock lock;
    } rwlock_flexfile[FLEXDB_LOCK_SHARDING_COUNT];
};

struct flexdb_ref {
    struct flexdb *db;
    u8 *kvbuf;
    u8 *itvbuf;
    void *mrefs[2];
    void *priv; // the rring placeholder
};

extern struct flexdb *flexdb_open(const char *const path, const u64 cache_cap_mb);

extern void flexdb_close(struct flexdb *const db);

extern struct flexdb_ref *flexdb_ref(struct flexdb *const db);

extern struct flexdb *flexdb_deref(struct flexdb_ref *const dbref);

extern int flexdb_put(struct flexdb_ref *const dbref, struct kv *const kv);

extern struct kv *flexdb_get(struct flexdb_ref *const dbref, const struct kref* const kref, struct kv *const out);

extern unsigned int flexdb_probe(struct flexdb_ref *const dbref, const struct kref *const kref);

extern int flexdb_delete(struct flexdb_ref *const dbref, const struct kref *const kref);

extern void flexdb_sync(struct flexdb_ref *const dbref);

extern struct kv *flexdb_read_kv(const struct flexfile_handler *const ffh, u8 *const buf, struct kv *const out);

extern struct flexdb_iterator *flexdb_iterator_create(struct flexdb_ref *const dbref);

extern void flexdb_iterator_seek(struct flexdb_iterator *const iter, const struct kref *const kref);

extern bool flexdb_iterator_valid(const struct flexdb_iterator *const iter);

extern struct kv *flexdb_iterator_peek(const struct flexdb_iterator *const iter, struct kv *const out);

extern void flexdb_iterator_skip(struct flexdb_iterator *const iter, const u64 step);

extern struct kv *flexdb_iterator_next(struct flexdb_iterator *const iter, struct kv *const out);

extern void flexdb_iterator_destroy(struct flexdb_iterator *const iter);

extern void flexdb_iterator_park(struct flexdb_iterator *const iter);

extern void flexdb_fprint(const struct flexdb *const db, FILE *const f);

extern bool flexdb_merge(
        struct flexdb_ref *const dbref, const struct kref *const kref, kv_merge_func uf, void *const priv);

extern const struct kvmap_api kvmap_api_flexdb;

#endif
