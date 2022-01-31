#ifndef _FLEXTREE_H
#define _FLEXTREE_H

#include <stdio.h>
#include <assert.h>

#include "c/ctypes.h"
#include "c/lib.h"
#include "generic.h"


/*
 * Tunable parameters
 */

#define FLEXTREE_LEAF_CAP 60
#define FLEXTREE_INTERNAL_CAP 30
#define FLEXTREE_MAX_EXTENT_SIZE_LIMIT (64u << 20)

static_assert(FLEXTREE_MAX_EXTENT_SIZE_LIMIT*FLEXTREE_LEAF_CAP < ~(u32)0, "leaf size < 4G");

struct flextree_free_list;
struct flextree_path;

struct flextree_leaf_entry {
    struct flextree_extent {
        u32 loff;
        u32 len;
        u64 tag  :16;
        u64 poff :48;
        // 47 bits for address, 1 bit for hole. The max size of a file is 2^47-1 bytes (~128T, enough)
    } extents[FLEXTREE_LEAF_CAP] __attribute__((packed));
    struct flextree_node *prev;
    struct flextree_node *next;
};

static_assert(sizeof(struct flextree_extent) == 16, "16 bytes for extent");

struct flextree_internal_entry {
    u64 pivots[FLEXTREE_INTERNAL_CAP]; // n pivots
    struct {
        struct flextree_node *node;
        s64 shift;
    } children[FLEXTREE_INTERNAL_CAP+1]; // n+1 children
    u64 children_ids[FLEXTREE_INTERNAL_CAP+1]; // persistency helper
    u32 padding1[3];
};

struct flextree_node {
    u32 count; // actually u8 for path
    u8 is_leaf;
    u8 dirty;
    u8 padding1[2];
    struct flextree *tree;
    union {
        struct flextree_leaf_entry leaf_entry;
        struct flextree_internal_entry internal_entry;
    };
    u64 id;
};

static_assert(sizeof(struct flextree_node) == 1024, "1k for node");

struct flextree {
    // in-memory
    char *path;
    u64 max_loff;
    u32 max_extent_size;
    u64 version; // based on which persistent version
    struct flextree_node *root;
    struct flextree_node *leaf_head; // linked list head
    struct flextree_free_list *free_list;
    struct slab *node_slab;
    u64 in_memory_mode; // if in-memory, no real files are created
    // persistence helper
    file_type meta_fd;
    file_type node_fd;
    u64 node_count;
    u64 max_node_id;
    u64 root_id;
};

// range query result
struct flextree_query_result {
    u64 loff;
    u64 len;
    u64 count;
    struct {
        u64 poff;
        u64 len;
    } v[];
};

/*
 * A low-level position on leaf nodes
 * node: the leaf node pointer
 * loff: logical offset of the position
 * idx : the index of the entry
 * diff: the current position within the entry
 */

struct flextree_pos {
    struct flextree_node *node;
    u64 loff;
    u32 idx;
    u32 diff;
};

/*
 * Low-level APIs
 *
 * These APIs should be used with care, as it has strong presumptions
 * and the low-level access does not have global view
 */

extern struct flextree_pos flextree_pos_get_ll(const struct flextree *const tree, const u64 loff);

extern void flextree_pos_forward_ll(struct flextree_pos *const fp, const u64 step);

extern void flextree_pos_forward_extent_ll(struct flextree_pos *const fp);

extern void flextree_pos_backward_ll(struct flextree_pos *const fp, const u64 step);

extern void flextree_pos_rewind_ll(struct flextree_pos *const fp);

extern unsigned long flextree_pos_get_loff_ll(const struct flextree_pos *const fp);

extern unsigned long flextree_pos_get_poff_ll(const struct flextree_pos *const fp);

extern int flextree_pos_valid_ll(const struct flextree_pos *const fp);

extern int flextree_pos_get_tag_ll(const struct flextree_pos *const fp, u16 *const tag);

/*
 * High-level APIs
 */

extern void flextree_print(const struct flextree *const tree);

extern struct flextree *flextree_open(const char *const path, const u32 max_extent_size);

extern void flextree_close(struct flextree *const tree);

extern void flextree_sync(struct flextree *const tree);

extern int flextree_insert(struct flextree *const tree, const u64 loff, const u64 poff, const u32 len);

extern int flextree_delete(struct flextree *const tree, const u64 loff, const u64 len);

extern struct flextree_query_result *flextree_query(const struct flextree *const tree, const u64 loff, const u64 len);

extern struct flextree_query_result *flextree_query_wbuf(
        const struct flextree *const tree, const u64 loff, const u64 len, struct flextree_query_result *const rr);

extern int flextree_set_tag(struct flextree *const tree, const u64 loff, const u16 tag);

extern int flextree_get_tag(const struct flextree *const tree, const u64 loff, u16 *const tag);

extern int flextree_insert_wtag(
        struct flextree *const tree, const u64 loff, const u64 poff, const u32 len, const u16 tag);

// the following two functions are for testing
extern int flextree_pdelete(struct flextree *const tree, const u64 loff);

extern unsigned long flextree_pquery(const struct flextree *const tree, const u64 loff);

/*
 * Naive array implementation
 */

struct brute_force_extent {
    u64 loff :64; // logical offset (in file address space)
    u32 len  :32;  // segment length
    u64 poff :48; // physical offset (in device address space)
    u16 tag  :16;
} __attribute__((packed));

static_assert(sizeof(struct brute_force_extent) == 20, "20 bytes for bf extent");

struct brute_force {
    u64 count;
    u64 cap;
    u64 max_loff;
    u32 max_extent_size;
    struct brute_force_extent *extents;
};

extern void brute_force_print(const struct brute_force *const bf);

extern struct brute_force *brute_force_open(const u32 max_extent_size);

extern void brute_force_close(struct brute_force *const bf);

extern int brute_force_insert(struct brute_force *const bf, const u64 loff, const u64 poff, const u32 len);

extern unsigned long brute_force_pquery(const struct brute_force *const bf, const u64 loff);

extern struct flextree_query_result *brute_force_query(
        const struct brute_force *const bf, const u64 loff, const u64 len);

extern struct flextree_query_result *brute_force_query_wbuf(
        const struct brute_force *const bf, const u64 loff, const u64 len, struct flextree_query_result *const rr);

extern int brute_force_insert_wtag(
        struct brute_force *const bf, const u64 loff, const u64 poff, const u32 len, const u16 tag);

extern int brute_force_pdelete(struct brute_force *const bf, const u64 loff);

extern int brute_force_delete(struct brute_force *const bf, const u64 loff, const u64 len);

extern int brute_force_set_tag(struct brute_force *const bf, const u64 loff, const u16 tag);

extern int brute_force_get_tag(const struct brute_force*const bf, const u64 loff, u16 *const tag);

#endif
