#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "flextree.h"
#define FLEXTREE_MAX_EXTENT_SIZE_LIMIT (64u << 20)

// flextree {{{

#define FLEXTREE_HOLE (1lu << 47) // highest bit set to indicate a hole
#define FLEXTREE_POFF_MASK (0xffffffffffff) // 48 bits
#define FLEXTREE_PATH_DEPTH 7 // at most 7 levels

struct flextree_path {
    u8 level;
    u8 path[FLEXTREE_PATH_DEPTH];
    struct flextree_node *nodes[FLEXTREE_PATH_DEPTH];
};

static_assert(FLEXTREE_LEAF_CAP < 254, "u8 for path");
static_assert(FLEXTREE_INTERNAL_CAP < 254, "u8 for path");

// free_list {{{

struct flextree_free_list {
    u64 cap;
    u64 count;
    u64 *list;
};

static struct flextree_free_list *flextree_free_list_create()
{
    struct flextree_free_list *const fl = generic_malloc(sizeof(*fl));
    fl->cap = 4096;
    fl->count = 0;
    fl->list = generic_malloc(sizeof(fl->list[0]) * fl->cap);
    return fl;
}

static void flextree_free_list_extend(struct flextree_free_list *const fl)
{
    fl->list = generic_realloc(fl->list, sizeof(fl->list[0]) * fl->cap * 2);
    fl->cap *= 2;
}

static void flextree_free_list_put(struct flextree_free_list *const fl, const u64 val)
{
    if (fl->cap == fl->count) {
        flextree_free_list_extend(fl);
    }
    fl->list[fl->count++] = val;
}

static inline u8 flextree_free_list_ne(const struct flextree_free_list *const fl)
{
    return (fl->count > 0) ? 1 : 0;
}

static inline u64 flextree_free_list_get(struct flextree_free_list *const fl)
{
    return fl->list[--fl->count];
}

static inline void flextree_free_list_destroy(struct flextree_free_list *const fl)
{
    generic_free(fl->list);
    generic_free(fl);
}

static void flextree_free_list_merge(struct flextree_free_list *const fl1, const struct flextree_free_list *const fl2)
{
    for (u64 i=0; i<fl2->count; i++) {
        flextree_free_list_put(fl1, fl2->list[i]);
    }
}

// }}} free_list

// helper {{{

static void flextree_print_node_rec(const struct flextree_node *node)
{
    generic_printf("\n[Node]: %p count %u is_leaf %u\n        ", node, node->count, node->is_leaf);
    generic_printf("flextree %p dirty %u id %lu\n", node->tree, node->dirty, node->id);
    if (node->is_leaf) {
        const struct flextree_leaf_entry *const le = &node->leaf_entry;
        generic_printf("leaf_entry\n");
        for (u32 i=0; i<node->count; i++) {
            const struct flextree_extent *fe = &le->extents[i];
            generic_printf("  extent %d loff %u poff %lu len %u\n",
                    i, fe->loff, fe->poff & FLEXTREE_POFF_MASK, fe->len);
        }
    }
    else {
        const struct flextree_internal_entry *const ie = &node->internal_entry;
        generic_printf("internal_entry\n");
        for (u32 i=0; i<node->count+1; i++) {
            if (i != 0) {
                generic_printf("  pivot %lu\n", ie->pivots[i-1]);
            }
            generic_printf("  children %u pointer %p shift %ld id %lu\n",
                    i, ie->children[i].node, ie->children[i].shift, ie->children_ids[i]);
        }
        for (u32 i=0; i<node->count+1; i++) {
            flextree_print_node_rec(ie->children[i].node);
        }
    }
}

static inline void flextree_node_free(struct flextree_node *const node)
{
    flextree_free_list_put(node->tree->free_list, node->id);
    node->tree->node_count--;
    slab_free_unsafe(node->tree->node_slab, node);
}

static void flextree_node_free_rec(struct flextree_node *const node)
{
    if (!node->is_leaf) {
        for (u32 i=0; i<node->count+1; i++) {
            flextree_node_free_rec(node->internal_entry.children[i].node);
        }
    }
    flextree_node_free(node);
}

static inline struct flextree_node *flextree_path_parent_node(const struct flextree_path *const path)
{
    if (path->level == 0) {
        return NULL;
    }
    return path->nodes[path->level-1];
}

static inline struct flextree_node *flextree_path_grandparent_node(const struct flextree_path *const path)
{
    if (path->level < 2) {
        return NULL;
    }
    return path->nodes[path->level-2];
}

static inline u32 flextree_path_parent_idx(const struct flextree_path *const path)
{
    if (path->level == 0) {
        return -1u;
    }
    return path->path[path->level-1];
}

static inline u32 flextree_path_grandparent_idx(const struct flextree_path *const path)
{
    if (path->level < 2) {
        return -1u;
    }
    return path->path[path->level-2];
}

// }}} helper

// search {{{

// binary search, to find the key in leaf node
static u32 flextree_find_pos_in_leaf(const struct flextree_node *const node, const u64 loff)
{
    debug_assert(node->is_leaf);
    u32 hi = node->count;
    u32 lo = 0;
    const struct flextree_leaf_entry *const le = &node->leaf_entry;
    // binary search

    //while ((lo + 7) < hi) {
    //    const u32 target = (lo + hi) >> 1;
    //    const struct flextree_extent *const fe = &le->extents[target];
    //    cpu_prefetch0(fe);
    //    cpu_prefetch0(&le->extents[(target+1+hi)>>1]);
    //    cpu_prefetch0(&le->extents[(lo+target)>>1]);
    //    if (fe->loff <= loff) {
    //        if (fe->loff + fe->len > loff) {
    //            return target;
    //        } else {
    //            lo = target + 1;
    //        }
    //    } else {
    //        hi = target;
    //    }
    //}

    while (lo < hi) {
        const u32 target = (lo + hi) >> 1;
        const struct flextree_extent *const fe = &le->extents[target];
        if (fe->loff <= loff) {
            if (fe->loff + fe->len > loff) {
                return target;
            } else {
                lo = target + 1;
            }
        } else {
            hi = target;
        }
    }
    return lo;
}

// binary search, to find the key in intarnal node
static u32 flextree_find_pos_in_internal(const struct flextree_node *const node, const u64 loff)
{
    debug_assert(!node->is_leaf);
    const struct flextree_internal_entry *const ie = &node->internal_entry;
    u32 hi = node->count;
    u32 lo = 0;
    // binary search
    while (lo < hi) {
        const u32 target = (lo + hi) >> 1;
        u64 base = ie->pivots[target];
        if (base <= loff) {
            lo = target + 1;
        } else {
            hi = target;
        }
    }
    return lo;
}

// with a loff, get the leaf node that the loff belongs to..
// if it is larger than any nodes, the rightmost leaf node is returned
static struct flextree_node *flextree_find_leaf_node(
        const struct flextree *const tree, struct flextree_path *const path, u64 *const ploff)
{
    struct flextree_node *node = tree->root;
    debug_assert(node);
    if (unlikely(node->is_leaf)) {
        return node;
    }
    u64 loff = *ploff;
    do {
        u32 target = flextree_find_pos_in_internal(node, loff);
        const struct flextree_internal_entry * const ie = &node->internal_entry;
#ifndef FLEXTREE_NAIVE
        loff -= (u64)ie->children[target].shift;
#endif

        path->nodes[path->level]  = node;
        path->path[path->level++] = (u8)target; // although it is u32
        if (path->level > FLEXTREE_PATH_DEPTH) {
            printf("the tree is too high, panic.\n");
            fflush(stdout);
            exit(1);
        }
        node = ie->children[target].node;
        if (node->is_leaf) {
            *ploff = loff;
            return node;
        }

        // internal
        //cpu_prefetch0(&node->internal_entry.pivots[4]);
        //cpu_prefetch0(&node->internal_entry.pivots[12]);
        //cpu_prefetch0(&node->internal_entry.pivots[20]);
    } while (true);
}

static u64 flextree_range_count(const struct flextree *const tree, const u64 loff, const u64 len)
{
    if (loff + len > tree->max_loff) {
        return 0;
    }
    u64 tlen = len;
    u64 tloff = loff;
    u64 ret = 0;
    struct flextree_pos fp = flextree_pos_get_ll(tree, tloff);
    // now we are at the starting point
    while (tlen > 0) {
        u32 remain = fp.node->leaf_entry.extents[fp.idx].len - fp.diff;
        u32 step = remain > tlen ? (u32)tlen : remain;
        tlen -= step;
        tloff += step;
        ret++;
        flextree_pos_forward_ll(&fp, step);
    }
    return ret;
}

// }}} search

// node {{{

static inline u64 flextree_node_alloc_id(struct flextree *const tree)
{
    if (flextree_free_list_ne(tree->free_list)) {
        return flextree_free_list_get(tree->free_list);
    }
    return tree->max_node_id++;
}

static inline u8 flextree_node_full(const struct flextree_node *const node)
{
    const u32 cap = (node->is_leaf) ? FLEXTREE_LEAF_CAP : FLEXTREE_INTERNAL_CAP;
    return (cap - 1 <= node->count) ? 1 : 0;
}

static inline u8 flextree_node_empty(const struct flextree_node *const node)
{
    return (node->count == 0) ? 1 : 0;
}

static void flextree_node_rebase(struct flextree_node *node, const struct flextree_path *const path)
{
    debug_assert(node->is_leaf);
    debug_assert(path->level > 0);
    if (node->leaf_entry.extents[node->count-1].loff >= (u32)(~0) - node->tree->max_extent_size * 2) {
        const u32 new_base = node->leaf_entry.extents[0].loff;
        debug_assert(new_base != 0);
        const u32 p_idx = flextree_path_parent_idx(path);
        struct flextree_node *const parent = flextree_path_parent_node(path);
        parent->internal_entry.children[p_idx].shift += (s64)new_base;
        for (u32 i=0; i<node->count; i++) {
            node->leaf_entry.extents[i].loff -= new_base;
        }
        parent->dirty = 1;
        node->dirty = 1;
    }
}

#ifdef FLEXTREE_NAIVE
static void flextree_node_shift_recursive_apply(struct flextree_node *node, s64 shift)
{
    if (node->is_leaf) {
        struct flextree_leaf_entry *le = &node->leaf_entry;
        for (u32 i=0; i<node->count; i++) {
            le->extents[i].loff += shift;
        }
    } else {
        struct flextree_internal_entry *ie = &node->internal_entry;
        for (u32 i=0; i<node->count; i++) {
            ie->pivots[i] += shift;
        }
        for (u32 i=0; i<node->count+1; i++) {
            flextree_node_shift_recursive_apply(ie->children[i].node, shift);
        }
    }
}
#endif

static void flextree_node_shift_up_propagate(
        struct flextree_node *node, const struct flextree_path *const path, const s64 shift)
{
    debug_assert(node);
    struct flextree_path opath = *path;
    struct flextree_node *parent = flextree_path_parent_node(&opath);
    while (parent) {
        const u32 p_idx = flextree_path_parent_idx(&opath);
        node = parent;
        opath.level--;
        parent = flextree_path_parent_node(&opath);
        debug_assert(!node->is_leaf);
        struct flextree_internal_entry *const ie = &node->internal_entry;
        for (u32 i=p_idx; i<node->count; i++) {
            ie->pivots[i] += (u64)shift;
#ifdef FLEXTREE_NAIVE
            flextree_node_shift_recursive_apply(ie->children[i+1].node, shift);
#else
            ie->children[i+1].shift += shift;
#endif
        }
        node->dirty = 1;
    }
}

static void flextree_node_shift_apply(struct flextree_node *const node, const s64 shift)
{
    // only call this when confident no overflow will happen
    if (node->is_leaf) {
        struct flextree_leaf_entry *const le = &node->leaf_entry;
        debug_assert((u32)(le->extents[node->count-1].loff + (u64)shift) <= ~(u32)0);
        for (u32 i=0; i<node->count; i++) {
            le->extents[i].loff = (u32)(le->extents[i].loff + (u64)shift);
        }
    } else {
        struct flextree_internal_entry *const ie = &node->internal_entry;
        for (u32 i=0; i<node->count; i++) {
            ie->pivots[i] += (u64)shift;
        }
        for (u32 i=0; i<node->count+1; i++) {
            ie->children[i].shift += shift;
        }
    }
    node->dirty = 1;
}

static struct flextree_node *flextree_create_node(struct flextree *const tree)
{
    struct flextree_node *const node= slab_alloc_unsafe(tree->node_slab);
    memset(node, 0, sizeof(*node));
    node->count = 0;
    node->tree = tree;
    node->id = flextree_node_alloc_id(tree);
    node->dirty = 1;
    tree->node_count++;
    return node;
}

static inline struct flextree_node *flextree_create_leaf_node(struct flextree *const tree)
{
    struct flextree_node *const node = flextree_create_node(tree);
    node->is_leaf = 1;
    return node;
}

static inline struct flextree_node *flextree_create_internal_node(struct flextree *const tree)
{
    struct flextree_node *const node = flextree_create_node(tree);
    return node;
}

static void flextree_split_internal_node(struct flextree_node *const node, const struct flextree_path *const path)
{
    debug_assert(!node->is_leaf);
    struct flextree_node *const node1 = node;
    struct flextree_node *const node2 = flextree_create_internal_node(node1->tree);
    struct flextree_internal_entry *const ie1 = &node1->internal_entry;
    struct flextree_internal_entry *const ie2 = &node2->internal_entry;
    const u32 count = (node1->count + 1) / 2;
    const u64 new_base = ie1->pivots[count];
    // manipulate node2
    node2->count = node1->count - count - 1;
    memmove(ie2->pivots, &ie1->pivots[count+1],
            node2->count * sizeof(ie2->pivots[0]));
    memmove(ie2->children, &ie1->children[count+1],
            (node2->count+1) * sizeof(ie2->children[0]));
    memmove(ie2->children_ids, &ie1->children_ids[count+1],
            (node2->count+1) * sizeof(ie2->children_ids[0]));
    // manipulate node1
    node1->count = count;
    // if no parent, create one and set as root
    struct flextree_node *parent = flextree_path_parent_node(path);
    if (!parent) {
        parent = flextree_create_internal_node(node1->tree);
        debug_assert(parent->tree);
        parent->tree->root = parent;
        parent->tree->root_id = parent->id;
    }
    // insert one extent entry in parent
    struct flextree_internal_entry *const ie = &parent->internal_entry;
    if (parent->count == 0) { // which means the parent is the root now
        debug_assert(path->level == 0);
        ie->children[0].node = node1;
        ie->children[0].shift = 0;
        ie->children[0+1].node = node2;
        ie->children[0+1].shift = 0;
        ie->pivots[0] = new_base;
        ie->children_ids[0] = node1->id;
        ie->children_ids[0+1] = node2->id;
        parent->count = 1;
    }
    else {
        debug_assert(path->level > 0);
        const u32 p_idx = path->path[path->level-1];
        const s64 orig_shift = ie->children[p_idx].shift;
        memmove(&ie->pivots[p_idx+1], &ie->pivots[p_idx],
                (parent->count - p_idx) * sizeof(ie->pivots[0]));
        memmove(&ie->children[p_idx+2], &ie->children[p_idx+1],
                (parent->count - p_idx) * sizeof(ie->children[0]));
        memmove(&ie->children_ids[p_idx+2], &ie->children_ids[p_idx+1],
                (parent->count - p_idx) * sizeof(ie->children_ids[0]));
        ie->children[p_idx+1].node = node2;
        ie->children[p_idx+1].shift= orig_shift;
        ie->pivots[p_idx] = (u64)(new_base + (u64)orig_shift);
        ie->children_ids[p_idx+1] = node2->id;
        parent->count += 1;
    }
    parent->dirty = 1;
    node1->dirty = 1;
    node2->dirty = 1;
    if (flextree_node_full(parent)) {
        debug_assert(path->level > 0);
        struct flextree_path ppath = *path;
        ppath.level--;
        flextree_split_internal_node(parent, &ppath);
    }
}

static void flextree_link_two_nodes(struct flextree_node *const node1, struct flextree_node *const node2)
{
    debug_assert(node1->is_leaf && node2->is_leaf);
    struct flextree_leaf_entry *const le1 = &node1->leaf_entry;
    struct flextree_leaf_entry *const le2 = &node2->leaf_entry;
    le2->prev = node1;
    le2->next = le1->next;
    le1->next = node2;
    if (le2->next) {
        le2->next->leaf_entry.prev = node2;
    }
}

static void flextree_split_leaf_node(struct flextree_node *const node, const struct flextree_path *const path)
{
    debug_assert(node->is_leaf);
    struct flextree_node *const node1 = node;
    struct flextree_node *const node2 = flextree_create_leaf_node(node1->tree);
    flextree_link_two_nodes(node1, node2);
    struct flextree_leaf_entry *const le1 = &node1->leaf_entry;
    struct flextree_leaf_entry *const le2 = &node2->leaf_entry;
    const u32 count = (node1->count + 1) / 2;
    // manipulate node2
    node2->count = node1->count - count;
    memmove(le2->extents, &le1->extents[count], node2->count * sizeof(le2->extents[0]));
    // manipulate node1
    node1->count = count;
    // if no parent, create one and set as root
    struct flextree_node *parent = flextree_path_parent_node(path);
    if (!parent) {
        parent = flextree_create_internal_node(node1->tree);
        parent->tree->root = parent;
        parent->tree->root_id = parent->id;
    }
    // insert one extent entry in parent
    struct flextree_internal_entry *const ie = &parent->internal_entry;
    if (parent->count == 0) {
        ie->children[0].node = node1;
        ie->children[0].shift = 0;
        ie->children[0+1].node = node2;
        ie->children[0+1].shift = 0;
        ie->pivots[0] = le2->extents[0].loff;
        ie->children_ids[0] = node1->id;
        ie->children_ids[0+1] = node2->id;
        parent->count = 1;
    }
    else {
        const u32 p_idx = flextree_path_parent_idx(path);
        const s64 orig_shift = ie->children[p_idx].shift;
        memmove(&ie->pivots[p_idx+1], &ie->pivots[p_idx],
                (parent->count - p_idx) * sizeof(ie->pivots[0]));
        memmove(&ie->children[p_idx+2], &ie->children[p_idx+1],
                (parent->count - p_idx) * sizeof(ie->children[0]));
        memmove(&ie->children_ids[p_idx+2], &ie->children_ids[p_idx+1],
                (parent->count - p_idx) * sizeof(ie->children_ids[0]));
        ie->children[p_idx+1].node = node2;
        ie->children[p_idx+1].shift = orig_shift;
        ie->pivots[p_idx] = (u64)(le2->extents[0].loff + (u64)orig_shift);
        ie->children_ids[p_idx+1] = node2->id;
        parent->count += 1;
    }
    parent->dirty = 1;
    node1->dirty = 1;
    node2->dirty = 1;
    // try rebase
    if (path->level > 0) {
        flextree_node_rebase(node1, path);
        struct flextree_path spath = *path;
        spath.path[spath.level-1]++;
        flextree_node_rebase(node2, &spath);
    }
    if (flextree_node_full(parent)) {
        debug_assert(path->level > 0);
        struct flextree_path ppath = *path;
        ppath.level--;
        flextree_split_internal_node(parent, &ppath);
    }
}

static void flextree_node_recycle_linked_list(const struct flextree_node *const node)
{
    debug_assert(node->is_leaf);
    debug_assert(node->tree->root != node); // should not be root
    const struct flextree_leaf_entry *const le = &node->leaf_entry;
    struct flextree_node *const prev = le->prev;
    struct flextree_node *const next = le->next;
    debug_assert(prev || next);
    if (prev) {
        prev->leaf_entry.next = next;
    }
    else {
        node->tree->leaf_head = next;
    }
    if (next) {
        next->leaf_entry.prev = prev;
    }
}

// I'm too lazy to implement merging.. just recycle empty nodes and
// it is fine in most cases.
static void flextree_recycle_node(struct flextree_node *const node, const struct flextree_path *const path)
{
    debug_assert(node->count == 0);
    struct flextree_node *const parent = flextree_path_parent_node(path);
    const u32 p_idx = flextree_path_parent_idx(path);
    u8 parent_exist = 0;
    if (parent) {
        parent_exist = 1;
    }
    // here comes the recycle part!
    if (node->tree->root == node) {
        // case 1: node is the root
        // just some assertions here..
        debug_assert(parent == NULL);
        debug_assert(node->is_leaf);
        debug_assert(node->count == 0);
        debug_assert(node->leaf_entry.prev == NULL);
        debug_assert(node->leaf_entry.next== NULL);
    } else if (parent->count == 1) {
        // case 2: only one pivot in parent
        debug_assert(p_idx <= 1);
        const u32 s_idx = p_idx == 0 ? 1 : 0;
        const s64 s_shift = parent->internal_entry.children[s_idx].shift;
        struct flextree_node *const s_node = parent->internal_entry.children[s_idx].node;
        // pre-process the linked list if node is leaf
        if (node->is_leaf) {
            flextree_node_recycle_linked_list(node);
        }
        // free nodes
        flextree_node_free(node);
        flextree_node_free(parent);
        // do recycling
        if (s_node->tree->root == parent) {
            // case 2.1: parent is root
            // no more shift pointers, so need to apply
            flextree_node_shift_apply(s_node, s_shift);
            // set new root to sibling
            s_node->tree->root = s_node;
            s_node->tree->root_id = s_node->id;
            debug_assert(!s_node->is_leaf || s_node->tree->leaf_head == s_node);
        } else {
            // case 2.2: parent is not root
            debug_assert(path->level > 1);
            struct flextree_node *const gparent = flextree_path_grandparent_node(path);
            const u32 gp_idx = flextree_path_grandparent_idx(path);
            gparent->internal_entry.children[gp_idx].node = s_node;
            gparent->internal_entry.children[gp_idx].shift += s_shift;
            gparent->internal_entry.children_ids[gp_idx] += s_node->id;
            gparent->dirty = 1;
        }
        parent_exist = 0;
    } else {
        // case 3: normal case, remove one pivot from parent..
        if (node->is_leaf) {
            flextree_node_recycle_linked_list(node);
        }
        flextree_node_free(node);
        struct flextree_internal_entry *const ie = &parent->internal_entry;
        if (p_idx == 0) {
            // case 3.1: first child
            memmove(ie->pivots, &ie->pivots[1],
                    (parent->count-1) * sizeof(ie->pivots[0]));
            memmove(ie->children, &ie->children[1],
                    (parent->count) * sizeof(ie->children[0]));
            memmove(ie->children_ids, &ie->children_ids[1],
                    (parent->count) * sizeof(ie->children_ids[0]));
        } else {
            // case 3.2: not first child, quite similar
            memmove(&ie->pivots[p_idx-1], &ie->pivots[p_idx],
                    (parent->count - p_idx) * sizeof(ie->pivots[0]));
            memmove(&ie->children[p_idx], &ie->children[p_idx+1],
                    (parent->count - p_idx + 1) * sizeof(ie->children[0]));
            memmove(&ie->children_ids[p_idx], &ie->children_ids[p_idx+1],
                    (parent->count - p_idx + 1) * sizeof(ie->children_ids[0]));
        }
        parent->count--;
        parent->dirty = 1;
    }

    if (parent_exist == 1 && flextree_node_empty(parent)) {
        struct flextree_path ppath = *path;
        ppath.level--;
        flextree_recycle_node(parent, path);
    }
}

// to check (loff, poff) is continous with extent
static inline u8 flextree_extent_sequential(
        const struct flextree_extent *const extent, const u32 max_extent_size,
        const u64 loff, const u64 poff, const u64 len)
{
    u8 ret = 0;
    if (extent->poff + extent->len == poff && extent->loff + extent->len == loff &&
            extent->len + len <= max_extent_size && (extent->poff / max_extent_size) == (poff / max_extent_size)) {
        ret = 1;
    }
    return ret;
}

// once know the leaf node, insert some entry in it
// noted that the loff here is the already shifted one
static void flextree_insert_to_leaf_node(
        struct flextree_node *const node, const u32 loff, const u64 poff, const u32 len, const u16 tag)
{
    debug_assert(node->is_leaf);
    const struct flextree_extent t = {.loff = loff, .len = len, .poff = poff & FLEXTREE_POFF_MASK, .tag = tag};
    // find insertion position
    const u32 target = flextree_find_pos_in_leaf(node, loff);
    struct flextree_leaf_entry *const le = &node->leaf_entry;

    u32 shift = 1; // since which idx to add current len

    if (target == node->count) {
        if (target > 0 && tag == 0 &&
                flextree_extent_sequential(&le->extents[target-1], node->tree->max_extent_size, loff, poff, len)) {
            // sequential
            le->extents[target-1].len += len;
            shift = 0;
        } else {
            le->extents[node->count++] = t;
        }
    }
    else {
        struct flextree_extent *const curr_extent = &le->extents[target];
        if (curr_extent->loff == loff) {
            if (target > 0 && tag == 0 &&
                    flextree_extent_sequential(curr_extent-1, node->tree->max_extent_size, loff, poff, len)) {
                // sequential
                (curr_extent-1)->len += len;
                shift = 0;
            } else {
                memmove(curr_extent + 1, curr_extent, sizeof(*curr_extent) *(node->count - target));
                *curr_extent = t;
                node->count++;
            }
        } else { // need to split!
            debug_assert(curr_extent->loff < loff);
            shift = 2;
            const u32 so = loff - curr_extent->loff;
            memmove(curr_extent + 3, curr_extent + 1, sizeof(*curr_extent) *(node->count - target - 1));
            // callback the split function provided by upper level
            const struct flextree_extent left = {.loff = curr_extent->loff, .len = so,
                                                 .poff = curr_extent->poff, .tag = curr_extent->tag};
            const struct flextree_extent right = {.loff = curr_extent->loff + so, .len = curr_extent->len - so,
                                                  .poff = curr_extent->poff + so, .tag = 0};
            le->extents[target] = left;
            le->extents[target+2] = right;
            le->extents[target+1] = t;
            node->count += 2;
        }
    }
    for (u32 i=target+shift; i<node->count; i++) {
        le->extents[i].loff += len;
    }
    node->dirty = 1;
}

// }}} node

// persistency {{{

static void flextree_sync_cow_rec(
        struct flextree_node *const node, const struct flextree_path *const path, struct flextree_free_list *const tffl)
{
    if (!node->is_leaf) {
        struct flextree_path tpath = *path;
        for (u32 i=0; i<node->count+1; i++) {
            tpath.nodes[tpath.level] = node;
            tpath.path[tpath.level] = (u8)i;
            tpath.level++;
            flextree_sync_cow_rec(node->internal_entry.children[i].node, &tpath, tffl);
            tpath.level--;
        }
    }
    // start sync now
    if (!node->dirty) {
        return;
    }
    flextree_free_list_put(tffl, node->id);
    node->id = flextree_node_alloc_id(node->tree);
    struct flextree_node *const parent = flextree_path_parent_node(path);
    const u32 p_idx = flextree_path_parent_idx(path);
    if (parent) {
        parent->internal_entry.children_ids[p_idx] = node->id;
    }
    const ssize_t r = generic_pwrite(node->tree->node_fd, node, sizeof(*node), (off_t)(node->id*sizeof(*node)));
    if (r != sizeof(*node)) {
        generic_printf("flextree node cow sync failed, exit\n");
        generic_exit(1);
    }
    node->dirty = 0;
}

static void flextree_sync_meta(const struct flextree *const tree)
{
    ssize_t r = generic_pwrite(tree->meta_fd, tree, sizeof(*tree), 0);
    if (r != sizeof(*tree)) {
        generic_printf("flextree meta sync failed, exit\n");
        generic_exit(1);
    }
    int r2 = generic_fdatasync(tree->meta_fd);
    debug_assert(r2 == 0);
    (void)r;
    (void)r2;
}

static void flextree_load_node_rec(struct flextree *const tree, const u64 id, struct flextree_node **const node)
{
    *node = slab_alloc_unsafe(tree->node_slab);
    const ssize_t r = generic_pread(tree->node_fd, *node, sizeof(**node), (off_t)(id*sizeof(**node)));
    debug_assert(r == sizeof(**node));
    (void)r;
    (*node)->tree = tree;
    if (!(*node)->is_leaf) {
        for (u32 i=0; i<(*node)->count + 1; i++) {
            flextree_load_node_rec(
                    tree, (*node)->internal_entry.children_ids[i], &((*node)->internal_entry.children[i].node));
        }
    }
    (*node)->dirty = 0;
}

static void flextree_persistent_init(struct flextree *const tree)
{
    char *const tpath = generic_malloc(sizeof(char) * (strlen(tree->path) + 32));
    // tree
    generic_sprintf(tpath, "%s/%s", tree->path, "TREE");
    tree->meta_fd = generic_open(tpath, O_RDWR | O_CREAT, 0644);
    debug_assert(tree->meta_fd > 0);

    // node
    generic_sprintf(tpath, "%s/%s", tree->path, "NODE");
    tree->node_fd = generic_open(tpath, O_RDWR | O_CREAT, 0644);
    debug_assert(tree->node_fd > 0);

    generic_free(tpath);
}

static void flextree_rebuild_linked_list(
        struct flextree *const tree, struct flextree_node* const node, struct flextree_node **const last)
{
    if (node->is_leaf) {
        struct flextree_leaf_entry *const le = &node->leaf_entry;
        if (*last == NULL) {
            tree->leaf_head = node;
            le->prev = NULL;
            le->next = NULL;
        }
        else {
            (*last)->leaf_entry.next = node;
            le->prev = (*last);
            le->next = NULL;
        }
        *last = node;
    }
    else {
        const struct flextree_internal_entry *ie = &node->internal_entry;
        for (u64 i=0; i<node->count+1; i++) {
            flextree_rebuild_linked_list(tree, ie->children[i].node, last);
        }
    }
}

static void flextree_rebuild_node_slots_rec(const struct flextree_node *const node, u8 *const node_slots)
{
    if (!node->is_leaf) {
        const struct flextree_internal_entry *const ie = &node->internal_entry;
        for (u32 i=0; i<node->count+1; i++) {
            flextree_rebuild_node_slots_rec(ie->children[i].node, node_slots);
        }
    }
    node_slots[node->id] = 1;
}

static void flextree_load(struct flextree *const tree)
{
    // read tree
    struct flextree _tree;
    ssize_t r = generic_pread(tree->meta_fd, &_tree, sizeof(_tree), 0);
    debug_assert(r == sizeof(_tree));
    (void)r;

    tree->version = _tree.version;
    tree->max_loff = _tree.max_loff;
    tree->max_extent_size = _tree.max_extent_size;
    tree->root_id = _tree.root_id;
    tree->node_count = _tree.node_count;
    tree->max_node_id = _tree.max_node_id;

    // read nodes
    flextree_load_node_rec(tree, tree->root_id, &(tree->root));
    struct flextree_node *last = NULL;
    // rebuild linked list
    flextree_rebuild_linked_list(tree, tree->root, &last);
    // rebuild free list
    u8 *const node_slots = generic_malloc(sizeof(u8) * (tree->max_node_id));
    debug_assert(node_slots);
    memset(node_slots, 0, sizeof(u8) * (tree->max_node_id));
    flextree_rebuild_node_slots_rec(tree->root, node_slots);
    for (u64 i=0; i<tree->max_node_id; i++) {
        if (node_slots[i] == 0) {
            flextree_free_list_put(tree->free_list, i);
        }
    }
    generic_free(node_slots);
}

// }}} persistency

// low-level operations {{{

struct flextree_pos flextree_pos_get_ll(const struct flextree *const tree, const u64 loff)
{
    struct flextree_path path;
    path.level = 0;
    u64 oloff = loff;
    struct flextree_node *node = flextree_find_leaf_node(tree, &path, &oloff);
    debug_assert(node->is_leaf);
    u32 target = flextree_find_pos_in_leaf(node, oloff);
    u32 diff = 0;
    const struct flextree_extent *const curr_extent = &node->leaf_entry.extents[target];
    if (target < node->count && curr_extent->loff <= oloff && curr_extent->loff + curr_extent->len > oloff) {
        diff = (u32)(oloff - curr_extent->loff);
    }
    else {
        node = NULL;
        target = 0;
    }
    return (struct flextree_pos){.node = node, .loff = loff, .idx = target, .diff = diff};
}

// step forwards/backwards and change the location of *fp
void flextree_pos_forward_ll(struct flextree_pos *const fp, const u64 step)
{
    debug_assert(fp->node);
    debug_assert(fp->node->is_leaf);
    u64 ostep = step;
    do {
        const u32 len = fp->node->leaf_entry.extents[fp->idx].len;
        const u32 add = len - fp->diff > ostep ? (u32)ostep : len - fp->diff;
        ostep -= add;
        fp->diff += add;
        if (fp->diff == len) {
            if (fp->idx + 1 < fp->node->count) {
                fp->idx++;
            } else {
                fp->node = fp->node->leaf_entry.next;
                fp->idx = 0;
            }
            fp->diff = 0;
        }
        fp->loff += add;
    } while (ostep > 0 && fp->node);
}

void flextree_pos_forward_extent_ll(struct flextree_pos *const fp)
{
    debug_assert(fp->node);
    debug_assert(fp->node->is_leaf);
    const u32 remain = fp->node->leaf_entry.extents[fp->idx].len - fp->diff;
    return flextree_pos_forward_ll(fp, (u64)remain);
}

void flextree_pos_backward_ll(struct flextree_pos *const fp, const u64 step)
{
    debug_assert(fp->node);
    debug_assert(fp->node->is_leaf);
    u64 ostep = step;
    while (ostep > 0 && fp->node) {
        const u32 minus = fp->diff > ostep ? (u32)ostep : fp->diff;
        ostep -= minus;
        fp->diff -= minus;
        if (ostep > 0) {
            if (fp->idx > 0) {
                fp->idx--;
            } else {
                fp->node = fp->node->leaf_entry.prev;
                if (fp->node) {
                    fp->idx = fp->node->count - 1;
                }
            }
            if (fp->node) {
                fp->diff = fp->node->leaf_entry.extents[fp->idx].len;
            }
        }
        fp->loff -= minus;
    }
}

void flextree_pos_rewind_ll(struct flextree_pos *const fp)
{
    u32 len = 0;
    if (fp->idx > 0) {
        for (u32 i=0; i<fp->idx-1; i++) {
            len += fp->node->leaf_entry.extents[i].len;
        }
    }
    len += fp->diff;
    fp->loff -= len;
    fp->idx = 0;
    fp->diff = 0;
}

inline unsigned long flextree_pos_get_poff_ll(const struct flextree_pos *const fp)
{
    debug_assert(fp->node);
    const struct flextree_extent *const fi = &fp->node->leaf_entry.extents[fp->idx];
    return fi->poff + fp->diff;
}

inline unsigned long flextree_pos_get_loff_ll(const struct flextree_pos *const fp)
{
    return fp->loff;
}

inline int flextree_pos_valid_ll(const struct flextree_pos *const fp)
{
    return fp->node ? 1 : 0;
}

int flextree_pos_get_tag_ll(const struct flextree_pos *const fp, u16 *const tag)
{
    if (fp->diff != 0) {
        // not the beginning of an extent
        return -1;
    }
    *tag = fp->node->leaf_entry.extents[fp->idx].tag;
    return 0;
}

// }}} low-level operations

// high-level operations {{{

void flextree_print(const struct flextree *const tree)
{
    generic_printf("*** flextree ***\n");
    generic_printf("path %s\n", tree->path);
    generic_printf("version %lu\n", tree->version);
    generic_printf("node_count %lu\n", tree->node_count);
    generic_printf("max_node_id %lu\n", tree->max_node_id);
    generic_printf("root_id %ld\n", tree->root_id);
    generic_printf("max_loff %lu\n", tree->max_loff);
    flextree_print_node_rec(tree->root);
}

struct flextree *flextree_open(const char* const path, const u32 max_extent_size)
{
    if (max_extent_size > FLEXTREE_MAX_EXTENT_SIZE_LIMIT) {
        return NULL;
    }
    struct flextree *const tree = generic_malloc(sizeof(*tree));
    memset(tree, 0, sizeof(*tree));

    if (!path) {
        tree->in_memory_mode = 1;
    }

    u8 new = 0;
    tree->node_slab = slab_create(sizeof(struct flextree_node), 1lu << 21);
    tree->free_list = flextree_free_list_create();

    if (tree->in_memory_mode) {
        new = 1;
    }
    else {
        if (access(path, F_OK) == -1) {
            new = 1;
            file_type r = generic_mkdir(path, 0755);
            if (r != 0) {
                generic_printf("flextree mkdir failed, exit\n");
                generic_exit(-1);
            }
        }
        tree->path = generic_malloc(sizeof(char) * (strlen(path) + 1));
        strcpy(tree->path, path);
        flextree_persistent_init(tree);
    }

    if (new) {
        tree->version = 0;
        tree->root = flextree_create_leaf_node(tree);
        tree->leaf_head = tree->root;
        tree->root_id = tree->root->id;
        tree->max_loff = 0; //size
        tree->max_extent_size = max_extent_size;
        flextree_sync(tree);
    } else {
        flextree_load(tree);
    }

    return tree;
}

void flextree_close(struct flextree *const tree)
{
    if (!tree->in_memory_mode) {
        flextree_sync(tree);
        generic_close(tree->meta_fd);
        generic_close(tree->node_fd);
    }
    flextree_node_free_rec(tree->root);
    slab_destroy(tree->node_slab);
    flextree_free_list_destroy(tree->free_list);
    generic_free(tree->path);
    generic_free(tree);
}

void flextree_sync(struct flextree *const tree)
{
    // hey, fsync could fail, but if it fails what should we do? idk
    if (tree->in_memory_mode) {
        return;
    }
    tree->version += 1; // increase the version here
    struct flextree_free_list *const tffl = flextree_free_list_create();
    struct flextree_path path;
    path.level = 0;
    flextree_sync_cow_rec(tree->root, &path, tffl);
    int r = generic_fdatasync(tree->node_fd);
    debug_assert(r == 0);
    tree->root_id = tree->root->id;
    flextree_sync_meta(tree);
    flextree_free_list_merge(tree->free_list, tffl);
    flextree_free_list_destroy(tffl);
    (void)r;
}

static int flextree_insert_r(struct flextree *const tree, const u64 loff, const u64 poff, const u32 len, const u16 tag)
{
    if (len == 0) {
        return 0;
    }
    if (len > tree->max_extent_size) {
        return -1;
    }
    if (loff > tree->max_loff) {
        u64 hlen = loff - tree->max_loff;
        u64 hloff = tree->max_loff;
        u64 hpoff = FLEXTREE_HOLE;
        while (hlen != 0) {
            u32 thlen = hlen > tree->max_extent_size ? tree->max_extent_size : (u32)hlen;
            int r = flextree_insert_r(tree, hloff, hpoff, thlen, 0);
            if (r != 0) {
                return -1;
            }
            hlen -= thlen;
            hloff += thlen;
            hpoff += thlen;
        }
    }
    const u8 need_propagate = (loff == tree->max_loff) ? 0 : 1; // is this an append?
    u64 oloff = loff;
    struct flextree_path path;
    path.level = 0;
    struct flextree_node *const node = flextree_find_leaf_node(tree, &path, &oloff);
    flextree_insert_to_leaf_node(node, (u32)oloff, poff, len, tag);
#ifndef FLEXTREE_NAIVE
    if (path.level > 0) {
        flextree_node_rebase(node, &path);
    }
#endif
    node->dirty = 1;
    if (need_propagate) {
        flextree_node_shift_up_propagate(node, &path, (s64)len);
    }
    if (flextree_node_full(node)) {
        flextree_split_leaf_node(node, &path);
    }
    tree->max_loff += len;
    return 0;
}

inline int flextree_insert(struct flextree *const tree, const u64 loff, const u64 poff, const u32 len)
{
    return flextree_insert_r(tree, loff, poff, len, 0);
}

int flextree_delete(struct flextree *const tree, const u64 loff, const u64 len)
{
    if (loff + len > tree->max_loff) {
        return -1;
    }
    u64 olen = len;
    while (olen > 0) {
        u64 tloff = loff;
        struct flextree_path path;
        path.level = 0;
        struct flextree_node *const node = flextree_find_leaf_node(tree, &path, &tloff);
        debug_assert(node->is_leaf);
        const u32 target = flextree_find_pos_in_leaf(node, tloff);
        debug_assert(target < node->count);
        struct flextree_leaf_entry *const le = &node->leaf_entry;
        struct flextree_extent *const curr_extent = &le->extents[target];
        u32 tlen = curr_extent->loff + curr_extent->len - (u32)tloff;
        tlen = tlen > olen ? (u32)olen : tlen;
        debug_assert(tlen > 0);

        u32 shift = 1;
        if (curr_extent->loff == tloff) {
            curr_extent->len -= tlen;
            curr_extent->poff += tlen;
            curr_extent->tag = 0; // the tag is deleted!
            if (curr_extent->len == 0) { // now the extent is empty
                memmove(curr_extent, curr_extent + 1, sizeof(*curr_extent) * (node->count - target - 1));
                node->count--;
                shift = 0;
            }
        }
        else { // maybe need to split
            const u32 tmp = (u32)(tloff - curr_extent->loff); // hey u32 is good
            if (curr_extent->len - tmp == tlen) {// no split for this case
                curr_extent->len -= tlen;
            }
            else {
                struct flextree_extent right = {.loff = (u32)tloff + tlen,
                                                .len = (curr_extent->len - tmp - tlen),
                                                .poff = curr_extent->poff + tmp + tlen,
                                                .tag = 0}; // the right part is new, so no tag
                memmove(curr_extent + 2, curr_extent + 1, sizeof(*curr_extent) * (node->count - target - 1));
                curr_extent->len = tmp;
                *(curr_extent+1) = right;
                node->count++;
            }
        }
        for (u32 i=target+shift; i<node->count; i++) {
            le->extents[i].loff -= tlen;
        }

        node->dirty = 1;
        flextree_node_shift_up_propagate(node, &path, -(s64)tlen);

        olen -= tlen;
        tree->max_loff -= tlen;

        if (flextree_node_full(node)) {
            flextree_split_leaf_node(node, &path);
        }
        else if (flextree_node_empty(node)) {
            flextree_recycle_node(node, &path); // path is invalid then
        }
    }
    return 0;
}

// returns 0 on success, -1 on error (no extent available)
int flextree_set_tag(struct flextree *const tree, const u64 loff, const u16 tag)
{
    if (loff >= tree->max_loff) {
        return -1;
    }
    u64 oloff = loff;
    struct flextree_path path;
    path.level = 0;
    struct flextree_node *const node = flextree_find_leaf_node(tree, &path, &oloff);

    const u32 target = flextree_find_pos_in_leaf(node, oloff);
    struct flextree_leaf_entry *const le = &node->leaf_entry;

    if (target == node->count) {
        debug_assert(0);
        return -1;
    }
    else {
        struct flextree_extent *const curr_extent = &le->extents[target];
        if (curr_extent->loff == oloff) {
            // case 1: at border, no need to split
            curr_extent->tag = tag;
        } else {
            debug_assert(curr_extent->loff < oloff);
            // case 2: in the middle, need to split
            const u32 so = (u32)(oloff - curr_extent->loff);
            memmove(curr_extent + 2, curr_extent + 1, sizeof(*curr_extent) * (node->count - target - 1));
            // callback the split function provided by upper level
            const struct flextree_extent left = {.loff = curr_extent->loff, .len = so,
                                                 .poff = curr_extent->poff, .tag = curr_extent->tag};
            const struct flextree_extent right = {.loff = curr_extent->loff + so, .len = curr_extent->len - so,
                                                  .poff = curr_extent->poff + so, .tag = tag};
            le->extents[target] = left;
            le->extents[target+1] = right;
            node->count += 1;
        }
    }
    node->dirty = 1;
    if (flextree_node_full(node)) {
        flextree_split_leaf_node(node, &path);
    }
    return 0;
}

// returns 0 on success, -1 on no tag found
int flextree_get_tag(const struct flextree *const tree, const u64 loff, u16 *const tag)
{
    if (loff >= tree->max_loff) {
        return -1;
    }
    u64 oloff = loff;
    struct flextree_path path;
    path.level = 0;
    struct flextree_node *const node = flextree_find_leaf_node(tree, &path, &oloff);

    const u32 target = flextree_find_pos_in_leaf(node, oloff);
    struct flextree_leaf_entry *const le = &node->leaf_entry;

    if (target == node->count) {
        debug_assert(0);
        return -1;
    }
    else {
        struct flextree_extent *const curr_extent = &le->extents[target];
        if (curr_extent->loff == oloff && curr_extent->tag != 0) {
            // case 1: at border, so there is a tag
            *tag = curr_extent->tag;
        } else {
            // case 2: in the middle, no tag
            return -1;
        }
    }
    return 0;
}

inline int flextree_insert_wtag(
        struct flextree *const tree, const u64 loff, const u64 poff, const u32 len, const u16 tag)
{
    return flextree_insert_r(tree, loff, poff, len, tag);
}

inline int flextree_pdelete(struct flextree *const tree, const u64 loff)
{
    return flextree_delete(tree, loff, 1);
}

unsigned long flextree_pquery(const struct flextree *const tree, const u64 loff)
{
    if (loff >= tree->max_loff) {
        return -1lu;
    }
    struct flextree_path path;
    path.level = 0;
    u64 oloff = loff;
    const struct flextree_node *const node = flextree_find_leaf_node(tree, &path, &oloff);
    const struct flextree_leaf_entry *const le = &node->leaf_entry;
    const u32 target = flextree_find_pos_in_leaf(node, oloff);
    if (target == node->count) {
        return -1lu;
    }
    const struct flextree_extent *const extent = &le->extents[target];
    if (extent->loff <= oloff && extent->loff + extent->len > oloff) {
        return (extent->poff + oloff - extent->loff);
    }
    return -1lu;
}

inline struct flextree_query_result *flextree_query(const struct flextree *const tree, const u64 loff, const u64 len)
{
    if (loff + len > tree->max_loff) {
        return NULL;
    }
    const u64 count = flextree_range_count(tree, loff, len);
    if (count == 0) {
        return NULL;
    }
    struct flextree_query_result *const ret =
        generic_malloc(sizeof(*ret) + count * sizeof(((struct flextree_query_result *)0)->v[0]));
    return flextree_query_wbuf(tree, loff, len, ret);
}

struct flextree_query_result *flextree_query_wbuf(
        const struct flextree *const tree, const u64 loff, const u64 len, struct flextree_query_result *const rr)
{
    if (loff + len > tree->max_loff) {
        return NULL;
    }
    struct flextree_pos fp = flextree_pos_get_ll(tree, loff);
    rr->loff = loff;
    rr->len = len;
    u64 i = 0;
    u64 tlen = len;
    while (tlen > 0) {
        u64 remain = fp.node->leaf_entry.extents[fp.idx].len - fp.diff;
        u64 step = remain > tlen ? tlen : remain;
        rr->v[i].poff = flextree_pos_get_poff_ll(&fp);
        rr->v[i].len = step;
        tlen -= step;
        flextree_pos_forward_ll(&fp, step);
        i++;
    }
    rr->count = i;
    return rr;
}

// }}} high-level operations

// }}} flextree

// brute_force {{{

#define BF_INIT_CAP 1024

void brute_force_print(const struct brute_force *const bf)
{
    generic_printf("*** Print struct brute_force ***\n");
    generic_printf("Total extents %lu\n", bf->count);
    for (u64 i=0; i<bf->count; i++) {
        generic_printf("BF %lu: loff %lu poff %ld len %u\n",
                i, bf->extents[i].loff, bf->extents[i].poff & FLEXTREE_POFF_MASK, bf->extents[i].len);
    }
}

struct brute_force *brute_force_open(const u32 max_extent_size)
{
    struct brute_force *const bf = generic_malloc(sizeof(struct brute_force));
    bf->count = 0;
    bf->cap = BF_INIT_CAP;
    bf->max_loff = 0;
    bf->max_extent_size = max_extent_size;
    bf->extents = generic_malloc(sizeof(struct brute_force_extent) * BF_INIT_CAP);
    return bf;
}

void brute_force_close(struct brute_force *const bf)
{
    generic_free(bf->extents);
    generic_free(bf);
}

static inline u8 brute_force_full(const struct brute_force *const bf)
{
    return (bf->cap <= bf->count + 3) ? 1 : 0;
}

static void brute_force_extend(struct brute_force *const bf)
{
    bf->extents =
        (struct brute_force_extent *)generic_realloc(bf->extents, bf->cap * 10 * sizeof(struct brute_force_extent));
    bf->cap *= 10;
}

static u64 brute_force_find_pos(const struct brute_force *const bf, const u64 loff)
{
    u64 target = 0; // find position
    u64 hi = bf->count;
    u64 lo = 0;
    // binary search
    while (lo + 1 < hi) {
        target = (lo + hi) / 2;
        const struct brute_force_extent *const curr_extent = &bf->extents[target];
        if (curr_extent->loff <= loff) {
            lo = target;
        }
        else {
            hi = target;
        }
    }
    target = lo;
    while (target < bf->count) {
        const struct brute_force_extent *const curr_extent = &bf->extents[target];
        if ((curr_extent->loff <= loff && curr_extent->loff + curr_extent->len > loff) ||
            curr_extent->loff > loff) {
            break;
        }
        else {
            target++;
        }
    }
    return target;
}

// to check (loff, poff) is continous with extent
static u8 brute_force_extent_sequential(const struct brute_force_extent *const extent,
        const u64 max_extent_size, const u64 loff, const u64 poff, const u64 len)
{
    u8 ret = 0;
    if (extent->poff + extent->len == poff && extent->loff + extent->len == loff &&
            extent->len + len <= max_extent_size) {
        ret = 1;
    }
    return ret;
}

static int brute_force_insert_r(
        struct brute_force *const bf, const u64 loff, const u64 poff, const u32 len, const u16 tag)
{
    if (len == 0) {
        return 0;
    }
    if (len > bf->max_extent_size) {
        return -1;
    }
    if (loff > bf->max_loff) {
        u64 hlen = loff - bf->max_loff;
        u64 hloff = bf->max_loff;
        u64 hpoff = FLEXTREE_HOLE;
        while (hlen != 0) {
            const u32 thlen = hlen > bf->max_extent_size ? bf->max_extent_size : (u32)hlen;
            const int r = brute_force_insert_r(bf, hloff, hpoff, thlen, 0);
            if (r != 0) {
                return -1;
            }
            hlen -= thlen;
            hloff += thlen;
            hpoff += thlen;
        }
    }
    const struct brute_force_extent t = {.loff = loff, .len = len,
                                         .poff = poff & FLEXTREE_POFF_MASK, .tag = tag};
    if (brute_force_full(bf)) {
        brute_force_extend(bf);
    }

    const u64 target = brute_force_find_pos(bf, loff);

    u32 shift = 1; // since which idx to add current len
    if (target == bf->count) {
        if (target > 0 && tag == 0 && brute_force_extent_sequential(&bf->extents[target-1],
                    bf->max_extent_size, loff, poff, len)) {
            bf->extents[target-1].len += len;
        } else {
            bf->extents[bf->count++] = t;
        }
    } else {
        struct brute_force_extent *const curr_extent = &bf->extents[target];
        if (curr_extent->loff == loff) { //insert directly
            if (target > 0 && tag == 0 && brute_force_extent_sequential(curr_extent-1,
                        bf->max_extent_size, loff, poff, len)) {
                // sequential
                (curr_extent-1)->len += len;
                shift = 0;
            } else {
                memmove(curr_extent + 1, curr_extent, sizeof(struct brute_force_extent) * (bf->count - target));
                *curr_extent = t;
                bf->count++;
            }
        } else { // split!
            debug_assert(curr_extent->loff < loff);
            shift = 2;
            const u32 so = (u32)(loff - curr_extent->loff);
            memmove(curr_extent + 3, curr_extent + 1, sizeof(struct brute_force_extent)*(bf->count - target - 1));
            const struct brute_force_extent left = {.loff = curr_extent->loff, .poff = curr_extent->poff,
                                                    .len = so, .tag = curr_extent->tag};
            const struct brute_force_extent right = {.loff = curr_extent->loff + so, .poff = curr_extent->poff + so,
                                                     .len = curr_extent->len - so, .tag = 0};
            bf->extents[target] = left;
            bf->extents[target+2] = right;
            bf->extents[target+1] = t;
            bf->count += 2;
        }
    }
    for (u64 i=target+shift; i<bf->count; i++) {
        bf->extents[i].loff += len;
    }
    bf->max_loff += len;
    return 0;
}

int brute_force_insert(struct brute_force *const bf, const u64 loff, const u64 poff, const u32 len)
{
    return brute_force_insert_r(bf, loff, poff, len, 0);
}

int brute_force_insert_wtag(struct brute_force *const bf, const u64 loff, const u64 poff, const u32 len, const u16 tag)
{
    return brute_force_insert_r(bf, loff, poff, len, tag);
}

u64 brute_force_pquery(const struct brute_force *const bf, const u64 loff)
{
    if (loff >= bf->max_loff) {
        return -1lu;
    }
    const u64 target = brute_force_find_pos(bf, loff);
    if (target >= bf->count) {
        return -1lu;
    }
    const struct brute_force_extent * extent = &bf->extents[target];
    if (extent->loff <= loff && extent->loff + extent->len > loff){
        return extent->poff + loff - extent->loff;
    }
    return -1lu;
}

int brute_force_pdelete(struct brute_force *const bf, const u64 loff)
{
    return brute_force_delete(bf, loff, 1);
}

static u64 brute_force_range_count(const struct brute_force *const bf, const u64 loff, const u64 len)
{
    u64 ret = 0;
    u64 oloff = loff;
    u64 olen = len;
    u64 target = brute_force_find_pos(bf, oloff);
    while (olen > 0) {
        const struct brute_force_extent *const curr_extent = &bf->extents[target];
        u64 tlen = curr_extent->loff + curr_extent->len - oloff;
        tlen = tlen > olen ? olen : tlen;
        oloff += tlen;
        olen -= tlen;
        ret++;
        target++;
    }
    return ret;
}

inline struct flextree_query_result *brute_force_query(
        const struct brute_force *const bf, const u64 loff, const u64 len)
{
    if (loff + len > bf->max_loff) {
        return NULL;
    }
    const u64 count = brute_force_range_count(bf, loff, len);
    if (count == 0) {
        return NULL;
    }
    struct flextree_query_result *ret = generic_malloc(
            sizeof(*ret) + count * sizeof(((struct flextree_query_result *)0)->v[0]));
    return brute_force_query_wbuf(bf, loff, len, ret);
}

struct flextree_query_result *brute_force_query_wbuf(
        const struct brute_force *const bf, const u64 loff, const u64 len, struct flextree_query_result *const rr)
{
    if (loff + len > bf->max_loff) {
        return NULL;
    }
    rr->loff = loff;
    rr->len = len;
    rr->count = 0;
    u64 i = 0;
    u64 oloff = loff;
    u64 olen = len;
    u64 target = brute_force_find_pos(bf, oloff);
    while (olen > 0) {
        struct brute_force_extent *curr_extent = &bf->extents[target];
        if (target == bf->count || curr_extent->loff > oloff || curr_extent->loff + curr_extent->len <= oloff) {
            return NULL;
        }
        u64 tlen = curr_extent->loff + curr_extent->len - oloff;
        tlen = tlen > olen ? olen : tlen;
        rr->v[i].poff = curr_extent->poff + (oloff - curr_extent->loff);
        rr->v[i].len = tlen;
        oloff += tlen;
        olen -= tlen;
        i++;
        target++;
    }
    rr->count = i;
    return rr;
}

int brute_force_delete(
        struct brute_force *const bf, const u64 loff, const u64 len)
{
    if (brute_force_full(bf)) {
        brute_force_extend(bf);
    }
    if (loff + len > bf->max_loff) {
        return -1;
    }
    u64 olen = len;
    while (olen > 0) {
        const u64 target = brute_force_find_pos(bf, loff);
        struct brute_force_extent *curr_extent = &bf->extents[target];
        u32 tlen = (u32)(curr_extent->loff + curr_extent->len - loff);
        tlen = tlen > olen ? (u32)olen : tlen;
        debug_assert(tlen > 0);

        u32 shift = 1;
        if (curr_extent->loff == loff) {
            curr_extent->len -= tlen;
            curr_extent->poff += tlen;
            curr_extent->tag = 0;
            if (curr_extent->len == 0) { // now the extent is empty
                memmove(curr_extent, curr_extent + 1, sizeof(*curr_extent) * (bf->count - target - 1));
                bf->count--;
                shift = 0;
            }
        }
        else { // maybe need to split
            const u32 tmp = (u32)(loff - curr_extent->loff);
            if (curr_extent->len - tmp == tlen) {// no split for this case
                curr_extent->len -= tlen;
            }
            else {
                const struct brute_force_extent right = {.loff = loff + tlen, .poff = curr_extent->poff + tmp + tlen,
                                                         .len = curr_extent->len - tmp - tlen, .tag = 0};
                debug_assert(right.len > 0);
                memmove(curr_extent + 2, curr_extent + 1, sizeof(*curr_extent) * (bf->count - target - 1));
                curr_extent->len = tmp;
                *(curr_extent+1) = right;
                bf->count++;
            }
        }
        for (u64 i=target+shift; i<bf->count; i++) {
            bf->extents[i].loff -= tlen;
        }

        olen -= tlen;
    }
    bf->max_loff -= len;
    return 0;
}

int brute_force_set_tag(struct brute_force *const bf, const u64 loff, const u16 tag)
{
    if (loff >= bf->max_loff) {
        return -1;
    }
    if (brute_force_full(bf)) {
        brute_force_extend(bf);
    }
    const u64 target = brute_force_find_pos(bf, loff);

    if (target == bf->count) {
        debug_assert(0);
        return -1;
    } else {
        struct brute_force_extent *const curr_extent = &bf->extents[target];
        if (curr_extent->loff == loff) {
            curr_extent->tag = tag;
        } else { // split!
            const u32 so = (u32)(loff - curr_extent->loff);
            memmove(curr_extent + 2, curr_extent + 1, sizeof(struct brute_force_extent)*(bf->count - target - 1));
            const struct brute_force_extent left = {.loff = curr_extent->loff, .poff = curr_extent->poff,
                                                    .len = so, .tag = curr_extent->tag};
            const struct brute_force_extent right = {.loff = curr_extent->loff + so, .poff = curr_extent->poff + so,
                                                     .len = curr_extent->len - so, .tag = tag};
            bf->extents[target] = left;
            bf->extents[target+1] = right;
            bf->count += 1;
        }
    }
    return 0;
}

int brute_force_get_tag(const struct brute_force*const bf, const u64 loff, u16 *const tag)
{
    if (loff >= bf->max_loff) {
        return -1;
    }
    const u64 target = brute_force_find_pos(bf, loff);
    if (target == bf->count) {
        debug_assert(0);
        return -1;
    } else {
        struct brute_force_extent *const curr_extent = &bf->extents[target];
        if (curr_extent->loff == loff && curr_extent->tag != 0) {
            *tag = curr_extent->tag;
        } else {
            return -1;
        }
    }
    return 0;
}

// }}} brute_force
