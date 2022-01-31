#include "flexdb.h"

// sparse index tree {{{

static struct flexdb_tree_node *flexdb_tree_create_node(
        struct flexdb_tree *const tree, struct flexdb_tree_node *const parent)
{
    struct flexdb_tree_node *const node = slab_alloc_unsafe(tree->node_slab);
    if (!node) {
        return node;
    }
    memset(node, 0, sizeof(*node));
    node->parent = parent;
    node->count = 0;
    node->tree = tree;
    return node;
}

static inline struct flexdb_tree_node *flexdb_tree_create_leaf_node(
        struct flexdb_tree *const tree, struct flexdb_tree_node *const parent)
{
    struct flexdb_tree_node *const node = flexdb_tree_create_node(tree, parent);
    node->is_leaf = 1;
    return node;
}

static inline struct flexdb_tree_node *flexdb_tree_create_internal_node(
        struct flexdb_tree *const tree, struct flexdb_tree_node *const parent)
{
    // note: parent_id is not initialized here
    return flexdb_tree_create_node(tree, parent);
}

static void flexdb_tree_free_node_rec(struct flexdb_tree_node *const node)
{
    debug_assert(node);
    if (!node->is_leaf) {
        struct flexdb_tree_internal_entry *const ie = &node->internal_entry;
        for (u32 i=0; i<node->count+1; ++i) {
            flexdb_tree_free_node_rec(ie->children[i].node);
        }
        for (u32 i=0; i<node->count; i++) {
            free(ie->pivots[i]);
        }
    }
    else {
        struct flexdb_tree_leaf_entry *const le = &node->leaf_entry;
        for (u32 i=0; i<node->count; i++) {
            free(le->anchors[i]->key);
            slab_free_unsafe(node->tree->anchor_slab, le->anchors[i]);
        }
    }
    slab_free_unsafe(node->tree->node_slab, node);
}

static void flexdb_tree_destroy(struct flexdb_tree *const tree)
{
    flexdb_tree_free_node_rec(tree->root);
    slab_destroy(tree->node_slab);
    slab_destroy(tree->anchor_slab);
    free(tree);
}

static inline u8 flexdb_tree_node_full(const struct flexdb_tree_node *const node)
{
    const u64 cap = node->is_leaf ? FLEXDB_TREE_LEAF_CAP : FLEXDB_TREE_INTERNAL_CAP;
    return (cap - 1 <= node->count) ? 1 : 0;
}

static inline u8 flexdb_tree_node_empty(const struct flexdb_tree_node *const node)
{
    return (node->count == 0) ? 1 : 0;
}

static void flexdb_tree_node_rebase(struct flexdb_tree_node *const node)
{
    debug_assert(node->is_leaf);
    struct flexdb_tree_leaf_entry *const le = &node->leaf_entry;
    if (le->anchors[node->count-1]->loff >= (u32)(~0) >> 2) {
        const u64 new_base = le->anchors[0]->loff;
        debug_assert(new_base != 0);
        const u32 p_idx = node->parent_id;
        struct flexdb_tree_node *const parent = node->parent;
        parent->internal_entry.children[p_idx].shift += new_base;
        for (u32 i=0; i<node->count; i++) {
            le->anchors[i]->loff -= new_base;
        }
    }
}

static void flexdb_tree_link_two_nodes(struct flexdb_tree_node *const node1, struct flexdb_tree_node *const node2)
{
    struct flexdb_tree_leaf_entry *const le1 = &node1->leaf_entry;
    struct flexdb_tree_leaf_entry *const le2 = &node2->leaf_entry;
    le2->prev = node1;
    le2->next = le1->next;
    le1->next = node2;
    if (le2->next) {
        le2->next->leaf_entry.prev = node2;
    }
}

static void flexdb_tree_node_shift_apply(struct flexdb_tree_node *const node, const s64 shift)
{
    // only call this when confident no overflow will happen
    if (node->is_leaf) {
        struct flexdb_tree_leaf_entry *const le = &node->leaf_entry;
        debug_assert((u32)(le->anchors[node->count-1]->loff + (u64)shift) <= ~(u32)0); // always true
        for (u32 i=0; i<node->count; i++) {
            le->anchors[i]->loff = (u32)(le->anchors[i]->loff + (u64)shift);
        }
    } else {
        struct flexdb_tree_internal_entry *const ie = &node->internal_entry;
        for (u32 i=0; i<node->count+1; i++) {
            ie->children[i].shift += shift;
        }
    }
}

static void flexdb_tree_split_internal_node(struct flexdb_tree_node *const node)
{
    debug_assert(!node->is_leaf);
    struct flexdb_tree_node *const node1 = node;
    struct flexdb_tree_node *const node2 = flexdb_tree_create_internal_node(node1->tree, node1->parent);
    struct flexdb_tree_internal_entry *const ie1 = &node1->internal_entry;
    struct flexdb_tree_internal_entry *const ie2 = &node2->internal_entry;
    const u32 count = (node1->count + 1) / 2;
    struct kv *const new_base = ie1->pivots[count];
    //manipulate node2
    node2->count = node1->count - count - 1;
    memmove(ie2->pivots, &ie1->pivots[count+1], node2->count * sizeof(ie2->pivots[0]));
    memmove(ie2->children, &ie1->children[count+1], (node2->count+1) * sizeof(ie2->children[0]));
    //manipulate node1
    node1->count = count;
    // if no parent, create one and set as root
    struct flexdb_tree_node *parent = node1->parent;
    if (!node1->parent) {
        parent = flexdb_tree_create_internal_node(node1->tree, node1->parent);
        node1->parent = parent;
        node2->parent = parent;
        debug_assert(parent->tree);
        parent->tree->root = parent;
    }
    // insert one index entry in parent
    struct flexdb_tree_internal_entry *const ie = &parent->internal_entry;
    if (parent->count == 0) {
        ie->children[0].node = node1;
        ie->children[0].shift = 0;
        ie->children[0+1].node = node2;
        ie->children[0+1].shift = 0;
        ie->pivots[0] = new_base;
        parent->count = 1;
        node1->parent_id = 0;
        node2->parent_id = 1;
    }
    else {
        const u32 p_idx = node1->parent_id;
        const s64 orig_shift = ie->children[p_idx].shift;
        memmove(&ie->pivots[p_idx+1], &ie->pivots[p_idx], (parent->count - p_idx) * sizeof(ie->pivots[0]));
        memmove(&ie->children[p_idx+2], &ie->children[p_idx+1], (parent->count - p_idx) * sizeof(ie->children[0]));
        ie->children[p_idx+1].node = node2;
        ie->children[p_idx+1].shift = orig_shift;
        ie->pivots[p_idx] = new_base;
        parent->count += 1;
        node2->parent_id = node1->parent_id;
        for (u32 i=p_idx+1; i<parent->count+1; i++) {
            ie->children[i].node->parent_id++;
        }
    }
    for (u32 i=0; i<node2->count+1; ++i) {
        ie2->children[i].node->parent_id = i;
        ie2->children[i].node->parent = node2;
    }
    if (flexdb_tree_node_full(parent)) {
        flexdb_tree_split_internal_node(parent);
    }
}

static void flexdb_tree_split_leaf_node(struct flexdb_tree_node *const node)
{
    debug_assert(node->is_leaf);
    struct flexdb_tree_node *const node1 = node;
    struct flexdb_tree_node *const node2 = flexdb_tree_create_leaf_node(node1->tree, node1->parent);
    flexdb_tree_link_two_nodes(node1, node2);
    struct flexdb_tree_leaf_entry *const le1 = &node1->leaf_entry;
    struct flexdb_tree_leaf_entry *const le2 = &node2->leaf_entry;
    const u32 count = (node1->count + 1) / 2;
    // manipulate node2
    node2->count = node1->count - count;
    memmove(le2->anchors, &le1->anchors[count], node2->count * sizeof(le2->anchors[0]));
    // manipulate node1
    node1->count = count;
    // if no parent, create one and set as root
    struct flexdb_tree_node *parent = node1->parent;
    if (!parent) {
        parent = flexdb_tree_create_internal_node(node1->tree, node1->parent);
        node1->parent = parent;
        node2->parent = parent;
        parent->tree->root = parent;
    }
    // insert one index entry in parent
    struct flexdb_tree_internal_entry *const ie = &parent->internal_entry;
    if (parent->count == 0) {
        ie->children[0].node = node1;
        ie->children[0].shift = 0;
        ie->children[0+1].node = node2;
        ie->children[0+1].shift = 0;
        ie->pivots[0] = kv_dup_key(node2->leaf_entry.anchors[0]->key);
        node1->parent_id = 0;
        node2->parent_id = 1;
        parent->count = 1;
    }
    else {
        const u32 p_idx = node1->parent_id;
        const s64 orig_shift = ie->children[p_idx].shift;
        memmove(&ie->pivots[p_idx+1], &ie->pivots[p_idx], (parent->count - p_idx) * sizeof(ie->pivots[0]));
        memmove(&ie->children[p_idx+2], &ie->children[p_idx+1], (parent->count - p_idx) * sizeof(ie->children[0]));
        ie->children[p_idx+1].node = node2;
        ie->children[p_idx+1].shift = orig_shift;
        ie->pivots[p_idx] = kv_dup_key(node2->leaf_entry.anchors[0]->key);
        node2->parent_id = node1->parent_id;
        parent->count += 1;
        for (u32 i=p_idx+1; i<parent->count+1; i++) {
            ie->children[i].node->parent_id++;
        }
    }
    if (node->parent->count > 1) {
        flexdb_tree_node_rebase(node1);
        flexdb_tree_node_rebase(node2);
    }

    if (flexdb_tree_node_full(parent)) {
        flexdb_tree_split_internal_node(parent);
    }
}

static void flexdb_tree_node_shift_up_propagate(struct flexdb_tree_node_handler *const nh, const s64 shift)
{
    struct flexdb_tree_node *node = nh->node;
    const u32 target = nh->idx;
    debug_assert(node->is_leaf);
    for (u32 i=target+1; i<node->count; i++) { // cannot use SIMD
        node->leaf_entry.anchors[i]->loff += shift;
    }

    while (node->parent) {
        const u32 p_idx = node->parent_id;
        node = node->parent;
        debug_assert(!node->is_leaf);
        struct flexdb_tree_internal_entry *const ie = &node->internal_entry;
        for (u32 i=p_idx; i<node->count; i++) {
            ie->children[i+1].shift += shift;
        }
    }
}

static u32 flexdb_tree_find_pos_in_leaf_le(const struct flexdb_tree_node *const node, const struct kref *const key)
{
    debug_assert(node->is_leaf);
    u32 target = 0; // find position
    u32 hi = node->count;
    u32 lo = 0;
    const struct flexdb_tree_leaf_entry *const le = &node->leaf_entry;
    // binary search
    // while ((lo + 3) < hi) {
    //     target = (lo + hi) >> 1;
    //     const struct kv * const curr = le->anchors[target]->key;
    //     cpu_prefetch0(curr);
    //     cpu_prefetch0(&le->anchors[(lo+target)>>1]);
    //     cpu_prefetch0(&le->anchors[(target+hi)>>1]);
    //     const int cmp = kref_kv_compare(key, curr);
    //     if (cmp > 0) {
    //         lo = target;
    //     }
    //     else if (cmp < 0) {
    //         hi = target;
    //     }
    //     else {
    //         return target;
    //     }
    // }
    while ((lo + 1) < hi) {
        target = (lo + hi) >> 1;
        const int cmp = kref_kv_compare(key, le->anchors[target]->key);
        if (cmp > 0) {
            lo = target;
        }
        else if (cmp < 0) {
            hi = target;
        }
        else {
            return target;
        }
    }
    return lo;
}

static u32 flexdb_tree_find_pos_in_internal(const struct flexdb_tree_node *const node, const struct kref *const key)
{
    debug_assert(!node->is_leaf);
    u32 hi = node->count;
    u32 lo = 0;
    const struct flexdb_tree_internal_entry *const ie = &node->internal_entry;
    // binary search
    // while ((lo + 2) < hi) {
    //     const u32 target = (lo + hi) >> 1;
    //     const struct kv * const curr = ie->pivots[target];
    //     //cpu_prefetch0(curr+4); // the 2nd line ?
    //     cpu_prefetch0(ie->pivots[(lo+target)>>1]);
    //     cpu_prefetch0(ie->pivots[(target+1+hi)>>1]);
    //     const int cmp = kref_kv_compare(key, curr);
    //     if (cmp >= 0) {
    //         lo = target + 1;
    //     }
    //     else {
    //         hi = target;
    //     }
    // }

    while (lo < hi) {
        const u32 target = (lo + hi) >> 1;
        const int cmp = kref_kv_compare(key, ie->pivots[target]);
        if (cmp >= 0) {
            lo = target + 1;
        }
        else {
            hi = target;
        }
    }

    return lo;
}

static struct flexdb_tree *flexdb_tree_create(struct flexdb *const db)
{
    struct flexdb_tree *const tree = malloc(sizeof(*tree));
    debug_assert(tree);
    tree->db = db;

    // slabs
    tree->node_slab = slab_create(sizeof(struct flexdb_tree_node), 2lu << 20);
    tree->anchor_slab = slab_create(sizeof(struct flexdb_tree_anchor), 2lu << 20);
    if (tree->node_slab == NULL || tree->anchor_slab == NULL) {
        printf("node_slab or anchor_slab alloc failed\n");
        exit(1);
    }

    tree->root = flexdb_tree_create_leaf_node(tree, NULL);
    tree->leaf_head = tree->root;
    debug_assert(tree->root);

    // insert smallest key
    struct flexdb_tree_anchor *const sanchor = slab_alloc_unsafe(tree->anchor_slab);
    *sanchor= (struct flexdb_tree_anchor)
              {.key = kv_dup_key(kv_null()), .loff = 0, .psize = 0, .unsorted = 0, .cache_entry = NULL};
    kv_update_hash(sanchor->key);
    tree->root->leaf_entry.anchors[0] = sanchor;
    tree->root->count++;

    return tree;
}

static void flexdb_tree_find_anchor_pos(const struct flexdb_tree *const tree,
        const struct kref *const key, struct flexdb_tree_node_handler * const nh)
{
    s64 shift = 0;
    struct flexdb_tree_node *node = tree->root;
    while (!node->is_leaf) {
        const u32 target = flexdb_tree_find_pos_in_internal(node, key);
        const struct flexdb_tree_internal_entry *const ie = &node->internal_entry;
        shift += ie->children[target].shift;
        node = ie->children[target].node;
    }
    debug_assert(node->is_leaf);
    nh->node = node;
    nh->shift = shift;
    nh->idx = flexdb_tree_find_pos_in_leaf_le(node, key);
}

static struct flexdb_tree_anchor *flexdb_tree_handler_insert(
        const struct flexdb_tree_node_handler *const nh, struct kv *const key,
        const u64 loff, const u32 psize)
{
    struct flexdb_tree_node *const node = nh->node;
    debug_assert(node->is_leaf);
    struct flexdb_tree_anchor *const t = slab_alloc_unsafe(nh->node->tree->anchor_slab);
    *t = (struct flexdb_tree_anchor)
         {.key = key, .loff = (u32)(loff - (u64)nh->shift), .psize = psize, .unsorted = 0, .cache_entry = NULL};
    // find insertion position
    const u32 target = nh->idx;
    struct flexdb_tree_leaf_entry *const le = &node->leaf_entry;

    if (target == node->count) {
        le->anchors[node->count++] = t;
    }
    else {
        struct flexdb_tree_anchor **const curr_anchor= &le->anchors[target];
        memmove(curr_anchor+ 1, curr_anchor, sizeof(*curr_anchor) * (node->count - target));
        *curr_anchor= t;
        node->count++;
    }
    if (flexdb_tree_node_full(node)) {
        flexdb_tree_split_leaf_node(node);
    }
    return t;
}

static void flexdb_tree_node_update_smallest_key(struct flexdb_tree_node *const since, const struct kv *const key)
{
    u32 p_idx = since->parent_id;
    struct flexdb_tree_node *tnode = since->parent;
    while (tnode) {
        if (p_idx == 0) {
            p_idx = tnode->parent_id;
            tnode = tnode->parent;
        } else {
            break;
        }
    }
    if (tnode) {
        // which means the current node is not the leftmost one
        free(tnode->internal_entry.pivots[p_idx-1]);
        tnode->internal_entry.pivots[p_idx-1] = kv_dup_key(key);
    }
}

static void flexdb_tree_node_recycle_linked_list(const struct flexdb_tree_node *const node)
{
    debug_assert(node->is_leaf);
    debug_assert(node->tree->root != node); // should not be root
    const struct flexdb_tree_leaf_entry *const le = &node->leaf_entry;
    struct flexdb_tree_node *const prev = le->prev;
    struct flexdb_tree_node *const next = le->next;
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

static struct kv *flexdb_tree_node_find_smallest_key(struct flexdb_tree_node *const node)
{
    struct flexdb_tree_node *tnode = node;
    while (!tnode->is_leaf) {
        debug_assert(tnode->count > 0);
        tnode = tnode->internal_entry.children[0].node;
    }
    debug_assert(tnode->count > 0);
    return tnode->leaf_entry.anchors[0]->key;
}

static void flexdb_tree_recycle_node(struct flexdb_tree_node *const node)
{
    debug_assert(node->count == 0);
    struct flexdb_tree_node *const parent = node->parent;
    const u32 p_idx = node->parent_id;
    u8 parent_exist = 0;
    if (parent) {
        parent_exist = 1;
    }
    if (node->tree->root == node) {
        // case 1: node is the root
        debug_assert(0); // this should not happen
    } else if (parent->count == 1) {
        // case 2: only one pivot in parent
        debug_assert(p_idx <= 1);
        const u32 s_idx = p_idx == 0 ? 1 : 0;
        const s64 s_shift = parent->internal_entry.children[s_idx].shift;
        struct flexdb_tree_node *const s_node = parent->internal_entry.children[s_idx].node;
        // pre-process the linked list if node is leaf
        if (node->is_leaf) {
            flexdb_tree_node_recycle_linked_list(node);
        }
        // free nodes
        slab_free_unsafe(node->tree->node_slab, node);
        // do recycling
        if (s_node->tree->root == parent) {
            free(parent->internal_entry.pivots[0]);
            slab_free_unsafe(node->tree->node_slab, parent);
            // case 2.1: parent is root
            // no more shift pointers, so need to apply
            flexdb_tree_node_shift_apply(s_node, s_shift);
            // set new root to sibling
            s_node->tree->root = s_node;
            s_node->parent = NULL;
            s_node->parent_id = 0;
            debug_assert(!s_node->is_leaf || s_node->tree->leaf_head == s_node);
        } else {
            // case 2.2: parent is not root
            debug_assert(node->parent->parent);
            struct flexdb_tree_node *const gparent = parent->parent;
            const u32 gp_idx = parent->parent_id;
            gparent->internal_entry.children[gp_idx].node = s_node;
            gparent->internal_entry.children[gp_idx].shift += s_shift;
            s_node->parent = gparent;
            s_node->parent_id = gp_idx;

            free(parent->internal_entry.pivots[0]);
            slab_free_unsafe(node->tree->node_slab, parent);
            // now the gparent's child points to s_node
            // if the sibling is the right one, we need to update the pivots
            struct kv *new_pivot = NULL;
            if (s_idx == 1) {
                debug_assert(s_node->count > 0);
                new_pivot = flexdb_tree_node_find_smallest_key(s_node);
            }
            if (new_pivot) {
                if (gp_idx == 0) {
                    flexdb_tree_node_update_smallest_key(gparent, new_pivot);
                } else {
                    free(gparent->internal_entry.pivots[gp_idx-1]);
                    gparent->internal_entry.pivots[gp_idx-1] = kv_dup_key(new_pivot);
                }
            }
        }
        parent_exist = 0;
    } else {
        // case 3: normal case, remove one pivot from parent..
        if (node->is_leaf) {
            flexdb_tree_node_recycle_linked_list(node);
        }
        slab_free_unsafe(node->tree->node_slab, node);
        struct flexdb_tree_internal_entry *const ie = &parent->internal_entry;
        if (p_idx == 0) {
            // case 3.1: first child
            free(ie->pivots[0]);
            memmove(ie->pivots, &ie->pivots[1],
                    (parent->count-1) * sizeof(ie->pivots[0]));
            memmove(ie->children, &ie->children[1],
                    (parent->count) * sizeof(ie->children[0]));
            parent->count--;
            for (u32 i=0; i<parent->count+1; i++) {
                debug_assert(ie->children[i].node->parent_id > 0);
                ie->children[i].node->parent_id--;
            }
            // smilarly, needs to update the pivot in this case
            debug_assert(ie->children[0].node->count > 0);
            struct kv *new_pivot = flexdb_tree_node_find_smallest_key(parent);
            flexdb_tree_node_update_smallest_key(parent, new_pivot);
        } else {
            // case 3.2: not first child, quite similar
            // but no need to update the key here
            free(ie->pivots[p_idx-1]);
            memmove(&ie->pivots[p_idx-1], &ie->pivots[p_idx],
                    (parent->count - p_idx) * sizeof(ie->pivots[0]));
            memmove(&ie->children[p_idx], &ie->children[p_idx+1],
                    (parent->count - p_idx + 1) * sizeof(ie->children[0]));
            parent->count--;
            for (u32 i=p_idx; i<parent->count+1; i++) {
                debug_assert(ie->children[i].node->parent_id > 0);
                ie->children[i].node->parent_id--;
            }
        }
    }

    if (parent_exist == 1 && flexdb_tree_node_empty(parent)) {
        flexdb_tree_recycle_node(parent);
    }
}

// }}} sparse index tree

// {{{ cache

#define FLEXDB_CACHE_ENTRY_FIND_EQ (1u << 31)

static struct flexdb_cache *flexdb_cache_create(struct flexdb *const db, const u64 cache_cap_mb)
{
    struct flexdb_cache *const cache = malloc(sizeof(*cache));
    cache->cap = cache_cap_mb * (1lu << 20);
    cache->db = db;

    // init partitions
    for (u32 i=0; i<FLEXDB_CACHE_PARTITION_COUNT; i++) {
        struct flexdb_cache_partition *const p = &cache->partitions[i];
        p->cap = cache->cap / FLEXDB_CACHE_PARTITION_COUNT;
        p->size = 0;
        p->entry_slab = slab_create(sizeof(struct flexdb_cache_entry), 2lu << 20);
        if (p->entry_slab == NULL) {
            printf("entry slab alloc failed for partition %u\n", i);
            exit(1);
        }
        p->cache = cache;
        spinlock_init(&p->spinlock);
        p->tick = NULL;
    }

    return cache;
}

static inline u32 flexdb_cache_entry_get_access(const struct flexdb_cache_entry *const entry)
{
    return entry->access;
}

static inline void flexdb_cache_entry_set_access(struct flexdb_cache_entry *const entry)
{
    if (entry->access < FLEXDB_CACHE_ENTRY_CHANCE) {
        entry->access = FLEXDB_CACHE_ENTRY_CHANCE;
    }
}

static inline void flexdb_cache_entry_set_access_warmup(struct flexdb_cache_entry *const entry)
{
    entry->access = FLEXDB_CACHE_ENTRY_CHANCE_WARMUP;
}

static inline void flexdb_cache_entry_waste_access(struct flexdb_cache_entry *const entry)
{
    if (entry->access > 0) entry->access--;
}

static inline u32 flexdb_cache_entry_get_refcnt(const struct flexdb_cache_entry *const entry)
{
    return entry->refcnt;
}

static inline void flexdb_cache_entry_inc_refcnt(struct flexdb_cache_entry *const entry)
{
    entry->refcnt++;
}

static inline void flexdb_cache_entry_dec_refcnt(struct flexdb_cache_entry *const entry)
{
    entry->refcnt--;
}

static inline void flexdb_cache_entry_set_frag(struct flexdb_cache_entry *const entry, const u64 frag)
{
    if (frag > (entry->count >> 1)) {
        entry->frag = 1; // need defrag
    }
}

static inline void flexdb_cache_entry_clear_frag(struct flexdb_cache_entry *const entry)
{
    entry->frag = 0;
}

static inline u16 flexdb_cache_entry_get_frag(const struct flexdb_cache_entry *const entry)
{
    return entry->frag;
}

static struct flexdb_cache_entry *flexdb_cache_partition_find_victim(struct flexdb_cache_partition *const partition)
{
    struct flexdb_cache_entry *victim = partition->tick;
    u32 access = flexdb_cache_entry_get_access(victim);
    u32 refcnt = flexdb_cache_entry_get_refcnt(victim);
    while (access > 0 || refcnt > 0) {
        if (refcnt == 0) {
            flexdb_cache_entry_waste_access(victim); // lose one chance
        }
        victim = victim->next;
        partition->tick = victim;
        access = flexdb_cache_entry_get_access(victim);
        refcnt = flexdb_cache_entry_get_refcnt(victim);
    }
    return victim;
}

static inline void flexdb_cache_partition_free_kv(struct flexdb_cache_partition *const partition, struct kv *const kv)
{
    // no need for a lock as it is already au64
    partition->size -= kv_size(kv);
    free(kv);
}

// returns freed space
static u64 flexdb_cache_partition_free_entry(
        struct flexdb_cache_partition *const partition, struct flexdb_cache_entry *const entry)
{
    // if we call this function, it must be a known interval
    const u64 size = sizeof(*entry) + entry->size;
    for (u32 i=0; i<entry->count; i++) {
        free(entry->kv_interval[i]);
    }
    if (entry->anchor) {
        entry->anchor->cache_entry = NULL;
    } // else, this is an orphan..
    entry->prev->next = entry->next;
    entry->next->prev = entry->prev;

    slab_free_unsafe(partition->entry_slab, entry);

    return size;
}

// caller holds the spinlock
static u64 flexdb_cache_partition_regain(struct flexdb_cache_partition *const partition, const u64 size)
{
    if (size > partition->cap) {
        return 0;
    }
    const u64 need = size - (partition->cap - partition->size);
    u64 gained = 0;
    struct flexdb_cache_entry *victim = NULL;
    while (gained < need) {
        victim = flexdb_cache_partition_find_victim(partition);
        if (partition->tick == victim) {
            if (victim->next != victim) {
                partition->tick = victim->next;
            }
            else {
                partition->tick = NULL;
            }
        }
        gained += flexdb_cache_partition_free_entry(partition, victim);
    }
    // printf("total gained %lu bytes\n", gained);
    partition->size -= gained;
    return gained;
}

// TODO: alloc and add size first, then clean the partition

// caller holds the spinlock
static struct flexdb_cache_entry *flexdb_cache_partition_alloc_entry(
        struct flexdb_cache_partition *const partition, struct flexdb_tree_anchor *const anchor)
{
    struct flexdb_cache_entry *entry = slab_alloc_unsafe(partition->entry_slab);
    memset(entry, 0, sizeof(*entry));
    entry->anchor = anchor;
    if (partition->tick == NULL) {
        partition->tick = entry;
        entry->prev = entry;
        entry->next = entry;
    } else {
        entry->prev = partition->tick->prev;
        entry->next = partition->tick;
        entry->prev->next = entry;
        entry->next->prev = entry;
        // cache->tick unchanged
    }
    return entry;
}

// static inline void flexdb_cache_kv_access(struct kv *const kv)
// {
//     kv->priv = 1;
// }


// caller holds the spinlock
static inline void flexdb_cache_partition_calibrate(struct flexdb_cache_partition *const partition)
{
    if (partition->size > partition->cap) {
        flexdb_cache_partition_regain(partition, partition->size - partition->cap);
    }
}

static inline struct kv *flexdb_cache_entry_read_kv(const struct flexdb_cache_entry *const entry, const u32 idx)
{
    return entry->kv_interval[idx];
}

static inline u16 flexdb_cache_entry_fingerprint(const u32 hash32)
{
    const u16 fp = ((u16)hash32) ^ ((u16)(hash32 >> 16));
    return fp ? fp : 1;
}

static void flexdb_cache_entry_insert(
        struct flexdb_cache_entry *const entry, struct kv *const kv, const u32 idx)
{
    debug_assert(entry);
    debug_assert(kv);
    const u32 count = entry->count;
    if (idx < count) {
        memmove(&entry->kv_interval[idx+1], &entry->kv_interval[idx], (count - idx) * sizeof(entry->kv_interval[0]));
        memmove(&entry->kv_fps[idx+1], &entry->kv_fps[idx], (count - idx) * sizeof(entry->kv_fps[0]));
    }
    entry->kv_interval[idx] = kv;
    entry->kv_fps[idx] = flexdb_cache_entry_fingerprint(kv->hashlo);
    entry->size += kv_size(kv);
    entry->count++;
}

static void flexdb_cache_entry_append(
        struct flexdb_cache_entry *const entry, struct kv *const kv)
{
    entry->kv_interval[entry->count] = kv;
    entry->kv_fps[entry->count] = flexdb_cache_entry_fingerprint(kv->hashlo);
    entry->size += kv_size(kv);
    entry->count++;
}

static void flexdb_cache_entry_delete(
        struct flexdb_cache_entry *const entry, const u32 idx,
        struct flexdb_cache_partition *const partition)
{
    const u32 count = entry->count;
    struct kv *const okv = entry->kv_interval[idx];

    entry->count--;
    entry->size -= kv_size(okv);

    flexdb_cache_partition_free_kv(partition, okv);
    if (idx + 1 < count) {
        memmove(&entry->kv_interval[idx], &entry->kv_interval[idx+1], (count - idx) * sizeof(entry->kv_interval[0]));
        memmove(&entry->kv_fps[idx], &entry->kv_fps[idx+1], (count - idx) * sizeof(entry->kv_fps[0]));
    }
    entry->kv_fps[count-1] = 0;
    entry->kv_interval[count-1] = NULL;
}

static void flexdb_cache_entry_replace(
        struct flexdb_cache_entry *const entry, struct kv *const kv,
        const u32 idx, struct flexdb_cache_partition *const partition)
{
    struct kv *const okv = entry->kv_interval[idx];
    const u32 osize = (u32)kv_size(okv);
    flexdb_cache_partition_free_kv(partition, okv);
    entry->kv_interval[idx] = kv;
    const u32 size = (u32)kv_size(kv);
    entry->size += (size - osize);
    // entry->kv_fps[idx] = flexdb_cache_entry_fingerprint(kv->hashlo);
}

static u8 *flexdb_cache_partition_read_interval(
        const struct flexdb_cache_partition *const partition,
        const struct flexdb_tree_anchor *const anchor, const u64 loff, u64 *const frag, u8 *const itvbuf,
        void *const priv)
{
    if (anchor->psize == 0) {
        return NULL;
    }
    const struct flexdb *const db = partition->cache->db;
    // read db from flexfile to cache
#ifdef FLEXDB_USE_RRING
    const ssize_t r = flexfile_read_fragmentation_rring(
            db->flexfile, itvbuf, loff, anchor->psize, frag, (struct flexfile_rring *)priv);
#else
    const ssize_t r = flexfile_read_fragmentation(db->flexfile, itvbuf, loff, anchor->psize, frag);
    (void)priv;
#endif
    debug_assert(r == (ssize_t)anchor->psize);
    (void)r;
    return itvbuf;
}

static struct flexdb_cache_entry *flexdb_cache_partition_get_entry_new_anchor(
        struct flexdb_cache_partition *const partition, struct flexdb_tree_anchor *const anchor)
{
    spinlock_lock(&partition->spinlock);
    struct flexdb_cache_entry *fce = anchor->cache_entry;
    debug_assert(!fce);
    fce = flexdb_cache_partition_alloc_entry(partition, anchor);
    partition->size += sizeof(*fce);
    flexdb_cache_entry_set_access(fce);
    flexdb_cache_entry_inc_refcnt(fce);
    anchor->cache_entry = fce;
    flexdb_cache_partition_calibrate(partition);
    spinlock_unlock(&partition->spinlock);
    return fce;
}

static u32 flexdb_cache_entry_kv_interval_dedup(
        struct kv **const kv_interval, u16 *const kv_fps, u8 *const anchor_count)
{
    const u8 count = *anchor_count;
    u32 size = 0;
    u8 idx = 0; // slower
    u8 sidx = 1; // faster

    // now all the kv->hashlo is still there so we can use them
    while (sidx < count) {
        const bool identical = (kv_interval[idx]->hashlo == kv_interval[sidx]->hashlo) &&
            kv_match(kv_interval[idx], kv_interval[sidx]);
        if (identical) {
            free(kv_interval[idx]);
        } else {
            size += kv_size(kv_interval[idx++]);
        }
        kv_interval[idx] = kv_interval[sidx]; // move sidx to the next slot
        kv_fps[idx] = kv_fps[sidx];

        sidx++;
    }
    size += kv_size(kv_interval[idx++]); // the last one
    *anchor_count = idx;

    return size;
}

static inline int kv_compare_priv(const void *const _kv1, const void *const _kv2)
{
    const struct kv *const kv1 = *(const struct kv **)_kv1;
    const struct kv *const kv2 = *(const struct kv **)_kv2;
    const u32 len = kv1->klen < kv2->klen ? kv1->klen : kv2->klen;
    const int cmp = memcmp(kv1->kv, kv2->kv, (size_t)len);
    if (cmp) {
        return cmp;
    }

    const u64 x1 = ((u64)kv1->klen) << 32 | kv1->privhi;
    const u64 x2 = ((u64)kv2->klen) << 32 | kv2->privhi;
    debug_assert(x1 != x2);
    return x1 < x2 ? -1 : 1;
}

static void flexdb_cache_partition_load_interval(
        struct flexdb_cache_partition *const partition, struct flexdb_tree_anchor *const anchor,
        struct flexdb_cache_entry *const fce, const u64 loff, u8 *const itvbuf, void *const priv)
{
    u64 frag = 0;
    u8 *const interval = flexdb_cache_partition_read_interval(partition, anchor, loff, &frag, itvbuf, priv);
    fce->size = 0;
    fce->count = 0;
    if (interval != NULL) { // which means psize > 0
        const u8 *kv = interval;
        size_t psize = 0;
        const u8 * const end = interval + anchor->psize;
        while (kv < end) {
            struct kv *const r = kv128_decode_kv(kv, NULL, &psize); // malloc
            r->privhi = (u32)fce->count;
            // r->privhi -> the idx
            // r->hashlo -> the crc32 of the kv
            flexdb_cache_entry_append(fce, r); // here the fps are calculated
            kv += psize;
        }
        debug_assert(kv == interval+anchor->psize);

        if (anchor->unsorted > 0) {
            // 1. sort
            qsort(fce->kv_interval, fce->count, sizeof(fce->kv_interval[0]), kv_compare_priv);
            for (u32 i=0; i<fce->count; i++) {
                fce->kv_fps[i] = flexdb_cache_entry_fingerprint(fce->kv_interval[i]->hashlo);
            }
            // 2. de-duplicate, and get the latest .count, .size, .psize
            const u32 new_size = flexdb_cache_entry_kv_interval_dedup(fce->kv_interval, fce->kv_fps, &fce->count);
            debug_assert(new_size <= fce->size);
            fce->size = new_size;
            // .count and .size now updated, .psize is still the original one to record the correct offset
            // but leave .unsorted as positive for writers to re-write the whole interval
            // now any readers can safely consume this fce
        }
        // for (u32 j=0; j<fce->count; j++) {
        //     fce->kv_interval[j]->priv = 0; // clean the priv here, to count access when evicting
        // }
    } else {
        // psize == 0
        debug_assert(anchor->psize == 0);
    }
    flexdb_cache_entry_set_frag(fce, frag); // it is now by default
}

static struct flexdb_cache_entry *flexdb_cache_partition_get_entry(
        struct flexdb_cache_partition *const partition, struct flexdb_tree_anchor *const anchor, const u64 loff,
        u8 *const itvbuf, void *const priv)
{
    u64 total_size = 0;
    spinlock_lock(&partition->spinlock);
    struct flexdb_cache_entry *fce = anchor->cache_entry;
    if (fce) { // hit
        flexdb_cache_entry_inc_refcnt(fce);
        if (unlikely(fce->loading == 1)) {
            spinlock_unlock(&partition->spinlock);
            while (fce->loading == 1) {
                cpu_pause();
            }
            spinlock_lock(&partition->spinlock);
        }
    } else { // miss; slow
        fce = flexdb_cache_partition_alloc_entry(partition, anchor);
        total_size += sizeof(*fce);
        flexdb_cache_entry_inc_refcnt(fce);
        fce->loading = 1; // let me load it!
        anchor->cache_entry = fce;
        // do real loading, no need for a lock
        spinlock_unlock(&partition->spinlock);
        flexdb_cache_partition_load_interval(partition, anchor, fce, loff, itvbuf, priv);
        total_size += fce->size;
        fce->loading = 0;
        spinlock_lock(&partition->spinlock);
    }
    // TODO: trylock + wait ?

    if (total_size > 0) {
        partition->size += total_size;
        flexdb_cache_partition_calibrate(partition);
    }
    flexdb_cache_entry_set_access(fce);
    spinlock_unlock(&partition->spinlock);

    return fce;
}

static inline struct flexdb_cache_entry *flexdb_cache_partition_get_entry_unsorted(
        struct flexdb_cache_partition *const partition, struct flexdb_tree_anchor *const anchor, const u64 loff,
        u8 *const itvbuf, void *const priv)
{
    spinlock_lock(&partition->spinlock);
    struct flexdb_cache_entry *fce = anchor->cache_entry;
    spinlock_unlock(&partition->spinlock);
    if (fce || anchor->unsorted >= FLEXDB_UNSORTED_WRITE_QUOTA_COUNT
            || anchor->psize >= FLEXDB_TREE_SPARSE_INTERVAL_SIZE ) {
        fce = flexdb_cache_partition_get_entry(partition, anchor, loff, itvbuf, priv);
        debug_assert(fce);
    }
    return fce;
}

static inline void flexdb_cache_partition_release_entry(struct flexdb_cache_entry *const entry)
{
    flexdb_cache_entry_dec_refcnt(entry);
}

static inline struct flexdb_cache_partition *flexdb_cache_get_partition(
        struct flexdb_cache *const cache, const struct flexdb_tree_anchor *const anchor)
{
    const u32 id = anchor->key->hash & FLEXDB_CACHE_PARTITION_MASK;
    return &cache->partitions[id];
}

static void flexdb_cache_destroy(struct flexdb_cache *const cache)
{
    for (u32 i=0; i<FLEXDB_CACHE_PARTITION_COUNT; i++) {
        struct flexdb_cache_partition *const partition = &cache->partitions[i];
        if (partition->tick) {
            struct flexdb_cache_entry *sentry = partition->tick;
            struct flexdb_cache_entry *entry = sentry->next;
            while (entry != sentry) {
                struct flexdb_cache_entry *const next = entry->next;
                partition->size -= flexdb_cache_partition_free_entry(partition, entry);
                entry = next;
            }
            partition->size -= flexdb_cache_partition_free_entry(partition, entry);
        }
        slab_destroy(partition->entry_slab);
        // printf("paritition %u size after free %lu \n", i, partition->size);
    }
    free(cache);
}

static u32 flexdb_cache_entry_find_key_ge(
        const struct flexdb_cache_entry *const entry, const struct kref *const key)
{
    // return a position
    u32 hi = entry->count;
    u32 lo = 0;
    // binary search
    while (lo < hi) {
        const u32 target = (lo + hi) >> 1;
        const int cmp = kref_kv_compare(key, entry->kv_interval[target]);
        if (cmp > 0) {
            lo = target + 1;
        }
        else if (cmp < 0) {
            hi = target;
        }
        else {
            return target | FLEXDB_CACHE_ENTRY_FIND_EQ;
        }
    }
    return lo;
}

static u32 flexdb_cache_entry_find_key_eq(
        const struct flexdb_cache_entry *const entry, const struct kref *const key)
{
    debug_assert(key->hash32 == kv_crc32c(key->ptr, key->len));
    const u16 fp = flexdb_cache_entry_fingerprint(key->hash32);
#if defined(__x86_64__)
#if defined(__AVX2__)
    const u32 inc = sizeof(m256) / sizeof(u16);
    const m256 fpv = _mm256_set1_epi16((short)fp);
#else // SSE4.2 (minimal requirement on x86_64)
    const u32 inc = sizeof(m128) / sizeof(u16);
    const m128 fpv = _mm_set1_epi16((short)fp);
#endif // AVX2

    for (u32 i = 0; i < FLEXDB_TREE_SPARSE_INTERVAL; i += inc) {
#if defined(__AVX2__)
        const m256 fps = _mm256_loadu_si256((const void *)(entry->kv_fps+i));
        u32 mask = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi16(fpv, fps));
#else // SSE4.2 (minimal requirement on x86_64)
        const m128 fps = _mm_loadu_si128((const void *)(entry->kv_fps+i));
        u32 mask = (u32)_mm_movemask_epi8(_mm_cmpeq_epi16(fpv, fps));
#endif // __AVX2__
        while (mask) {
            const u32 shift = __builtin_ctz(mask);
            const u32 target = i + (shift >> 1);
            if (kref_kv_match(key, entry->kv_interval[target])) {
                return target;
            }
            mask ^= (3u << shift);
        }
    }
#elif defined(__aarch64__)
    const m128 fpv = vreinterpretq_u8_u16(vdupq_n_u16(fp));
    const u32 inc = sizeof(m128) / sizeof(u16);
    static const uint16x8_t mbits = {0x3, 0xc, 0x30, 0xc0, 0x300, 0xc00, 0x3000, 0xc000};

    for (u32 i = 0; i < FLEXDB_TREE_SPARSE_INTERVAL; i += inc) {
        const uint16x8_t fps = vld1q_u16((const u16 *)(entry->kv_fps+i)); // load 16 bytes at s
        const uint16x8_t cmp = vceqq_u16(vreinterpretq_u16_u8(fpv), fps); // cmpeq => 0xffff or 0x0000
        u32 mask = (u32)vaddvq_u16(vandq_u16(cmp, mbits));
        while (mask) {
            const u32 shift = __builtin_ctz(mask);
            const u32 target = i + (shift >> 1);
            if (kref_kv_match(key, entry->kv_interval[target])) {
                return target;
            }
            mask ^= (3u << shift);
        }
    }
#else // no simd
    for (u32 i = 0; i < entry->count; i++) {
        if (entry->kv_fps[i] == fp && kref_kv_match(key, entry->kv_interval[i])) {
            return i;
        }
    }
#endif // __x86_64__
    return FLEXDB_TREE_SPARSE_INTERVAL; // not found
}

// }}} cache

// {{{ file tagging

#define FLEXDB_FILE_TAG_ANCHOR_SHIFT (0u)
#define FLEXDB_FILE_TAG_ANCHOR_MASK ((u16)0x1) // 1 bit
#define FLEXDB_FILE_TAG_UNSORTED_WRITE_SHIFT (1u)
#define FLEXDB_FILE_TAG_UNSORTED_WRITE_MASK ((u16)0x7f) // 7 bits

static inline u16 flexdb_file_tag_generate(const u8 is_anchor, const u8 unsorted)
{
    return (u16)((unsorted & FLEXDB_FILE_TAG_UNSORTED_WRITE_MASK) << FLEXDB_FILE_TAG_UNSORTED_WRITE_SHIFT) |
           (u16)((is_anchor & FLEXDB_FILE_TAG_ANCHOR_MASK) << FLEXDB_FILE_TAG_ANCHOR_SHIFT);
}

static inline u8 flexdb_file_tag_get_anchor(const u16 tag)
{
    return ((tag >> FLEXDB_FILE_TAG_ANCHOR_SHIFT) & FLEXDB_FILE_TAG_ANCHOR_MASK) ? 1 : 0;
}

static inline u8 flexdb_file_tag_get_unsorted(const u16 tag)
{
    return (tag >> FLEXDB_FILE_TAG_UNSORTED_WRITE_SHIFT) & FLEXDB_FILE_TAG_UNSORTED_WRITE_MASK;
}

// }}} file tagging

// {{{ helpers

static void flexdb_tree_insert_anchor(
        struct flexdb *const db, struct flexdb_tree_node_handler *const nh,
        struct flexdb_cache_partition *const partition, struct flexdb_cache_entry *const fce)
{
    struct flexdb_tree_anchor *const anchor = nh->node->leaf_entry.anchors[nh->idx];

    const u32 count = fce->count;
    const u32 right_count = count / 2;
    const u32 left_count = count - right_count;
    const u64 anchor_loff = anchor->loff + (u64)nh->shift;

    u32 left_size = 0;
    u32 left_psize = 0;

    const struct kv *kv = NULL;
    for (u32 i=0; i<left_count; i++) {
        kv = flexdb_cache_entry_read_kv(fce, i);
        left_psize += (u32)kv128_estimate_kv(kv);
        left_size += (u32)kv_size(kv);
    }

    const u64 loff = anchor_loff + left_psize;

    const u32 right_size = fce->size - left_size;
    const u32 right_psize = anchor->psize - left_psize;

    // read new anchor key
    kv = flexdb_cache_entry_read_kv(fce, left_count);
    // NOTE: the following 3 lines cause bugs with tags
    // const struct kv *const lastkv = flexdb_cache_entry_read_kv(fce, left_count-1);
    // const u32 lcp = kv_key_lcp(lastkv, kv);
    // struct kv *const new_anchor_key = kv_dup2_key_prefix(kv, NULL, lcp+1);
    struct kv *const new_anchor_key = kv_dup_key(kv);
    //printf("%u %s %s %s\n", lcp, lastkv->kv, kv->kv, new_anchor_key->kv);
    kv_update_hash(new_anchor_key);

    // insert new anchor key, possibly some split here..
    nh->idx++;
    struct flexdb_tree_anchor *const new_anchor =
        flexdb_tree_handler_insert(nh, new_anchor_key, loff, right_psize);
    nh->idx--;

    // cache
    struct flexdb_cache_partition *const new_partition = flexdb_cache_get_partition(db->cache, new_anchor);
    struct flexdb_cache_entry *const new_fce = flexdb_cache_partition_get_entry_new_anchor(new_partition, new_anchor);

    memmove(new_fce->kv_interval, &fce->kv_interval[left_count], right_count * sizeof(fce->kv_interval[0]));
    memmove(new_fce->kv_fps, &fce->kv_fps[left_count], right_count * sizeof(fce->kv_fps[0]));

    // update right
    new_fce->count = (u8)right_count;
    new_fce->size = right_size;
    new_fce->frag = fce->frag;

    if (partition != new_partition) {
        partition->size -= right_size;
        new_partition->size += right_size;
        // someone will clean this (NOTE: is there a better way to do this?)
    }

    // update left
    fce->count = (u8)left_count;
    fce->size = left_size;
    anchor->psize = left_psize;

    // flexdb_cache_partition_release_entry(fce); // don't release here
    flexdb_cache_partition_release_entry(new_fce);

    // now the new anchor is inserted, set a tag for it
    debug_assert(loff != 0);
    debug_assert(new_anchor->unsorted == 0);
    const u16 tag = flexdb_file_tag_generate(1, new_anchor->unsorted);
    flexfile_set_tag(db->flexfile, loff, tag);
}

static void flexdb_tree_node_handler_info_update(struct flexdb_tree_node_handler *const nh)
{
    debug_assert(nh->node);
    const struct flexdb_tree_node *node = nh->node;
    s64 shift = 0;
    while (node->parent) {
        const u32 p_idx = node->parent_id;
        node = node->parent;
        const struct flexdb_tree_internal_entry *const ie = &node->internal_entry;
        shift += ie->children[p_idx].shift;
    }
    nh->shift = shift;
}

static void flexdb_tree_node_handler_next_anchor(
        struct flexdb_tree *const tree, struct flexdb_tree_node_handler *const nh, const struct kref *const kref)
{
    if (!nh->node) {
        flexdb_tree_find_anchor_pos(tree, kref, nh);
        return;
    }
    // note: this function only cares about nh->node now, it just use its
    // current node as an approximate starting point for hinted search,
    // which could save a few key comparisons along the path downwards.
    // when cnode is determined, it starts search downwards,
    // finally it updates the idx and shift in nh...
    // this is sort of dirty but seems to be working
    struct flexdb_tree_node *node = nh->node;
    struct flexdb_tree_node *cnode = nh->node;
    while (cnode->parent) {
        const u32 parent_id = cnode->parent_id;
        if (parent_id + 1 > cnode->parent->count) {
            // either curr node is the right most one
            // or it was split!
            cnode = cnode->parent;
        } else {
            // not the right most one
            if (kref_kv_compare(kref, cnode->parent->internal_entry.pivots[parent_id]) < 0) {
                // current is the correct node
                node = cnode;
                break;
            } else {
                // current is not the correct node, search from parent
                node = cnode->parent;
                cnode = node;
            }
        }
    }
    // now we find the nearest parent node
    while (!node->is_leaf) {
        const u32 target = flexdb_tree_find_pos_in_internal(node, kref);
        const struct flexdb_tree_internal_entry *const ie = &node->internal_entry;
        node = ie->children[target].node;
    }
    debug_assert(node->is_leaf);
    nh->node = node;
    nh->idx = flexdb_tree_find_pos_in_leaf_le(node, kref);
    flexdb_tree_node_handler_info_update(nh);
}

static inline u32 flexdb_enter_flexfile(const struct flexdb_ref *const dbref, const u32 hash32)
{
    const u32 lockid = hash32 & FLEXDB_LOCK_SHARDING_MASK;
    rwlock_lock_read(&dbref->db->rwlock_flexfile[lockid].lock);
    return lockid;
}

static inline u32 flexdb_enter_memtable(const struct flexdb_ref *const dbref, const u32 hash32)
{
    const u32 lockid = hash32 & FLEXDB_LOCK_SHARDING_MASK;
    rwlock_lock_read(&dbref->db->rwlock_memtable[lockid].lock);
    return lockid;
}

static inline void flexdb_exit_flexfile(const struct flexdb_ref *const dbref, const u32 lockid)
{
    rwlock_unlock_read(&dbref->db->rwlock_flexfile[lockid].lock);
}

static inline void flexdb_exit_memtable(const struct flexdb_ref *const dbref, const u32 lockid)
{
    rwlock_unlock_read(&dbref->db->rwlock_memtable[lockid].lock);
}

// }}} helpers

// {{{ passthrough functions

static void flexdb_put_passthrough_unsorted(
        struct flexdb *const db, const struct kv *const okv, struct flexdb_tree_node_handler *const nh,
        struct flexdb_tree_anchor *const anchor) {
    // not in cache, and quota is not used up
    const u64 anchor_loff = anchor->loff + (u64)nh->shift;
    size_t tsize = 0;
    u8 *const kv128 = kv128_encode_kv(okv, db->kvbuf1, &tsize);
    debug_assert(kv128 == db->kvbuf1);
    const u32 psize = (u32)tsize;
    const u64 loff = anchor_loff + (u64)anchor->psize;

    // update file and sparse index shifts
    flexfile_insert(db->flexfile, kv128, loff, psize);
    flexdb_tree_node_shift_up_propagate(nh, (s64)(u64)psize);

    // update in-memory anchor metadata
    anchor->psize += psize;
    anchor->unsorted++; // these two are must

    // don't forget to also update the tag (yes it is not cheap...)
    const u16 tag = flexdb_file_tag_generate(1, anchor->unsorted);
    flexfile_set_tag(db->flexfile, anchor_loff, tag);

    if (nh->node->parent) {
        flexdb_tree_node_rebase(nh->node);
    }
}

static void flexdb_put_passthrough_rewrite(
        struct flexdb *const db, struct flexdb_tree_node_handler *const nh, struct flexdb_tree_anchor *const anchor,
        struct flexdb_cache_entry *const fce)
{
    const u64 anchor_loff = anchor->loff + (u64)nh->shift;
    u32 new_psize = 0;
    u8 *const buf = db->itvbuf;
    u32 boff = 0;
    for (u32 i=0; i<fce->count; i++) {
        size_t ppsize = 0;
        u8 *pkv = kv128_encode_kv(fce->kv_interval[i], buf+boff , &ppsize);
        debug_assert(ppsize > 0 && pkv);
        (void)pkv;
        boff += (u32)ppsize;
        new_psize += (u32)ppsize;
    }
    debug_assert(new_psize <= anchor->psize);
    flexfile_update(db->flexfile, buf, anchor_loff, new_psize, anchor->psize);
    if (new_psize != anchor->psize) {
        flexdb_tree_node_shift_up_propagate(nh, (s64)new_psize - (s64)anchor->psize);
    }
    anchor->psize = new_psize; // now consistent
    anchor->unsorted = 0;
    const u16 tag = flexdb_file_tag_generate(1, anchor->unsorted);
    flexfile_set_tag(db->flexfile, anchor_loff, tag);
}

static void flexdb_put_passthrough_r(
        struct flexdb *const db, const struct kv *const okv, const struct kref *const kref,
        struct flexdb_tree_node_handler *const nh, struct flexdb_tree_anchor *const anchor,
        struct flexdb_cache_partition *const partition, struct flexdb_cache_entry *const fce)
{
    const u64 anchor_loff = anchor->loff + (u64)nh->shift;
    // alloc kv and update stat
    struct kv *const kv = kv_dup(okv);
    kv_update_hash(kv);

    spinlock_lock(&partition->spinlock);
    partition->size += kv_size(kv);
    flexdb_cache_partition_calibrate(partition);
    spinlock_unlock(&partition->spinlock);

    u64 loff = anchor_loff;

    u32 i = flexdb_cache_entry_find_key_ge(fce, kref);
    u8 update = 0;

    if (i & FLEXDB_CACHE_ENTRY_FIND_EQ) {
        update = 1;
    }
    i &= ~(FLEXDB_CACHE_ENTRY_FIND_EQ);
    for (u32 j=0; j<i; j++) {
        loff += kv128_estimate_kv(fce->kv_interval[j]);
    }
    size_t tsize = 0;
    u8 *const kv128 = kv128_encode_kv(kv, db->kvbuf1, &tsize);
    debug_assert(tsize > 0 && kv128 == db->kvbuf1);
    const u32 psize = (u32)tsize;
    if (update) {
        const u32 opsize = (u32)kv128_estimate_kv(fce->kv_interval[i]);
        flexdb_cache_entry_replace(fce, kv, i, partition);
        flexfile_update(db->flexfile, kv128, loff, psize, opsize); // implicitly updated the tag
        if (psize != opsize) {
            flexdb_tree_node_shift_up_propagate(nh, (s64)psize - (s64)opsize);
        }
        anchor->psize += (psize - opsize);
    } else {
        flexdb_cache_entry_insert(fce, kv, i);
        if (i == 0) {
            // if the key is the first in the interval, and the interval is not the first one..
            debug_assert(anchor->unsorted == 0);
            debug_assert(loff == anchor_loff);
            flexfile_set_tag(db->flexfile, loff, 0); // clear the tag
            flexfile_insert(db->flexfile, kv128, loff, psize);
            const u16 tag = flexdb_file_tag_generate(1, anchor->unsorted);
            flexfile_set_tag(db->flexfile, loff, tag);
        } else {
            // otherwise, insert only
            flexfile_insert(db->flexfile, kv128, loff, psize);
        }
        flexdb_tree_node_shift_up_propagate(nh, (s64)(u64)psize);
        anchor->psize += psize;
    }

    if (nh->node->parent) {
        flexdb_tree_node_rebase(nh->node);
    }
}

static int flexdb_put_passthrough(
        struct flexdb *const db, const struct kv *const okv, struct flexdb_tree_node_handler *const nh)
{
    const struct kref kref = kv_kref(okv);

    flexdb_tree_node_handler_next_anchor(db->tree, nh, &kref);
    debug_assert(nh->node);

    struct flexdb_tree_anchor *const anchor = nh->node->leaf_entry.anchors[nh->idx];

    // cache
    struct flexdb_cache_partition *const partition = flexdb_cache_get_partition(db->cache, anchor);
    struct flexdb_cache_entry *fce = flexdb_cache_partition_get_entry_unsorted(
            partition, anchor, anchor->loff + (u64)nh->shift, db->itvbuf, db->priv);

    if (!fce) {
        flexdb_put_passthrough_unsorted(db, okv, nh, anchor);
    } else {
        // if .unsorted > 0, rewrite it first
        if (anchor->unsorted > 0) {
            debug_assert(anchor->psize > 0);
            flexdb_put_passthrough_rewrite(db, nh, anchor, fce);
            flexdb_cache_entry_clear_frag(fce);
        }
        flexdb_put_passthrough_r(db, okv, &kref, nh, anchor, partition, fce);
        // try to defrag
        if (flexdb_cache_entry_get_frag(fce)) {
            debug_assert(anchor->psize > 0);
            flexdb_put_passthrough_rewrite(db, nh, anchor, fce);
            flexdb_cache_entry_clear_frag(fce);
        }

        if (fce->count >= FLEXDB_TREE_SPARSE_INTERVAL_COUNT || anchor->psize >= FLEXDB_TREE_SPARSE_INTERVAL_SIZE) {
            debug_assert(anchor->unsorted == 0);
            flexdb_tree_insert_anchor(db, nh, partition, fce);
        }

        flexdb_cache_partition_release_entry(fce);
    }
    return 0;
}

static struct kv *flexdb_get_passthrough(
        struct flexdb_ref *const dbref, const struct kref *const kref, u8 *const buf)
{
    struct flexdb *const db = dbref->db;
    struct flexdb_tree_node_handler nh;
    flexdb_tree_find_anchor_pos(db->tree, kref, &nh);
    struct flexdb_tree_anchor *const anchor = nh.node->leaf_entry.anchors[nh.idx];

    // cache
    struct flexdb_cache_partition *partition = flexdb_cache_get_partition(db->cache, anchor);
    struct flexdb_cache_entry *fce =
        flexdb_cache_partition_get_entry(partition, anchor, anchor->loff + (u64)nh.shift, dbref->itvbuf, dbref->priv);
    debug_assert(fce);

    struct kv *kv = NULL;
    const u32 i = flexdb_cache_entry_find_key_eq(fce, kref);
    if (i < FLEXDB_TREE_SPARSE_INTERVAL) {
        debug_assert(i < fce->count);
        // flexdb_cache_kv_access(fce->kv_interval[i]);
        memcpy(buf, fce->kv_interval[i], kv_size(fce->kv_interval[i]));
        kv = (struct kv *)buf;
    }

    flexdb_cache_partition_release_entry(fce);
    return kv;
}

static u8 flexdb_probe_passthrough(struct flexdb_ref *const dbref, const struct kref *const kref)
{
    struct flexdb *const db = dbref->db;
    struct flexdb_tree_node_handler nh;
    flexdb_tree_find_anchor_pos(db->tree, kref, &nh);
    struct flexdb_tree_anchor *anchor= nh.node->leaf_entry.anchors[nh.idx];

    // cache
    struct flexdb_cache_partition *partition = flexdb_cache_get_partition(db->cache, anchor);
    struct flexdb_cache_entry *fce =
        flexdb_cache_partition_get_entry(partition, anchor, anchor->loff + (u64)nh.shift, dbref->itvbuf, dbref->priv);

    u8 ret = 0;
    const u32 i = flexdb_cache_entry_find_key_eq(fce, kref);
    if (i < FLEXDB_TREE_SPARSE_INTERVAL) {
        debug_assert(i < fce->count);
        // flexdb_cache_kv_access(fce->kv_interval[i & (~(FLEXDB_CACHE_ENTRY_FIND_EQ))]);
        ret = 1;
    }

    flexdb_cache_partition_release_entry(fce);
    return ret;
}

static void flexdb_delete_passthrough_update_tree(struct flexdb_tree_node_handler *const nh)
{
    struct flexdb_tree_node *const node = nh->node;
    struct flexdb_tree_anchor *const anchor = node->leaf_entry.anchors[nh->idx];
    struct flexdb_cache_entry *const fce = anchor->cache_entry; // the caller holds a ref
    const struct kv *new_pivot = NULL;
    if (anchor->psize == 0) {
        // now the interval is empty.. needs to clean it up
        // make the fce an orphan first
        anchor->cache_entry->anchor = NULL;
        anchor->cache_entry = NULL;
        // free the current anchor key
        free(anchor->key);
        slab_free_unsafe(node->tree->anchor_slab, anchor);

        // memmove remaining anchors leftwards
        if (nh->idx < node->count - 1) {
            memmove(&node->leaf_entry.anchors[nh->idx], &node->leaf_entry.anchors[nh->idx+1],
                    sizeof(node->leaf_entry.anchors[0]) * (node->count - nh->idx - 1));
        }
        node->count--;
        // if the node is empty.. recycle
        if (node->count == 0) {
            flexdb_tree_recycle_node(node);
        }
        else {
            // if the node is not empty, and we removed the first anchor, we will need to update the pivots recursively
            if (nh->idx == 0) {
                // the node has a new smallest key
                debug_assert(node->is_leaf);
                new_pivot = node->leaf_entry.anchors[0]->key; // it is nh->idx.. because memmove shifted it
            }
        }
    } else {
        // if the interval is not empty, update the anchor key by the current smallest
        debug_assert(fce->count > 0);
        const u64 old_hash = anchor->key->hash;
        free(anchor->key);
        anchor->key = kv_dup_key(fce->kv_interval[0]);
        // Re-use old hash here!!
        anchor->key->hash = old_hash;
        // if the removed key is also the smallest in the node, update it recursively..
        if (nh->idx == 0) {
            new_pivot = anchor->key; // steal it
        }
    }
    if (new_pivot) {
        // update pivots
        flexdb_tree_node_update_smallest_key(node, new_pivot);
    }
    nh->node = NULL; // invalidate the nh..
}

static int flexdb_delete_passthrough(
        struct flexdb *const db, const struct kref *const kref, struct flexdb_tree_node_handler *const nh)
{
    flexdb_tree_node_handler_next_anchor(db->tree, nh, kref);
    debug_assert(nh->node);

    struct flexdb_tree_anchor *const anchor= nh->node->leaf_entry.anchors[nh->idx];

    // cache
    struct flexdb_cache_partition *const partition = flexdb_cache_get_partition(db->cache, anchor);
    struct flexdb_cache_entry *const fce =
        flexdb_cache_partition_get_entry(partition, anchor, anchor->loff + (u64)nh->shift, db->itvbuf, db->priv);

    debug_assert(fce);

    if (anchor->unsorted > 0) {
        debug_assert(anchor->psize > 0);
        flexdb_put_passthrough_rewrite(db, nh, anchor, fce);
        flexdb_cache_entry_clear_frag(fce);
    }

    u64 loff = anchor->loff + (u64)nh->shift;

    const u32 i = flexdb_cache_entry_find_key_eq(fce, kref);
    if (i < FLEXDB_TREE_SPARSE_INTERVAL) {
        debug_assert(i < fce->count);
        const u32 psize = (u32)kv128_estimate_kv(fce->kv_interval[i]);
        for (u32 j=0; j<i; j++) {
            loff += kv128_estimate_kv(fce->kv_interval[j]);
        }
        flexdb_cache_entry_delete(fce, i, partition);
        if (i == 0 && fce->count > 1) {
            // if the deleted kv is the first one in the interval, and the interval is not empty after the deletion
            debug_assert(loff == anchor->loff + (u64)nh->shift);
            flexfile_collapse(db->flexfile, loff, psize);
            const u16 tag = flexdb_file_tag_generate(1, anchor->unsorted);
            flexfile_set_tag(db->flexfile, loff, tag);
        } else {
            flexfile_collapse(db->flexfile, loff, psize);
        }
        flexdb_tree_node_shift_up_propagate(nh, -(s64)(u64)psize);
        anchor->psize -= psize;

        // now comes to the special case, we need to update the sparse index if:
        // (1) the deleted key is the first key, which means the anchor key is changed, or
        // (2) the anchor is now empty
        // and the current anchor is not the smallest one.
        // for case (1), we need to update the first non-leftmost ancestor node's pivot key to be the second key
        // for case (2), we need to remove the anchor and do similarly
        // this is to reduce the read-amp on recovery

        if (i == 0 && anchor->key->klen) {
            flexdb_delete_passthrough_update_tree(nh);
        }
    }

    if (flexdb_cache_entry_get_frag(fce) && anchor->psize > 0) {
        debug_assert(anchor->psize > 0);
        flexdb_put_passthrough_rewrite(db, nh, anchor, fce);
        flexdb_cache_entry_clear_frag(fce);
    }

    // if (fce->count>= FLEXDB_TREE_SPARSE_INTERVAL_COUNT || anchor->psize >= FLEXDB_TREE_SPARSE_INTERVAL_SIZE) {
    //     debug_assert(anchor->unsorted == 0);
    //     flexdb_tree_insert_anchor(db, nh, partition, fce);
    // }

    flexdb_cache_partition_release_entry(fce);
    return 0;
    // NOTE: if some anchor's interval becomes empty here, its tag is also removed by the _collapse
}

// }}} passthrough functions

// {{{ recovery

// this function is an "it just works" version.. don't expect too much
static void flexdb_log_redo(struct flexdb *const db, const int fd)
{
    u8 *const buf = db->kvbuf1;
    struct kv *const kv = (struct kv *)db->kvbuf2;
    off_t loff = 8;

    while (1) {
        ssize_t r = pread(fd, buf, sizeof(*kv), loff);
        if (r == 0) {
            break;
        }
        const u32 psize = (u32)kv128_size(buf);
        r = pread(fd, buf, psize, loff);
        if ((u32)r != psize) {
            printf("flexdb log corrupted, some updates may have lost");
            printf("%zd %u\n", r, psize);
            fflush(stdout);
            break;
        }
        size_t ppsize = 0;
        kv128_decode_kv(buf, kv, &ppsize);
        debug_assert(psize == ppsize);

        struct kref kref;
        kref_ref_kv_hash32(&kref, kv); // calculate hash32
        struct flexdb_tree_node_handler nh;
        flexdb_tree_find_anchor_pos(db->tree, &kref, &nh);
        if (kv->vlen == 0) { // deletion
            r = flexdb_delete_passthrough(db, &kref, &nh);
        } else { // put or update
            r = flexdb_put_passthrough(db, kv, &nh);
        }
        loff += psize;
    }

    flexfile_sync(db->flexfile);
}

struct flexdb_recovery_worker_info {
    // pass in
    u64 start;
    u64 end;
    struct flexdb *db;
    // pass out
    u64 count;
    struct flexdb_recovery_anchor {
        struct kv *anchor;
        u64 loff;
        u8 unsorted;
    } *anchors;
};

static void *flexdb_recovery_worker(void *oinfo)
{
    struct flexdb_recovery_worker_info *const info = oinfo;
    const u64 start = info->start;
    const u64 end = info->end;
    u64 cap = 65536;
    u64 count = 0;
    info->anchors = malloc(cap * sizeof(info->anchors[0]));
    u8 *const kvbuf1 = malloc(FLEXDB_MAX_KV_SIZE);
    struct kv *const kvbuf2 = malloc(FLEXDB_MAX_KV_SIZE);

    u16 tag = 0;
    struct flexfile_handler ffh = flexfile_get_handler(info->db->flexfile, start);
    while (flexfile_handler_valid(&ffh) && flexfile_handler_get_loff(&ffh) < end) {
        if (flexfile_handler_get_tag(&ffh, &tag) == 0 && flexdb_file_tag_get_anchor(tag)) {
            const u64 loff = flexfile_handler_get_loff(&ffh);
            const u8 unsorted = flexdb_file_tag_get_unsorted(tag);
            struct kv *const kv = flexdb_read_kv(&ffh, kvbuf1, kvbuf2);
            debug_assert(kv);
            struct kv *anchor = NULL;
            if (loff > 0) {
                anchor = kv_dup_key(kv);
                kv_update_hash(anchor);
            }
            info->anchors[count++] =
                (struct flexdb_recovery_anchor){.anchor = anchor, .loff = loff, .unsorted = unsorted};
            if (count == cap) {
                cap <<= 1;
                info->anchors = realloc(info->anchors, cap * sizeof(info->anchors[0]));
                debug_assert(info->anchors);
            }
        }
        flexfile_handler_forward_extent(&ffh);
    }
    info->count = count;
    free(kvbuf1);
    free(kvbuf2);
    return NULL;
}

static void flexdb_recovery(struct flexdb *const db)
{
    // concurrent rebuild
    pthread_t workers[FLEXDB_RECOVERY_WORKER_COUNT];
    struct flexdb_recovery_worker_info info[FLEXDB_RECOVERY_WORKER_COUNT];
    const u64 filesz = flexfile_size(db->flexfile);
    const u64 plen = filesz / FLEXDB_RECOVERY_WORKER_COUNT;
    for (u32 i=0; i<FLEXDB_RECOVERY_WORKER_COUNT; i++) {
        info[i].start = i * plen;
        info[i].end = (i == FLEXDB_RECOVERY_WORKER_COUNT - 1) ? filesz : info[i].start + plen;
        info[i].db = db;
        int r = pthread_create(&workers[i], NULL, flexdb_recovery_worker, (void *)(&info[i]));
        debug_assert(r == 0);
        (void)r;
    }
    for (u32 i=0; i<FLEXDB_RECOVERY_WORKER_COUNT; i++) {
        pthread_join(workers[i], NULL);
        //printf("count %lu\n", info[i].count);
    }
    //printf("co reco time %lf\n", (double)time_diff_nsec(t)/1e9);

    u64 last_anchor_rloff = 0;
    const struct kref nullref = kv_kref(kv_null());
    struct flexdb_tree_node_handler nh;
    flexdb_tree_find_anchor_pos(db->tree, &nullref, &nh);
    for (u64 i=0; i<FLEXDB_RECOVERY_WORKER_COUNT; i++) {
        for (u64 j=0; j<info[i].count; j++) {
            const u64 anchor_loff = info[i].anchors[j].loff;
            last_anchor_rloff = anchor_loff;
            if (anchor_loff == 0) {
                // special case, the very first key, now nh points to it
                struct flexdb_tree_anchor *const anchor = nh.node->leaf_entry.anchors[nh.idx];
                anchor->unsorted = info[i].anchors[j].unsorted;
            } else {
                // otherwise, it is a normal anchor
                struct kv *const new_anchor_key = info[i].anchors[j].anchor;
                const struct kref kref = kv_kref(new_anchor_key);
                struct flexdb_tree_anchor *const anchor = nh.node->leaf_entry.anchors[nh.idx];
                debug_assert(anchor->psize == 0);
                // update the old anchor's psize
                anchor->psize = (u32)(anchor_loff - (anchor->loff + (u64)nh.shift));
                // insert new anchor
                nh.idx++;
                struct flexdb_tree_anchor *const new_anchor =
                    flexdb_tree_handler_insert(&nh, new_anchor_key, anchor_loff, 0); // psize temporarily be 0
                new_anchor->unsorted = info[i].anchors[j].unsorted;
                nh.idx--;
                // make nh points to it
                flexdb_tree_node_handler_next_anchor(db->tree, &nh, &kref);
            }
        }
    }
    struct flexdb_tree_anchor *const last_anchor = nh.node->leaf_entry.anchors[nh.idx];
    // now nh points to the last anchor, update the real last one
    last_anchor->psize = (u32)(flexfile_size(db->flexfile) - last_anchor_rloff);
    // printf("index rebuild time %lf\n", (double)time_diff_nsec(t)/1e9);

    for (u32 i=0; i<FLEXDB_RECOVERY_WORKER_COUNT; i++) {
        free(info[i].anchors);
    }

    // redo log
    const int fd1 = db->memtables[0].log_fd;
    const int fd2 = db->memtables[1].log_fd;
    const off_t r1 = lseek(fd1, 0, SEEK_END);
    const off_t r2 = lseek(fd2, 0, SEEK_END);
    if (r1 > 64) {
        u64 t1 = 0;
        pread(fd1, &t1, sizeof(t1), 0);
        if (r2 > 64) {
            // both dirty
            u64 t2 = 0;
            pread(fd2, &t2, sizeof(t2), 0);
            if (t1 > t2) {
                flexdb_log_redo(db, fd1);
                flexdb_log_redo(db, fd2);
            } else {
                flexdb_log_redo(db, fd2);
                flexdb_log_redo(db, fd1);
            }
        } else {
            flexdb_log_redo(db, fd1);
        }
    } else if (r2 > 64) {
        flexdb_log_redo(db, fd2);
    }
    // flexdb_cache_destroy(db->cache);
    // db->cache = flexdb_cache_create(db, cache_cap_mb);
}

static void flexdb_recovery_sanity_check(const struct flexdb *const db)
{
    if (FLEXDB_RECOVERY_SANITY_CHECK == 0) {
        return;
    }
    printf("flexdb recovery sanity check\n");
    // 1. check the flextree is consistent or not
    u64 ext_len = 0;
    struct flextree_pos fp = flextree_pos_get_ll(db->flexfile->flextree, 0);
    while (flextree_pos_valid_ll(&fp)) {
        const struct flextree_extent *const ext = &fp.node->leaf_entry.extents[fp.idx];
        ext_len += ext->len;
        flextree_pos_forward_extent_ll(&fp);
    }
    printf("ft check max_loff %lu and ext_len %lu\n", db->flexfile->flextree->max_loff, ext_len);

    // 2. make sure that flextree size is same to the recovered sparse index
    u64 anchor_psize = 0;
    const struct flexdb_tree_node *node = db->tree->leaf_head;
    while (node) {
        for (u32 i=0; i<node->count; i++) {
            const struct flexdb_tree_anchor *anchor = node->leaf_entry.anchors[i];
            anchor_psize += anchor->psize;
        }
        node = node->leaf_entry.next;
    }
    printf("ft check max_loff %lu and anchor_psize %lu\n", db->flexfile->flextree->max_loff, anchor_psize);
    fflush(stdout);
}

// }}} recovery

// {{{ memtable

#ifdef FLEXDB_MEMTABLE_WORMHOLE
#define flexdb_memtable_kvmap_create(...) wormhole_create(__VA_ARGS__)
#define flexdb_memtable_kvmap_api kvmap_api_wormhole
#define flexdb_memtable_kvmap_api_safe  kvmap_api_whsafe
#define flexdb_memtable_kvmap_api_unsafe  kvmap_api_whunsafe
#else
#define flexdb_memtable_kvmap_create(...) skiplist_create(__VA_ARGS__)
#define flexdb_memtable_kvmap_api kvmap_api_skiplist
#define flexdb_memtable_kvmap_api_safe  kvmap_api_skipsafe
#define flexdb_memtable_kvmap_api_unsafe  kvmap_api_skiplist
#endif

// create, destroy, iter, ref, unref

static struct flexdb_memtable *flexdb_memtable_init(
        struct flexdb *const db, struct flexdb_memtable *const memtable, const int fd)
{
    memset(memtable, 0, sizeof(*memtable));
    memtable->db = db;
    memtable->map = flexdb_memtable_kvmap_create(NULL);
    debug_assert(memtable->map);
    memtable->log_fd = fd;
    memtable->log_buffer = malloc(2*FLEXDB_MEMTABLE_LOG_BUFFER_CAP);
    memtable->log_buffer_size = 0;
    spinlock_init(&memtable->log_buffer_lock);
    memtable->size = 0;
    memtable->hidden = 1;
    // memtable->count = 0;
    return memtable;
}

static void flexdb_memtable_destroy(struct flexdb_memtable *const memtable)
{
    flexdb_memtable_kvmap_api.destroy(memtable->map);
    free(memtable->log_buffer);
}

static inline u8 flexdb_memtable_full(const struct flexdb *const db)
{
    return (db->memtables[db->active_memtable].size >= FLEXDB_MEMTABLE_CAP);
}

struct flexdb_memtable_put_info {
    struct kv *kv;
    u32 osize;
    u32 size;
};

static inline struct kv *flexdb_memtable_put_merge_func(struct kv *const kv, void *const priv)
{
    struct flexdb_memtable_put_info *const info = priv;
    info->osize = kv ? (u32)kv_size(kv) : 0;
    return info->kv;
}

static void flexdb_memtable_log_buffer_flush(struct flexdb_memtable *const memtable)
{
    if (memtable->log_buffer_size == 0) return;
    const ssize_t r = write(memtable->log_fd, memtable->log_buffer, memtable->log_buffer_size);
    debug_assert((u32)r == memtable->log_buffer_size);
    (void)r;
    memtable->log_buffer_size = 0;
}

static void flexdb_memtable_log_append(struct flexdb_memtable *const memtable, struct kv *const kv)
{
    if (!kv) {
        return;
    }
    // write log
    spinlock_lock(&memtable->log_buffer_lock);
    if (memtable->log_buffer_size >= FLEXDB_MEMTABLE_LOG_BUFFER_CAP) {
        flexdb_memtable_log_buffer_flush(memtable);
    }
    size_t psize = 0;
    u8 *pkv = kv128_encode_kv(kv, &memtable->log_buffer[memtable->log_buffer_size], &psize);
    debug_assert(psize > 0 && pkv);
    (void)pkv;
    memtable->log_buffer_size += psize;
    spinlock_unlock(&memtable->log_buffer_lock); // ...
}


// must be the active one, so use safe api
static int flexdb_memtable_put(struct flexdb_ref *const dbref, struct kv *const kv)
{
    struct flexdb_memtable *const memtable = &dbref->db->memtables[dbref->db->active_memtable];
    flexdb_memtable_log_append(memtable, kv);
    struct flexdb_memtable_put_info info = {kv, 0, (u32)kv_size(kv)};
    const struct kref kref = kv_kref(kv);
    const bool ret = flexdb_memtable_kvmap_api_safe.merge(
            dbref->mrefs[dbref->db->active_memtable], &kref, flexdb_memtable_put_merge_func, &info);
    if (!ret) {
        return -1;
    }
    memtable->size += (info.size - info.osize);
    if (memtable->hidden == 1) {
        memtable->hidden = 0;
    }
    // if (info.osize == 0) memtable->count++;
    return 0;
}

static inline void flexdb_memtable_inp_get_func(struct kv *const kv, void *const priv)
{
    struct kv **const tkv = priv;
    *tkv = kv;
}

static inline void flexdb_memtable_inp_probe_func(struct kv *const kv, void *const priv)
{
    u8 *const stat = priv;
    if (!kv) {
        *stat = 0;
    } else {
        if (kv->vlen == 0) {
            *stat = 1;
        }
        else {
            *stat = 2;
        }
    }
}

// either active one, use safe api
static struct kv *flexdb_memtable_get(
        const struct flexdb_ref *const dbref, const struct kref *const kref, u8 *const buf)
{
    const struct flexdb *const db = dbref->db;
    const u32 active = db->active_memtable;
    if (db->memtables[active].hidden == 1) {
        return NULL;
    }
    return flexdb_memtable_kvmap_api_safe.get(dbref->mrefs[active], kref, (struct kv *)buf);
}

// or the inactive one, can safely use unsafe api
static struct kv *flexdb_memtable_geti(
        const struct flexdb_ref *const dbref, const struct kref *const kref, u8 *const buf)
{
    const struct flexdb *const db = dbref->db;
    const u32 inactive = 1 - db->active_memtable;
    if (db->memtables[inactive].hidden == 1) {
        return NULL;
    }
    return flexdb_memtable_kvmap_api_unsafe.get(db->memtables[inactive].map, kref, (struct kv *)buf);
}

static void flexdb_memtable_flush_cache_warmup(
        const struct flexdb *const db, const struct kref *const kref, struct flexdb_tree_node_handler *const nh)
{
    flexdb_tree_node_handler_next_anchor(db->tree, nh, kref);
    debug_assert(nh->node);
    struct flexdb_tree_anchor *const anchor = nh->node->leaf_entry.anchors[nh->idx];
    // cache
    struct flexdb_cache_partition *const partition = flexdb_cache_get_partition(db->cache, anchor);
    struct flexdb_cache_entry *const fce = flexdb_cache_partition_get_entry_unsorted(
            partition, anchor, anchor->loff + (u64)nh->shift, db->itvbuf, db->priv);
    if (fce) {
        flexdb_cache_entry_set_access_warmup(fce);
        flexdb_cache_partition_release_entry(fce);
    }
}

static int flexdb_memtable_flush_r(
        struct flexdb *const db, const struct kv *const kv, struct flexdb_tree_node_handler *const nh)
{
    int r = 0;
    if (kv->vlen == 0) { // deletion
        struct kref kref;
        kref_ref_kv_hash32(&kref, kv);
        r = flexdb_delete_passthrough(db, &kref, nh);
    } else { // put or update
        r = flexdb_put_passthrough(db, kv, nh);
    }
    if (r != 0) {
        printf("memtable flush failed\n");
        exit(1);
    }
    return r;
}

// must be inactive one, so use unsafe api directly
static void flexdb_memtable_flush(struct flexdb *const db)
{
    // now, no locks are held
    struct flexdb_memtable *const memtable = &db->memtables[1-db->active_memtable];
    void *const iter = flexdb_memtable_kvmap_api_unsafe.iter_create(memtable->map);
    const struct kref nullref = kv_kref(kv_null());
    flexdb_memtable_kvmap_api_unsafe.iter_seek(iter, &nullref);
    struct flexdb_tree_node_handler nh;
    flexdb_tree_find_anchor_pos(db->tree, &nullref, &nh);

    const struct kv *batch[FLEXDB_MEMTABLE_FLUSH_BATCH];
    u32 count = 0;
    struct kv *kv = NULL;

    while (flexdb_memtable_kvmap_api_unsafe.iter_inp(iter, flexdb_memtable_inp_get_func, &kv)) {
        // check if the active table is full or not
        if (flexdb_memtable_full(db)) {
            break;
            // now, the batch has content, and the iterator might be pointing to something
        }

        batch[count++] = kv;
        if (count == FLEXDB_MEMTABLE_FLUSH_BATCH) {
            struct flexdb_tree_node_handler tnh = nh;
            for (u32 i=0; i<count; i++) {
                const struct kref kref = kv_kref(batch[i]);
                flexdb_memtable_flush_cache_warmup(db, &kref, &tnh);
            }

            // do real flush
            for (u32 i=0; i<FLEXDB_LOCK_SHARDING_COUNT; i++) {
                rwlock_lock_write_hp(&db->rwlock_flexfile[i].lock);
            }
            for (u32 i=0; i<count; i++) {
                flexdb_memtable_flush_r(db, batch[i], &nh);
            }
            for (u32 i=0; i<FLEXDB_LOCK_SHARDING_COUNT; i++) {
                rwlock_unlock_write(&db->rwlock_flexfile[i].lock);
            }

            count = 0;
        }
        flexdb_memtable_kvmap_api_unsafe.iter_skip(iter, 1);
    }

    // do remaining batches, no matther what
    struct flexdb_tree_node_handler tnh = nh;
    for (u32 i=0; i<count; i++) {
        const struct kref kref = kv_kref(batch[i]);
        flexdb_memtable_flush_cache_warmup(db, &kref, &tnh);
    }

    for (u32 i=0; i<FLEXDB_LOCK_SHARDING_COUNT; i++) {
        rwlock_lock_write_hp(&db->rwlock_flexfile[i].lock);
    }
    for (u32 i=0; i<count; i++) {
        flexdb_memtable_flush_r(db, batch[i], &nh);
    }
    // which means break was called, frontend is full, writers are hanging
    while (flexdb_memtable_kvmap_api_unsafe.iter_inp(iter, flexdb_memtable_inp_get_func, &kv)) {
        flexdb_memtable_flush_r(db, kv, &nh);
        flexdb_memtable_kvmap_api_unsafe.iter_skip(iter, 1);
    }
    for (u32 i=0; i<FLEXDB_LOCK_SHARDING_COUNT; i++) {
        rwlock_unlock_write(&db->rwlock_flexfile[i].lock);
    }

    flexdb_memtable_kvmap_api_unsafe.iter_destroy(iter);
    // return, also no lock
    return;
}

static void *flexdb_memtable_flush_worker(void *const odb)
{
    // pin to the last core on the NUMA node
    u32 cores[128];
    u32 ncores = process_getaffinity_list(128, cores);
    u32 core = 0;
    for (u32 i=0; i<ncores; i++) {
        core = cores[i];
    }
    thread_pin(core);

    struct flexdb *const db = odb;
    double t = time_sec();
    while (db->flush_worker.work > 0) {
        if ((db->flush_worker.immediate_work == 1 || flexdb_memtable_full(db) ||
                time_diff_sec(t) >= FLEXDB_MEMTABLE_FLUSH_TIME) &&
                    db->memtables[db->active_memtable].hidden == 0) {
            // block all access to memtable
            for (u32 i=0; i<FLEXDB_LOCK_SHARDING_COUNT; i++) {
                rwlock_lock_write_hp(&db->rwlock_memtable[i].lock);
            }

            // swap memtable
            db->active_memtable = 1 - db->active_memtable;

            // now memtable can be accessed
            for (u32 i=0; i<FLEXDB_LOCK_SHARDING_COUNT; i++) {
                rwlock_unlock_write(&db->rwlock_memtable[i].lock);
            }

            // flush imm table
            flexdb_memtable_log_buffer_flush(&db->memtables[1-db->active_memtable]);
            fdatasync(db->memtables[1-db->active_memtable].log_fd);

            // real flush
            flexdb_memtable_flush(db);

            // persist flush
            flexfile_sync(db->flexfile);

            // truncate log
            const u64 flag = time_nsec();
            lseek(db->memtables[1-db->active_memtable].log_fd, 0, SEEK_SET);
            ftruncate(db->memtables[1-db->active_memtable].log_fd, 0);
            ssize_t r = write(db->memtables[1-db->active_memtable].log_fd, &flag, sizeof(flag));
            debug_assert(r == sizeof(flag));
            (void)r;

            // clean the imm table
            for (u32 i=0; i<FLEXDB_LOCK_SHARDING_COUNT; i++) {
                rwlock_lock_write_hp(&db->rwlock_memtable[i].lock);
            }
            db->memtables[1-db->active_memtable].hidden = 1;
            db->memtables[1-db->active_memtable].size = 0;
            for (u32 i=0; i<FLEXDB_LOCK_SHARDING_COUNT; i++) {
                rwlock_unlock_write(&db->rwlock_memtable[i].lock);
            }

            // clean the old table "asynchronously"
            flexdb_memtable_kvmap_api_unsafe.clean(db->memtables[1-db->active_memtable].map);

            t = time_sec();
        }
        db->flush_worker.immediate_work = 0;
        usleep(1000);
    }
    // now work = 0, clean up
    while (db->refcnt);

    // flush current inactive
    flexdb_memtable_log_buffer_flush(&db->memtables[1-db->active_memtable]);
    fdatasync(db->memtables[1-db->active_memtable].log_fd);
    flexdb_memtable_flush(db);
    flexfile_sync(db->flexfile);
    lseek(db->memtables[1-db->active_memtable].log_fd, 0, SEEK_SET);
    ftruncate(db->memtables[1-db->active_memtable].log_fd, 0);

    // flush current active
    db->active_memtable = 1 - db->active_memtable;
    flexdb_memtable_log_buffer_flush(&db->memtables[1-db->active_memtable]);
    fdatasync(db->memtables[1-db->active_memtable].log_fd);
    flexdb_memtable_flush(db);
    flexfile_sync(db->flexfile);
    lseek(db->memtables[1-db->active_memtable].log_fd, 0, SEEK_SET);
    ftruncate(db->memtables[1-db->active_memtable].log_fd, 0);

    return NULL;
}

// }}} memtable

// {{{ iterator

struct flexdb_file_iterator {
    struct flexdb_ref *dbref;
    struct kv *kv;
    struct flexdb_tree_node *node;
    struct flexdb_cache_entry *fce;
    u32 anchor_idx;
    u32 idx;
    u64 loff;
};

static inline void flexdb_file_iterator_set_null(struct flexdb_file_iterator *const iter)
{
    if (iter->fce) {
        flexdb_cache_partition_release_entry(iter->fce);
        iter->fce = NULL;
    }
    iter->kv = NULL;
    iter->node = NULL;
    iter->anchor_idx = 0;
    iter->idx = 0;
    iter->loff = ~(u64)0;
}

static u8 flexdb_file_iterator_skip_once(struct flexdb_file_iterator *const iter)
{
    if (iter->kv == NULL) {
        return 1;
    }
    struct flexdb_tree_anchor *const anchor = iter->node->leaf_entry.anchors[iter->anchor_idx];
    // just +1 in kv_interval of fce
    if (iter->idx + 1 < iter->fce->count) {
        iter->kv = iter->fce->kv_interval[++iter->idx];
        return 1;
    }
    const u64 new_loff = iter->loff + anchor->psize;
    // current fce has finished its job
    if (iter->fce) {
        flexdb_cache_partition_release_entry(iter->fce);
        iter->fce = NULL;
    }
    // move to next anchor in same node (iter->node unchanged)
    if (iter->anchor_idx + 1 < iter->node->count) {
        iter->anchor_idx++;
        iter->idx = 0;
        iter->loff = new_loff;
        struct flexdb_tree_anchor *const new_anchor = iter->node->leaf_entry.anchors[iter->anchor_idx];
        iter->fce = flexdb_cache_partition_get_entry(
                flexdb_cache_get_partition(iter->dbref->db->cache, new_anchor),
                new_anchor, new_loff, iter->dbref->itvbuf, iter->dbref->priv);
        if (iter->fce->count == 0) {
            return 0;
        }
        iter->kv = iter->fce->kv_interval[iter->idx];
        return 1;
    }
    // otherwise, has to move to the next node
    if (!iter->node->leaf_entry.next) {
        // no next anchor
        flexdb_file_iterator_set_null(iter);
        return 1;
    }
    // has next node and so, next anchor
    iter->node = iter->node->leaf_entry.next;
    iter->anchor_idx = 0;
    iter->idx = 0;
    iter->loff = new_loff;
    debug_assert(iter->node->count > 0);
    struct flexdb_tree_anchor *const new_anchor = iter->node->leaf_entry.anchors[0];
    iter->fce = flexdb_cache_partition_get_entry(
        flexdb_cache_get_partition(iter->dbref->db->cache, new_anchor),
        new_anchor, new_loff, iter->dbref->itvbuf, iter->dbref->priv);
    if (iter->fce->count == 0) {
        return 0;
    }
    iter->kv = iter->fce->kv_interval[0];
    return 1;
}

static void flexdb_file_iterator_skip1(struct flexdb_file_iterator *const iter)
{
    while (!flexdb_file_iterator_skip_once(iter));
}

static struct flexdb_file_iterator *flexdb_file_iterator_create(struct flexdb_ref *const dbref)
{
    struct flexdb_file_iterator *iter = malloc(sizeof(*iter));
    memset(iter, 0, sizeof(*iter));
    flexdb_file_iterator_set_null(iter);
    iter->dbref = dbref;
    return iter;
}

static void flexdb_file_iterator_seek(struct flexdb_file_iterator *const iter, const struct kref *const kref)
{
    flexdb_file_iterator_set_null(iter);
    struct flexdb_tree_node_handler nh;
    flexdb_tree_find_anchor_pos(iter->dbref->db->tree, kref, &nh);
    struct flexdb_tree_anchor *const anchor = nh.node->leaf_entry.anchors[nh.idx];
    const u64 loff = nh.node->leaf_entry.anchors[nh.idx]->loff + (u64)nh.shift;
    struct flexdb_cache_entry *const fce = flexdb_cache_partition_get_entry(
            flexdb_cache_get_partition(iter->dbref->db->cache, anchor),
            anchor, loff, iter->dbref->itvbuf, iter->dbref->priv);
    u32 idx = flexdb_cache_entry_find_key_ge(fce, kref);
    idx &= ~(FLEXDB_CACHE_ENTRY_FIND_EQ);
    // no key greater than kref
    iter->kv = fce->kv_interval[idx];
    iter->node = nh.node;
    iter->fce = fce;
    iter->anchor_idx = nh.idx;
    iter->idx = idx;
    iter->loff = loff;
    // this is a corner case, while a key to seek is going to be inserted
    // at the end of the current anchor interval
    // the idx == anchor->count but it should step further
    if (idx >= fce->count) {
        iter->kv = (struct kv *)0x1; // need to set it here to avoid seek failures
        flexdb_file_iterator_skip1(iter);
    }
}

static struct kv *flexdb_file_iterator_peek(const struct flexdb_file_iterator *const iter, struct kv *const out)
{
    struct kv *kv = iter->kv;
    if (kv) {
        if (out) {
            memcpy(out, kv, kv_size(kv));
            kv = out;
        } else {
            kv = kv_dup(kv);
        }
    }
    return kv;
}

static void flexdb_file_iterator_skip(struct flexdb_file_iterator *const iter, const u64 step)
{
    for (u64 i=0; i<step; i++) {
        flexdb_file_iterator_skip1(iter);
    }
}

static inline void flexdb_file_iterator_destroy(struct flexdb_file_iterator *const iter)
{
    flexdb_file_iterator_set_null(iter);
    free(iter);
}

static bool flexdb_file_iterator_kref(struct flexdb_file_iterator *const iter, struct kref *const kref)
{
    if (iter->kv) {
        *kref = kv_kref(iter->kv);
        return true;
    }
    return false;
}

static bool flexdb_file_iterator_kvref(struct flexdb_file_iterator *const iter, struct kvref *const kvref)
{
    if (iter->kv) {
        kvref_ref_kv(kvref, iter->kv);
        return true;
    }
    return false;
}

static u64 flexdb_file_iterator_retain(struct flexdb_file_iterator *const iter)
{
    if (!iter->fce) {
        return 0lu;
    }
    flexdb_cache_entry_inc_refcnt(iter->fce);
    return (u64)iter->fce;
}

static void flexdb_file_iterator_release(struct flexdb_file_iterator *const iter, const u64 opaque)
{
    if (opaque == 0) {
        return;
    }
    flexdb_cache_entry_dec_refcnt((struct flexdb_cache_entry *)opaque);
    (void)iter;
}

static const struct kvmap_api kvmap_api_flexdb_file_iterator = {
    // .hashkey = true,
    .readonly = true,
    .ordered = true,
    .unique = true,
    .iter_create = (void *)flexdb_file_iterator_create,
    .iter_seek = (void *)flexdb_file_iterator_seek,
    .iter_peek = (void *)flexdb_file_iterator_peek,
    .iter_skip1 = (void *)flexdb_file_iterator_skip1,
    .iter_skip = (void *)flexdb_file_iterator_skip,
    .iter_destroy = (void *)flexdb_file_iterator_destroy,
    .iter_kref = (void *)flexdb_file_iterator_kref,
    .iter_kvref = (void *)flexdb_file_iterator_kvref,
    .iter_park = (void *)flexdb_file_iterator_set_null,
    .iter_retain = (void *)flexdb_file_iterator_retain,
    .iter_release = (void *)flexdb_file_iterator_release,
};

struct flexdb_iterator *flexdb_iterator_create(struct flexdb_ref *const dbref)
{
    struct flexdb_iterator *const iter = malloc(sizeof(*iter));
    memset(iter, 0, sizeof(*iter));
    iter->dbref = dbref;
    iter->miter = miter_create();
    iter->status.parked = 1;
    iter->status.a = (u8)(~(u8)0u);
    iter->status.h1 = (u8)(~(u8)0u);
    iter->status.h2 = (u8)(~(u8)0u);
    iter->status.h2 = (u8)(~(u8)0u);
    iter->status.mt_lockid = (u32)(~(u32)0u);
    iter->status.ff_lockid = (u32)(~(u32)0u);
    return iter;
}

static void flexdb_iterator_update(struct flexdb_iterator *const iter)
{
    miter_kvref(iter->miter, &iter->kvref);
    while (iter->kvref.hdr.vlen == 0) {
        miter_skip_unique(iter->miter);
        if (!miter_kvref(iter->miter, &iter->kvref)) {
            break;
        }
    }
}

void flexdb_iterator_seek(struct flexdb_iterator *const iter, const struct kref *const kref)
{
    if (iter->status.parked == 1) {
        // parked, grab locks
        iter->status.mt_lockid = flexdb_enter_memtable(iter->dbref, kref->hash32);
        iter->status.ff_lockid = flexdb_enter_flexfile(iter->dbref, kref->hash32);

        const struct flexdb *const db = iter->dbref->db;
        const u32 active = db->active_memtable;

        // if status changed from last locking
        if (iter->status.a != active || iter->status.h1 != db->memtables[active].hidden ||
                iter->status.h2 != db->memtables[1-active].hidden) {
            // re-add iters
            miter_clean(iter->miter);
            miter_add(iter->miter, &kvmap_api_flexdb_file_iterator, iter->dbref);
            if (db->memtables[1-active].hidden == 0) {
                miter_add(iter->miter, &flexdb_memtable_kvmap_api_unsafe, db->memtables[1-active].map);
            }
            if (db->memtables[active].hidden == 0) {
                miter_add_ref(iter->miter, &flexdb_memtable_kvmap_api_safe, iter->dbref->mrefs[active]);
            }
            // update status
            iter->status.a = (u8)active;
            iter->status.h1 = db->memtables[active].hidden;
            iter->status.h2 = db->memtables[1-active].hidden;
        }
        iter->status.parked = 0;
    }
    miter_seek(iter->miter, kref);
    flexdb_iterator_update(iter);
}

inline struct kv *flexdb_iterator_peek(const struct flexdb_iterator *const iter, struct kv *const out)
{
    return miter_peek(iter->miter, out);
}

void flexdb_iterator_skip(struct flexdb_iterator *const iter, const u64 step)
{
    for (u64 i=0; i<step; i++) {
        miter_skip_unique(iter->miter);
    }
    flexdb_iterator_update(iter);
}

struct kv *flexdb_iterator_next(struct flexdb_iterator *const iter, struct kv *const out)
{
    struct kv *const ret = flexdb_iterator_peek(iter, out);
    flexdb_iterator_skip(iter, 1);
    return ret;
}

void flexdb_iterator_destroy(struct flexdb_iterator *const iter)
{
    // destroy miter
    if (iter->status.parked == 0) {
        flexdb_iterator_park(iter);
    }
    miter_destroy(iter->miter);
    free(iter);
}

inline bool flexdb_iterator_valid(const struct flexdb_iterator *const iter)
{
    return (miter_valid(iter->miter) && iter->status.parked == 0) ? true : false;
}

void flexdb_iterator_park(struct flexdb_iterator *const iter)
{
    if (iter->status.parked == 1) return;
    miter_park(iter->miter); // only park here, no need for clean
    flexdb_exit_flexfile(iter->dbref, iter->status.ff_lockid);
    flexdb_exit_memtable(iter->dbref, iter->status.mt_lockid);
    iter->status.mt_lockid = (u32)(~(u32)0u);
    iter->status.ff_lockid = (u32)(~(u32)0u);

    iter->status.parked = 1;
}

// }}} iterator

// db {{{

struct flexdb *flexdb_open(const char *const path, const u64 cache_cap_mb)
{
    debug_assert(path);
    struct flexdb *const db = malloc(sizeof(*db));
    memset(db, 0, sizeof(*db));
    u8 new = 0;
    if (access(path, F_OK) == -1) {
        new = 1;
        int r = mkdir(path, 0755);
        debug_assert(r == 0);
        if (r != 0) {
            return NULL;
        }
    }

    db->path = malloc((strlen(path)+1)*sizeof(char));
    strcpy(db->path, path);

    char buf[1024];

    // open flexfile
    sprintf(buf, "%s/%s", db->path, "FLEXFILE");
    db->flexfile = flexfile_open(buf);
    debug_assert(db->flexfile);

    // create an in-memory kv index
    db->tree = flexdb_tree_create(db); // pure in-memory structure
    debug_assert(db->tree);

    // open memtable
    // note: currently fd1 and fd2 is not usable
    sprintf(buf, "%s/%s", db->path, "LOG1");
    const int fd1 = open(buf, O_RDWR | O_CREAT, 0644);
    debug_assert(fd1);
    flexdb_memtable_init(db, &db->memtables[0], fd1);

    sprintf(buf, "%s/%s", db->path, "LOG2");
    const int fd2 = open(buf, O_RDWR | O_CREAT, 0644);
    debug_assert(fd2);
    flexdb_memtable_init(db, &db->memtables[1], fd2);
    db->active_memtable = 0;

    // open cache
    db->cache = flexdb_cache_create(db, cache_cap_mb);
    debug_assert(db->cache);

    // init locks
    for (u32 i=0; i<FLEXDB_LOCK_SHARDING_COUNT; i++) {
        rwlock_init(&db->rwlock_flexfile[i].lock);
        rwlock_init(&db->rwlock_memtable[i].lock);
    }

    // alloc kv bufs
    db->kvbuf1 = malloc(FLEXDB_MAX_KV_SIZE);
    db->kvbuf2 = malloc(FLEXDB_MAX_KV_SIZE);
    db->itvbuf = malloc(FLEXDB_TREE_SPARSE_INTERVAL_SIZE + FLEXDB_MAX_KV_SIZE);

    // priv (rring)
#if defined(FLEXFILE_IO_URING) && defined(FLEXDB_USE_RRING)
    struct flexfile_rring *const rring = flexfile_rring_create(
            db->flexfile, db->itvbuf, FLEXDB_TREE_SPARSE_INTERVAL_SIZE + FLEXDB_MAX_KV_SIZE);
#else
    void *const rring = NULL;
#endif
    db->priv = rring;

    // rebuild index and redo log
    if (!new) {
        flexdb_recovery(db);
        flexdb_recovery_sanity_check(db);
    } else {
        // don't forget to tag the very first interval
        const u16 tag = flexdb_file_tag_generate(1, 0);
        flexfile_set_tag(db->flexfile, 0, tag);
    }

    // reset log
    lseek(fd1, 0, SEEK_SET);
    lseek(fd2, 0, SEEK_SET);
    int r = ftruncate(fd1, 0);
    debug_assert(r == 0);
    r = ftruncate(fd2, 0);
    debug_assert(r == 0);
    u64 flag = time_nsec();
    ssize_t r2 = write(fd1, &flag, sizeof(flag));
    debug_assert(r2 == sizeof(flag));
    flag = time_nsec();
    r2 = write(fd2, &flag, sizeof(flag));
    debug_assert(r2 == sizeof(flag));

    // start flush worker
    db->flush_worker.work = 1;
    db->flush_worker.immediate_work = 0;
    r = pthread_create(&db->flush_worker.thread, NULL, flexdb_memtable_flush_worker, (void *)db);
    debug_assert(r == 0);

    return db;
    (void)r;
    (void)r2;
}

struct flexdb_ref *flexdb_ref(struct flexdb *const db)
{
    if (db->flush_worker.work == 0) return NULL;
    struct flexdb_ref *const dbref = malloc(sizeof(*dbref));
    dbref->db = db;
    dbref->kvbuf = malloc(FLEXDB_MAX_KV_SIZE);
    dbref->itvbuf = malloc(FLEXDB_TREE_SPARSE_INTERVAL_SIZE + FLEXDB_MAX_KV_SIZE);
#if defined(FLEXFILE_IO_URING) && defined(FLEXDB_USE_RRING)
    struct flexfile_rring *const rring = flexfile_rring_create(
            db->flexfile, dbref->itvbuf, FLEXDB_TREE_SPARSE_INTERVAL_SIZE + FLEXDB_MAX_KV_SIZE);
#else
    void *const rring = NULL;
#endif
    dbref->priv = rring;

    dbref->mrefs[0] = kvmap_ref(&flexdb_memtable_kvmap_api_safe, db->memtables[0].map);
    dbref->mrefs[1] = kvmap_ref(&flexdb_memtable_kvmap_api_safe, db->memtables[1].map);
    db->refcnt++;
    return dbref;
}

struct flexdb *flexdb_deref(struct flexdb_ref *const dbref)
{
    struct flexdb *const db = dbref->db;
    db->refcnt--;
    free(dbref->kvbuf);
    free(dbref->itvbuf);
#if defined(FLEXFILE_IO_URING) && defined(FLEXDB_USE_RRING)
    flexfile_rring_destroy(dbref->priv);
#endif
    free(dbref);
    return db;
}

struct kv *flexdb_read_kv(const struct flexfile_handler *const ffh, u8 *const buf, struct kv *const out)
{
    if (!buf || !out) return NULL;

    ssize_t r = flexfile_handler_read(ffh, buf, 16);
    const u32 psize = (u32)kv128_size(buf);
    r = flexfile_handler_read(ffh, buf, (u64)psize);
    debug_assert((u32)r == psize);

    size_t ppsize = 0;
    struct kv *const kv = kv128_decode_kv(buf, out, &ppsize);
    debug_assert(ppsize == psize);
    debug_assert(kv == out);

    return kv;
    (void)r;
}

int flexdb_put(struct flexdb_ref *const dbref, struct kv *const kv)
{
    while (flexdb_memtable_full(dbref->db)) {
        cpu_pause();
    }
    if (kv_size(kv) >= FLEXDB_MAX_KV_SIZE) {
        printf("fatal: too large kv, ignore\n");
        return 1;
    }
    const u32 lockid = flexdb_enter_memtable(dbref, kv->hashlo);
    int r = flexdb_memtable_put(dbref, kv);
    flexdb_exit_memtable(dbref, lockid);
    debug_assert(r == 0);
    return r;
}

void flexdb_sync(struct flexdb_ref *const dbref)
{
    const u32 lockid = flexdb_enter_flexfile(dbref, (u32)rand());
    dbref->db->flush_worker.immediate_work = 1;
    flexdb_exit_flexfile(dbref, lockid);
    while (dbref->db->flush_worker.immediate_work)
      cpu_pause();
}

struct kv *flexdb_get(struct flexdb_ref *const dbref, const struct kref *const kref, struct kv *const out)
{
    struct kv *kv = NULL;
    u8 *const buf = out ? (u8 *)out : dbref->kvbuf;
    if (!dbref->db->memtables[0].hidden || !dbref->db->memtables[1].hidden) {
        const u32 lockid1 = flexdb_enter_memtable(dbref, kref->hash32);
        kv = flexdb_memtable_get(dbref, kref, buf);
        if (!kv) {
            kv = flexdb_memtable_geti(dbref, kref, buf);
        }
        flexdb_exit_memtable(dbref, lockid1);
    }
    if (!kv) {
        const u32 lockid2 = flexdb_enter_flexfile(dbref, kref->hash32);
        kv = flexdb_get_passthrough(dbref, kref, buf);
        flexdb_exit_flexfile(dbref, lockid2);
    }
    else if (kv->vlen == 0) {
        kv = NULL;
    }

    if (kv && !out) {
        kv = kv_dup((struct kv *)buf);
    }
    return kv;
}

unsigned int flexdb_probe(struct flexdb_ref *const dbref, const struct kref *const kref)
{
    const u32 lockid1 = flexdb_enter_memtable(dbref, kref->hash32);
    u8 ret = 0;
    u8 mprobe = 0;
    const u32 active = dbref->db->active_memtable;
    // safe api
    flexdb_memtable_kvmap_api_safe.inpr(dbref->mrefs[active], kref, flexdb_memtable_inp_probe_func, &mprobe);
    if (mprobe == 0) {
        // unsafe api
        flexdb_memtable_kvmap_api_unsafe.inpr(
                dbref->db->memtables[1-active].map, kref, flexdb_memtable_inp_probe_func, &mprobe);
    }
    flexdb_exit_memtable(dbref, lockid1);
    if (mprobe == 2) {
        ret = 1;
    }
    else if (mprobe == 1) {
        ret = 0;
    }
    else {
        const u32 lockid2 = flexdb_enter_flexfile(dbref, kref->hash32);
        ret = flexdb_probe_passthrough(dbref, kref);
        flexdb_exit_flexfile(dbref, lockid2);
    }
    return (unsigned int)ret;
}

int flexdb_delete(struct flexdb_ref *const dbref, const struct kref *const kref)
{
    // no need to lock here!
    struct kv *const tombstone = (struct kv *)dbref->kvbuf;
    tombstone->klen = kref->len;
    tombstone->vlen = 0;
    memcpy(tombstone->kv, kref->ptr, kref->len);
    const int ret = flexdb_put(dbref, tombstone);
    debug_assert(ret == 0);
    return ret;
}

void flexdb_close(struct flexdb *const db)
{
    // sync everything
    struct flexdb_ref *const dbref = flexdb_ref(db);
    flexdb_sync(dbref);
    flexdb_deref(dbref);
    // stop flush worker
    db->flush_worker.work = 0;
    pthread_join(db->flush_worker.thread, NULL);
    // destroy memtable
    flexdb_memtable_log_buffer_flush(&db->memtables[0]);
    fdatasync(db->memtables[0].log_fd);
    close(db->memtables[0].log_fd);
    flexdb_memtable_destroy(&db->memtables[0]);
    flexdb_memtable_log_buffer_flush(&db->memtables[1]);
    fdatasync(db->memtables[1].log_fd);
    close(db->memtables[1].log_fd);
    flexdb_memtable_destroy(&db->memtables[1]);
    // destroy cache
    flexdb_cache_destroy(db->cache);
    // check consistency for next open
    flexdb_recovery_sanity_check(db);
    // destroy kv index
    flexdb_tree_destroy(db->tree);
#if defined(FLEXFILE_IO_URING) && defined(FLEXDB_USE_RRING)
    // destroy rring before closing the file..
    flexfile_rring_destroy(db->priv);
#endif
    // close flexfile
    flexfile_close(db->flexfile);
    free(db->path);
    free(db->kvbuf1);
    free(db->kvbuf2);
    free(db->itvbuf);
    free(db);
}

inline void flexdb_fprint(const struct flexdb *const db, FILE *const f)
{
    (void)f;
    (void)db;
}

struct flexdb_merge_ctx {
    kv_merge_func uf;
    void *priv;
    struct kv *old;
    struct kv *new;
    u8 second; // the seocnd merge does not require an existing key
    u8 merged;
};

static struct kv *flexdb_merge_merge_func(struct kv * const kv0, void *const priv)
{
    struct flexdb_merge_ctx *const ctx = priv;
    if (kv0 || ctx->second) {
        struct kv * const ret = ctx->uf(kv0 ? kv0 : ctx->old, ctx->priv);
        ctx->merged = 1;
        ctx->new = ret; // will write log
        return ret;
    } else { // must retry with GET in imm and sparse index
        return kv0;
    }
}

bool flexdb_merge(
        struct flexdb_ref *const dbref, const struct kref *const kref, kv_merge_func uf, void *const priv)
{
    struct flexdb_merge_ctx ctx = {.uf = uf, .priv = priv};
    struct flexdb *const db = dbref->db;
    const u32 lockid1 = flexdb_enter_memtable(dbref, kref->hash32);

    void *const mref = dbref->mrefs[db->active_memtable];
    struct flexdb_memtable *const memtable = &dbref->db->memtables[dbref->db->active_memtable];

    const bool r1 = flexdb_memtable_kvmap_api_safe.merge(mref, kref, flexdb_merge_merge_func, &ctx);
    if (ctx.merged) {
        debug_assert(ctx.new);
        flexdb_memtable_log_append(memtable, ctx.new);
        flexdb_exit_memtable(dbref, lockid1);
        return true;
    }
    if (!r1) {
        flexdb_exit_memtable(dbref, lockid1);
        return r1; // false
    }

    // not succeed in the active table, now see the imm table and flexfile

    struct kv *old = db->memtables[1-db->active_memtable].hidden ?
        NULL : flexdb_memtable_geti(dbref, kref, dbref->kvbuf);

    // now holds the lock for memtable, but no need to release
    if (!old) {
        const u32 lockid2 = flexdb_enter_flexfile(dbref, kref->hash32);
        old = flexdb_get_passthrough(dbref, kref, dbref->kvbuf);
        flexdb_exit_flexfile(dbref, lockid2);
    }
    ctx.old = old;
    ctx.second = 1;

    const bool r2 = flexdb_memtable_kvmap_api_safe.merge(mref, kref, flexdb_merge_merge_func, &ctx);
    debug_assert(ctx.new);
    flexdb_memtable_log_append(memtable, ctx.new);
    flexdb_exit_memtable(dbref, lockid1);
    return r2;
}

// }}} db

// {{{ kvmap_api

static bool kvmap_flexdb_put(void *const mapref, struct kv *const kv)
{
    int ret = flexdb_put((struct flexdb_ref *)mapref, kv);
    return (ret == 0) ? true : false;
}

static bool kvmap_flexdb_del(void *const mapref, const struct kref *const kref)
{
    int ret = flexdb_delete((struct flexdb_ref *)mapref, kref);
    return (ret == 0) ? true : false;
}

static bool kvmap_flexdb_probe(void *const mapref, const struct kref *const kref)
{
    unsigned int ret = flexdb_probe((struct flexdb_ref *)mapref, kref);
    return (ret == 1) ? true : false;
}

const struct kvmap_api kvmap_api_flexdb = {
    .hashkey = true,
    .put = (void *)kvmap_flexdb_put,
    .del = (void *)kvmap_flexdb_del,
    .probe = (void *)kvmap_flexdb_probe,
    .get = (void *)flexdb_get,
    .ref = (void *)flexdb_ref,
    .unref = (void *)flexdb_deref,
    .destroy = (void *)flexdb_close,
    .iter_create = (void *)flexdb_iterator_create,
    .iter_seek = (void *)flexdb_iterator_seek,
    .iter_valid = (void *)flexdb_iterator_valid,
    .iter_peek = (void *)flexdb_iterator_peek,
    .iter_skip = (void *)flexdb_iterator_skip,
    .iter_next = (void *)flexdb_iterator_next,
    .iter_destroy = (void *)flexdb_iterator_destroy,
    .iter_park = (void *)flexdb_iterator_park,
    .fprint = (void *)flexdb_fprint,
    .merge = (void *)flexdb_merge,
    .sync = (void *)flexdb_sync,
};

static void *flexdb_kvmap_api_create(const char *const name, const struct kvmap_mm *const mm, char **argv)
{
    if (strcmp(name, "flexdb") != 0) {
        return NULL;
    }
    (void)mm;
    return flexdb_open(argv[0], strtoull(argv[1], NULL, 10));
}

__attribute__((constructor))
static void flexdb_kvmap_api_init(void)
{
    kvmap_api_register(2, "flexdb", "<path> <cache_size_mb>", flexdb_kvmap_api_create, &kvmap_api_flexdb);
}

// }}} kvmap_api
