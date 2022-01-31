#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "flexdb.h"

static void print_flexdb_tree_node_rec(const struct flexdb_tree_node *const node)
{
    printf("\n[Node]: %p count %u is_leaf %u\n        ", node, node->count, node->is_leaf);
    printf("flexdb_tree %p parent %p parent_id %u\n", node->tree, node->parent, node->parent_id);
    if (node->is_leaf) {
        const struct flexdb_tree_leaf_entry *const le = &node->leaf_entry;
        for (u32 i=0; i<node->count; ++i) {
            if (le->anchors[i]->key->klen > 0) {
                printf("  anchor %u key %s loff %u psize %u\n",
                        i, le->anchors[i]->key->kv, le->anchors[i]->loff,
                        le->anchors[i]->psize);
            } else {
                printf("  anchor %u key nil loff %u psize %u\n",
                        i, le->anchors[i]->loff,
                        le->anchors[i]->psize);
            }
        }
    }
    else {
        const struct flexdb_tree_internal_entry *const ie = &node->internal_entry;
        printf("internal_entry\n");
        for (u32 i=0; i<node->count+1; ++i) {
            if (i == 0) {
                printf("  children %u pointer %p shift %ld\n", i, ie->children[i].node, ie->children[i].shift);
            } else {
                printf("  base %s\n", ie->pivots[i-1]->kv);
                printf("  children %u pointer %p shift %ld\n", i, ie->children[i].node, ie->children[i].shift);
            }
        }
        for (u32 i=0; i<node->count+1; ++i) {
            print_flexdb_tree_node_rec(ie->children[i].node);
        }
    }
}

static inline void print_flexdb_tree(const struct flexdb_tree *const kt)
{
    print_flexdb_tree_node_rec(kt->root);
}

static void print_flexdb_tree_count(const struct flexdb_tree *const kt)
{
    const struct flexdb_tree_node *node = kt->leaf_head;
    u32 idx = 0;
    u32 count = 0;
    const struct kv *key = NULL;
    while (node) {
        while (idx < node->count) {
            if (key) {
                if (kv_compare(key, node->leaf_entry.anchors[idx]->key) > 0) {
                    printf("wrong order!\n");
                    exit(1);
                }
            }
            key = node->leaf_entry.anchors[idx]->key;
            count++;
            idx++;
        }
        node = node->leaf_entry.next;
        idx = 0;
    }
    printf("%u\n", count);
}

static void print_flextree_kv_count(struct flexdb_ref *const dbref)
{
    flexdb_sync(dbref);
    while (!rwlock_trylock_read(&dbref->db->rwlock_flexfile[0].lock)) {
        cpu_pause();
    }
    struct flexfile_handler ffh = flexfile_get_handler(dbref->db->flexfile, 0);
    if (!ffh.file) {
        rwlock_unlock_read(&dbref->db->rwlock_flexfile[0].lock);
        return;
    }
    u64 i = 0;
    struct kv *pkey = NULL;
    u8 *buf = malloc(FLEXDB_MAX_KV_SIZE);
    struct kv *const kv = malloc(FLEXDB_MAX_KV_SIZE);
    while (ffh.fp.node) {
        struct kv *tkv = flexdb_read_kv(&ffh, buf, kv);
        if (tkv != kv) {
            printf("print_flextree_kv_count error");
            exit(1);
        }
        i++;
        //printf("%s\n", kv->kv);
        if (pkey) {
            free(pkey);
            struct kv *key = kv_dup_key(kv);
            if (kv_compare(pkey, key) > 0) {
                printf("wrong order!\n");
                exit(1);
            }
            pkey = key;
        }
        flexfile_handler_forward(&ffh, kv128_estimate_kv(kv));
    }
    free(pkey);
    free(buf);
    free(kv);

    printf("total kv %lu\n", i);
    rwlock_unlock_read(&dbref->db->rwlock_flexfile[0].lock);
}

static void rand_str(char *const buf, u32 len)
{
    const char alphabet[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    for (u32 i=0; i<len; ++i) {
        buf[i] = alphabet[(u64)rand() % strlen(alphabet)];
    }
    buf[len] = '\0';
}

static int qsort_cmp(const void *_a, const void *_b)
{
    const struct kv *a = *(struct kv**)_a;
    const struct kv *b = *(struct kv**)_b;
    return kv_compare(a, b);
}

static void test_db()
{
    struct flexdb *db = flexdb_open("/tmp/flexdb", 32);
    //pause();
    struct flexdb_ref *dbref = flexdb_ref(db);
    u64 c = 10000000;
    u64 ts = time_nsec();
    struct kv **kvps = malloc(sizeof(*kvps) * c);
    memset(kvps, 0, sizeof(*kvps) * c);
    for (u64 i=0; i<c; ++i) {
        u32 klen = 20 - (i % 10);
        u32 vlen = 20 + (i % 10);
        kvps[i] = malloc(sizeof(*kvps[i]) + klen + vlen);
        memset(kvps[i], 0, sizeof(*kvps[i]) + klen + vlen);
        kvps[i]->klen = klen;
        kvps[i]->vlen = vlen;
        rand_str((char *)kvps[i]->kv, klen-1);
        rand_str((char *)kvps[i]->kv+klen, vlen-1);
    }
    //qsort(kvps, c, sizeof(kvps[0]), qsort_cmp);
    printf("gen %lu ms\n",time_diff_nsec(ts)/1000/1000);

    ts = time_nsec();
    u64 ts2 = time_nsec();
    for (u64 i=0; i<c; i++) {
        //printf("%u: insert k %s\n", i, kvps[i]->kv); fflush(stdout);
        kv_update_hash(kvps[i]);
        flexdb_put(dbref, kvps[i]);
        if (i > 0 && i % 100000 == 0) {
            printf("%lu: w ops %lu \n", i/100000, 100000000 / (time_diff_nsec(ts2) / 1000000));
            ts2 = time_nsec();
        }
    }
    printf("average w ops %lu \n", c * 1000 * 1000 * 1000 / time_diff_nsec(ts));

    flexdb_sync(dbref);

    u64 it = 0;
    (void)it;
    struct flexdb_iterator *iter = flexdb_iterator_create(dbref);
    flexdb_iterator_park(iter);
    struct kref nkref = kv_kref(kv_null());
    for (u64 i=0; i<c; i++) {
        struct kref kk = kv_kref(kvps[i]);
        flexdb_iterator_seek(iter, &kk);
        flexdb_iterator_skip(iter, 5);
        if (flexdb_iterator_valid(iter)) it++;
    }
    flexdb_iterator_destroy(iter);

    printf("iterates over %lu\n", it);

    ts = time_nsec();
    ts2 = time_nsec();
    for (u64 i=0; i<c; i++) {
        kvps[i]->vlen--;
        flexdb_put(dbref, kvps[i]);
        if (i > 0 && i % 100000 == 0) {
            printf("%lu: u ops %lu \n", i/100000, 100000 * 1000 / (time_diff_nsec(ts2) / 1000 / 1000));
            ts2 = time_nsec();
        }
    }
    printf("average u ops %lu \n", c * 1000 *1000*1000 / time_diff_nsec(ts));
    fflush(stdout);

    print_flextree_kv_count(dbref);

    it = 0;
    iter = flexdb_iterator_create(dbref);
    nkref = kv_kref(kv_null());
    flexdb_iterator_seek(iter, &nkref);
    while (flexdb_iterator_valid(iter)) {
        free(flexdb_iterator_next(iter, NULL));
        it++;
    }
    flexdb_iterator_destroy(iter);
    printf("iterates over %lu\n", it);

    u64 hit = 0;
    ts2 = time_nsec();
    for (u64 i=0; i<c; ++i) {
        //printf("insert k %s\n", kvps[i]->kv);
        struct kref kref = kv_kref(kvps[i]);
        struct kv *r = flexdb_get(dbref, &kref, NULL);
        if (!r) {
            continue;
        }
        hit++;
        if (kv_compare(kvps[i], r) != 0) {
            printf("diff!\n");
        }
        //printf("%s\n", r->kv);
        (void)r;
        free(r);
        if (i > 0 && i % 100000 == 0) {
            printf("%lu: r ops %lu \n", i/100000, 100000 * 1000 / (time_diff_nsec(ts2) / 1000 / 1000));
            ts2 = time_nsec();
        }
    }
    printf("hit %lu total %lu\n", hit, c);

    ts2 = time_nsec();
    for (u64 i=0; i<c; i++) {
        //printf("insert k %s\n", kvps[i]->kv);
        struct kref kref = kv_kref(kvps[i]);
        flexdb_delete(dbref, &kref);
        if (i > 0 && i % 100000 == 0) {
            printf("%lu: d ops %lu \n", i/100000, 50000 * 1000 / (time_diff_nsec(ts2) / 1000 / 1000));
            ts2 = time_nsec();
        }
    }

    it = 0;
    iter = flexdb_iterator_create(dbref);
    nkref = kv_kref(kv_null());
    flexdb_iterator_seek(iter, &nkref);
    while (flexdb_iterator_valid(iter)) {
        free(flexdb_iterator_next(iter, NULL));
        it++;
    }
    flexdb_iterator_destroy(iter);
    printf("iterates over %lu\n", it);

    printf("sync\n");
    printf("%u %u\n", dbref->db->memtables[0].hidden, dbref->db->memtables[1].hidden);
    fflush(stdout);
    flexdb_sync(dbref);
    printf("%u %u\n", dbref->db->memtables[0].hidden, dbref->db->memtables[1].hidden);
    fflush(stdout);
    sleep(15);
    printf("1 1: %u %u\n", dbref->db->memtables[0].hidden, dbref->db->memtables[1].hidden);
    fflush(stdout);

    print_flexdb_tree(dbref->db->tree);

    qsort(kvps, c, sizeof(kvps[0]), qsort_cmp);

    ts2 = time_nsec();
    for (u64 i=c-1; i>=0; i--) {
        //printf("%u: insert k %s\n", i, kvps[i]->kv); fflush(stdout);
        kv_update_hash(kvps[i]);
        flexdb_put(dbref, kvps[i]);
        if (i > 0 && i % 100000 == 0) {
            printf("%lu: rev-w ops %lu \n", i/100000, 100000000 / (time_diff_nsec(ts2) / 1000000));
            ts2 = time_nsec();
        }
        if (i == 0) break;
    }

    hit = 0;
    ts2 = time_nsec();
    for (u64 i=0; i<c; ++i) {
        //printf("insert k %s\n", kvps[i]->kv);
        struct kref kref = kv_kref(kvps[i]);
        struct kv *r = flexdb_get(dbref, &kref, NULL);
        if (!r) {
            continue;
        }
        hit++;
        if (kv_compare(kvps[i], r) != 0) {
            printf("diff!\n");
        }
        if (memcmp(kvps[i]->kv+kvps[i]->klen, r->kv + r->klen, r->vlen) != 0) {
            printf("diff\n");
        }
        //printf("%s\n", r->kv);
        (void)r;
        free(r);
        if (i > 0 && i % 100000 == 0) {
            printf("%lu: r ops %lu \n", i/100000, 100000 * 1000 / (time_diff_nsec(ts2) / 1000 / 1000));
            ts2 = time_nsec();
        }
    }
    printf("hit %lu total %lu\n", hit, c);

    flexdb_deref(dbref);
    flexdb_close(db);

    printf("reopen\n");
    db = flexdb_open("/tmp/flexdb", 32);
    dbref = flexdb_ref(db);

    hit = 0;
    ts2 = time_nsec();
    for (u64 i=0; i<c; ++i) {
        struct kref kref = kv_kref(kvps[i]);
        struct kv *r = flexdb_get(dbref, &kref, NULL);
        if (!r) {
            continue;
        }
        hit++;
        if (kv_compare(kvps[i], r) != 0) {
            printf("diff!\n");
        }
        if (memcmp(kvps[i]->kv+kvps[i]->klen, r->kv + r->klen, r->vlen) != 0) {
            printf("diff\n");
        }
        //printf("%s\n", r->kv);
        (void)r;
        free(r);
        if (i > 0 && i % 100000 == 0) {
            printf("%lu: r ops %lu \n", i/100000, 100000 * 1000 / (time_diff_nsec(ts2) / 1000 / 1000));
            ts2 = time_nsec();
        }
    }
    printf("hit %lu total %lu\n", hit, c);

    it = 0;
    iter = flexdb_iterator_create(dbref);
    nkref = kv_kref(kv_null());
    flexdb_iterator_seek(iter, &nkref);
    while (flexdb_iterator_valid(iter)) {
        struct kv *kv1 = flexdb_iterator_next(iter, NULL);
        struct kref kref1 = kv_kref(kv1);
        struct kv *kv2 = flexdb_get(dbref, &kref1, NULL);
        if (strncmp((char *)kv1->kv, (char *)kv2->kv, kv1->klen + kv1->vlen) != 0) {
            printf("integrity broken at %lu\n", it);
            exit(0);
        }
        free(kv1);
        free(kv2);
        it++;
    }
    flexdb_iterator_destroy(iter);
    printf("iterates over %lu\n", it);

    print_flextree_kv_count(dbref);
    flexdb_deref(dbref);
    flexdb_close(db);

    for (u64 i=0; i<c; i++) {
        free(kvps[i]);
    }
    free(kvps);
}

int main()
{
    (void)print_flexdb_tree;
    (void)print_flexdb_tree_count;
    (void)qsort_cmp;
    test_db();
    //system("rm -rf /tmp/flexdb"); // dangerous.. though
}
