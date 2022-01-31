#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

#include "flextree.h"

#define PATH "/tmp/flextree"

volatile u64 __global_r = 0;

u64 seed = 42;

static void setrand(void)
{
    seed = 42;
}

static u64 rand_int(void)
{
    seed = (seed + 100000037);
    return seed;
    // a very fake function!
}

static void print_query_result(const struct flextree_query_result *const rr)
{
    generic_printf("count %lu loff %lu len %lu\n", rr->count, rr->loff, rr->len);
    for (u64 i=0; i<rr->count; i+=1) {
        generic_printf("%ld %ld\n", rr->v[i].poff, rr->v[i].len);
    }
}

static u8 query_result_equal(
        const struct flextree_query_result *const rr1, const struct flextree_query_result *const rr2)
{
    if (rr1 == NULL && rr2 == NULL) return 1;
    if (rr1 != rr2 && (rr1 == NULL || rr2 == NULL)) return 0;
    debug_assert(rr1);
    debug_assert(rr2);
    if (rr1->count != rr2->count) return 0;
    for (u64 i=0; i<rr1->count; i++) {
        if (rr1->v[i].poff != rr2->v[i].poff || rr1->v[i].len != rr2->v[i].len) {
            return 0;
        }
    }
    return 1;
}

static void random_insert(struct flextree *const ft, struct brute_force *const bf, const u64 count)
{
    u64 ts = 0;
    if (ft) {
        setrand();
        ts = time_nsec();
        flextree_insert(ft, 0, 0, 4);
        u64 max_loff = 4;
        for (u64 i=0; i<count; i++) {
            u32 len = (u32)(rand_int() % 1000 + 1);
            flextree_insert_wtag(ft, max_loff % 1000, max_loff, len, (u16)(i % 0xffff));
            max_loff += len;
        }
        generic_printf("insert to flextree %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);
    }

    if (bf) {
        setrand();
        ts = time_nsec();
        brute_force_insert(bf, 0, 0, 4);
        u64 max_loff = 4;
        for (u64 i=0; i<count; i++) {
            u32 len = (u32)(rand_int() % 1000 + 1);
            brute_force_insert_wtag(bf, max_loff % 1000, max_loff, len, (u16)(i % 0xffff));
            max_loff += len;
        }
        generic_printf("insert to bruteforce %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);
    }
    generic_printf("insert finished\n");
}

static void random_append(struct flextree *const ft, struct brute_force *const bf, const u64 count)
{
    u64 ts = 0;
    if (ft) {
        setrand();
        ts = time_nsec();
        flextree_insert(ft, 0, 0, 4);
        u64 max_loff = 4;
        for (u64 i=0; i<count; i++) {
            u32 len = (u32)(rand_int() % 1000 + 1);
            flextree_insert_wtag(ft, max_loff, max_loff, len, (u16)(i % 0xffff));
            max_loff += len;
        }
        generic_printf("append to flextree %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);
    }

    if (bf) {
        setrand();
        ts = time_nsec();
        brute_force_insert(bf, 0, 0, 4);
        u64 max_loff = 4;
        for (u64 i=0; i<count; i++) {
            u32 len = (u32)(rand_int() % 1000 + 1);
            brute_force_insert_wtag(bf, max_loff, max_loff, len, (u16)(i % 0xffff));
            max_loff += len;
        }
        generic_printf("append to bruteforce %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);
    }
    generic_printf("append finished\n");
}

static void random_query(struct flextree *const ft, struct brute_force *const bf, u64 total_size)
{
    if (!ft || !bf) return;
    u64 ts = 0;

    u64 *const seq = malloc(sizeof(u64) * total_size);
    for (u64 i=0; i<total_size; i++) {
        seq[i] = i;
    }

    shuffle_u64(seq, total_size);

    ts = time_nsec();
    for (u64 i=0; i<total_size; i++) {
        __global_r += flextree_pquery(ft, seq[i]);
    }
    generic_printf("random lookup flextree %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);

    ts = time_nsec();
    for (u64 i=0; i<total_size; i++) {
        __global_r += brute_force_pquery(bf, seq[i]);
    }
    generic_printf("random lookup bruteforce %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);

    generic_printf("total item searched %ld\n", total_size);

    u8 correct = 1;
    for (u64 i=0; i<total_size; i++) {
        u64 fr = flextree_pquery(ft, seq[i]);
        u64 br = brute_force_pquery(bf, seq[i]);
        if (fr != br) {
            generic_printf("Error encourted on %ld %ld %ld\n", i, fr, br);
            correct = 0;
            break;
        }
    }
    correct ? generic_printf("\033[0;32m[results correct]\033[0m\n") :
              generic_printf("\033[0;31m[results wrong]\033[0m\n");

    free(seq);
}

static void sequential_query_r(struct flextree *const ft, struct brute_force *const bf, u64 total_size, u8 vo)
{
    if (!ft || !bf) return;
    u64 ts = 0;

    if (!vo) {
        ts = time_nsec();
        for (u64 i=0; i<total_size; i++) {
            __global_r += flextree_pquery(ft, i);
        }
        generic_printf("sequential lookup flextree %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);

        ts = time_nsec();
        for (u64 i=0; i<total_size; i++) {
            __global_r += brute_force_pquery(bf, i);
        }
        generic_printf("sequential lookup bruteforce %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);

        generic_printf("total item searched %ld\n", total_size);
    }

    u8 correct = 1;
    for (u64 i=total_size; i>=0; i--) {
        u64 fr = flextree_pquery(ft, i);
        u64 br = brute_force_pquery(bf, i);
        if (fr != br) {
            generic_printf("Error encourted on %ld %ld %ld\n", i, fr, br);
            correct = 0;
            break;
        }
        if (i == 0) {
            break;
        }
    }
    correct ? generic_printf("\033[0;32m[results correct]\033[0m\n") :
              generic_printf("\033[0;31m[results wrong]\033[0m\n");
}

static void sequential_query(struct flextree *const ft, struct brute_force *const bf, u64 total_size)
{
    return sequential_query_r(ft, bf, total_size, 0);
}


static void sequential_query_vo(struct flextree *const ft, struct brute_force *const bf, u64 total_size)
{
    return sequential_query_r(ft, bf, total_size, 1);
}

static void random_range_query(
        struct flextree *const ft, struct brute_force *const bf, u64 total_size, const u64 count)
{
    if (!ft || !bf) return;
    u64 ts = 0;

    setrand();
    ts = time_nsec();
    for (u64 i=0; i<count; i++) {
        generic_free(flextree_query(ft, rand_int() % total_size, rand_int() % 100));
    }
    generic_printf("range lookup flextree %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);

    setrand();
    ts = time_nsec();
    for (u64 i=0; i<count; i++) {
        generic_free(brute_force_query(bf, rand_int() % total_size, rand_int() % 100));
    }
    generic_printf("range lookup bruteforce %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);

    u8 correct = 1;
    setrand();
    for (u64 i=0; i<count; i++) {
        u64 loff = rand_int() % total_size;
        u64 len = rand_int() % 100;
        struct flextree_query_result *fr = flextree_query(ft, loff, len);
        struct flextree_query_result *br = brute_force_query(bf, loff, len);
        if (!query_result_equal(fr, br)) {
            print_query_result(fr);
            print_query_result(br);
            correct = 0;
            generic_free(fr);
            generic_free(br);
            break;
        }
        generic_free(fr);
        generic_free(br);
    }
    correct ? generic_printf("\033[0;32m[results correct]\033[0m\n") :
              generic_printf("\033[0;31m[results wrong]\033[0m\n");
}

static void random_pdelete(
        struct flextree *const ft, struct brute_force *const bf, u64 total_size, const u64 count)
{
    u64 ts = 0;

    if (ft) {
        setrand();
        ts = time_nsec();
        for (u64 i=0; i<count; i++) {
            u64 tmp = rand_int() % total_size;
            flextree_pdelete(ft, tmp);
        }
        generic_printf("delete flextree %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);
    }

    if (bf) {
        setrand();
        ts = time_nsec();
        for (u64 i=0; i<count; i++) {
            u64 tmp = rand_int() % total_size;
            brute_force_pdelete(bf, tmp);
        }
        generic_printf("delete bruteforce %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);
    }
}

static void random_delete(
        struct flextree *const ft, struct brute_force *const bf, u64 total_size, const u64 count)
{
    if (!ft || !bf) return;
    u64 ts = 0;

    u64 osize = total_size;
    if (ft) {
        setrand();
        ts = time_nsec();
        for (u64 i=0; i<count; i++) {
            u64 tmp = rand_int() % total_size;
            u64 tmp2 = rand_int() % 10 + 1;
            flextree_delete(ft, tmp, tmp2);
            total_size -= tmp2;
        }
        generic_printf("delete flextree %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);
    }

    total_size = osize;
    if (bf) {
        setrand();
        ts = time_nsec();
        for (u64 i=0; i<count; i++) {
            u64 tmp = rand_int() % total_size;
            u64 tmp2 = rand_int() % 10 + 1;
            brute_force_delete(bf, tmp, tmp2);
            total_size -= tmp2;
        }
        generic_printf("delete bruteforce %ld milliseconds elapsed\n", time_diff_nsec(ts)/1000000);
    }
}

static void sequential_tag_query(struct flextree *const ft, struct brute_force *const bf, u64 total_size)
{
    if (!ft || !bf) return;

    u16 ft_tag = 0;
    u16 bf_tag = 0;

    flextree_set_tag(ft, ft->max_loff-1, 0xffff);
    brute_force_set_tag(bf, bf->max_loff-1, 0xffff);

    u8 correct = 1;
    for (u64 i=0; i<total_size; i++) {
        int fr = flextree_get_tag(ft, i, &ft_tag);
        int br = brute_force_get_tag(bf, i, &bf_tag);
        if (fr != br || ft_tag != bf_tag) {
            generic_printf("Error encourted on %ld return %d %d tag %u %u\n", i, fr, br, ft_tag, bf_tag);
            correct = 0;
            break;
        }
    }
    correct ? generic_printf("\033[0;32m[results correct]\033[0m\n") :
              generic_printf("\033[0;31m[results wrong]\033[0m\n");
}


static void test1(const u64 count)
{
    generic_printf("---test1 insertion and point lookup %lu---\n", count);
    struct flextree *ft = flextree_open(NULL, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    struct brute_force *bf = brute_force_open(FLEXTREE_MAX_EXTENT_SIZE_LIMIT);

    random_insert(ft, bf, count);
    debug_assert(ft->max_loff == bf->max_loff);
    sequential_query(ft, bf, ft->max_loff);
    random_query(ft, bf, ft->max_loff);
    brute_force_close(bf);
    flextree_close(ft);
}

static void test2(const u64 count)
{
    generic_printf("---test2 point deletion and point lookup %lu---\n", count);
    struct flextree *ft = flextree_open(NULL, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    struct brute_force *bf = brute_force_open(FLEXTREE_MAX_EXTENT_SIZE_LIMIT);

    random_insert(ft, bf, count);
    random_pdelete(ft, bf, ft->max_loff, count);
    debug_assert(ft->max_loff == bf->max_loff);
    sequential_query(ft, bf, ft->max_loff);
    brute_force_close(bf);
    flextree_close(ft);
}

static void test3(const u64 count)
{
    generic_printf("---test3 random range deletion %lu---\n", count);
    struct flextree *ft = flextree_open(NULL, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    struct brute_force *bf = brute_force_open(FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    random_insert(ft, bf, count);
    debug_assert(ft->max_loff == bf->max_loff);
    random_delete(ft, bf, ft->max_loff, count);
    debug_assert(ft->max_loff == bf->max_loff);
    sequential_query(ft, bf, ft->max_loff);
    flextree_close(ft);
    brute_force_close(bf);
}

static void test4(const u64 count)
{
    generic_printf("---test4 range query %lu---\n", count);
    struct flextree *ft = flextree_open(NULL, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    struct brute_force *bf = brute_force_open(FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    random_insert(ft, bf, count);
    debug_assert(ft->max_loff == bf->max_loff);
    random_delete(ft, bf, ft->max_loff, count);
    debug_assert(ft->max_loff == bf->max_loff);
    random_range_query(ft, bf, ft->max_loff, count);
    flextree_close(ft);
    brute_force_close(bf);
}

static void count_leaf_nodes(struct flextree_node *const node, u64 *const c)
{
    if (node->is_leaf) {
        (*c)++;
    }
    else {
        const struct flextree_internal_entry *ie = &node->internal_entry;
        for (u64 i=0; i<node->count+1; i++) {
            count_leaf_nodes(ie->children[i].node, c);
        }
    }
}

static void test5(const u64 count)
{
    generic_printf("---test5 linked list %lu---\n", count);
    struct flextree *ft = flextree_open(PATH, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    struct brute_force *bf = brute_force_open(FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    random_insert(ft, bf, count);
    u64 c1 = 0, c2 = 0;
    struct flextree_node *node = ft->leaf_head;
    while (node) {
        // generic_printf("%lu ", node->leaf_entry->id);
        node = node->leaf_entry.next;
        c1++;
    }
    count_leaf_nodes(ft->root, &c2);
    generic_printf("%ld %ld\n", c1, c2);
    flextree_close(ft);

    ft = flextree_open(PATH, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    c1 = 0;
    node = ft->leaf_head;
    while (node) {
        node = node->leaf_entry.next;
        c1++;
    }
    generic_printf("%ld %ld\n", c1, c2);
    flextree_delete(ft, ft->max_loff / 4, ft->max_loff / 4 * 3);
    brute_force_delete(bf, bf->max_loff / 4, bf->max_loff / 4 * 3);
    c2 = 0;
    count_leaf_nodes(ft->root, &c2);
    flextree_close(ft);

    ft = flextree_open(PATH, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    sequential_query(ft, bf, ft->max_loff);
    debug_assert(ft->max_loff == bf->max_loff);
    c1 = 0;
    node = ft->leaf_head;
    while (node) {
        node = node->leaf_entry.next;
        c1++;
    }
    generic_printf("%ld %ld\n", c1, c2);
    flextree_delete(ft, 0, ft->max_loff);
    brute_force_delete(bf, 0, bf->max_loff);
    c2 = 0;
    count_leaf_nodes(ft->root, &c2);
    flextree_close(ft);

    ft = flextree_open(PATH, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    generic_printf("final\n");
    flextree_print(ft);
    debug_assert(ft->max_loff == bf->max_loff);
    c1 = 0;
    node = ft->leaf_head;
    while (node) {
        node = node->leaf_entry.next;
        c1++;
    }
    generic_printf("%ld %ld\n", c1, c2);
    sequential_query(ft, bf, ft->max_loff);
    flextree_close(ft);
    brute_force_close(bf);
}

static void flextree_check(struct flextree *ft)
{
    u64 total_len = 0;
    struct flextree_pos fp = flextree_pos_get_ll(ft, 0);
    while (flextree_pos_valid_ll(&fp)) {
        struct flextree_extent *ext = &fp.node->leaf_entry.extents[fp.idx];
        total_len += ext->len;
        flextree_pos_forward_extent_ll(&fp);
    }
    printf("ft check max_loff %lu, total_len %lu\n", ft->max_loff, total_len);
    u8 correct = 0;
    if (ft->max_loff == total_len) {
        correct = 1;
    }
    correct ? generic_printf("\033[0;32m[results correct]\033[0m\n") :
              generic_printf("\033[0;31m[results wrong]\033[0m\n");
}

static void test0(const u64 count)
{
    generic_printf("---test0 insertion and point lookup %lu---\n", count);
    struct flextree *ft = flextree_open(NULL, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);

    random_insert(ft, NULL, count);
    random_delete(ft, NULL, ft->max_loff, count);
    sequential_query(ft, NULL, ft->max_loff);

    u64 loff = 0;
    while (ft->max_loff > 100) {
        loff = (loff + 0xabcd12) % (ft->max_loff - 100);
        flextree_delete(ft, loff, 100);
    }
    flextree_delete(ft, 0, ft->max_loff-10);
    generic_printf("final 10\n");
    generic_printf("slab %lu\n", *(u64 *)((char *)ft->node_slab+72));
    flextree_print(ft);
    flextree_delete(ft, 0, ft->max_loff);
    generic_printf("final\n");
    generic_printf("slab %lu\n", *(u64 *)((char *)ft->node_slab+72));
    flextree_print(ft);

    flextree_close(ft);
}

static void test6(const u64 count)
{
    generic_printf("---test6 address hole handling %lu---\n", count);
    struct flextree *ft = flextree_open(NULL, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    struct brute_force *bf = brute_force_open(FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    random_insert(ft, bf, count);
    // insert holes
    generic_printf("%lu\n", ft->max_loff);
    int r = flextree_insert(ft, 1lu<<34, 1lu<<40, 50);
    debug_assert(r == 0);
    r = brute_force_insert(bf, 1lu<<34, 1lu<<40, 50);
    debug_assert(r == 0);
    generic_printf("%lu\n", ft->max_loff);
    random_range_query(ft, bf, ft->max_loff, count);
    brute_force_close(bf);
    flextree_close(ft);
    (void)r;
}

static void test7(const u64 count)
{
    generic_printf("---test7 range deletion %lu---\n", count);
    struct flextree *ft = flextree_open(NULL, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    struct brute_force *bf = brute_force_open(FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    random_insert(ft, bf, count);
    debug_assert(ft->max_loff == bf->max_loff);
    flextree_delete(ft, ft->max_loff / 4, ft->max_loff / 4 * 3);
    brute_force_delete(bf, bf->max_loff / 4, bf->max_loff / 4 * 3);
    debug_assert(ft->max_loff == bf->max_loff);
    sequential_query(ft, bf, ft->max_loff);
    flextree_close(ft);
    brute_force_close(bf);
}

static void test8(const u64 count)
{
    generic_printf("---test8 tags %lu---\n", count);
    struct flextree *ft = flextree_open(NULL, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    struct brute_force *bf = brute_force_open(FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    random_insert(ft, bf, count);
    debug_assert(ft->max_loff == bf->max_loff);
    random_delete(ft, bf, ft->max_loff, count);
    debug_assert(ft->max_loff == bf->max_loff);
    sequential_tag_query(ft, bf, ft->max_loff);
    flextree_close(ft);
    brute_force_close(bf);
}

static void test9(const u64 count)
{
    generic_printf("---test9 large persistent tree %lu---\n", count);
    struct flextree *ft = flextree_open(PATH, 128lu<<10);
    for (u64 i=0; i<2600000000; i++) {
        if (i % 100000000 == 0) {
            printf("%lu ", i);
            flextree_sync(ft);
            fflush(stdout);
        }
        flextree_insert(ft, i*156, i*156, 156);
    }
    printf("\n");
    fflush(stdout);
    flextree_close(ft);
    ft = flextree_open(PATH, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
    for (u32 i=0; i<100; i++) {
        random_append(ft, NULL, count);
        flextree_close(ft);
        ft = flextree_open(PATH, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
        random_insert(ft, NULL, count);
        flextree_close(ft);
        ft = flextree_open(PATH, FLEXTREE_MAX_EXTENT_SIZE_LIMIT);
        flextree_check(ft);
    }
    flextree_close(ft);
    (void)sequential_query_vo;
}

int main(int argc, char ** argv) {
    typeof(test0) *tests [11] = {test0, test1, test2, test3, test4, test5, test6, test7, test8, test9, NULL};
    char * cmd = argc > 1 ? argv[1] : "0123456789";
    for (u32 i = 0; i < 10; i++) {
      if (strchr(cmd, (char)((u8)'0'+i)) && tests[i])
        tests[i](500000);
    }
    return 0;
}
