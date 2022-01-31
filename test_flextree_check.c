#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

#include "flextree.h"

#define FLEXTREE_HOLE (1lu << 47) // highest bit set to indicate a hole

static u8 flextree_pos_consistent(struct flextree_pos *const fp1, struct flextree_pos *const fp2)
{
    if (memcmp(fp1, fp2, sizeof(*fp1)) != 0) {
        return 0;
    }
    return 1;
}

void flextree_check(struct flextree *ft)
{
    struct flextree_pos fp = flextree_pos_get_ll(ft, 0);
    while (flextree_pos_valid_ll(&fp)) {
        struct flextree_extent *ext = &fp.node->leaf_entry.extents[fp.idx];
        struct flextree_pos fp2 = flextree_pos_get_ll(ft, fp.loff);
        printf("extent loff %lu poff %lu len %u is_hole %u consistent %u\n",
                fp.loff, ext->poff, ext->len, (ext->poff & FLEXTREE_HOLE) ? 1 : 0, flextree_pos_consistent(&fp, &fp2));
        flextree_pos_forward_extent_ll(&fp);
    }
    printf("ft check max_loff %lu\n", ft->max_loff);
}

int main(int argc, char **argv)
{
    if (argc != 2) {
        return 0;
    }
    struct flextree *const ft = flextree_open(argv[1], 128u<<10);
    flextree_check(ft);
    flextree_close(ft);
    return 0;
}
