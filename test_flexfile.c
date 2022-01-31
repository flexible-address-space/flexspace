#include "flexfile.h"

#define PATH "/tmp/flexfile"

int main()
{
    struct flexfile *ff = flexfile_open(PATH);
    char *a = "abc";
    char *b = "def";
    char *c = "123";
    ssize_t r = flexfile_write(ff, a, 0, 3);
    debug_assert(r == 3);
    r = flexfile_write(ff, b, 1, 3);
    debug_assert(r == 3);
    r = flexfile_write(ff, c, 2, 3);
    debug_assert(r == 3);
    char result[256];
    flexfile_close(ff);
    ff = flexfile_open(PATH);
    flexfile_read(ff, result, 0, 9);
    result[9] = 0;
    printf("ad123efbc: %s\n", result); // should be "ad123efbc"
    flexfile_write(ff, a, 1, 3);
    flexfile_close(ff);
    ff = flexfile_open(PATH);
    flexfile_read(ff, result, 0, 12);
    result[12] = 0;
    printf("aabcd123efbc: %s\n", result); // should be "aabcd123efbc"
    flexfile_sync(ff);
    flexfile_defrag(ff, "aabc", 0, 4); // should append "aabc" to the end of file
    flextree_print(ff->flextree);
    flexfile_read(ff, result, 0, 12);
    result[12] = 0;
    printf("aabcd123efbc: %s\n", result); // should be "aabcd123efbc"
    flexfile_close(ff);
    return 0;
    (void)r;
}
