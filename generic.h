#ifndef _GENERIC_H
#define _GENERIC_H
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

typedef int file_type;

#ifndef static_assert
#ifdef __GNUC__
#define static_assert(cond, msg) \
    typedef char __static_assertion_failed[(!!(cond))*2-1]
#endif
#endif

#ifndef debug_assert
#define debug_assert(x) (assert(x))
#endif

#define generic_printf(args...) printf(args)
#define generic_sprintf(args...) sprintf(args)
#define generic_fflush(args...) fflush(args)

extern void *generic_malloc(size_t size);

extern void *generic_realloc(void *ptr, size_t size);

extern void generic_free(void * ptr);

extern file_type generic_open(const char *path, int flags, mode_t mode);

extern file_type generic_mkdir(const char *path, mode_t mode);

extern off_t generic_lseek(file_type fildes, off_t offset, int whence);

extern int generic_close(file_type close);

extern ssize_t generic_pread(file_type fd, void *buf, size_t nbyte, off_t offset);

extern ssize_t generic_pwrite(file_type fd, const void *buf, size_t nbyte, off_t offset);

extern int generic_fdatasync(file_type fildes);

extern void generic_exit(int status);

extern int generic_ftruncate(file_type fildes, off_t length);

#endif
