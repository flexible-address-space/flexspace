#include "generic.h"

inline void *generic_malloc(size_t size)
{
    return malloc(size);
}


inline void *generic_realloc(void *ptr, size_t size)
{
    return realloc(ptr, size);
};

inline void generic_free(void *ptr)
{
    return free(ptr);
}

inline file_type generic_open(const char *pathname, int flags, mode_t mode)
{
    return (file_type)open(pathname, flags, mode);
}

inline file_type generic_mkdir(const char *path, mode_t mode)
{
    return mkdir(path, mode);
};

inline off_t generic_lseek(file_type fildes, off_t offset, int whence)
{
    return lseek(fildes, offset, whence);
}

inline int generic_close(file_type fd)
{
    return close(fd);
}

inline ssize_t generic_pread(file_type fd, void *buf, size_t nbyte, off_t offset)
{
    return pread(fd, buf, nbyte, offset);
}

inline ssize_t generic_pwrite(file_type fd, const void *buf, size_t count, off_t offset)
{
    return pwrite(fd, buf, count, offset);
}

inline int generic_fdatasync(file_type fildes)
{
    return fdatasync(fildes);
}

inline void generic_exit(int status)
{
    fflush(stdout);
    return exit(status);
}

inline int generic_ftruncate(file_type fildes, off_t length)
{
    return ftruncate(fildes, length);
}
