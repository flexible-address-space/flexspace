#ifndef _WRAPPER_H
#define _WRAPPER_H

#include "flexfile.h"

extern int open(const char *pathname, int flags, ...);

extern int close(int fd);

extern ssize_t read(int fd, void *buf, size_t count);

extern ssize_t pread(int fd, void *buf, size_t count, off_t offset);

extern ssize_t write(int fd, const void *buf, size_t count);

extern ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);

extern int fsync(int fd);

extern off_t lseek(int fd, off_t offset, int whence);

extern int ftruncate(int fd, off_t length);

extern int dup(int oldfd);

extern int dup2(int oldfd, int newfd);

#endif
