#define _GNU_SOURCE

#include <dlfcn.h>
#include "wrapper.h"

static struct flexfile *files[65536]; // int fd -> struct flexfile *, mapping of open files per process
static FILE *tmpfiles[65536]; // int fd -> FILE *, to avoid memory leaks

#define O_FLEXFILE 0xffffffff
#define FIXED_PATH "/tmp/flexfile"

static int (*real_open)(const char *pathname, int flags, ...) = NULL;

int open(const char *pathname, int flags, ...)
{
    if ((flags & O_FLEXFILE) != O_FLEXFILE &&
            strncmp(pathname, FIXED_PATH, strlen(pathname)) != 0) {
        va_list args;
        va_start(args, flags);
        mode_t mode = va_arg(args, mode_t);
        va_end(args);
        if (!real_open) {
            real_open = (typeof(real_open))dlsym(RTLD_NEXT, "open");
        }
        return real_open(pathname, flags, mode);
    }
    printf("open %s\n", pathname);
    fflush(stdout);
    struct flexfile *const ff = flexfile_open(pathname);
    if (ff) {
        FILE *const fp = tmpfile();
        const int fd = fileno(fp);
        printf("open succeed, fake fd %d\n", fd);
        fflush(stdout);
        files[fd] = ff;
        tmpfiles[fd] = fp;
        return fd;
    }
    return 0;
    (void)flags;
}

static int (*real_close)(int fd) = NULL;

int close(int fd)
{
    if (!files[fd]) {
        if (!real_close) {
            real_close = (typeof(real_close))dlsym(RTLD_NEXT, "close");
        }
        return real_close(fd);
    }
    printf("close %d\n", fd);
    fflush(stdout);
    struct flexfile *const ff = files[fd];
    FILE *const fp = tmpfiles[fd];
    fclose(fp);
    flexfile_close(ff);
    files[fd] = NULL;
    tmpfiles[fd] = NULL;
    return 0;
}

static ssize_t (*real_read)(int fd, void *buf, size_t count) = NULL;

ssize_t read(int fd, void *buf, size_t count)
{
    if (!files[fd]) {
        if (!real_read) {
            real_read= (typeof(real_read))dlsym(RTLD_NEXT, "read");
        }
        return real_read(fd, buf, count);
    }
    const off_t off = lseek(fd, 0, SEEK_CUR);
    return pread(fd, buf, count, off);
}

static ssize_t (*real_pread)(int fd, void *buf, size_t count, off_t offset) = NULL;

ssize_t pread(int fd, void *buf, size_t count, off_t offset)
{
    if (!files[fd]) {
        if (!real_pread) {
            real_pread= (typeof(real_pread))dlsym(RTLD_NEXT, "pread");
        }
        return real_pread(fd, buf, count, offset);
    }
    struct flexfile *const ff = files[fd];
    return flexfile_read(ff, buf, offset, count);
}

static ssize_t (*real_write)(int fd, const void *buf, size_t count) = NULL;

ssize_t write(int fd, const void *buf, size_t count)
{
    if (!files[fd]) {
        if (!real_write) {
            real_write = (typeof(real_write))dlsym(RTLD_NEXT, "write");
        }
        return real_write(fd, buf, count);
    }
    const off_t off = lseek(fd, 0, SEEK_CUR);
    return pwrite(fd, buf, count, off);
}

static ssize_t (*real_pwrite)(int fd, const void *buf, size_t count, off_t offset) = NULL;

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset)
{
    if (!files[fd]) {
        if (!real_pwrite) {
            real_pwrite = (typeof(real_pwrite))dlsym(RTLD_NEXT, "pwrite");
        }
        return real_pwrite(fd, buf, count, offset);
    }
    struct flexfile *const ff = files[fd];
    return flexfile_write(ff, buf, offset, count);
}

static int (*real_fsync)(int fd) = NULL;

int fsync(int fd)
{
    if (!files[fd]) {
        if (!real_fsync) {
            real_fsync = (typeof(real_fsync))dlsym(RTLD_NEXT, "fsync");
        }
        return real_fsync(fd);
    }
    struct flexfile *const ff = files[fd];
    flexfile_sync(ff);
    return 0;
}

static int (*real_ftruncate)(int fd, off_t length) = NULL;

int ftruncate(int fd, off_t length)
{
    if (!files[fd]) {
        if (!real_ftruncate) {
            real_ftruncate = (typeof(real_ftruncate))dlsym(RTLD_NEXT, "ftruncate");
        }
        return real_ftruncate(fd, length);
    }

    struct flexfile *const ff = files[fd];
    return flexfile_ftruncate(ff, length);
}

static int (*real_dup)(int oldfd) = NULL;

int dup(int oldfd)
{
    if (!files[oldfd]) {
        if (!real_dup) {
            real_dup = (typeof(real_dup))dlsym(RTLD_NEXT, "dup");
        }
        return real_dup(oldfd);
    }
    printf("dup is not implemented for flexfile\n");
    fflush(stdout);
    exit(1);
}

static int (*real_dup2)(int oldfd, int newfd) = NULL;

int dup2(int oldfd, int newfd)
{
    if (!files[oldfd]) {
        if (!real_dup2) {
            real_dup2 = (typeof(real_dup2))dlsym(RTLD_NEXT, "dup2");
        }
        return real_dup2(oldfd, newfd);
    }
    printf("dup2 is not implemented for flexfile\n");
    fflush(stdout);
    exit(1);
}
