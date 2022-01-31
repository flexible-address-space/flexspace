/*
 * Copyright (c) 2016--2021  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#pragma once

// includes {{{
// C headers
#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

// POSIX headers
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

// Linux headers
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>

// SIMD
#if defined(__x86_64__)
#include <x86intrin.h>
#elif defined(__aarch64__)
#include <arm_acle.h>
#include <arm_neon.h>
#endif
// }}} includes

#ifdef __cplusplus
extern "C" {
#endif

// types {{{
typedef char            s8;
typedef short           s16;
typedef int             s32;
typedef long            s64;
typedef __int128_t      s128;
static_assert(sizeof(s8) == 1, "sizeof(s8)");
static_assert(sizeof(s16) == 2, "sizeof(s16)");
static_assert(sizeof(s32) == 4, "sizeof(s32)");
static_assert(sizeof(s64) == 8, "sizeof(s64)");
static_assert(sizeof(s128) == 16, "sizeof(s128)");

typedef unsigned char   u8;
typedef unsigned short  u16;
typedef unsigned int    u32;
typedef unsigned long   u64;
typedef __uint128_t     u128;
static_assert(sizeof(u8) == 1, "sizeof(u8)");
static_assert(sizeof(u16) == 2, "sizeof(u16)");
static_assert(sizeof(u32) == 4, "sizeof(u32)");
static_assert(sizeof(u64) == 8, "sizeof(u64)");
static_assert(sizeof(u128) == 16, "sizeof(u128)");

#if defined(__x86_64__)
typedef __m128i m128;
#if defined(__AVX2__)
typedef __m256i m256;
#endif // __AVX2__
#if defined(__AVX512F__)
typedef __m512i m512;
#endif // __AVX512F__
#elif defined(__aarch64__)
typedef uint8x16_t m128;
#else
#error Need x86_64 or AArch64.
#endif
// }}} types

// defs {{{
#define likely(____x____)   __builtin_expect(____x____, 1)
#define unlikely(____x____) __builtin_expect(____x____, 0)

// ansi colors
// 3X:fg; 4X:bg; 9X:light fg; 10X:light bg;
// X can be one of the following colors:
// 0:black;   1:red;     2:green;  3:yellow;
// 4:blue;    5:magenta; 6:cyan;   7:white;
#define TERMCLR(____code____) "\x1b[" #____code____ "m"
// }}} defs

// const {{{
#define PGSZ ((4096lu))
// }}} const

// math {{{
  extern u64
mhash64(const u64 v);

  extern u32
mhash32(const u32 v);

  extern u64
gcd64(u64 a, u64 b);
// }}} math

// random {{{
  extern u64
random_u64(void);

  extern void
srandom_u64(const u64 seed);

  extern double
random_double(void);
// }}} random

// timing {{{
  extern u64
time_nsec(void);

  extern double
time_sec(void);

  extern u64
time_diff_nsec(const u64 last);

  extern double
time_diff_sec(const double last);

  extern void
time_stamp(char * str, const size_t size);

  extern void
time_stamp2(char * str, const size_t size);
// }}} timing

// cpucache {{{
  extern void
cpu_pause(void);

  extern void
cpu_mfence(void);

  extern void
cpu_cfence(void);

  extern void
cpu_prefetch0(const void * const ptr);

  extern void
cpu_prefetch1(const void * const ptr);

  extern void
cpu_prefetch2(const void * const ptr);

  extern void
cpu_prefetch3(const void * const ptr);

  extern void
cpu_prefetchw(const void * const ptr);
// }}} cpucache

// crc32c {{{
  extern u32
crc32c_u8(const u32 crc, const u8 v);

  extern u32
crc32c_u16(const u32 crc, const u16 v);

  extern u32
crc32c_u32(const u32 crc, const u32 v);

  extern u32
crc32c_u64(const u32 crc, const u64 v);

// 1 <= nr <= 3
  extern u32
crc32c_inc_123(const u8 * buf, u32 nr, u32 crc);

// nr % 4 == 0
  extern u32
crc32c_inc_x4(const u8 * buf, u32 nr, u32 crc);

  extern u32
crc32c_inc(const u8 * buf, u32 nr, u32 crc);
// }}} crc32c

// debug {{{
  extern void
debug_break(void);

  extern void
debug_backtrace(void);

  extern void
watch_u64_usr1(u64 * const ptr);

#ifndef NDEBUG
  extern void
debug_assert(const bool v);
#else
#define debug_assert(expr) ((void)0)
#endif

__attribute__((noreturn))
  extern void
debug_die(void);

__attribute__((noreturn))
  extern void
debug_die_perror(void);

  extern void
debug_dump_maps(FILE * const out);

  extern bool
debug_perf_switch(void);
// }}} debug

// mm {{{
#ifdef ALLOCFAIL
  extern bool
alloc_fail(void);
#endif

  extern void *
xalloc(const size_t align, const size_t size);

  extern void *
yalloc(const size_t size);

  extern void **
malloc_2d(const size_t nr, const size_t size);

  extern void **
calloc_2d(const size_t nr, const size_t size);

  extern void
pages_unmap(void * const ptr, const size_t size);

  extern void
pages_lock(void * const ptr, const size_t size);

/* hugepages */
// force posix allocators: -DVALGRIND_MEMCHECK
  extern void *
pages_alloc_4kb(const size_t nr_4kb);

  extern void *
pages_alloc_2mb(const size_t nr_2mb);

  extern void *
pages_alloc_1gb(const size_t nr_1gb);

  extern void *
pages_alloc_best(const size_t size, const bool try_1gb, u64 * const size_out);
// }}} mm

// process/thread {{{
  extern void
thread_get_name(const pthread_t pt, char * const name, const size_t len);

  extern void
thread_set_name(const pthread_t pt, const char * const name);

  extern long
process_get_rss(void);

  extern u32
process_affinity_count(void);

  extern u32
process_getaffinity_list(const u32 max, u32 * const cores);

  extern void
thread_setaffinity_list(const u32 nr, const u32 * const list);

  extern void
thread_pin(const u32 cpu);

  extern u64
process_cpu_time_usec(void);

// if args == true, argx is void **
// if args == false, argx is void *
  extern u64
thread_fork_join(u32 nr, void *(*func) (void *), const bool args, void * const argx);

  extern int
thread_create_at(const u32 cpu, pthread_t * const thread, void *(*start_routine) (void *), void * const arg);
// }}} process/thread

// locking {{{
typedef union {
  u32 opaque;
} spinlock;

  extern void
spinlock_init(spinlock * const lock);

  extern void
spinlock_lock(spinlock * const lock);

  extern bool
spinlock_trylock(spinlock * const lock);

  extern void
spinlock_unlock(spinlock * const lock);

typedef union {
  u32 opaque;
} rwlock;

  extern void
rwlock_init(rwlock * const lock);

  extern bool
rwlock_trylock_read(rwlock * const lock);

// low-priority reader-lock; use with trylock_write_hp
  extern bool
rwlock_trylock_read_lp(rwlock * const lock);

  extern bool
rwlock_trylock_read_nr(rwlock * const lock, u16 nr);

  extern void
rwlock_lock_read(rwlock * const lock);

  extern void
rwlock_unlock_read(rwlock * const lock);

  extern bool
rwlock_trylock_write(rwlock * const lock);

  extern bool
rwlock_trylock_write_nr(rwlock * const lock, u16 nr);

  extern void
rwlock_lock_write(rwlock * const lock);

// writer has higher priority; new readers are blocked
  extern bool
rwlock_trylock_write_hp(rwlock * const lock);

  extern bool
rwlock_trylock_write_hp_nr(rwlock * const lock, u16 nr);

  extern void
rwlock_lock_write_hp(rwlock * const lock);

  extern void
rwlock_unlock_write(rwlock * const lock);

  extern void
rwlock_write_to_read(rwlock * const lock);

typedef union {
  u64 opqaue[8];
} mutex;

  extern void
mutex_init(mutex * const lock);

  extern void
mutex_lock(mutex * const lock);

  extern bool
mutex_trylock(mutex * const lock);

  extern void
mutex_unlock(mutex * const lock);

  extern void
mutex_deinit(mutex * const lock);
// }}} locking

// bits {{{
  extern u32
bits_reverse_u32(const u32 v);

  extern u64
bits_reverse_u64(const u64 v);

  extern u64
bits_rotl_u64(const u64 v, const u8 n);

  extern u64
bits_rotr_u64(const u64 v, const u8 n);

  extern u32
bits_rotl_u32(const u32 v, const u8 n);

  extern u32
bits_rotr_u32(const u32 v, const u8 n);

  extern u64
bits_p2_up_u64(const u64 v);

  extern u32
bits_p2_up_u32(const u32 v);

  extern u64
bits_p2_down_u64(const u64 v);

  extern u32
bits_p2_down_u32(const u32 v);

  extern u64
bits_round_up(const u64 v, const u8 power);

  extern u64
bits_round_up_a(const u64 v, const u64 a);

  extern u64
bits_round_down(const u64 v, const u8 power);

  extern u64
bits_round_down_a(const u64 v, const u64 a);
// }}} bits

// simd {{{
  extern u32
m128_movemask_u8(const m128 v);

//  extern u32
//m128_movemask_u16(const m128 v);
//
//  extern u32
//m128_movemask_u32(const m128 v);
// }}} simd

// vi128 {{{
  extern u32
vi128_estimate_u32(const u32 v);

  extern u8 *
vi128_encode_u32(u8 * dst, u32 v);

  extern const u8 *
vi128_decode_u32(const u8 * src, u32 * const out);

  extern u32
vi128_estimate_u64(const u64 v);

  extern u8 *
vi128_encode_u64(u8 * dst, u64 v);

  extern const u8 *
vi128_decode_u64(const u8 * src, u64 * const out);
// }}} vi128

// misc {{{
// TODO: only works on little endian?
struct entry13 { // what a beautiful name
  union {
    u16 e1;
    struct { // easy for debugging
      u64 e1_64:16;
      u64 e3:48;
    };
    u64 v64;
    void * ptr;
  };
};

static_assert(sizeof(struct entry13) == 8, "sizeof(entry13) != 8");

// directly access read .e1 and .e3
// directly write .e1
// use entry13_update() to update the entire entry

  extern struct entry13
entry13(const u16 e1, const u64 e3);

  extern void
entry13_update_e3(struct entry13 * const e, const u64 e3);

  extern void *
u64_to_ptr(const u64 v);

  extern u64
ptr_to_u64(const void * const ptr);

  extern size_t
m_usable_size(void * const ptr);

  extern size_t
fdsize(const int fd);

  extern u32
memlcp(const u8 * const p1, const u8 * const p2, const u32 max);

__attribute__ ((format (printf, 2, 3)))
  extern void
logger_printf(const int fd, const char * const fmt, ...);
// }}} misc

// bitmap {{{
struct bitmap;

  extern struct bitmap *
bitmap_create(const u64 nbits);

  extern void
bitmap_init(struct bitmap * const bm, const u64 nbits);

  extern bool
bitmap_test(const struct bitmap * const bm, const u64 idx);

  extern bool
bitmap_test_all1(struct bitmap * const bm);

  extern bool
bitmap_test_all0(struct bitmap * const bm);

  extern void
bitmap_set1(struct bitmap * const bm, const u64 idx);

  extern void
bitmap_set0(struct bitmap * const bm, const u64 idx);

  extern void
bitmap_set1_safe64(struct bitmap * const bm, const u64 idx);

  extern void
bitmap_set0_safe64(struct bitmap * const bm, const u64 idx);

  extern u64
bitmap_count(struct bitmap * const bm);

  extern u64
bitmap_first(struct bitmap * const bm);

  extern void
bitmap_set_all1(struct bitmap * const bm);

  extern void
bitmap_set_all0(struct bitmap * const bm);
// }}} bitmap

// slab {{{
struct slab;

  extern struct slab *
slab_create(const u64 obj_size, const u64 blk_size);

  extern bool
slab_reserve_unsafe(struct slab * const slab, const u64 nr);

  extern void *
slab_alloc_unsafe(struct slab * const slab);

  extern void *
slab_alloc_safe(struct slab * const slab);

  extern void
slab_free_unsafe(struct slab * const slab, void * const ptr);

  extern void
slab_free_safe(struct slab * const slab, void * const ptr);

  extern void
slab_free_all(struct slab * const slab);

  extern u64
slab_get_nalloc(struct slab * const slab);

  extern void
slab_destroy(struct slab * const slab);
// }}}  slab

// qsort {{{
  extern int
compare_u16(const void * const p1, const void * const p2);

  extern void
qsort_u16(u16 * const array, const size_t nr);

  extern u16 *
bsearch_u16(const u16 v, const u16 * const array, const size_t nr);

  extern void
shuffle_u16(u16 * const array, const u64 nr);

  extern int
compare_u32(const void * const p1, const void * const p2);

  extern void
qsort_u32(u32 * const array, const size_t nr);

  extern u32 *
bsearch_u32(const u32 v, const u32 * const array, const size_t nr);

  extern void
shuffle_u32(u32 * const array, const u64 nr);

  extern int
compare_u64(const void * const p1, const void * const p2);

  extern void
qsort_u64(u64 * const array, const size_t nr);

  extern u64 *
bsearch_u64(const u64 v, const u64 * const array, const size_t nr);

  extern void
shuffle_u64(u64 * const array, const u64 nr);

  extern int
compare_double(const void * const p1, const void * const p2);

  extern void
qsort_double(double * const array, const size_t nr);

  extern void
qsort_u64_sample(const u64 * const array0, const u64 nr, const u64 res, FILE * const out);

  extern void
qsort_double_sample(const double * const array0, const u64 nr, const u64 res, FILE * const out);
// }}} qsort

// string {{{
// XXX strdec_ and strhex_ functions does not append the trailing '\0' to the output string
// size of out should be >= 10
  extern void
strdec_32(void * const out, const u32 v);

// size of out should be >= 20
  extern void
strdec_64(void * const out, const u64 v);

// size of out should be >= 8
  extern void
strhex_32(void * const out, const u32 v);

// size of out should be >= 16
  extern void
strhex_64(void * const out, const u64 v);

  extern u64
a2u64(const void * const str);

  extern u32
a2u32(const void * const str);

  extern s64
a2s64(const void * const str);

  extern s32
a2s32(const void * const str);

  extern void
str_print_hex(FILE * const out, const void * const data, const u32 len);

  extern void
str_print_dec(FILE * const out, const void * const data, const u32 len);

// user should free returned ptr (and nothing else) after use
  extern char **
strtoks(const char * const str, const char * const delim);

  extern u32
strtoks_count(const char * const * const toks);
// }}} string

// damp {{{
struct damp;

  extern struct damp *
damp_create(const u64 cap, const double dshort, const double dlong);

  extern double
damp_avg(const struct damp * const d);

  extern double
damp_ravg(const struct damp * const d);

  extern double
damp_min(const struct damp * const d);

  extern double
damp_max(const struct damp * const d);

  extern void
damp_add(struct damp * const d, const double v);

  extern bool
damp_test(struct damp * const d);

  extern bool
damp_add_test(struct damp * const d, const double v);

  extern void
damp_clean(struct damp * const d);

  extern void
damp_destroy(struct damp * const d);
// }}} damp

// vctr {{{
struct vctr;

  extern struct vctr *
vctr_create(const size_t nr);

  extern size_t
vctr_size(const struct vctr * const v);

  extern void
vctr_add(struct vctr * const v, const u64 i, const size_t n);

  extern void
vctr_add1(struct vctr * const v, const u64 i);

  extern void
vctr_add_atomic(struct vctr * const v, const u64 i, const size_t n);

  extern void
vctr_add1_atomic(struct vctr * const v, const u64 i);

  extern void
vctr_set(struct vctr * const v, const u64 i, const size_t n);

  extern size_t
vctr_get(const struct vctr * const v, const u64 i);

  extern void
vctr_merge(struct vctr * const to, const struct vctr * const from);

  extern void
vctr_reset(struct vctr * const v);

  extern void
vctr_destroy(struct vctr * const v);
// }}} vctr

// rgen {{{
struct rgen;

typedef u64 (*rgen_next_func)(struct rgen * const);

extern struct rgen * rgen_new_rnd64(void);
extern struct rgen * rgen_new_rnd64s(const u64 seed);
extern struct rgen * rgen_new_const(const u64 c);
extern struct rgen * rgen_new_expo(const double percentile, const double range);
extern struct rgen * rgen_new_incs(const u64 min, const u64 max);
extern struct rgen * rgen_new_incu(const u64 min, const u64 max);
extern struct rgen * rgen_new_skips(const u64 min, const u64 max, const s64 inc);
extern struct rgen * rgen_new_skipu(const u64 min, const u64 max, const s64 inc);
extern struct rgen * rgen_new_decs(const u64 min, const u64 max);
extern struct rgen * rgen_new_decu(const u64 min, const u64 max);
extern struct rgen * rgen_new_zipfian(const u64 min, const u64 max);
extern struct rgen * rgen_new_xzipfian(const u64 min, const u64 max);
extern struct rgen * rgen_new_unizipf(const u64 min, const u64 max, const u64 ufactor);
extern struct rgen * rgen_new_zipfuni(const u64 min, const u64 max, const u64 ufactor);
extern struct rgen * rgen_new_latest(const u64 zipf_range);
extern struct rgen * rgen_new_uniform(const u64 min, const u64 max);
extern struct rgen * rgen_new_shuffle(const u64 min, const u64 max);
extern struct rgen * rgen_new_trace32(const char * const filename, const u64 bufsize);

  extern u64
rgen_min(struct rgen * const gen);

  extern u64
rgen_max(struct rgen * const gen);

  extern u64
rgen_next(struct rgen * const gen);

// same to next() for regular gen; different only in async rgen
  extern u64
rgen_next_nowait(struct rgen * const gen);

  extern u64
rgen_next_write(struct rgen * const gen);

  extern void
rgen_destroy(struct rgen * const gen);

  extern void
rgen_helper_message(void);

  extern int
rgen_helper(const int argc, char ** const argv, struct rgen ** const gen_out);

  extern void
rgen_async_wait(struct rgen * const gen);

  extern void
rgen_async_wait_all(struct rgen * const gen);

  extern struct rgen *
rgen_fork(struct rgen * const gen0);

  extern void
rgen_join(struct rgen * const gen);

  extern struct rgen *
rgen_async_create(struct rgen * const gen0, const u32 cpu);
// }}} rgen

// qsbr {{{
struct qsbr;
struct qsbr_ref {
#ifdef QSBR_DEBUG
  u64 debug[16];
#endif
  u64 opaque[3];
};

  extern struct qsbr *
qsbr_create(void);

  extern bool
qsbr_register(struct qsbr * const q, struct qsbr_ref * const qref);

  extern void
qsbr_unregister(struct qsbr * const q, struct qsbr_ref * const qref);

  extern void
qsbr_update(struct qsbr_ref * const qref, const u64 v);

  extern void
qsbr_park(struct qsbr_ref * const qref);

  extern void
qsbr_resume(struct qsbr_ref * const qref);

  extern void
qsbr_wait(struct qsbr * const q, const u64 target);

  extern void
qsbr_destroy(struct qsbr * const q);
// }}} qsbr

// forker {{{
#define FORKER_END_TIME ((0))
#define FORKER_END_COUNT ((1))
typedef bool (*forker_perf_analyze_func)(void * const passdata[2], const u64, const struct vctr *, struct damp *, char *);

typedef void * (*forker_worker_func)(void *);

struct pass_info {
  struct rgen * gen0;
  void * passdata[2]; // if not testing kv
  u64 vctr_size;
  forker_worker_func wf;
  forker_perf_analyze_func af;
};

struct forker_worker_info {
  struct rgen * gen;
  rgen_next_func rgen_next;
  rgen_next_func rgen_next_write; // identical to rgen_next except for sync-latest
  void * passdata[2]; // if not testing kv
  void * priv;
  u32 end_type;
  u32 padding;
  u64 end_magic;
  struct vctr * vctr;

  u64 worker_id; // <= conc
  struct rgen * gen_back;
  u32 conc; // number of threads
  int argc;// user args
  char ** argv;
  u64 seed;
  void * (*thread_func)(void *);
  // PAPI
  u64 papi_vctr_base;
};

  extern int
forker_pass(const int argc, char ** const argv, char ** const pref,
    struct pass_info * const pi, const int nr_wargs0);

  extern int
forker_passes(int argc, char ** argv, char ** const pref0,
    struct pass_info * const pi, const int nr_wargs0);

  extern void
forker_passes_message(void);

  extern bool
forker_main(int argc, char ** argv, int(*test_func)(const int, char ** const));
// }}} forker

#ifdef __cplusplus
}
#endif
// vim:fdm=marker
