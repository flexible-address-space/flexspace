/*
 * Copyright (c) 2016--2021  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// skiplist {{{
struct skiplist;

  extern struct skiplist *
skiplist_create(const struct kvmap_mm * const mm);

  extern struct kv *
skiplist_get(struct skiplist * const list, const struct kref * const key, struct kv * const out);

  extern bool
skiplist_probe(struct skiplist * const list, const struct kref * const key);

  extern bool
skiplist_put(struct skiplist * const list, struct kv * const kv);

  extern bool
skipsafe_put(struct skiplist * const list, struct kv * const kv);

  extern bool
skiplist_merge(struct skiplist * const list, const struct kref * const kref,
    kv_merge_func uf, void * const priv);

  extern bool
skipsafe_merge(struct skiplist * const list, const struct kref * const kref,
    kv_merge_func uf, void * const priv);

  extern bool
skiplist_inp(struct skiplist * const list, const struct kref * const key,
    kv_inp_func uf, void * const priv);

  extern bool
skiplist_del(struct skiplist * const list, const struct kref * const key);

  extern void
skiplist_clean(struct skiplist * const list);

  extern void
skiplist_destroy(struct skiplist * const list);

  extern void
skiplist_fprint(struct skiplist * const list, FILE * const out);

struct skiplist_iter;

  extern struct skiplist_iter *
skiplist_iter_create(struct skiplist * const list);

  extern void
skiplist_iter_seek(struct skiplist_iter * const iter, const struct kref * const key);

  extern bool
skiplist_iter_valid(struct skiplist_iter * const iter);

  extern struct kv *
skiplist_iter_peek(struct skiplist_iter * const iter, struct kv * const out);

  extern bool
skiplist_iter_kref(struct skiplist_iter * const iter, struct kref * const kref);

  extern bool
skiplist_iter_kvref(struct skiplist_iter * const iter, struct kvref * const kvref);

  extern void
skiplist_iter_skip1(struct skiplist_iter * const iter);

  extern void
skiplist_iter_skip(struct skiplist_iter * const iter, const u32 nr);

  extern struct kv *
skiplist_iter_next(struct skiplist_iter * const iter, struct kv * const out);

  extern bool
skiplist_iter_inp(struct skiplist_iter * const iter, kv_inp_func uf, void * const priv);

  extern void
skiplist_iter_destroy(struct skiplist_iter * const iter);

extern const struct kvmap_api kvmap_api_skiplist;
extern const struct kvmap_api kvmap_api_skipsafe;
// }}} skiplist

// rdb {{{
#ifdef ROCKSDB
struct rdb;
struct rdb_iter;

  extern struct rdb *
rdb_create(const char * const path, const u64 cache_size_mb);

  extern struct kv *
rdb_get(struct rdb * const map, const struct kref * const key, struct kv * const out);

  extern bool
rdb_probe(struct rdb * const map, const struct kref * const key);

  extern bool
rdb_put(struct rdb * const map, struct kv * const kv);

  extern bool
rdb_del(struct rdb * const map, const struct kref * const key);

  extern void
rdb_destroy(struct rdb * const map);

  extern void
rdb_fprint(struct rdb * const map, FILE * const out);

  extern struct rdb_iter *
rdb_iter_create(struct rdb * const map);

  extern void
rdb_iter_seek(struct rdb_iter * const iter, const struct kref * const key);

  extern bool
rdb_iter_valid(struct rdb_iter * const iter);

  extern struct kv *
rdb_iter_peek(struct rdb_iter * const iter, struct kv * const out);

  extern void
rdb_iter_skip1(struct rdb_iter * const iter);

  extern void
rdb_iter_skip(struct rdb_iter * const iter, const u32 nr);

  extern struct kv *
rdb_iter_next(struct rdb_iter * const iter, struct kv * const out);

  extern void
rdb_iter_destroy(struct rdb_iter * const iter);

extern const struct kvmap_api kvmap_api_rdb;
#endif // ROCKSDB
// }}} rdb

#ifdef __cplusplus
}
#endif
// vim:fdm=marker
