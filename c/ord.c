/*
 * Copyright (c) 2016--2021  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#define _GNU_SOURCE

// headers {{{
#include <assert.h> // static_assert
#include "lib.h"
#include "ctypes.h"
#include "kv.h"
#include "ord.h"
// }}} headers

// skiplist {{{
#define SL_MAXH ((32))
struct skipnode {
  struct kv * kv;
  union {
    struct skipnode * ptr;
    au64 a;
  } next[0];
};

struct skippath {
  struct skipnode * vec[SL_MAXH][2]; // left and right
};

struct skiplist {
  mutex lock;
  u64 padding[7];
  struct kvmap_mm mm;
  u64 height;
  struct skipnode n0;
};

  struct skiplist *
skiplist_create(const struct kvmap_mm * const mm)
{
  const size_t sz = sizeof(struct skiplist) + (sizeof(void *) * SL_MAXH);
  struct skiplist * const list = yalloc(sz);
  if (list == NULL)
    return NULL;

  memset(list, 0, sz);
  mutex_init(&(list->lock));
  list->mm = mm ? (*mm) : kvmap_mm_dup;
  list->height = 1;
  return list;
}

  static inline struct skipnode *
skiplist_next(struct skipnode * const node, const u64 h)
{
  return (struct skipnode *)(void *)atomic_load_explicit(&node->next[h].a, MO_ACQUIRE);
}

  static inline void
skiplist_update_next(struct skipnode * const node, const u64 h, struct skipnode * const next)
{
  atomic_store_explicit(&(node->next[h].a), (u64)next, MO_RELEASE);
}

  static inline void
skiplist_lock(struct skiplist * const list)
{
  mutex_lock(&(list->lock));
}

  static inline void
skiplist_unlock(struct skiplist * const list)
{
  mutex_unlock(&(list->lock));
}

// perform a search on skiplist and record the path
// return true for match, false for mismatch
// *out will have the node >= key
// on match, the path could be incomplete (the path is not used unless for del, which will be handled separately)
// on mismatch, at every level of the path, left < key < right (or NULL); a new key should be inserted in between
  static bool
skiplist_search_ge_path(struct skiplist * const list, const struct kref * const key,
    struct skipnode ** const out, struct skippath * const path, u64 h)
{
  debug_assert(h);
  struct skipnode * left = &(list->n0); // leftmost
  struct skipnode * next;
  while ((--h) < SL_MAXH) {
    while ((next = skiplist_next(left, h)) != NULL) {
      const int cmp = kref_kv_compare(key, next->kv);
      if (cmp > 0) { // forward and continue
        left = next;
      } else if (cmp < 0) { // done at this level
        break;
      } else { // match
        *out = next;
        return true;
      }
    }
    path->vec[h][0] = left;
    path->vec[h][1] = next;
  }
  // no match; return the first node > key
  *out = next;
  return false;
}

  static bool
skiplist_search_ge(struct skiplist * const list, const struct kref * const key,
    struct skipnode ** const out)
{
  u64 h = list->height;
  debug_assert(h);
  struct skipnode * left = &(list->n0); // leftmost
  struct skipnode * next;
  while ((--h) < SL_MAXH) {
    while ((next = skiplist_next(left, h)) != NULL) {
      const int cmp = kref_kv_compare(key, next->kv);
      if (cmp > 0) { // forward and continue
        left = next;
      } else if (cmp < 0) { // done at this level
        break;
      } else { // match
        *out = next;
        return true;
      }
    }
  }
  // no match; return the first node > key
  *out = next;
  return false;
}

  struct kv *
skiplist_get(struct skiplist * const list, const struct kref * const key, struct kv * const out)
{
  struct skipnode * node;
  if (skiplist_search_ge(list, key, &node)) {
    debug_assert(node && node->kv);
    return list->mm.out(node->kv, out);
  }
  return NULL;
}

  bool
skiplist_probe(struct skiplist * const list, const struct kref * const key)
{
  struct skipnode * node;
  return skiplist_search_ge(list, key, &node);
}

// generate a random height; if it's higher than hh, fill the path
  static u64
skiplist_random_height(struct skiplist * const list, struct skippath * const path, const u64 hh)
{
  const u64 r = random_u64(); // r can be 0
  // 1 <= height <= 32
  const u64 height = (u64)(__builtin_ctzl(r ? r : 1) >> 1) + 1;
  for (u64 i = hh; i < height; i++) {
    path->vec[i][0] = &(list->n0); // the very beginning
    path->vec[i][1] = NULL; // the very end
  }

  return height;
}

  static bool
skiplist_insert_height(struct skiplist * const list, struct skippath * const path,
    struct kv * const kv, const u64 height)
{
  if (height > list->height)
    list->height = height;

  const u64 nodesize = sizeof(list->n0) + (sizeof(list->n0.next[0]) * height);
  struct skipnode * const node = malloc(nodesize);
  if (!node) { // malloc failed
    list->mm.free(kv, list->mm.priv);
    return false;
  }
  kv->privptr = NULL; // end of chain
  node->kv = kv;
  for (u64 i = 0; i < height; i++) {
    node->next[i].ptr = path->vec[i][1];
    skiplist_update_next(path->vec[i][0], i, node);
  }
  return true;
}

  static bool
skiplist_insert_fix_path(struct skippath * const path, const u64 height, struct kv * const kv)
{
  for (u64 h = 0; h < height; h++) { // must check every level
    struct skipnode * left = path->vec[h][0];
    struct skipnode * right = left->next[h].ptr;

    if (likely(right == path->vec[h][1]))
      continue;

    debug_assert(right); // insertions only; won't disappear

    do {
      const int cmp = kv_compare(kv, right->kv);
      if (cmp < 0) { // right is ok
        break;
      } else if (cmp > 0) { // forward path[h]
        left = right;
        right = left->next[h].ptr;
      } else {
        kv->privptr = right->kv;
        right->kv = kv;
        // insertion is already done
        return true;
      }
    } while (right);
    path->vec[h][0] = left;
    path->vec[h][1] = right;
  }
  // should continue insert
  return false;
}

  static bool
skiplist_insert_helper(struct skiplist * const list, struct skippath * const path,
    const u64 hh, struct kv * const kv, const bool safe)
{
  const u64 height = skiplist_random_height(list, path, hh);

  if (safe) { // other writers may insert keys to the path
    skiplist_lock(list);
    if (skiplist_insert_fix_path(path, height, kv)) {
      skiplist_unlock(list);
      return true;
    }
  }

  const bool r = skiplist_insert_height(list, path, kv, height);

  if (safe)
    skiplist_unlock(list);
  return r;
}

  static bool
skiplist_put_helper(struct skiplist * const list, struct kv * const kv, const bool safe)
{
  struct kv * const newkv = list->mm.in(kv, list->mm.priv);
  if (!newkv)
    return false;

  struct kref kref;
  kref_ref_kv(&kref, kv);
  struct skipnode * node;
  struct skippath path;
  const u64 hh = list->height;
  const bool r = skiplist_search_ge_path(list, &kref, &node, &path, hh);
  if (r) { // replace
    if (safe) {
      skiplist_lock(list);
      newkv->privptr = node->kv;
      node->kv = newkv;
      skiplist_unlock(list);
    } else {
      list->mm.free(node->kv, list->mm.priv);
      newkv->privptr = NULL;
      node->kv = newkv;
    }
    return true;
  }

  return skiplist_insert_helper(list, &path, hh, newkv, safe);
}

  bool
skiplist_put(struct skiplist * const list, struct kv * const kv)
{
  return skiplist_put_helper(list, kv, false);
}

  bool
skipsafe_put(struct skiplist * const list, struct kv * const kv)
{
  return skiplist_put_helper(list, kv, true);
}

  static bool
skiplist_merge_helper(struct skiplist * const list, const struct kref * const kref,
    kv_merge_func uf, void * const priv, const bool safe)
{
  struct skipnode * node;
  struct skippath path;
  const u64 hh = list->height;

  const bool r = skiplist_search_ge_path(list, kref, &node, &path, hh);

  if (r) {
    if (safe) {
      skiplist_lock(list);
      struct kv * const old = node->kv;
      struct kv * const kv = uf(old, priv);
      if ((kv != old) && (kv != NULL)) { // replace
        struct kv * const newkv = list->mm.in(kv, list->mm.priv);
        if (!newkv) {
          skiplist_unlock(list);
          return false;
        }
        newkv->privptr = old;
        node->kv = newkv;
      }
      skiplist_unlock(list);
    } else { // unsafe
      struct kv * const old = node->kv;
      struct kv * const kv = uf(old, priv);
      if (kv != old) { // replace
        struct kv * const newkv = list->mm.in(kv, list->mm.priv);
        if (!newkv)
          return false;

        list->mm.free(old, list->mm.priv);
        newkv->privptr = NULL;
        node->kv = newkv;
      }
    }
    return true;
  }

  struct kv * const kv = uf(NULL, priv);
  if (!kv) // do nothing
    return true;

  struct kv * const newkv = list->mm.in(kv, list->mm.priv);
  if (!newkv)
    return false;

  return skiplist_insert_helper(list, &path, hh, newkv, safe);
}

  bool
skiplist_merge(struct skiplist * const list, const struct kref * const kref,
    kv_merge_func uf, void * const priv)
{
  return skiplist_merge_helper(list, kref, uf, priv, false);
}

  bool
skipsafe_merge(struct skiplist * const list, const struct kref * const kref,
    kv_merge_func uf, void * const priv)
{
  return skiplist_merge_helper(list, kref, uf, priv, true);
}

  bool
skiplist_inp(struct skiplist * const list, const struct kref * const key,
    kv_inp_func uf, void * const priv)
{
  struct skipnode * node;
  const bool r = skiplist_search_ge(list, key, &node);
  uf(r ? node->kv : NULL, priv);
  return r;
}

// return the previous node if ret->next matches the key
  static u64
skiplist_search_del_prev(struct skiplist * const list, const struct kref * const key,
    struct skipnode ** const prev)
{
  debug_assert(list->height);
  u64 h = list->height;
  struct skipnode * left = &(list->n0); // leftmost
  struct skipnode * next;
  while ((--h) < SL_MAXH) {
    while ((next = skiplist_next(left, h)) != NULL) {
      const int cmp = kref_kv_compare(key, next->kv);
      if (cmp > 0) { // forward and continue
        left = next;
      } else if (cmp < 0) { // done at this level
        break;
      } else { // match
        *prev = left;
        return h;
      }
    }
  }
  return SL_MAXH;
}

// for unsafe skiplist only
  bool
skiplist_del(struct skiplist * const list, const struct kref * const key)
{
  struct skipnode * prev = NULL;
  u64 h = skiplist_search_del_prev(list, key, &prev);
  if (h == SL_MAXH)
    return false;

  struct skipnode * const victim = skiplist_next(prev, h);
  prev->next[h].ptr = victim->next[h].ptr;

  while ((--h) < SL_MAXH) {
    while (prev->next[h].ptr != victim)
      prev = prev->next[h].ptr;
    prev->next[h].ptr = victim->next[h].ptr;
  }

  list->mm.free(victim->kv, list->mm.priv);
  free(victim);
  return true;
}

  void
skiplist_clean(struct skiplist * const list)
{
  struct skipnode * iter = list->n0.next[0].ptr;
  while (iter) {
    struct skipnode * const next = iter->next[0].ptr;
    struct kv * kviter = iter->kv;
    while (kviter) { // free the chain
      struct kv * tmp = kviter->privptr;
      list->mm.free(kviter, list->mm.priv);
      kviter = tmp;
    }
    free(iter);
    iter = next;
  }
  for (u64 i = 0; i < SL_MAXH; i++)
    list->n0.next[i].ptr = NULL;
}

  void
skiplist_destroy(struct skiplist * const list)
{
  skiplist_clean(list);
  free(list);
}

  void
skiplist_fprint(struct skiplist * const list, FILE * const out)
{
  u64 hs[SL_MAXH] = {};
  u32 costs[SL_MAXH];
  struct skipnode * nexts[SL_MAXH+1];
  const u64 hh = list->height;
  debug_assert(hh && hh < SL_MAXH);
  for (u64 i = 0; i < hh; i++) {
    nexts[i] = skiplist_next(&list->n0, i);
    costs[i] = 1u;
  }
  nexts[hh] = NULL;

  struct skipnode * iter = nexts[0]; // the first item
  u64 totcost = 0;
  u64 totkv = 0;
  while (iter) {
    u64 h = 0;
    while ((h+1) < SL_MAXH && nexts[h+1] == iter) {
      costs[h] = 1;
      nexts[h] = skiplist_next(iter, h);
      h++;
    }
    nexts[h] = skiplist_next(iter, h);
    hs[h]++;

    u32 cost = 0;
    for (u64 i = h; i < hh; i++)
      cost += costs[i];

    // uncomment to print
    //fprintf(out, "h=%2lu c=%3u", h, cost);
    //kv_print(iter->kv, "sn", out);

    costs[h]++;
    iter = skiplist_next(iter, 0);
    totcost += cost;
    totkv++;
  }

  const double avgcost = (double)totcost / (double)totkv;
  fprintf(out, "SKIPLIST count %lu height %lu avgcost %.3lf\n", totkv, hh, avgcost);
  for (u64 i = 0; i < hh; i += 4) {
    fprintf(out, "SKIPLIST H[%lu] %lu H[%lu] %lu H[%lu] %lu H[%lu] %lu\n",
        i, hs[i], i+1, hs[i+1], i+2, hs[i+2], i+3, hs[i+3]);
  }
}

struct skiplist_iter {
  struct skipnode * curr;
  struct skiplist * list;
};

  struct skiplist_iter *
skiplist_iter_create(struct skiplist * const list)
{
  struct skiplist_iter * const iter = malloc(sizeof(*iter));
  if (iter == NULL)
    return NULL;

  iter->curr = NULL; // invalid
  iter->list = list;
  return iter;
}

  void
skiplist_iter_seek(struct skiplist_iter * const iter, const struct kref * const key)
{
  struct skiplist * list = iter->list;
  skiplist_search_ge(list, key, &iter->curr);
}

  bool
skiplist_iter_valid(struct skiplist_iter * const iter)
{
  return iter->curr != NULL;
}

  struct kv *
skiplist_iter_peek(struct skiplist_iter * const iter, struct kv * const out)
{
  if (!skiplist_iter_valid(iter))
    return NULL;
  struct kv * const curr = iter->curr->kv;
  struct kv * const ret = iter->list->mm.out(curr, out);
  return ret;
}

  bool
skiplist_iter_kref(struct skiplist_iter * const iter, struct kref * const kref)
{
  if (!skiplist_iter_valid(iter))
    return false;
  kref_ref_kv(kref, iter->curr->kv);
  return true;
}

  bool
skiplist_iter_kvref(struct skiplist_iter * const iter, struct kvref * const kvref)
{
  if (!skiplist_iter_valid(iter))
    return false;
  kvref_ref_kv(kvref, iter->curr->kv);
  return true;
}

  void
skiplist_iter_skip1(struct skiplist_iter * const iter)
{
  if (skiplist_iter_valid(iter))
    iter->curr = skiplist_next(iter->curr, 0);
}

  void
skiplist_iter_skip(struct skiplist_iter * const iter, const u32 nr)
{
  for (u32 i = 0; i < nr; i++) {
    if (!skiplist_iter_valid(iter))
      return;
    iter->curr = skiplist_next(iter->curr, 0);
  }
}

  struct kv *
skiplist_iter_next(struct skiplist_iter * const iter, struct kv * const out)
{
  struct kv * const ret = skiplist_iter_peek(iter, out);
  skiplist_iter_skip1(iter);
  return ret;
}

  bool
skiplist_iter_inp(struct skiplist_iter * const iter, kv_inp_func uf, void * const priv)
{
  struct kv * const kv = iter->curr ? iter->curr->kv : NULL;
  uf(kv, priv); // call uf even if (kv == NULL)
  return kv != NULL;
}

  void
skiplist_iter_destroy(struct skiplist_iter * const iter)
{
  free(iter);
}

const struct kvmap_api kvmap_api_skiplist = {
  .ordered = true,
  .unique = true,
  .put = (void *)skiplist_put,
  .get = (void *)skiplist_get,
  .probe = (void *)skiplist_probe,
  .del = (void *)skiplist_del,
  .inpr = (void *)skiplist_inp,
  .inpw = (void *)skiplist_inp,
  .merge = (void *)skiplist_merge,
  .iter_create = (void *)skiplist_iter_create,
  .iter_seek = (void *)skiplist_iter_seek,
  .iter_valid = (void *)skiplist_iter_valid,
  .iter_peek = (void *)skiplist_iter_peek,
  .iter_kref = (void *)skiplist_iter_kref,
  .iter_kvref = (void *)skiplist_iter_kvref,
  .iter_skip1 = (void *)skiplist_iter_skip1,
  .iter_skip = (void *)skiplist_iter_skip,
  .iter_next = (void *)skiplist_iter_next,
  .iter_inp = (void *)skiplist_iter_inp,
  .iter_destroy = (void *)skiplist_iter_destroy,
  .clean = (void *)skiplist_clean,
  .destroy = (void *)skiplist_destroy,
  .fprint = (void *)skiplist_fprint,
};

const struct kvmap_api kvmap_api_skipsafe = {
  .ordered = true,
  .unique = true,
  .irefsafe = true,
  .put = (void *)skipsafe_put,
  .get = (void *)skiplist_get,
  .probe = (void *)skiplist_probe,
  .del = NULL,
  .inpr = (void *)skiplist_inp,
  .inpw = (void *)skiplist_inp,
  .merge = (void *)skipsafe_merge,
  .iter_create = (void *)skiplist_iter_create,
  .iter_seek = (void *)skiplist_iter_seek,
  .iter_valid = (void *)skiplist_iter_valid,
  .iter_peek = (void *)skiplist_iter_peek,
  .iter_kref = (void *)skiplist_iter_kref,
  .iter_kvref = (void *)skiplist_iter_kvref,
  .iter_skip1 = (void *)skiplist_iter_skip1,
  .iter_skip = (void *)skiplist_iter_skip,
  .iter_next = (void *)skiplist_iter_next,
  .iter_inp = (void *)skiplist_iter_inp,
  .iter_destroy = (void *)skiplist_iter_destroy,
  .clean = (void *)skiplist_clean,
  .destroy = (void *)skiplist_destroy,
  .fprint = (void *)skiplist_fprint,
};

  static void *
skiplist_kvmap_api_create(const char * const name, const struct kvmap_mm * const mm, char ** args)
{
  if (strcmp(name, "skiplist") && strcmp(name, "skipsafe"))
    return NULL;
  (void)args;
  return skiplist_create(mm);
}

__attribute__((constructor))
  static void
skiplist_kvmap_api_init(void)
{
  kvmap_api_register(0, "skiplist", "", skiplist_kvmap_api_create, &kvmap_api_skiplist);
  kvmap_api_register(0, "skipsafe", "", skiplist_kvmap_api_create, &kvmap_api_skipsafe);
}
// }}} skiplist

// rdb {{{
#ifdef ROCKSDB
#define ROCKSDB_SYNC_SIZE ((1lu<<25))
#include <rocksdb/c.h>
struct rdb {
  rocksdb_t * db;
  rocksdb_filterpolicy_t * bf;
  rocksdb_options_t * dopt;
  u64 sync_size;
  rocksdb_writeoptions_t * wopt;
  rocksdb_writeoptions_t * wopt_sync;
  rocksdb_readoptions_t * ropt;
  rocksdb_cache_t * cache;
  char * path;
  char * err;
};

  struct rdb *
rdb_create(const char * const path, const u64 cache_size_mb)
{
  rocksdb_options_t* const dopt = rocksdb_options_create();
  rocksdb_options_set_compression(dopt, 0);
  rocksdb_options_set_create_if_missing(dopt, 1);

  // statistics of everything; uncomment to enable
  //rocksdb_options_enable_statistics(dopt);

  // Total ordered database, flash storage
  // https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
  rocksdb_env_t * const env = rocksdb_create_default_env();
  rocksdb_env_set_background_threads(env, 4);
  rocksdb_options_set_env(dopt, env);

  rocksdb_options_set_compaction_style(dopt, rocksdb_level_compaction);
  rocksdb_options_set_write_buffer_size(dopt, 1024lu << 20);
  rocksdb_options_set_max_write_buffer_number(dopt, 1);
  rocksdb_options_set_max_open_files(dopt, 65536);
  rocksdb_options_set_target_file_size_base(dopt, 64lu << 20);
  rocksdb_options_set_max_background_compactions(dopt, 4);
  rocksdb_options_set_level0_file_num_compaction_trigger(dopt, 8);
  rocksdb_options_set_level0_slowdown_writes_trigger(dopt, 17);
  rocksdb_options_set_level0_stop_writes_trigger(dopt, 24);
  rocksdb_options_set_num_levels(dopt, 4);
  rocksdb_options_set_max_bytes_for_level_base(dopt, 512lu << 20);
  rocksdb_options_set_max_bytes_for_level_multiplier(dopt, 8);
  rocksdb_options_set_allow_mmap_writes(dopt, 0);

  // table options
  rocksdb_block_based_table_options_t* const topt = rocksdb_block_based_options_create();
  // bf
  rocksdb_filterpolicy_t * bf = rocksdb_filterpolicy_create_bloom(10);
  rocksdb_block_based_options_set_filter_policy(topt, bf);
  // cache
  rocksdb_cache_t * cache = NULL;
  if (cache_size_mb) { // use block cache
    cache = rocksdb_cache_create_lru(cache_size_mb << 20); // MB
    rocksdb_block_based_options_set_block_cache(topt, cache);
    rocksdb_options_set_allow_mmap_reads(dopt, 0);
    rocksdb_block_based_options_set_cache_index_and_filter_blocks(topt, 1);
    rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(topt, 1);
    rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(topt, 1);
  } else { // use mmap
    rocksdb_options_set_allow_mmap_reads(dopt, 1);
    rocksdb_block_based_options_set_no_block_cache(topt, 1);
  }

  rocksdb_options_set_block_based_table_factory(dopt, topt);

  struct rdb * const rdb = malloc(sizeof(*rdb));
  if (rdb == NULL)
    return NULL;
  rdb->db = rocksdb_open(dopt, path, &rdb->err);
  if (rdb->db == NULL) {
    free(rdb);
    return NULL;
  }
  rdb->path = strdup(path);
  rdb->dopt = dopt;
  rocksdb_block_based_options_destroy(topt);

  rdb->bf = bf;
  rdb->sync_size = 0;
  rdb->wopt = rocksdb_writeoptions_create();
  rdb->wopt_sync = rocksdb_writeoptions_create();
  rocksdb_writeoptions_set_sync(rdb->wopt_sync, 1);
  rdb->ropt = rocksdb_readoptions_create();
  rdb->cache = cache;
  rocksdb_env_destroy(env);
  rocksdb_readoptions_set_fill_cache(rdb->ropt, 1);
  rocksdb_readoptions_set_verify_checksums(rdb->ropt, 0);
  return rdb;
}

  struct kv *
rdb_get(struct rdb * const map, const struct kref * const key, struct kv * const out)
{
  size_t vlen;
  char * const ret = rocksdb_get(map->db, map->ropt, (const char *)key->ptr,
      (size_t)key->len, &vlen, &map->err);
  if (ret) {
    if (out) {
      kv_refill(out, key->ptr, key->len, ret, vlen);
      free(ret);
      return out;
    } else {
      struct kv * const new = kv_create(key->ptr, key->len, ret, vlen);
      free(ret);
      return new;
    }
  } else {
    return NULL;
  }
}

  bool
rdb_probe(struct rdb * const map, const struct kref * const key)
{
  size_t vlen;
  char * const ret = rocksdb_get(map->db, map->ropt, (const char *)key->ptr,
      (size_t)key->len, &vlen, &map->err);
  free(ret);
  return ret != NULL;
}

  bool
rdb_put(struct rdb * const map, struct kv * const kv)
{
  map->sync_size += (kv->klen + kv->vlen);
  const bool do_sync = (map->sync_size >= ROCKSDB_SYNC_SIZE);
  if (do_sync)
    map->sync_size -= ROCKSDB_SYNC_SIZE;

  rocksdb_put(map->db, (do_sync ? map->wopt_sync : map->wopt),
      (const char *)kv->kv, (size_t)kv->klen,
      (const char *)kv_vptr_c(kv), (size_t)kv->vlen, &map->err);
  return true;
}

  bool
rdb_del(struct rdb * const map, const struct kref * const key)
{
  rocksdb_delete(map->db, map->wopt, (const char *)key->ptr, (size_t)key->len, &map->err);
  return true;
}

  void
rdb_destroy(struct rdb * const map)
{
  // XXX: rocksdb/gflags has memoryleak on exit.
  //rocksdb_filterpolicy_destroy(map->bf); // destroyed by rocksdb_options_destroy()
  rocksdb_close(map->db);
  rocksdb_readoptions_destroy(map->ropt);
  rocksdb_writeoptions_destroy(map->wopt);
  rocksdb_writeoptions_destroy(map->wopt_sync);
  if (map->cache)
    rocksdb_cache_destroy(map->cache);
  // uncomment to remove db files
  //rocksdb_destroy_db(map->dopt, map->path, &map->err);
  free(map->path);
  rocksdb_options_destroy(map->dopt);
  free(map);
}

  void
rdb_fprint(struct rdb * const map, FILE * const out)
{
  char * str = rocksdb_options_statistics_get_string(map->dopt);
  if (str)
    fprintf(out, "%s", str);
}

  struct rdb_iter *
rdb_iter_create(struct rdb * const map)
{
  struct rdb_iter * const iter = (typeof(iter))rocksdb_create_iterator(map->db, map->ropt);
  return iter;
}

  void
rdb_iter_seek(struct rdb_iter * const iter, const struct kref * const key)
{
  rocksdb_iter_seek((rocksdb_iterator_t *)iter, (const char *)key->ptr, (size_t)key->len);
}

  bool
rdb_iter_valid(struct rdb_iter * const iter)
{
  struct rocksdb_iterator_t * riter = (typeof(riter))iter;
  return rocksdb_iter_valid(riter);
}

  struct kv *
rdb_iter_peek(struct rdb_iter * const iter, struct kv * const out)
{
  struct rocksdb_iterator_t * riter = (typeof(riter))iter;
  if (rocksdb_iter_valid(riter)) {
    size_t klen, vlen;
    const char * const key = rocksdb_iter_key(riter, &klen);
    const char * const value = rocksdb_iter_value(riter, &vlen);
    if (out) {
      kv_refill(out, (u8 *)key, (u32)klen, (u8 *)value, (u32)vlen);
      return out;
    } else {
      return kv_create((u8 *)key, (u32)klen, (u8 *)value, (u32)vlen);
    }
  }
  return NULL;
}

  void
rdb_iter_skip1(struct rdb_iter * const iter)
{
  struct rocksdb_iterator_t * riter = (typeof(riter))iter;
  if (rocksdb_iter_valid(riter))
    rocksdb_iter_next(riter);
}

  void
rdb_iter_skip(struct rdb_iter * const iter, const u32 nr)
{
  struct rocksdb_iterator_t * riter = (typeof(riter))iter;
  for (u32 i = 0; i < nr; i++) {
    if (!rocksdb_iter_valid(riter))
      break;
    rocksdb_iter_next(riter);
  }
}

  struct kv *
rdb_iter_next(struct rdb_iter * const iter, struct kv * const out)
{
  struct kv * const ret = rdb_iter_peek(iter, out);
  rdb_iter_skip1(iter);
  return ret;
}

  void
rdb_iter_destroy(struct rdb_iter * const iter)
{
  rocksdb_iter_destroy((rocksdb_iterator_t *)iter);
}

const struct kvmap_api kvmap_api_rdb = {
  .ordered = true,
  .threadsafe = true,
  .unique = true,
  .put = (void *)rdb_put,
  .get = (void *)rdb_get,
  .probe = (void *)rdb_probe,
  .del = (void *)rdb_del,
  .iter_create = (void *)rdb_iter_create,
  .iter_seek = (void *)rdb_iter_seek,
  .iter_valid = (void *)rdb_iter_valid,
  .iter_peek = (void *)rdb_iter_peek,
  .iter_skip1 = (void *)rdb_iter_skip1,
  .iter_skip = (void *)rdb_iter_skip,
  .iter_next = (void *)rdb_iter_next,
  .iter_destroy = (void *)rdb_iter_destroy,
  .destroy = (void *)rdb_destroy,
  .fprint = (void *)rdb_fprint,
};

  static void *
rdb_kvmap_api_create(const char * const name, const struct kvmap_mm * const mm, char ** args)
{
  if (strcmp(name, "rdb") != 0)
    return NULL;
  (void)mm;
  return rdb_create(args[0], a2u64(args[1]));
}

// alternatively, call the register function from main()
__attribute__((constructor))
  static void
rdb_kvmap_api_init(void)
{
  kvmap_api_register(2, "rdb", "<path> <cache-mb>", rdb_kvmap_api_create, &kvmap_api_rdb);
}
#endif // ROCKSDB
// }}} rdb

// kvell {{{
#ifdef KVELL
// https://github.com/wuxb45/KVell
// make libkvell.so and install to /usr/local/lib
// https://github.com/wuxb45/KVell/blob/master/kvell.h
  extern void
kvell_init(const char * prefix, const char * nd, const char * wpd, const char * cgb, const char * qd);

  extern void
kvell_get_submit(const void * key, size_t klen, const uint64_t hash, void (*func)(void * item, uint64_t arg1, uint64_t arg2), uint64_t arg1, uint64_t arg2);

  extern void
kvell_put_submit(const void * key, size_t klen, const uint64_t hash, const void * value, size_t vlen, void (*func)(void * item, uint64_t arg1, uint64_t arg2), uint64_t arg1, uint64_t arg2);

  extern void
kvell_del_submit(const void * key, size_t klen, const uint64_t hash, void (*func)(void * item, uint64_t arg1, uint64_t arg2), uint64_t arg1, uint64_t arg2);

// XXX does 50 scans
  extern void
kvell_scan50(const void * key, size_t klen, const uint64_t hash, void (*func)(void * item, uint64_t arg1, uint64_t arg2), uint64_t arg1, uint64_t arg2);

  void *
kvell_create(char ** args)
{
  kvell_init(args[0], args[1], args[2], args[3], args[4]);

  struct vctr ** const vctrs = malloc(sizeof(vctrs[0]) * 4); // SET/DEL/GET/SCAN
  for (u32 i = 0; i < 4; i++) {
    vctrs[i] = vctr_create(1lu << 16);
  }
  return vctrs;
}

  static void
kvell_cb(void * item, u64 x, u64 y)
{
  (void)item;
  (void)x;
  (void)y;
}

  static void
kvelll_cb(void * item, u64 vctr_ptr, u64 t0)
{
  (void)item;
  struct vctr * const vctr = (typeof(vctr))vctr_ptr;
  const u64 dt_us = (time_diff_nsec(t0) + 999) / 1000;
  vctr_add1_atomic(vctr, dt_us < (1lu << 16) ? dt_us : (1lu << 16)-1);
}

// GET
  struct kv *
kvell_get(void * const map, const struct kref * const key, struct kv * const out)
{
  (void)map;
  kvell_get_submit(key->ptr, key->len, kv_crc32c_extend(key->hash32), kvell_cb, 0, 0);
  return out;
}

  struct kv *
kvelll_get(void * const map, const struct kref * const key, struct kv * const out)
{
  struct vctr ** const vctrs = map;
  kvell_get_submit(key->ptr, key->len, kv_crc32c_extend(key->hash32), kvelll_cb, (u64)vctrs[2], time_nsec());
  return out;
}

// PROBE
  bool
kvell_probe(void * const map, const struct kref * const key)
{
  (void)map;
  kvell_get_submit(key->ptr, key->len, kv_crc32c_extend(key->hash32), kvell_cb, 0, 0);
  return true;
}

  bool
kvelll_probe(void * const map, const struct kref * const key)
{
  struct vctr ** const vctrs = map;
  kvell_get_submit(key->ptr, key->len, kv_crc32c_extend(key->hash32), kvelll_cb, (u64)vctrs[2], time_nsec());
  return true;
}

// SET
  bool
kvell_put(void * const map, struct kv * const kv)
{
  (void)map;
  kvell_put_submit(kv_kptr(kv), kv->klen, kv->hash, kv_vptr(kv), kv->vlen, kvell_cb, 0, 0);
  return true;
}

  bool
kvelll_put(void * const map, struct kv * const kv)
{
  struct vctr ** const vctrs = map;
  kvell_put_submit(kv_kptr(kv), kv->klen, kv->hash, kv_vptr(kv), kv->vlen, kvelll_cb, (u64)vctrs[0], time_nsec());
  return true;
}

// DEL
  bool
kvell_del(void * const map, const struct kref * const key)
{
  (void)map;
  kvell_del_submit(key->ptr, key->len, kv_crc32c_extend(key->hash32), kvell_cb, 0, 0);
  return true;
}

  bool
kvelll_del(void * const map, const struct kref * const key)
{
  struct vctr ** const vctrs = map;
  kvell_del_submit(key->ptr, key->len, kv_crc32c_extend(key->hash32), kvelll_cb, (u64)vctrs[1], time_nsec());
  return true;
}

  void
kvell_clean(void * const map)
{
  (void)map;
}

  void
kvell_destroy(void * const map)
{
  sleep(2); // wait for async completion
  struct vctr ** const vctrs = map;
  for (u32 i = 0; i < 4; i++)
    vctr_destroy(vctrs[i]);
  free(map);
}

  void
kvell_latency_fprint(void * const map, FILE * const out)
{
  sleep(1); // wait for all async completion
  struct vctr ** const vctrs = map;
  for (u32 vi = 0; vi < 4; vi++) {
    struct vctr * const v = vctrs[vi];
    u64 sum = 0;
    for (u64 i = 0; i < (1lu << 16); i++)
      sum += vctr_get(v, i);
    if (sum == 0)
      continue;

    const u64 tot = sum;
    const double totd = (double)tot;
    sum = 0;
    u64 last = 0;
    fprintf(out, "[%u] (SET/DEL/GET/SCAN50)\ntime_us  count  delta  cdf\n0 0 0 0.000\n", vi);
    for (u64 i = 1; i < (1lu << 16); i++) {
      const u64 tmp = vctr_get(v, i);
      if (tmp) {
        if ((i-1) != last)
          fprintf(out, "%lu %lu %lu %.3lf\n", i-1, sum, 0lu, (double)sum * 100.0 / totd);
        sum += tmp;
        fprintf(out, "%lu %lu %lu %.3lf\n", i, sum, tmp, (double)sum * 100.0 / totd);
        last = i;
      }
    }
    fprintf(out, "total %lu\n", tot);
    vctr_reset(v);
  }
}

  void
kvell_fprint(void * const map, FILE * const out)
{
  (void)map;
  (void)out;
}

  void *
kvell_iter_create(void * const map)
{
  return map;
}

// SCAN50
  void
kvell_iter_seek(void * const iter, const struct kref * const key)
{
  (void)iter;
  // XXX: YCSB: do everything in seek
  kvell_scan50(key->ptr, key->len, kv_crc32c_extend(key->hash32), kvell_cb, 0, 0);
}

  void
kvelll_iter_seek(void * const iter, const struct kref * const key)
{
  struct vctr ** const vctrs = iter;
  // XXX: YCSB: do everything in seek
  kvell_scan50(key->ptr, key->len, kv_crc32c_extend(key->hash32), kvelll_cb, (u64)vctrs[3], time_nsec());
}

  bool
kvell_iter_valid(void * const iter)
{
  (void)iter;
  return true;
}

  struct kv *
kvell_iter_peek(void * const iter, struct kv * const out)
{
  (void)iter;
  (void)out;
  return out;
}

  void
kvell_iter_skip1(void * const iter)
{
  (void)iter;
}

  void
kvell_iter_skip(void * const iter, const u32 nr)
{
  (void)iter;
  (void)nr;
}

  struct kv *
kvell_iter_next(void * const iter, struct kv * const out)
{
  (void)iter;
  (void)out;
  return NULL;
}

  void
kvell_iter_destroy(void * const iter)
{
  (void)iter; // is map
}

const struct kvmap_api kvmap_api_kvell = {
  .hashkey = true,
  .ordered = true,
  .threadsafe = true,
  .unique = true,
  .put = kvell_put,
  .get = kvell_get,
  .probe = kvell_probe,
  .del = kvell_del,
  .iter_create = kvell_iter_create,
  .iter_seek = kvell_iter_seek,
  .iter_valid = kvell_iter_valid,
  .iter_peek = kvell_iter_peek,
  .iter_skip1 = kvell_iter_skip1,
  .iter_skip = kvell_iter_skip,
  .iter_next = kvell_iter_next,
  .iter_destroy = kvell_iter_destroy,
  .clean = kvell_clean,
  .destroy = kvell_destroy,
  .fprint = kvell_fprint,
};

const struct kvmap_api kvmap_api_kvelll = {
  .hashkey = true,
  .ordered = true,
  .threadsafe = true,
  .unique = true,
  .async = true,
  .put = kvelll_put,
  .get = kvelll_get,
  .probe = kvelll_probe,
  .del = kvelll_del,
  .iter_create = kvell_iter_create,
  .iter_seek = kvelll_iter_seek,
  .iter_valid = kvell_iter_valid,
  .iter_peek = kvell_iter_peek,
  .iter_skip1 = kvell_iter_skip1,
  .iter_skip = kvell_iter_skip,
  .iter_next = kvell_iter_next,
  .iter_destroy = kvell_iter_destroy,
  .clean = kvell_clean,
  .destroy = kvell_destroy,
  .fprint = kvell_latency_fprint,
};

  static void *
kvell_kvmap_api_create(const char * const name, const struct kvmap_mm * const mm, char ** args)
{
  (void)mm;
  if (0 == strcmp(name, "kvell") || 0 == strcmp(name, "kvelll"))
    return kvell_create(args);
  else
    return NULL;
}

__attribute__((constructor))
  static void
kvell_kvmap_api_init(void)
{
  kvmap_api_register(5, "kvell", "<prefix> <ndisk> <wpd> <cache-GB> <queue-depth>", kvell_kvmap_api_create, &kvmap_api_kvell);
  kvmap_api_register(5, "kvelll", "<prefix> <ndisk> <wpd> <cache-GB> <queue-depth>", kvell_kvmap_api_create, &kvmap_api_kvelll);
}
#endif // KVELL
// }}} kvell

// vim:fdm=marker
