/*
 * Copyright (c) 2016--2021  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#define _GNU_SOURCE

#include "lib.h"
#include "kv.h"

struct priv {
  void * ref;
  u32 klen;
  union {
    u32 vlen;
    u32 nscan;
  };
  struct kv * tmp;
  struct kv * out;
};

#define VCTRSZ ((10000))

  static bool
kvmap_analyze(void * const passdata[2], const u64 dt, const struct vctr * const va, struct damp * const d, char * const out)
{
  (void)dt;
  (void)d;
  const struct kvmap_api * const api = passdata[0];
  if (api->async) {
    api->fprint(passdata[1], stdout);
    strcpy(out, "\n");
    return true;
  }

  u64 sum = 0;
  for (u64 i = 0; i < VCTRSZ; i++)
    sum += vctr_get(va, i);

  const u64 tot = sum;
  const double totd = (double)tot;
  sum = 0;
  u64 last = 0;
  printf("time_us count delta cdf\n0 0 0 0.000\n");
  for (u64 i = 1; i < VCTRSZ; i++) {
    const u64 tmp = vctr_get(va, i);
    if (tmp) {
      if ((i-1) != last)
        printf("%lu %lu %lu %.3lf\n", i-1, sum, 0lu, (double)sum * 100.0 / totd);
      sum += tmp;
      printf("%lu %lu %lu %.3lf\n", i, sum, tmp, (double)sum * 100.0 / totd);
      last = i;
    }
  }

  sprintf(out, "total %lu\n", tot);
  return true;
}

  static void
latency_add(struct vctr * const vctr, const u64 dt)
{
  debug_assert(dt);
  const u64 us = (dt + 999) / 1000;
  if (us < VCTRSZ) {
    vctr_add1_atomic(vctr, us);
  } else {
    vctr_add1_atomic(vctr, VCTRSZ-1);
    printf("%s micro-second %lu\n", __func__, us);
  }
}

// (parallel) load; nr <= nr_kvs
  static void
kvmap_batch_set_par(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  if (info->end_type != FORKER_END_COUNT)
    return;

  struct kv * const tmp = priv->tmp;
  const u64 nr1 = nr / info->conc;
  const u64 id0 = nr1 * info->worker_id;
  const u64 end = (info->worker_id == (info->conc - 1)) ? nr : (id0 + nr1);
  for (u64 i = id0; i < end; i++) {
    kv_refill_hex64_klen(tmp, i, priv->klen, NULL, 0);
    tmp->vlen = priv->vlen;
    const u64 t0 = time_nsec();
    (void)kvmap_kv_put(api, ref, tmp);
    const u64 dt = time_diff_nsec(t0);
    latency_add(info->vctr, dt);
  }
}

  static void
kvmap_batch_set(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  struct rgen * const gen = info->gen;
  rgen_next_func next = info->rgen_next_write;
  struct kv * const tmp = priv->tmp;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(tmp, next(gen), priv->klen, NULL, 0);
    tmp->vlen = priv->vlen;
    const u64 t0 = time_nsec();
    (void)kvmap_kv_put(api, ref, tmp);
    const u64 dt = time_diff_nsec(t0);
    latency_add(info->vctr, dt);
  }
}

  static void
kvmap_batch_del(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  struct rgen * const gen = info->gen;
  rgen_next_func next = info->rgen_next_write;
  struct kv * const tmp = priv->tmp;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(tmp, next(gen), priv->klen, NULL, 0);
    const u64 t0 = time_nsec();
    (void)kvmap_kv_del(api, ref, tmp);
    const u64 dt = time_diff_nsec(t0);
    latency_add(info->vctr, dt);
  }
}

  static void
kvmap_batch_get(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  struct rgen * const gen = info->gen;
  rgen_next_func next = info->rgen_next;
  struct kv * const tmp = priv->tmp;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(tmp, next(gen), priv->klen, NULL, 0);
    const u64 t0 = time_nsec();
    (void)kvmap_kv_get(api, ref, tmp, priv->out);
    const u64 dt = time_diff_nsec(t0);
    latency_add(info->vctr, dt);
  }
}

  static void
kvmap_batch_pro(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  struct rgen * const gen = info->gen;
  rgen_next_func next = info->rgen_next;
  struct kv * const tmp = priv->tmp;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(tmp, next(gen), priv->klen, NULL, 0);
    const u64 t0 = time_nsec();
    (void)kvmap_kv_probe(api, ref, tmp);
    const u64 dt = time_diff_nsec(t0);
    latency_add(info->vctr, dt);
  }
}

  static void
kvmap_batch_seek_next(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  void * const iter = api->iter_create(ref);
  const u32 nscan = priv->nscan;
  debug_assert(iter);
  struct rgen * const gen = info->gen;
  rgen_next_func next = info->rgen_next;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(priv->tmp, next(gen), priv->klen, NULL, 0);
    const u64 t0 = time_nsec();
    kvmap_kv_iter_seek(api, iter, priv->tmp);
    for (u32 j = 0; j < nscan; j++)
      api->iter_next(iter, priv->out);
    const u64 dt = time_diff_nsec(t0);
    latency_add(info->vctr, dt);
  }
  api->iter_destroy(iter);
}

  static void
kvmap_batch_seek_skip(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  void * const iter = api->iter_create(ref);
  const u32 nscan = priv->nscan;
  debug_assert(iter);
  struct rgen * const gen = info->gen;
  rgen_next_func next = info->rgen_next;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(priv->tmp, next(gen), priv->klen, NULL, 0);
    const u64 t0 = time_nsec();
    kvmap_kv_iter_seek(api, iter, priv->tmp);
    api->iter_skip(iter, nscan);
    const u64 dt = time_diff_nsec(t0);
    latency_add(info->vctr, dt);
  }
  api->iter_destroy(iter);
}

  static void *
kvmap_worker(void * const ptr)
{
  struct forker_worker_info * const info = (typeof(info))ptr;
  srandom_u64(info->seed);

  const char op = info->argv[0][0];
  typeof(kvmap_batch_pro) * batch_func = NULL;
  switch (op) {
  case 'p': batch_func = kvmap_batch_pro; break;
  case 'g': batch_func = kvmap_batch_get; break;
  case 's': batch_func = kvmap_batch_set; break;
  case 'S': batch_func = kvmap_batch_set_par; break;
  case 'd': batch_func = kvmap_batch_del; break;
  case 'n': batch_func = kvmap_batch_seek_next; break;
  case 'k': batch_func = kvmap_batch_seek_skip; break;
  default: debug_die(); break;
  }

  struct priv p;
  p.klen = a2u32(info->argv[1]);
  p.vlen = a2u32(info->argv[2]); // vlen/nscan
  const struct kvmap_api * const api = info->passdata[0];
  p.ref = kvmap_ref(api, info->passdata[1]);
  const u64 outlen = sizeof(struct kv) + p.klen + p.vlen + 4096;
  p.tmp = yalloc(outlen);
  debug_assert(p.tmp);
  memset(p.tmp, 0, outlen);
  p.out = yalloc(outlen);
  debug_assert(p.out);
  if (info->end_type == FORKER_END_TIME) {
    do {
      batch_func(info, &p, 1lu << 14); // batch size
    } while (time_nsec() < info->end_magic);
  } else if (info->end_type == FORKER_END_COUNT) {
    batch_func(info, &p, info->end_magic);
  }
  kvmap_unref(api, p.ref);
  free(p.out);
  free(p.tmp);
  return NULL;
}

#define NARGS ((3))
  static void
dbtest_help_message(void)
{
  fprintf(stderr, "%s Usage: {api ... {rgen ... {pass ...}}}\n", __func__);
  kvmap_api_helper_message();
  forker_passes_message();
  fprintf(stderr, "%s dbtest wargs[%d]: <sSdgpnk> <klen> <vlen/nscan>\n", __func__, NARGS);
  fprintf(stderr, "%s s:set S:load d:del g:get p:probe n:seeknext k:seekskip\n", __func__);
}

  static int
test_kvmap(const int argc, char ** const argv)
{
  const struct kvmap_api * api = NULL;
  void * map = NULL;
  const int n1 = kvmap_api_helper(argc, argv, NULL, &api, &map);
  if (n1 < 0)
    return n1;

  char *pref[64] = {};
  memcpy(pref, argv, sizeof(pref[0]) * (size_t)n1);

  struct pass_info pi = {};
  pi.passdata[0] = (void *)api;
  pi.passdata[1] = map;
  pi.vctr_size = VCTRSZ;
  pi.wf = kvmap_worker;
  pi.af = kvmap_analyze;
  const int n2 = forker_passes(argc - n1, argv + n1, pref, &pi, NARGS);

  if (api->fprint)
    api->fprint(map, stderr);

  api->destroy(map);
  if (n2 < 0) {
    return n2;
  } else {
    return n1 + n2;
  }
}

  int
main(int argc, char ** argv)
{
  if (argc < 3) {
    dbtest_help_message();
    exit(0);
  }

  const bool r = forker_main(argc - 1, argv + 1, test_kvmap);
  if (r == false)
    dbtest_help_message();
  return 0;
}
