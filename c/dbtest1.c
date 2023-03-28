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

#define XSA ((0)) // Set-All
#define XSS ((1)) // Set-Success
#define XDA ((2))
#define XDS ((3))
#define XGA ((4))
#define XGS ((5))
#define XPA ((6))
#define XPS ((7))
#define XNA ((8))
#define XNS ((9))
#define XKA ((10))
#define XKS ((11))
#define VCTRSZ ((12))

  static bool
kvmap_analyze(void * const passdata[2], const u64 dt, const struct vctr * const va, struct damp * const d, char * const out)
{
  (void)passdata;
  size_t v[VCTRSZ];
  for (u64 i = 0; i < VCTRSZ; i++)
    v[i] = vctr_get(va, i);

  const u64 nrop = v[XSA] + v[XDA] + v[XGA] + v[XPA] + v[XNA] + v[XKA];
  const double mops = ((double)nrop) * 1e3 / ((double)dt);
  const bool done = damp_add_test(d, mops);
  char buf[64];
  if (v[XSA]) {
    sprintf(buf, " set %zu %zu", v[XSA], v[XSS]);
  } else if (v[XDA]) {
    sprintf(buf, " del %zu %zu", v[XDA], v[XDS]);
  } else if (v[XGA]) {
    sprintf(buf, " get %zu %zu", v[XGA], v[XGS]);
  } else if (v[XPA]) {
    sprintf(buf, " pro %zu %zu", v[XPA], v[XPS]);
  } else if (v[XNA]) {
    sprintf(buf, " seeknext %zu %zu", v[XNA], v[XNS]);
  } else if (v[XKA]) {
    sprintf(buf, " seekskip %zu %zu", v[XKA], v[XKS]);
  } else {
    buf[0] = '\0';
  }
  sprintf(out, "%s mops %.4lf avg %.4lf ravg %.4lf\n", buf, mops, damp_avg(d), damp_ravg(d));
  return done;
}

  static void
kvmap_batch_nop(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  (void)info;
  (void)priv;
  for (u64 i = 0; i < nr; i++)
    cpu_pause();
}

// (parallel) load
  static void
kvmap_batch_put_par(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  if (info->end_type != FORKER_END_COUNT)
    return;

  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  struct kv * const tmp = priv->tmp;
  const u64 nr1 = nr / info->conc;
  const u64 id0 = nr1 * info->worker_id;
  const u64 end = (info->worker_id == (info->conc - 1)) ? nr : (id0 + nr1);
  u64 ss = 0;
  for (u64 i = id0; i < end; i++) {
    kv_refill_hex64_klen(tmp, i, priv->klen, NULL, 0);
    tmp->vlen = priv->vlen;
    if (kvmap_kv_put(api, ref, tmp))
      ss++;
  }
  vctr_add(info->vctr, XSA, end - id0);
  vctr_add(info->vctr, XSS, ss);
}

// (parallel) probe
  static void
kvmap_batch_probe_par(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  if (info->end_type != FORKER_END_COUNT)
    return;

  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  struct kv * const tmp = priv->tmp;
  const u64 nr1 = nr / info->conc;
  const u64 id0 = nr1 * info->worker_id;
  const u64 end = (info->worker_id == (info->conc - 1)) ? nr : (id0 + nr1);
  u64 ss = 0;
  for (u64 i = id0; i < end; i++) {
    kv_refill_hex64_klen(tmp, i, priv->klen, NULL, 0);
    if (kvmap_kv_probe(api, ref, tmp))
      ss++;
  }
  vctr_add(info->vctr, XPA, end - id0);
  vctr_add(info->vctr, XPS, ss);
}

// (parallel) get
  static void
kvmap_batch_get_par(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  if (info->end_type != FORKER_END_COUNT)
    return;

  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  struct kv * const tmp = priv->tmp;
  const u64 nr1 = nr / info->conc;
  const u64 id0 = nr1 * info->worker_id;
  const u64 end = (info->worker_id == (info->conc - 1)) ? nr : (id0 + nr1);
  u64 ss = 0;
  for (u64 i = id0; i < end; i++) {
    kv_refill_hex64_klen(tmp, i, priv->klen, NULL, 0);
    if (kvmap_kv_get(api, ref, tmp, priv->out))
      ss++;
  }
  vctr_add(info->vctr, XGA, end - id0);
  vctr_add(info->vctr, XGS, ss);
}

// (parallel) del
  static void
kvmap_batch_del_par(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  if (info->end_type != FORKER_END_COUNT)
    return;

  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  struct kv * const tmp = priv->tmp;
  const u64 nr1 = nr / info->conc;
  const u64 id0 = nr1 * info->worker_id;
  const u64 end = (info->worker_id == (info->conc - 1)) ? nr : (id0 + nr1);
  u64 ss = 0;
  for (u64 i = id0; i < end; i++) {
    kv_refill_hex64_klen(tmp, i, priv->klen, NULL, 0);
    if (kvmap_kv_del(api, ref, tmp))
      ss++;
  }
  vctr_add(info->vctr, XDA, end - id0);
  vctr_add(info->vctr, XDS, ss);
}

  static void
kvmap_batch_put(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  struct rgen * const gen = info->gen;
  rgen_next_func next = info->rgen_next_write;
  struct kv * const tmp = priv->tmp;
  u64 ss = 0lu;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(tmp, next(gen), priv->klen, NULL, 0);
    tmp->vlen = priv->vlen;
    if (kvmap_kv_put(api, ref, tmp))
      ss++;
  }
  vctr_add(info->vctr, XSA, nr);
  vctr_add(info->vctr, XSS, ss);
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
  u64 ss = 0lu;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(tmp, next(gen), priv->klen, NULL, 0);
    if (kvmap_kv_del(api, ref, tmp))
      ss++;
  }
  vctr_add(info->vctr, XDA, nr);
  vctr_add(info->vctr, XDS, ss);
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
  u64 ss = 0lu;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(tmp, next(gen), priv->klen, NULL, 0);
    if (kvmap_kv_get(api, ref, tmp, priv->out))
      ss++;
  }
  vctr_add(info->vctr, XGA, nr);
  vctr_add(info->vctr, XGS, ss);
}

  static void
kvmap_batch_probe(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  const struct kvmap_api * const api = info->passdata[0];
  void * const ref = priv->ref;
  struct rgen * const gen = info->gen;
  rgen_next_func next = info->rgen_next;
  struct kv * const tmp = priv->tmp;
  u64 ss = 0lu;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(tmp, next(gen), priv->klen, NULL, 0);
    if (kvmap_kv_probe(api, ref, tmp))
      ss++;
  }
  vctr_add(info->vctr, XPA, nr);
  vctr_add(info->vctr, XPS, ss);
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
  u64 ss = 0lu;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(priv->tmp, next(gen), priv->klen, NULL, 0);
    kvmap_kv_iter_seek(api, iter, priv->tmp);
    for (u32 j = 0; j < nscan; j++)
      api->iter_next(iter, priv->out);
    if (api->iter_valid(iter))
      ss++;
  }
  vctr_add(info->vctr, XNA, nr);
  vctr_add(info->vctr, XNS, ss);
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
  u64 ss = 0lu;

  for (u64 i = 0; i < nr; i++) {
    kv_refill_hex64_klen(priv->tmp, next(gen), priv->klen, NULL, 0);
    kvmap_kv_iter_seek(api, iter, priv->tmp);
    api->iter_skip(iter, nscan);
    if (api->iter_peek(iter, priv->out))
      ss++;
  }
  vctr_add(info->vctr, XKA, nr);
  vctr_add(info->vctr, XKS, ss);
  api->iter_destroy(iter);
}

  static void *
kvmap_worker(void * const ptr)
{
  struct forker_worker_info * const info = (typeof(info))ptr;
  srandom_u64(info->seed);

  const char op = info->argv[0][0];
  typeof(kvmap_batch_probe) * batch_func = NULL;
  switch (op) {
  case 's': batch_func = kvmap_batch_put; break;
  case 'd': batch_func = kvmap_batch_del; break;
  case 'p': batch_func = kvmap_batch_probe; break;
  case 'g': batch_func = kvmap_batch_get; break;
  case 'n': batch_func = kvmap_batch_seek_next; break;
  case 'k': batch_func = kvmap_batch_seek_skip; break;
  case 'S': batch_func = kvmap_batch_put_par; break;
  case 'D': batch_func = kvmap_batch_del_par; break;
  case 'P': batch_func = kvmap_batch_probe_par; break;
  case 'G': batch_func = kvmap_batch_get_par; break;
  default: batch_func = kvmap_batch_nop; break;
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
  fprintf(stderr, "%s dbtest wargs[%d]: <sdgpnkSDGP> <klen> <vlen/nscan>\n", __func__, NARGS);
  fprintf(stderr, "%s s:set d:del g:get p:probe n:seeknext k:seekskip\n", __func__);
  fprintf(stderr, "%s S:set D:del G:get P:probe (auto-parallel: magic-type=1; magic=nr_kvs; rgen ignored)\n", __func__);
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
  pref[n1] = NULL;

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
