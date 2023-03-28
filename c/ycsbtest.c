/*
 * Copyright (c) 2016--2021  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#define _GNU_SOURCE

#include "lib.h"
#include "kv.h"

struct priv {
  u32 klen;
  u32 vlen;
  u32 nscan;
  u32 cget;
  u32 cset;
  u32 cupd;
  u32 cscn;
  void * ref;
  void * iter;
  struct kv * tmp;
  struct kv * out;
  struct kv ** kvs;
};

// load (use set) same to insert for latest dist.
#define XSA ((0))
#define XSS ((1))
// update (use merge), same to rmw (workload f, rarely mentioned)
#define XMA ((2))
#define XMS ((3))
// read (get)
#define XGA ((4))
#define XGS ((5))
// scan (seek-next)
#define XNA ((6))
#define XNS ((7))
// total number
#define XNR ((8))

  static bool
kvmap_analyze(void * const passdata[2], const u64 dt, const struct vctr * const va, struct damp * const d, char * const out)
{
  (void)passdata;
  size_t v[XNR];
  for (u64 i = 0; i < XNR; i++)
    v[i] = vctr_get(va, i);

  const u64 nrop = v[XSA] + v[XMA] + v[XGA] + v[XNA];
  const double mops = ((double)nrop) * 1e3 / ((double)dt);
  const bool done = damp_add_test(d, mops);
  const char * const pat = " set %zu %zu upd %zu %zu get %zu %zu scan %zu %zu mops %.4lf avg %.4lf ravg %.4lf\n";
  sprintf(out, pat, v[XSA], v[XSS], v[XMA], v[XMS], v[XGA], v[XGS], v[XNA], v[XNS], mops, damp_avg(d), damp_ravg(d));
  return done;
}

  static struct kv *
kvmap_merge_dummy(struct kv * const key0, void * const priv)
{
  (void)key0;
  return (struct kv *)priv;
}

  static void
kvmap_batch(const struct forker_worker_info * const info,
    const struct priv * const priv, const u64 nr)
{
  const struct kvmap_api * const api = (typeof(api))info->passdata[0];
  void * const ref = priv->ref;
  struct vctr * const v = info->vctr;
  struct rgen * const gen = info->gen;
  rgen_next_func next = info->rgen_next;
  rgen_next_func next_write = info->rgen_next_write;
  struct kv * const tmp = priv->tmp;

  for (u64 i = 0; i < nr; i++) {
    const u32 p = random_u64() & 0xffffu;
    if (p < priv->cget) { // GET
      kv_refill_hex64_klen(tmp, next(gen), priv->klen, NULL, 0);
      vctr_add1(v, XGA);
      if (kvmap_kv_get(api, ref, tmp, priv->out))
        vctr_add1(v, XGS);

    } else if (p < priv->cscn) { // SCAN
      kv_refill_hex64_klen(tmp, next(gen), priv->klen, NULL, 0);
      vctr_add1(v, XNA);

      void * const iter = priv->iter;
      debug_assert(iter);

      kvmap_kv_iter_seek(api, iter, tmp);
      for (u32 k = 0; k < priv->nscan; k++)
        api->iter_next(iter, priv->out);
      if (api->iter_valid(iter))
        vctr_add1(v, XNS);

      // may need to park
      if (api->iter_park)
        api->iter_park(iter);

    } else if (p < priv->cset) { // SET
      kv_refill_hex64_klen(tmp, next_write(gen), priv->klen, NULL, 0);
      tmp->vlen = priv->vlen;
      vctr_add1(v, XSA);
      if (kvmap_kv_put(api, ref, tmp))
        vctr_add1(v, XSS);

    } else { // UPDATE (RMW)
      kv_refill_hex64_klen(tmp, next_write(gen), priv->klen, NULL, 0);
      tmp->vlen = priv->vlen;
      vctr_add1(v, XMA);
      if (api->merge) { // use merge()
        if (kvmap_kv_merge(api, ref, tmp, kvmap_merge_dummy, tmp))
          vctr_add1(v, XMS);
      } else { // GET & PUT
        (void)kvmap_kv_get(api, ref, tmp, priv->out); // read
        if (kvmap_kv_put(api, ref, tmp)) // write
          vctr_add1(v, XMS);
      }
    }
  }
}

  static void *
kvmap_worker(void * const ptr)
{
  struct forker_worker_info * const info = (typeof(info))ptr;
  srandom_u64(info->seed);
  const struct kvmap_api * const api = (typeof(api))info->passdata[0];

  struct priv p = {};
  const u32 pset = a2u32(info->argv[0]);
  const u32 pupd = a2u32(info->argv[1]);
  const u32 pget = a2u32(info->argv[2]);
  const u32 pscn = a2u32(info->argv[3]);
  // scaled to 65536
  p.cget = pget * 65536 / 100; // 1st
  p.cscn = (pget + pscn) * 65536 / 100; // 2nd
  p.cset = (pget + pscn + pset) * 65536 / 100; // 3rd
  p.cupd = 65536; // not used
  (void)pupd;

  p.klen = a2u32(info->argv[4]);
  p.vlen = a2u32(info->argv[5]);
  p.nscan = a2u32(info->argv[6]);
  p.ref = kvmap_ref(api, info->passdata[1]);

  if (pscn) {
    p.iter = api->iter_create(p.ref);
    if (api->iter_park)
      api->iter_park(p.iter);
  }

  const u64 outlen = sizeof(struct kv) + p.klen + p.vlen + 4096;
  p.tmp = yalloc(outlen);
  debug_assert(p.tmp);
  memset(p.tmp, 0, outlen);
  p.out = yalloc(outlen);
  debug_assert(p.out);

  if (info->end_type == FORKER_END_TIME) {
    do {
      kvmap_batch(info, &p, 1lu << 14);
    } while (time_nsec() < info->end_magic);
  } else if (info->end_type == FORKER_END_COUNT) {
    kvmap_batch(info, &p, info->end_magic);
  }

  if (pscn)
    api->iter_destroy(p.iter);

  kvmap_unref(api, p.ref);
  free(p.tmp);
  free(p.out);
  return NULL;
}

#define NARGS ((7))
  static void
maptest_help_message(void)
{
  fprintf(stderr, "%s Usage: {api ... {rgen ... {pass ...}}}\n", __func__);
  kvmap_api_helper_message();
  forker_passes_message();
  fprintf(stderr, "%s wargs[%d]: <pset> <pupd> <pget> <pscn> <klen> <vlen> <nscan>\n", __func__, NARGS);
  fprintf(stderr, "%s load kv samples at cpu: MAPTEST_KVLOAD_CPU=<cpu>; default:1\n", __func__);
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
  for (int i = 0; i < n1; i++) {
    pref[i] = argv[i];
  }
  pref[n1] = NULL;

  struct pass_info pi = {};
  pi.passdata[0] = (void *)api;
  pi.passdata[1] = map;
  pi.vctr_size = XNR;
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
    maptest_help_message();
    exit(0);
  }

  const bool r = forker_main(argc - 1, argv + 1, test_kvmap);
  if (r == false)
    maptest_help_message();
  return 0;
}
