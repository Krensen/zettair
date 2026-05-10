/* okapi.c implements the okapi metric for the zettair query
 * subsystem.  This file was automatically generated from
 * src/okapi.metric and src/metric.c
 * by scripts/metric.py on Mon, 02 Mar 2009 06:38:47 GMT.  
 *
 * DO NOT MODIFY THIS FILE, as changes will be lost upon 
 * subsequent regeneration (and this code is repetitive enough 
 * that you probably don't want to anyway).  
 * Go modify src/okapi.metric or src/metric.c instead.  
 * 
 * Comments from okapi.metric:
 *
 * okapi.metric is a functional description in our funny zettair metric
 * language (see metric.py) of how the okapi metric should operate.
 * 
 * The okapi metric is probably best described in
 * 'A probabalistic model of information retrieval: development and
 * comparative experiments' parts 1 & 2, by Sparck Jones, Walker and
 * Robertson, although it was (i believe) first presented in
 * 'Okapi at TREC-7: Automatic ad hoc, filtering, VLC and interactive track'
 * by Robertson, Walker, and Beaulieu.
 * 
 * written nml 2005-07-18
 *
 */

#include "zettair.h"

#include "postings.h"
#include "search.h"
#include "str.h"
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

/* Fine-grained accumulators for inner-loop timing inside or_decode_offsets.
 * commandline.c reads these to emit a JSON breakdown.  Reset by search.c
 * at the start of every index_search call. */
double zet_inner_decode_ms = 0.0;
double zet_inner_walk_ms   = 0.0;
double zet_inner_score_ms  = 0.0;
unsigned long int zet_inner_postings = 0;
unsigned long int zet_inner_walk_steps = 0;

static inline double tnow_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1e6 + tv.tv_usec;
}

/* ── Click prior — loaded from ZET_CLICK_PRIOR at startup ── */
static float        *g_click_prior     = NULL;
static unsigned int  g_click_prior_len = 0;
static double        g_click_alpha     = 0.3;

/* ── PRD-017: per-field BM25 boost ───────────────────────────────────────
 * Each posting offset has its field_id in the low POSTINGS_FIELD_BITS bits.
 * At score time, weighted_f_dt = sum over occurrences of g_field_boost[fid].
 * Body (field 0) defaults to 1.0; other fields default to 1.0 too (no boost)
 * unless overridden by ZET_BOOST_TITLE etc. env vars at startup. */
static double g_field_boost[POSTINGS_MAX_FIELDS] = {
    1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
    1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
};
static int g_field_boost_loaded = 0;
/* Fast-path flag: 1 = all boosts are 1.0, so the field-weight sum equals
 * f_dt and we can skip the per-occurrence walk entirely (pure scan).
 * Set by okapi_load_field_boosts based on env vars. */
static int g_field_boost_active = 0;

/* ── PRD-019: per-field BM25 (BM25F) ─────────────────────────────────────
 * A different scoring path: instead of one BM25_tf with a flat per-occurrence
 * weight, compute BM25_tf separately for each field with its own length
 * normalisation, then sum weighted by w_f. Activated by setting
 * ZET_PERFIELD_BM25=1 plus pointing ZET_FIELD_LENGTHS_PATH and
 * ZET_FIELD_STATS_PATH at the sidecars built by build_field_lengths.py. */
static int       g_perfield_active = 0;
static int       g_perfield_loaded = 0;
static uint32_t *g_field_lengths   = NULL;     /* N_docs * POSTINGS_MAX_FIELDS u32 */
static unsigned int g_field_lengths_ndocs = 0;
static double    g_field_avg_len[POSTINGS_MAX_FIELDS];
static unsigned int g_field_n_with[POSTINGS_MAX_FIELDS];
/* Per-field weights (w_f) and length-norm parameters (b_f). Defaults are
 * neutral: w=1.0, b=0.75 (standard BM25). Override per-field via env vars
 * ZET_FIELD_W_BODY / _TITLE / _CAPTION etc., and ZET_FIELD_B_*. */
static double g_field_w[POSTINGS_MAX_FIELDS] = {
    1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
    1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
};
static double g_field_b[POSTINGS_MAX_FIELDS] = {
    0.75, 0.75, 0.75, 0.75, 0.75, 0.75, 0.75, 0.75,
    0.75, 0.75, 0.75, 0.75, 0.75, 0.75, 0.75, 0.75,
};

void okapi_load_perfield(void);  /* forward decl, defined below */

void okapi_load_field_boosts(void) {
    static const struct { const char *name; unsigned int field; } envs[] = {
        {"ZET_BOOST_TITLE",    1},
        {"ZET_BOOST_CAPTION",  2},
        {"ZET_BOOST_CATEGORY", 3},
        {"ZET_BOOST_SEEALSO",  4},
        {"ZET_BOOST_INFOBOX",  5},
    };
    unsigned int i;
    if (g_field_boost_loaded) return;
    for (i = 0; i < sizeof(envs) / sizeof(envs[0]); i++) {
        const char *v = getenv(envs[i].name);
        if (v && *v) {
            char *end = NULL;
            double d = strtod(v, &end);
            if (end != v && d > 0.0) {
                g_field_boost[envs[i].field] = d;
                fprintf(stderr, "[field_boost] %s = %.2f (field_id=%u)\n",
                        envs[i].name, d, envs[i].field);
            }
        }
    }
    /* Recompute the active flag: if every boost is 1.0, we can skip
     * per-occurrence accumulation and use the original SCAN_OFFSETS path. */
    {
        int i;
        g_field_boost_active = 0;
        for (i = 0; i < POSTINGS_MAX_FIELDS; i++) {
            if (g_field_boost[i] != 1.0) {
                g_field_boost_active = 1;
                break;
            }
        }
    }
    g_field_boost_loaded = 1;
}

/* PRD-019: load per-field BM25 sidecars and config. Activated by setting
 * ZET_PERFIELD_BM25=1. Reads field_lengths.bin and field_stats.bin from
 * paths set by ZET_FIELD_LENGTHS_PATH and ZET_FIELD_STATS_PATH. Reads
 * per-field w and b from ZET_FIELD_W_<NAME> and ZET_FIELD_B_<NAME>. */
void okapi_load_perfield(void) {
    static const struct { const char *suffix; unsigned int field; } envs[] = {
        {"BODY",     0},
        {"TITLE",    1},
        {"CAPTION",  2},
        {"CATEGORY", 3},
        {"SEEALSO",  4},
        {"INFOBOX",  5},
    };
    const char *enable;
    const char *lengths_path;
    const char *stats_path;
    FILE *f;
    long sz;
    unsigned int n_docs_h, n_fields_h;
    unsigned int i;

    if (g_perfield_loaded) return;
    g_perfield_loaded = 1;

    enable = getenv("ZET_PERFIELD_BM25");
    if (!enable || !*enable || enable[0] == '0') {
        return;  /* not enabled */
    }

    lengths_path = getenv("ZET_FIELD_LENGTHS_PATH");
    stats_path   = getenv("ZET_FIELD_STATS_PATH");
    if (!lengths_path || !stats_path) {
        fprintf(stderr, "[perfield] ZET_PERFIELD_BM25 set but "
                "ZET_FIELD_LENGTHS_PATH or ZET_FIELD_STATS_PATH unset; disabled\n");
        return;
    }

    /* Load field_lengths.bin */
    f = fopen(lengths_path, "rb");
    if (!f) {
        fprintf(stderr, "[perfield] could not open %s; disabled\n", lengths_path);
        return;
    }
    fseek(f, 0, SEEK_END);
    sz = ftell(f);
    rewind(f);
    if (sz <= 0 || (sz % (sizeof(uint32_t) * POSTINGS_MAX_FIELDS)) != 0) {
        fprintf(stderr, "[perfield] %s has unexpected size %ld; disabled\n",
                lengths_path, sz);
        fclose(f);
        return;
    }
    g_field_lengths_ndocs = (unsigned int)(sz / (sizeof(uint32_t) * POSTINGS_MAX_FIELDS));
    g_field_lengths = (uint32_t *)malloc(sz);
    if (!g_field_lengths) {
        fprintf(stderr, "[perfield] OOM allocating field_lengths; disabled\n");
        fclose(f);
        return;
    }
    if (fread(g_field_lengths, 1, sz, f) != (size_t)sz) {
        fprintf(stderr, "[perfield] short read on %s; disabled\n", lengths_path);
        free(g_field_lengths);
        g_field_lengths = NULL;
        fclose(f);
        return;
    }
    fclose(f);

    /* Load field_stats.bin: header (n_docs u32, n_fields u32) then
     * per-field (avg_len double, n_with u32) for n_fields entries. */
    f = fopen(stats_path, "rb");
    if (!f) {
        fprintf(stderr, "[perfield] could not open %s; disabled\n", stats_path);
        free(g_field_lengths);
        g_field_lengths = NULL;
        return;
    }
    if (fread(&n_docs_h,   sizeof(uint32_t), 1, f) != 1 ||
        fread(&n_fields_h, sizeof(uint32_t), 1, f) != 1) {
        fprintf(stderr, "[perfield] short read on %s header; disabled\n", stats_path);
        free(g_field_lengths);
        g_field_lengths = NULL;
        fclose(f);
        return;
    }
    if (n_docs_h != g_field_lengths_ndocs || n_fields_h != POSTINGS_MAX_FIELDS) {
        fprintf(stderr, "[perfield] sidecar header mismatch (docs=%u fields=%u); disabled\n",
                n_docs_h, n_fields_h);
        free(g_field_lengths);
        g_field_lengths = NULL;
        fclose(f);
        return;
    }
    for (i = 0; i < POSTINGS_MAX_FIELDS; i++) {
        double avg;
        uint32_t nw;
        if (fread(&avg, sizeof(double), 1, f) != 1 ||
            fread(&nw,  sizeof(uint32_t), 1, f) != 1) {
            fprintf(stderr, "[perfield] short read on %s field %u; disabled\n", stats_path, i);
            free(g_field_lengths);
            g_field_lengths = NULL;
            fclose(f);
            return;
        }
        g_field_avg_len[i] = avg;
        g_field_n_with[i]  = nw;
    }
    fclose(f);

    /* Per-field w and b env vars. */
    for (i = 0; i < sizeof(envs) / sizeof(envs[0]); i++) {
        char buf[64];
        const char *v;
        char *end;
        double d;
        snprintf(buf, sizeof(buf), "ZET_FIELD_W_%s", envs[i].suffix);
        v = getenv(buf);
        if (v && *v) {
            d = strtod(v, &end);
            if (end != v && d >= 0.0) g_field_w[envs[i].field] = d;
        }
        snprintf(buf, sizeof(buf), "ZET_FIELD_B_%s", envs[i].suffix);
        v = getenv(buf);
        if (v && *v) {
            d = strtod(v, &end);
            if (end != v && d >= 0.0 && d <= 1.0) g_field_b[envs[i].field] = d;
        }
    }

    g_perfield_active = 1;
    fprintf(stderr, "[perfield] enabled: %u docs, %u fields\n",
            g_field_lengths_ndocs, POSTINGS_MAX_FIELDS);
    for (i = 0; i < POSTINGS_MAX_FIELDS; i++) {
        if (g_field_n_with[i] > 0 || g_field_w[i] != 1.0 || g_field_b[i] != 0.75) {
            fprintf(stderr, "[perfield] field %u: avg_L=%.2f n_with=%u w=%.2f b=%.2f\n",
                    i, g_field_avg_len[i], g_field_n_with[i],
                    g_field_w[i], g_field_b[i]);
        }
    }
}

void okapi_load_prior(const char *path, double alpha) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "[click_prior] could not open %s\n", path);
        return;
    }
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    rewind(f);
    free(g_click_prior);
    g_click_prior_len = (unsigned int)(sz / sizeof(float));
    g_click_prior     = (float *)malloc(sz);
    if (g_click_prior) {
        fread(g_click_prior, sizeof(float), g_click_prior_len, f);
        fprintf(stderr, "[click_prior] loaded %u docs from %s (alpha=%.2f)\n",
                g_click_prior_len, path, alpha);
    }
    fclose(f);
    g_click_alpha = alpha;
}

static inline double click_boost(unsigned long int docno) {
    /* No-op — click prior is applied additively in post() instead.
     * Multiplicative was tried but amplified the popularity-ordering of
     * tangentially-relevant docs (e.g. Google overshadowing Sundar
     * Pichai for query 'sundar pichai'). Additive with a small alpha
     * acts as a tie-breaker without dominating. */
    (void)docno;
    return 1.0;
}

static inline double click_addend(unsigned long int docno) {
    if (g_click_prior && docno < g_click_prior_len && g_click_prior[docno] > 0.0f)
        return g_click_alpha * log(1.0 + (double)g_click_prior[docno]);
    return 0.0;
}

#include <stdlib.h>

/* a structure describing the parameters used */
struct okapi_param {
    double k1;
    double k3;
    double b;
    int dummy;  /* dummy member to ensure non-empty struct */
};

/* Function for converting string args into the params structure. */
static enum search_ret parse(void *ptr, const char *str) {
    /* weak typing may suck sometimes, but it certainly has its uses */
    struct okapi_param *param = ptr;
    unsigned int i,
                 parts,
                 read = 0;
    char *dup;
    char **split = NULL;
    char *endptr = NULL;

    if (str == NULL) {
        return SEARCH_EINVAL;
    }

    if ((dup = str_dup(str)) && (split = str_split(dup, ",", &parts)) 
      && parts == 3) {
        for (i = 0; i < parts; i++) {
            if (!str_ncmp(split[i], "k1=", 3)) {
                if ((param->k1 = (double)strtod(split[i] + 3, &endptr)), !*endptr) {
                    read |= (1 << 0);
                } else {
                    free(split);
                    free(dup);
                    return SEARCH_EINVAL;
                }
            }
            else
            if (!str_ncmp(split[i], "k3=", 3)) {
                if ((param->k3 = (double)strtod(split[i] + 3, &endptr)), !*endptr) {
                    read |= (1 << 1);
                } else {
                    free(split);
                    free(dup);
                    return SEARCH_EINVAL;
                }
            }
            else
            if (!str_ncmp(split[i], "b=", 2)) {
                if ((param->b = (double)strtod(split[i] + 2, &endptr)), !*endptr) {
                    read |= (1 << 2);
                } else {
                    free(split);
                    free(dup);
                    return SEARCH_EINVAL;
                }
            }
        }
        free(split);
        free(dup);
        if (read == ((1 << 0) | (1 << 1) | (1 << 2))) {
            return SEARCH_OK;
        } else {
            return SEARCH_EINVAL;
        }
    } else {
        if (dup) {
            free(dup);
        }
        if (split) {
            free(split);
        }
        return SEARCH_EINVAL;
    }
}


/* processed contents from src/okapi.metric and src/metric.c follows... */


/* XXX: METRIC_STRUCT should possibly be subsumed into METRIC_DECL?  Either way,
 * it's messy */

/* XXX: for the moment, [or,and,thresh]_decode have been copied to an 
 * equivalent _decode_offsets function.  This is pretty nasty, but i expect to
 * be able to remove this duplication once offsets have been de-interleaved 
 * from the inverted lists. */

#include "metric.h"

#include "_index.h"
#include "_docmap.h"
#include "index_querybuild.h"

#include "def.h"
#include "objalloc.h"
#include "docmap.h"
#include "search.h"
#include "vec.h"

#include <assert.h>
#include <float.h>

static enum search_ret pre(struct index *idx, struct query *query, int opts, struct index_search_opt *opt) {
    if (zpthread_mutex_lock(&idx->docmap_mutex) == ZPTHREAD_OK) {
        /* METRIC_PRE */
        if (docmap_cache(idx->map, docmap_get_cache(idx->map) | DOCMAP_CACHE_WORDS) != DOCMAP_OK) return SEARCH_EINVAL;

        zpthread_mutex_unlock(&idx->docmap_mutex);
        return SEARCH_OK;
    } else {
        assert(!CRASH);
        return SEARCH_EINVAL;
    }
}

static enum search_ret post(struct index *idx, struct query *query, struct search_acc_cons *acc, int opts, struct index_search_opt *opt) {
    /* METRIC_POST */

    while (acc) {
        assert(acc->acc.docno < docmap_entries(idx->map));
        /* METRIC_POST_PER_DOC */

        /* Add click prior as a small additive nudge to final score */
        acc->acc.weight += click_addend(acc->acc.docno);

        acc = acc->next;
    }

    return SEARCH_OK;
}

/* macro to atomically read the next docno and f_dt from a vector 
 * (note: i also tried a more complicated version that tested for a long vec 
 * and used unchecked reads, but measurements showed no improvement) */
#define NEXT_DOC(v, docno, f_dt)                                              \
    (vec_vbyte_read(v, &docno_d)                                              \
      && (((vec_vbyte_read(v, &f_dt) && ((docno += docno_d + 1), 1))          \
        /* second read failed, reposition vec back to start of docno_d */     \
        || (((v)->pos -= vec_vbyte_len(docno_d)), 0))))

/* macro to scan over f_dt offsets from a vector/source */
#define SCAN_OFFSETS(src, v, f_dt)                                            \
    do {                                                                      \
        unsigned int toscan = f_dt,                                           \
                     scanned;                                                 \
        enum search_ret sret;                                                 \
                                                                              \
        do {                                                                  \
            if ((scanned = vec_vbyte_scan(v, toscan, &scanned)) == toscan) {  \
                toscan = 0;                                                   \
                break;                                                        \
            } else if (scanned < toscan) {                                    \
                toscan -= scanned;                                            \
                /* need to read more */                                       \
                if ((sret = src->read(src, VEC_LEN(v),                        \
                    (void **) &(v)->pos, &bytes)) == SEARCH_OK) {             \
                                                                              \
                    (v)->end = (v)->pos + bytes;                              \
                } else if (sret == SEARCH_FINISH) {                           \
                    /* shouldn't end while scanning offsets */                \
                    return SEARCH_EINVAL;                                     \
                } else {                                                      \
                    return sret;                                              \
                }                                                             \
            } else {                                                          \
                assert("can't get here" && 0);                                \
                return SEARCH_EINVAL;                                         \
            }                                                                 \
        } while (toscan);                                                     \
    } while (0)

/* PRD-017: read f_dt offsets and accumulate a field-weighted sum into
 * (weighted) instead of just counting them.  Used in the okapi scoring
 * loops so that title (and future field) hits count more.
 *
 * Optimization 1: the encoded offset is (gap << POSTINGS_FIELD_BITS) | field_id.
 * vbyte stores the low 7 bits of a value in the first byte. The field_id
 * therefore lives in the low POSTINGS_FIELD_BITS bits of the FIRST byte of
 * each vbyte sequence — we don't need to decode the full value, just peek
 * the first byte and then skip continuation bytes (high-bit-set).
 *
 * Optimization 2: when no field has a non-trivial boost (g_field_boost_active
 * == 0), the weighted sum equals f_dt exactly. Skip the per-byte accumulation
 * and use the original SCAN_OFFSETS path (just walk continuation bytes).
 *
 * weighted: double accumulator. f_dt: number of offsets. v: struct vec *. */
#define READ_OFFSETS_WEIGHTED(src, v, f_dt, weighted)                         \
    do {                                                                      \
        unsigned int toread = f_dt;                                           \
        enum search_ret sret;                                                 \
        unsigned char *p, *e;                                                 \
        if (!g_field_boost_active) {                                          \
            /* fast path: no boost active, sum equals f_dt; just skip past */ \
            SCAN_OFFSETS(src, v, f_dt);                                       \
            (weighted) = (double)(f_dt);                                      \
            break;                                                            \
        }                                                                     \
        (weighted) = 0.0;                                                     \
        while (toread) {                                                      \
            p = (unsigned char *)(v)->pos;                                    \
            e = (unsigned char *)(v)->end;                                    \
            while (toread && p < e) {                                         \
                unsigned char *start = p;                                     \
                /* walk continuation bytes (high bit set) */                  \
                while (p < e && (*p & 0x80)) p++;                             \
                if (p < e) {                                                  \
                    (weighted) += g_field_boost[*start & POSTINGS_FIELD_MASK];\
                    p++;                                                      \
                    toread--;                                                 \
                } else {                                                      \
                    /* ran out mid-vbyte — rewind to start, refill */         \
                    p = start;                                                \
                    break;                                                    \
                }                                                             \
            }                                                                 \
            (v)->pos = (char *)p;                                             \
            if (toread) {                                                     \
                if ((sret = src->read(src, VEC_LEN(v),                        \
                    (void **) &(v)->pos, &bytes)) == SEARCH_OK) {             \
                    (v)->end = (v)->pos + bytes;                              \
                } else if (sret == SEARCH_FINISH) {                           \
                    return SEARCH_EINVAL;                                     \
                } else {                                                      \
                    return sret;                                              \
                }                                                             \
            }                                                                 \
        }                                                                     \
    } while (0)

/* PRD-019: read f_dt offsets and accumulate per-field counts into the
 * f_dt_f[POSTINGS_MAX_FIELDS] array. Same vbyte trick as
 * READ_OFFSETS_WEIGHTED — the field_id is in the low bits of the first
 * byte of each offset's vbyte sequence. */
#define READ_OFFSETS_PERFIELD(src, v, f_dt, f_dt_f)                           \
    do {                                                                      \
        unsigned int toread = f_dt;                                           \
        enum search_ret sret;                                                 \
        unsigned char *p, *e;                                                 \
        unsigned int _i;                                                      \
        for (_i = 0; _i < POSTINGS_MAX_FIELDS; _i++) (f_dt_f)[_i] = 0;        \
        while (toread) {                                                      \
            p = (unsigned char *)(v)->pos;                                    \
            e = (unsigned char *)(v)->end;                                    \
            while (toread && p < e) {                                         \
                unsigned char *start = p;                                     \
                while (p < e && (*p & 0x80)) p++;                             \
                if (p < e) {                                                  \
                    (f_dt_f)[*start & POSTINGS_FIELD_MASK]++;                 \
                    p++;                                                      \
                    toread--;                                                 \
                } else {                                                      \
                    p = start;                                                \
                    break;                                                    \
                }                                                             \
            }                                                                 \
            (v)->pos = (char *)p;                                             \
            if (toread) {                                                     \
                if ((sret = src->read(src, VEC_LEN(v),                        \
                    (void **) &(v)->pos, &bytes)) == SEARCH_OK) {             \
                    (v)->end = (v)->pos + bytes;                              \
                } else if (sret == SEARCH_FINISH) {                           \
                    return SEARCH_EINVAL;                                     \
                } else {                                                      \
                    return sret;                                              \
                }                                                             \
            }                                                                 \
        }                                                                     \
    } while (0)

/* PRD-019: compute per-field BM25 score contribution given f_dt_f counts.
 * Returns sum_f w_f * BM25_tf(f_dt_f[f], L_f, avg_L_f, k1, b_f).
 * docno is the internal Zettair docno, used to index field_lengths. */
static inline double perfield_score(unsigned long int docno,
    const unsigned int *f_dt_f, double k1) {
    double r_dt = 0.0;
    unsigned int f;
    if (docno >= g_field_lengths_ndocs) return 0.0;
    for (f = 0; f < POSTINGS_MAX_FIELDS; f++) {
        unsigned int n = f_dt_f[f];
        double L_f, avg, b, tf;
        if (n == 0) continue;
        if (g_field_w[f] <= 0.0) continue;
        avg = g_field_avg_len[f];
        if (avg <= 0.0) continue;
        L_f = (double)g_field_lengths[docno * POSTINGS_MAX_FIELDS + f];
        if (L_f <= 0.0) L_f = avg;  /* missing data — neutral */
        b = g_field_b[f];
        tf = ((k1 + 1.0) * (double)n) /
             (k1 * ((1.0 - b) + b * L_f / avg) + (double)n);
        r_dt += g_field_w[f] * tf;
    }
    return r_dt;
}

static enum search_ret or_decode(struct index *idx, struct query *query,
  unsigned int qterm, unsigned long int docno, 
  struct search_metric_results *results, struct search_list_src *src, 
  int opts, struct index_search_opt *opt) {
    /* METRIC_STRUCT */ struct okapi_param *param = (void *) &opt->u;
    struct search_acc_cons *acc = results->acc,
                           **prevptr = &results->acc;
    unsigned int accs_added = 0;   /* number of accumulators added */
    unsigned long int f_dt,        /* number of offsets for this document */
                      docno_d;     /* d-gap */
    unsigned int bytes;
    struct vec v = {NULL, NULL};
    enum search_ret ret;
    /* METRIC_DECL */

    const unsigned int N = docmap_entries(idx->map);
    double avg_D_terms;
    double w_t;
    double r_dt;
    double weighted_f_dt;          /* PRD-017: field-boosted f_dt */

    double r_qt = (((param->k3) + 1) * (query->term[qterm].f_qt)) / ((param->k3) + (query->term[qterm].f_qt));
    if (docmap_avg_words(idx->map, &avg_D_terms) != DOCMAP_OK) {
        return SEARCH_EINVAL;
    }


    /* METRIC_PER_CALL */
    w_t = (double)log((N - (query->term[qterm].f_t) + 0.5) / ((query->term[qterm].f_t) + 0.5));
    /* fix for okapi bug, w_t shouldn't be 0 or negative. */
    if (w_t <= 0.0) {
        /* use a very small increment instead */
        w_t = FLT_EPSILON;
    }
    
    


    while (1) {
        while (NEXT_DOC(&v, docno, f_dt)) {
            weighted_f_dt = (double)f_dt;  /* PRD-017: no offsets here, equal weight */

            /* merge into accumulator list */
            while (acc && (docno > acc->acc.docno)) {
                prevptr = &acc->next;
                acc = acc->next;
            }

            if (acc && (docno == acc->acc.docno)) {
                /* METRIC_PER_DOC */
                r_dt = ((((param->k1) + 1) * weighted_f_dt) / ((param->k1) * ((1 - (param->b)) + (((param->b) * (DOCMAP_GET_WORDS(idx->map, acc->acc.docno))) / avg_D_terms)) + weighted_f_dt));
                (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

            } else {
                struct search_acc_cons *newacc;
                assert(!acc || docno < acc->acc.docno); 

                /* allocate a new accumulator (we have reserved allocators
                 * earlier, so this should never fail) */
                newacc = objalloc_malloc(results->alloc, sizeof(*newacc));
                assert(newacc);
                newacc->next = acc;
                acc = newacc;
                acc->acc.docno = docno;
                acc->acc.weight = 0.0;
                /* METRIC_PER_DOC */
                r_dt = ((((param->k1) + 1) * weighted_f_dt) / ((param->k1) * ((1 - (param->b)) + (((param->b) * (DOCMAP_GET_WORDS(idx->map, acc->acc.docno))) / avg_D_terms)) + weighted_f_dt));
                (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

                *prevptr = newacc;
                accs_added++;
            }
            assert(acc);

            /* go to next accumulator */
            prevptr = &acc->next;
            acc = acc->next;
        }

        /* need to read more data, preserving bytes that we already have */
        if ((ret = src->read(src, VEC_LEN(&v),
            (void **) &v.pos, &bytes)) == SEARCH_OK) {

            v.end = v.pos + bytes;
        } else if (ret == SEARCH_FINISH) {
            /* finished, update number of accumulators */
            results->accs += accs_added;
            results->total_results += accs_added;

            if (!VEC_LEN(&v)) {
                return SEARCH_OK;
            } else {
                return SEARCH_EINVAL;
            }
        } else {
            param = NULL;   /* avoid 'param not used' warning */
            return ret;
        }
    }
}
 
static enum search_ret or_decode_offsets(struct index *idx, struct query *query,
  unsigned int qterm, unsigned long int docno,
  struct search_metric_results *results, struct search_list_src *src,
  int opts, struct index_search_opt *opt) {
    /* METRIC_STRUCT */ struct okapi_param *param = (void *) &opt->u;
    struct search_acc_cons *acc = results->acc,
                           **prevptr = &results->acc;
    unsigned int accs_added = 0;   /* number of accumulators added */
    unsigned long int f_dt,        /* number of offsets for this document */
                      docno_d;     /* d-gap */
    unsigned int bytes;
    struct vec v = {NULL, NULL};
    enum search_ret ret;
    /* METRIC_DECL */

    const unsigned int N = docmap_entries(idx->map);
    double avg_D_terms;
    double w_t;
    double r_dt;
    double weighted_f_dt;          /* PRD-017: field-boosted f_dt */
    unsigned int f_dt_f[POSTINGS_MAX_FIELDS]; /* PRD-019: per-field counts */

    double r_qt = (((param->k3) + 1) * (query->term[qterm].f_qt)) / ((param->k3) + (query->term[qterm].f_qt));
    if (docmap_avg_words(idx->map, &avg_D_terms) != DOCMAP_OK) {
        return SEARCH_EINVAL;
    }


    /* METRIC_PER_CALL */
    w_t = (double)log((N - (query->term[qterm].f_t) + 0.5) / ((query->term[qterm].f_t) + 0.5));
    /* fix for okapi bug, w_t shouldn't be 0 or negative. */
    if (w_t <= 0.0) {
        /* use a very small increment instead */
        w_t = FLT_EPSILON;
    }




    while (1) {
        double t0 = tnow_us();
        while (NEXT_DOC(&v, docno, f_dt)) {
            unsigned long walk = 0;
            double t1, t2, t3;
            if (g_perfield_active) {
                READ_OFFSETS_PERFIELD(src, &v, f_dt, f_dt_f);
                weighted_f_dt = (double)f_dt;  /* unused on perfield path */
            } else {
                READ_OFFSETS_WEIGHTED(src, &v, f_dt, weighted_f_dt);
            }
            t1 = tnow_us();

            /* merge into accumulator list */
            while (acc && (docno > acc->acc.docno)) {
                prevptr = &acc->next;
                acc = acc->next;
                walk++;
            }
            t2 = tnow_us();

            if (acc && (docno == acc->acc.docno)) {
                /* METRIC_PER_DOC */
                if (g_perfield_active) {
                    r_dt = perfield_score(acc->acc.docno, f_dt_f, param->k1);
                } else {
                    r_dt = ((((param->k1) + 1) * weighted_f_dt) / ((param->k1) * ((1 - (param->b)) + (((param->b) * (DOCMAP_GET_WORDS(idx->map, acc->acc.docno))) / avg_D_terms)) + weighted_f_dt));
                }
                (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

            } else {
                struct search_acc_cons *newacc;
                assert(!acc || docno < acc->acc.docno);

                /* allocate a new accumulator (we have reserved allocators
                 * earlier, so this should never fail) */
                newacc = objalloc_malloc(results->alloc, sizeof(*newacc));
                assert(newacc);
                newacc->next = acc;
                acc = newacc;
                acc->acc.docno = docno;
                acc->acc.weight = 0.0;
                /* METRIC_PER_DOC */
                if (g_perfield_active) {
                    r_dt = perfield_score(acc->acc.docno, f_dt_f, param->k1);
                } else {
                    r_dt = ((((param->k1) + 1) * weighted_f_dt) / ((param->k1) * ((1 - (param->b)) + (((param->b) * (DOCMAP_GET_WORDS(idx->map, acc->acc.docno))) / avg_D_terms)) + weighted_f_dt));
                }
                (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

                *prevptr = newacc;
                accs_added++;
            }
            assert(acc);

            /* go to next accumulator */
            prevptr = &acc->next;
            acc = acc->next;

            t3 = tnow_us();
            zet_inner_decode_ms += (t1 - t0) / 1000.0;
            zet_inner_walk_ms   += (t2 - t1) / 1000.0;
            zet_inner_score_ms  += (t3 - t2) / 1000.0;
            zet_inner_postings++;
            zet_inner_walk_steps += walk;
            t0 = tnow_us();
        }

        /* need to read more data, preserving bytes that we already have */
        if ((ret = src->read(src, VEC_LEN(&v),
            (void **) &v.pos, &bytes)) == SEARCH_OK) {

            v.end = v.pos + bytes;
        } else if (ret == SEARCH_FINISH) {
            /* finished, update number of accumulators */
            results->accs += accs_added;
            results->total_results += accs_added;

            if (!VEC_LEN(&v)) {
                return SEARCH_OK;
            } else {
                return SEARCH_EINVAL;
            }
        } else {
            param = NULL;   /* avoid 'param not used' warning */
            return ret;
        }
    }
}

static enum search_ret and_decode(struct index *idx, struct query *query, 
  unsigned int qterm, unsigned long int docno, 
  struct search_metric_results *results, struct search_list_src *src,
  int opts, struct index_search_opt *opt) {
    /* METRIC_STRUCT */ struct okapi_param *param = (void *) &opt->u;
    struct search_acc_cons *acc = results->acc;
    unsigned long int f_dt,        /* number of offsets for this document */
                      docno_d;     /* d-gap */
    struct vec v = {NULL, NULL};
    unsigned int bytes,
                 missed = 0,       /* number of list entries that didn't match 
                                    * an accumulator */
                 hit = 0,          /* number of entries in both accs and list*/
                 decoded = 0;      /* number of list entries seen */
    enum search_ret ret;
    double cooc_rate;               /* co-occurrance rate for list entries and 
                                    * accumulators */
    /* METRIC_DECL */

    const unsigned int N = docmap_entries(idx->map);
    double avg_D_terms;
    double w_t;
    double r_dt;
    double weighted_f_dt;          /* PRD-017: field-boosted f_dt */

    double r_qt = (((param->k3) + 1) * (query->term[qterm].f_qt)) / ((param->k3) + (query->term[qterm].f_qt));
    if (docmap_avg_words(idx->map, &avg_D_terms) != DOCMAP_OK) {
        return SEARCH_EINVAL;
    }


    /* METRIC_PER_CALL */
    w_t = (double)log((N - (query->term[qterm].f_t) + 0.5) / ((query->term[qterm].f_t) + 0.5));
    /* fix for okapi bug, w_t shouldn't be 0 or negative. */
    if (w_t <= 0.0) {
        /* use a very small increment instead */
        w_t = FLT_EPSILON;
    }
    
    


    while (1) {
        while (NEXT_DOC(&v, docno, f_dt)) {
            decoded++;
            weighted_f_dt = (double)f_dt;  /* PRD-017: no offsets here, equal weight */

            /* merge into accumulator list */
            while (acc && (docno > acc->acc.docno)) {
                acc = acc->next;
            }

            if (acc && (docno == acc->acc.docno)) {
                /* METRIC_PER_DOC */
                r_dt = ((((param->k1) + 1) * weighted_f_dt) / ((param->k1) * ((1 - (param->b)) + (((param->b) * (DOCMAP_GET_WORDS(idx->map, acc->acc.docno))) / avg_D_terms)) + weighted_f_dt));
                (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);


                /* go to next accumulator */
                acc = acc->next;
                hit++;
            } else {
                missed++;
            }
        }

        /* need to read more data, preserving bytes that we already have */
        if ((ret = src->read(src, VEC_LEN(&v),
            (void **) &v.pos, &bytes)) == SEARCH_OK) {

            v.end = v.pos + bytes;
        } else if (ret == SEARCH_FINISH) {
            /* finished, estimate number of results */

            /* list entries now divide up into two portions:
             *   - matching an entry in the acc list (hit)
             *   - missed
             *
             * cooccurrance rate is the percentage of list items hit */
            assert(missed + hit == decoded);
            cooc_rate = hit/(double)decoded;

            /* now have sampled co-occurrance rate, use this to estimate 
             * population co-occurrance rate (assuming unbiased sampling) 
             * and then number of results from unrestricted evaluation */
            assert(results->total_results >= results->accs);
            cooc_rate *= results->total_results/(double)results->accs; 
            assert(cooc_rate >= 0.0);
            if (cooc_rate > 1.0) {
                cooc_rate = 1.0;
            }

            /* add number of things we think would have been added from the
             * things that were missed */
            results->total_results += (1 - cooc_rate) * missed;

            if (missed) {
                results->estimated |= 1;
            }

            if (!VEC_LEN(&v)) {
                return SEARCH_OK;
            } else {
                return SEARCH_EINVAL;
            }
        } else {
            param = NULL;   /* avoid 'param not used' warning */
            return ret;
        }
    }
}

static enum search_ret and_decode_offsets(struct index *idx, 
  struct query *query, 
  unsigned int qterm, unsigned long int docno, 
  struct search_metric_results *results, struct search_list_src *src,
  int opts, struct index_search_opt *opt) {
    /* METRIC_STRUCT */ struct okapi_param *param = (void *) &opt->u;
    struct search_acc_cons *acc = results->acc;
    unsigned long int f_dt,        /* number of offsets for this document */
                      docno_d;     /* d-gap */
    struct vec v = {NULL, NULL};
    unsigned int bytes,
                 missed = 0,       /* number of list entries that didn't match 
                                    * an accumulator */
                 hit = 0,          /* number of entries in both accs and list*/
                 decoded = 0;      /* number of list entries seen */
    enum search_ret ret;
    double cooc_rate;               /* co-occurrance rate for list entries and 
                                    * accumulators */
    /* METRIC_DECL */

    const unsigned int N = docmap_entries(idx->map);
    double avg_D_terms;
    double w_t;
    double r_dt;
    double weighted_f_dt;          /* PRD-017: field-boosted f_dt */

    double r_qt = (((param->k3) + 1) * (query->term[qterm].f_qt)) / ((param->k3) + (query->term[qterm].f_qt));
    if (docmap_avg_words(idx->map, &avg_D_terms) != DOCMAP_OK) {
        return SEARCH_EINVAL;
    }


    /* METRIC_PER_CALL */
    w_t = (double)log((N - (query->term[qterm].f_t) + 0.5) / ((query->term[qterm].f_t) + 0.5));
    /* fix for okapi bug, w_t shouldn't be 0 or negative. */
    if (w_t <= 0.0) {
        /* use a very small increment instead */
        w_t = FLT_EPSILON;
    }
    
    


    while (1) {
        while (NEXT_DOC(&v, docno, f_dt)) {
            READ_OFFSETS_WEIGHTED(src, &v, f_dt, weighted_f_dt);
            decoded++;

            /* merge into accumulator list */
            while (acc && (docno > acc->acc.docno)) {
                acc = acc->next;
            }

            if (acc && (docno == acc->acc.docno)) {
                /* METRIC_PER_DOC */
                r_dt = ((((param->k1) + 1) * weighted_f_dt) / ((param->k1) * ((1 - (param->b)) + (((param->b) * (DOCMAP_GET_WORDS(idx->map, acc->acc.docno))) / avg_D_terms)) + weighted_f_dt));
                (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);


                /* go to next accumulator */
                acc = acc->next;
                hit++;
            } else {
                missed++;
            }
        }

        /* need to read more data, preserving bytes that we already have */
        if ((ret = src->read(src, VEC_LEN(&v),
            (void **) &v.pos, &bytes)) == SEARCH_OK) {

            v.end = v.pos + bytes;
        } else if (ret == SEARCH_FINISH) {
            /* finished, estimate number of results */

            /* list entries now divide up into two portions:
             *   - matching an entry in the acc list (hit)
             *   - missed
             *
             * cooccurrance rate is the percentage of list items hit */
            assert(missed + hit == decoded);
            cooc_rate = hit/(double)decoded;

            /* now have sampled co-occurrance rate, use this to estimate 
             * population co-occurrance rate (assuming unbiased sampling) 
             * and then number of results from unrestricted evaluation */
            assert(results->total_results >= results->accs);
            cooc_rate *= results->total_results/(double)results->accs; 
            assert(cooc_rate >= 0.0);
            if (cooc_rate > 1.0) {
                cooc_rate = 1.0;
            }

            /* add number of things we think would have been added from the
             * things that were missed */
            results->total_results += (1 - cooc_rate) * missed;

            if (missed) {
                results->estimated |= 1;
            }

            if (!VEC_LEN(&v)) {
                return SEARCH_OK;
            } else {
                return SEARCH_EINVAL;
            }
        } else {
            param = NULL;   /* avoid 'param not used' warning */
            return ret;
        }
    }
}

/* tolerance value for thresholding estimates.  Should be >= 1.0.  Make higher
 * for stabler, but higher memory usage, processing. */
#define TOLERANCE 1.2

/* low-ish approximation of infinity, to make counting up to it acceptable */
#define INF 2000

static enum search_ret thresh_decode(struct index *idx, struct query *query,
  unsigned int qterm, unsigned long int docno, 
  struct search_metric_results *results, 
  struct search_list_src *src, unsigned int postings, 
  int opts, struct index_search_opt *opt) {
    /* METRIC_STRUCT */ struct okapi_param *param = (void *) &opt->u;
    struct search_acc_cons *acc, dummy, **prevptr = &results->acc;
    unsigned long int f_dt,           /* number of offsets for this document */
                      docno_d;        /* d-gap */

    /* initial number of accumulators */
    unsigned int initial_accs = results->accs,

                 decoded = 0,         /* number of postings decoded */
                 thresh,              /* current discrete threshold */
                 rethresh,            /* distance to recalculation of the threshold */
                 rethresh_dist,
                 bytes,
                 step,
                 missed = 0,          /* number of list entries that didn't match an accumulator */
                 hit = 0;             /* number of entries in both accs and list*/
 
    struct vec v = {NULL, NULL};
    enum search_ret ret = SEARCH_EIO;
    int infinite = 0;                 /* whether threshold is infinite */
    double cooc_rate;
    /* METRIC_DECL */

    const unsigned int N = docmap_entries(idx->map);
    double avg_D_terms;
    double w_t;
    double r_dt;
    double weighted_f_dt;          /* PRD-017: field-boosted f_dt */

    double r_qt = (((param->k3) + 1) * (query->term[qterm].f_qt)) / ((param->k3) + (query->term[qterm].f_qt));
    if (docmap_avg_words(idx->map, &avg_D_terms) != DOCMAP_OK) {
        return SEARCH_EINVAL;
    }


    /* METRIC_PER_CALL */

    w_t = (double)log((N - (query->term[qterm].f_t) + 0.5) / ((query->term[qterm].f_t) + 0.5));
    /* fix for okapi bug, w_t shouldn't be 0 or negative. */
    if (w_t <= 0.0) {
        /* use a very small increment instead */
        w_t = FLT_EPSILON;
    }

    rethresh_dist = rethresh = (postings + results->acc_limit - 1) 
      / results->acc_limit;

    if (results->v_t == FLT_MIN) {
        unsigned long int docno_copy = docno;

        /* this should be the first thresholded list, need to estimate threshold */
        assert(rethresh && rethresh < postings);
        thresh = 0;

        assert(rethresh < postings);
        while (rethresh) {
            while (rethresh && NEXT_DOC(&v, docno, f_dt)) {
                rethresh--;
                if (f_dt > thresh) {
                    thresh = f_dt;
                }
            }

            /* need to read more data, preserving bytes that we already have */
            if (rethresh && (ret = src->read(src, VEC_LEN(&v),
                (void **) &v.pos, &bytes)) == SEARCH_OK) {

                v.end = v.pos + bytes;
            } else if (rethresh) {
                assert(ret != SEARCH_FINISH);
                return ret;
            }
        }
        thresh--;

        acc = &dummy;
        acc->acc.docno = UINT_MAX;   /* shouldn't be used */
        acc->acc.weight = 0.0;
        f_dt = thresh;
        /* METRIC_CONTRIB */
        r_dt = ((((param->k1) + 1) * f_dt)       / ((param->k1) * ((1 - (param->b)) + (((param->b) * (avg_D_terms)) / avg_D_terms)) + f_dt));
        (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

        results->v_t = acc->acc.weight;

        /* reset source/vector to start */
        v.pos = v.end = NULL;
        if ((ret = src->reset(src)) != SEARCH_OK) {
            return ret;
        }

        acc = *prevptr;
        docno = docno_copy;
        rethresh = rethresh_dist;
    } else {
        /* translate the existing v_t threshold to an f_dt */
        acc = &dummy;
        acc->acc.docno = UINT_MAX;   /* shouldn't be used */
        f_dt = 0;
        do {
            acc->acc.weight = 0.0;
            f_dt++;
            /* METRIC_CONTRIB */
            r_dt = ((((param->k1) + 1) * f_dt)       / ((param->k1) * ((1 - (param->b)) + (((param->b) * (avg_D_terms)) / avg_D_terms)) + f_dt));
            (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

        } while (acc->acc.weight < results->v_t && f_dt < INF);
        thresh = f_dt; 
        acc = *prevptr;

        if (thresh == INF) {
            /* this is not a sensible term */
            infinite = 1;
            rethresh = postings + 1;
        }
    }

    /* set step to 1/2 of the threshold */
    step = (thresh + 1) / 2;
    step += !step; /* but don't let it become 0 */

    while (1) {
        while (NEXT_DOC(&v, docno, f_dt)) {
            decoded++;
            weighted_f_dt = (double)f_dt;  /* PRD-017: no offsets here, equal weight */

            /* merge into accumulator list */
            while (acc && (docno > acc->acc.docno)) {
                /* perform threshold test */
                if (acc->acc.weight < results->v_t) {
                    /* remove this accumulator */
                    *prevptr = acc->next;
                    objalloc_free(results->alloc, acc);
                    acc = (*prevptr);
                    results->accs--;
                } else {
                    /* retain this accumulator */
                    prevptr = &acc->next;
                    acc = acc->next;
                }
            }

            if (acc && (docno == acc->acc.docno)) {
                /* METRIC_PER_DOC */
                r_dt = ((((param->k1) + 1) * weighted_f_dt) / ((param->k1) * ((1 - (param->b)) + (((param->b) * (DOCMAP_GET_WORDS(idx->map, acc->acc.docno))) / avg_D_terms)) + weighted_f_dt));
                (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);


                if (acc->acc.weight < results->v_t) {
                    /* remove this accumulator */
                    *prevptr = acc->next;
                    objalloc_free(results->alloc, acc);
                    acc = *prevptr;
                    results->accs--;
                } else {
                    /* go to next accumulator */
                    prevptr = &acc->next;
                    acc = acc->next;
                }
                hit++;
            } else {
                if (f_dt > thresh) {
                    struct search_acc_cons *newacc;
                    assert(!acc || docno < acc->acc.docno); 

                    if ((newacc = objalloc_malloc(results->alloc, 
                      sizeof(*newacc)))) {
                        newacc->acc.docno = docno;
                        newacc->acc.weight = 0.0;
                        newacc->next = acc;
                        acc = newacc;
                        /* note that we have to be careful around here to 
                         * assign newacc to acc before using PER_DOC, 
                         * otherwise we end up with nonsense in some 
                         * accumulators */
                        /* METRIC_PER_DOC */
                        r_dt = ((((param->k1) + 1) * weighted_f_dt) / ((param->k1) * ((1 - (param->b)) + (((param->b) * (DOCMAP_GET_WORDS(idx->map, acc->acc.docno))) / avg_D_terms)) + weighted_f_dt));
                        (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

                        *prevptr = newacc;
                        results->accs++;
                    } else {
                        return SEARCH_ENOMEM;
                    }

                    /* go to next accumulator */
                    prevptr = &acc->next;
                    acc = acc->next;
                } else {
                    missed++;
                }
            }

            if (!--rethresh) {
                int estimate,
                    prev_thresh = thresh;

                estimate = results->accs 
                  + ((postings - decoded) 
                    * ((double)results->accs - initial_accs)) / decoded;

                if (estimate > TOLERANCE * results->acc_limit) {
                    thresh += step;
                } else if ((estimate < results->acc_limit / TOLERANCE) 
                  && thresh) {
                    if (thresh >= step) {
                        thresh -= step;
                    } else {
                        thresh = 0;
                    }
                }

                step = (step + 1) / 2;
                assert(step);

                /* note that we don't want to recalculate the threshold if it
                 * doesn't change because this involves re-discretising it */
                if (prev_thresh != thresh) {
                    /* recalculate contribution that corresponds to the new 
                     * threshold */
                    f_dt = thresh;
                    if (f_dt) {
                        acc = &dummy;
                        acc->acc.docno = UINT_MAX;   /* shouldn't be used */
                        acc->acc.weight = 0.0;
                        /* METRIC_CONTRIB */
                        r_dt = ((((param->k1) + 1) * f_dt)       / ((param->k1) * ((1 - (param->b)) + (((param->b) * (avg_D_terms)) / avg_D_terms)) + f_dt));
                        (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

                        results->v_t = acc->acc.weight;
                        acc = *prevptr;
                    } else {
                        results->v_t = FLT_MIN;
                    }
                }

                rethresh_dist *= 2;
                rethresh = rethresh_dist;
            }
        }

        /* need to read more data, preserving bytes that we already have */
        if ((ret = src->read(src, VEC_LEN(&v),
            (void **) &v.pos, &bytes)) == SEARCH_OK) {

            v.end = v.pos + bytes;
        } else if (ret == SEARCH_FINISH) {
            /* finished, estimate total results count */
            assert(postings == decoded);

            results->total_results += (int) (results->accs - initial_accs);

            /* list entries now divide up into three portions:
             *   - matching an entry in the acc list (hit)
             *   - missed
             *   - added
             *
             * cooccurrance rate is the percentage of list items hit */
            cooc_rate = hit/(double)decoded;

            /* now have sampled co-occurrance rate, use this to estimate 
             * population co-occurrance rate (assuming unbiased sampling) 
             * and then number of results from unrestricted evaluation */
            assert(results->total_results >= results->accs);
            cooc_rate *= results->total_results/(double)results->accs; 
            assert(cooc_rate >= 0.0);
            if (cooc_rate > 1.0) {
                cooc_rate = 1.0;
            }

            /* add number of things we think would have been added from the
             * things that were missed */
            results->total_results += (1 - cooc_rate) * missed;

            /* note that the total results are not an estimate if either there
             * were no accumulators in the list when we started (in which case
             * missed records exactly the number, uh, missing from the
             * accumulators) or there were none missed, in which case the
             * accumulators have fully accounted for everything in this list.
             * In either case, the (1 - cooc_rate) * missed maths above handles
             * it exactly (modulo doubleing point errors of course). */
            if (initial_accs && missed) {
                results->estimated |= 1;
            }

            if (!VEC_LEN(&v)) {
                if (!infinite) {
                    /* continue threshold evaluation */
                    return SEARCH_OK;
                } else {
                    /* switch to AND processing */
                    return SEARCH_FINISH;
                }
            } else {
                return SEARCH_EINVAL;
            }
        } else {
            param = NULL;   /* avoid 'param not used' warning */
            return ret;
        }
    }
}

static enum search_ret thresh_decode_offsets(struct index *idx, 
  struct query *query, unsigned int qterm, unsigned long int docno, 
  struct search_metric_results *results, 
  struct search_list_src *src, unsigned int postings, 
  int opts, struct index_search_opt *opt) {
    /* METRIC_STRUCT */ struct okapi_param *param = (void *) &opt->u;
    struct search_acc_cons *acc, dummy, **prevptr = &results->acc;
    unsigned long int f_dt,           /* number of offsets for this document */
                      docno_d;        /* d-gap */

    /* initial number of accumulators */
    unsigned int initial_accs = results->accs,

                 decoded = 0,         /* number of postings decoded */
                 thresh,              /* current discrete threshold */
                 rethresh,            /* distance to recalculation of the threshold */
                 rethresh_dist,
                 bytes,
                 step,
                 missed = 0,          /* number of list entries that didn't match an accumulator */
                 hit = 0;             /* number of entries in both accs and list*/
 
    struct vec v = {NULL, NULL};
    enum search_ret ret = SEARCH_EIO;
    int infinite = 0;                 /* whether threshold is infinite */
    double cooc_rate;
    /* METRIC_DECL */

    const unsigned int N = docmap_entries(idx->map);
    double avg_D_terms;
    double w_t;
    double r_dt;
    double weighted_f_dt;          /* PRD-017: field-boosted f_dt */

    double r_qt = (((param->k3) + 1) * (query->term[qterm].f_qt)) / ((param->k3) + (query->term[qterm].f_qt));
    if (docmap_avg_words(idx->map, &avg_D_terms) != DOCMAP_OK) {
        return SEARCH_EINVAL;
    }


    /* METRIC_PER_CALL */
    w_t = (double)log((N - (query->term[qterm].f_t) + 0.5) / ((query->term[qterm].f_t) + 0.5));
    /* fix for okapi bug, w_t shouldn't be 0 or negative. */
    if (w_t <= 0.0) {
        /* use a very small increment instead */
        w_t = FLT_EPSILON;
    }

    rethresh_dist = rethresh = (postings + results->acc_limit - 1) 
      / results->acc_limit;

    if (results->v_t == FLT_MIN) {
        unsigned long int docno_copy = docno;

        /* this should be the first thresholded list, need to estimate threshold */
        assert(rethresh && rethresh < postings);
        thresh = 0;

        assert(rethresh < postings);
        while (rethresh) {
            while (rethresh && NEXT_DOC(&v, docno, f_dt)) {
                rethresh--;
                READ_OFFSETS_WEIGHTED(src, &v, f_dt, weighted_f_dt);
                if (f_dt > thresh) {
                    thresh = f_dt;
                }
            }

            /* need to read more data, preserving bytes that we already have */
            if (rethresh && (ret = src->read(src, VEC_LEN(&v),
                (void **) &v.pos, &bytes)) == SEARCH_OK) {

                v.end = v.pos + bytes;
            } else if (rethresh) {
                assert(ret != SEARCH_FINISH);
                return ret;
            }
        }
        thresh--;

        acc = &dummy;
        acc->acc.docno = UINT_MAX;   /* shouldn't be used */
        acc->acc.weight = 0.0;
        f_dt = thresh;
        /* METRIC_CONTRIB */
        r_dt = ((((param->k1) + 1) * f_dt)       / ((param->k1) * ((1 - (param->b)) + (((param->b) * (avg_D_terms)) / avg_D_terms)) + f_dt));
        (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

        results->v_t = acc->acc.weight;

        /* reset source/vector to start */
        v.pos = v.end = NULL;
        if ((ret = src->reset(src)) != SEARCH_OK) {
            return ret;
        }

        acc = *prevptr;
        docno = docno_copy;
        rethresh = rethresh_dist;
    } else {
        /* translate the existing v_t threshold to an f_dt */
        acc = &dummy;
        acc->acc.docno = UINT_MAX;   /* shouldn't be used */
        f_dt = 0;
        do {
            acc->acc.weight = 0.0;
            f_dt++;
            /* METRIC_CONTRIB */
            r_dt = ((((param->k1) + 1) * f_dt)       / ((param->k1) * ((1 - (param->b)) + (((param->b) * (avg_D_terms)) / avg_D_terms)) + f_dt));
            (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

        } while (acc->acc.weight < results->v_t && f_dt < INF);
        thresh = f_dt; 
        acc = *prevptr;

        if (thresh == INF) {
            /* this is not a sensible term */
            infinite = 1;
            rethresh = postings + 1;
        }
    }

    /* set step to 1/2 of the threshold */
    step = (thresh + 1) / 2;
    step += !step; /* but don't let it become 0 */

    while (1) {
        while (NEXT_DOC(&v, docno, f_dt)) {
            double _t0 = tnow_us();
            unsigned long _walk = 0;
            double _t1, _t2;
            READ_OFFSETS_WEIGHTED(src, &v, f_dt, weighted_f_dt);
            decoded++;
            _t1 = tnow_us();

            /* merge into accumulator list */
            while (acc && (docno > acc->acc.docno)) {
                /* perform threshold test */
                if (acc->acc.weight < results->v_t) {
                    /* remove this accumulator */
                    *prevptr = acc->next;
                    objalloc_free(results->alloc, acc);
                    acc = (*prevptr);
                    results->accs--;
                } else {
                    /* retain this accumulator */
                    prevptr = &acc->next;
                    acc = acc->next;
                }
                _walk++;
            }
            _t2 = tnow_us();
            zet_inner_decode_ms += (_t1 - _t0) / 1000.0;
            zet_inner_walk_ms   += (_t2 - _t1) / 1000.0;
            zet_inner_postings++;
            zet_inner_walk_steps += _walk;

            if (acc && (docno == acc->acc.docno)) {
                /* METRIC_PER_DOC */
                r_dt = ((((param->k1) + 1) * weighted_f_dt) / ((param->k1) * ((1 - (param->b)) + (((param->b) * (DOCMAP_GET_WORDS(idx->map, acc->acc.docno))) / avg_D_terms)) + weighted_f_dt));
                (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);


                if (acc->acc.weight < results->v_t) {
                    /* remove this accumulator */
                    *prevptr = acc->next;
                    objalloc_free(results->alloc, acc);
                    acc = *prevptr;
                    results->accs--;
                } else {
                    /* go to next accumulator */
                    prevptr = &acc->next;
                    acc = acc->next;
                }
                hit++;
            } else {
                if (f_dt > thresh) {
                    struct search_acc_cons *newacc;
                    assert(!acc || docno < acc->acc.docno); 

                    if ((newacc = objalloc_malloc(results->alloc, 
                      sizeof(*newacc)))) {
                        newacc->acc.docno = docno;
                        newacc->acc.weight = 0.0;
                        newacc->next = acc;
                        acc = newacc;
                        /* note that we have to be careful around here to 
                         * assign newacc to acc before using PER_DOC, 
                         * otherwise we end up with nonsense in some 
                         * accumulators */
                        /* METRIC_PER_DOC */
                        r_dt = ((((param->k1) + 1) * weighted_f_dt) / ((param->k1) * ((1 - (param->b)) + (((param->b) * (DOCMAP_GET_WORDS(idx->map, acc->acc.docno))) / avg_D_terms)) + weighted_f_dt));
                        (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

                        *prevptr = newacc;
                        results->accs++;
                    } else {
                        return SEARCH_ENOMEM;
                    }

                    /* go to next accumulator */
                    prevptr = &acc->next;
                    acc = acc->next;
                } else {
                    missed++;
                }
            }

            if (!--rethresh) {
                int estimate,
                    prev_thresh = thresh;

                estimate = results->accs 
                  + ((postings - decoded) 
                    * ((double)results->accs - initial_accs)) / decoded;

                if (estimate > TOLERANCE * results->acc_limit) {
                    thresh += step;
                } else if ((estimate < results->acc_limit / TOLERANCE) 
                  && thresh) {
                    if (thresh >= step) {
                        thresh -= step;
                    } else {
                        thresh = 0;
                    }
                }

                step = (step + 1) / 2;
                assert(step);

                /* note that we don't want to recalculate the threshold if it
                 * doesn't change because this involves re-discretising it */
                if (prev_thresh != thresh) {
                    /* recalculate contribution that corresponds to the new 
                     * threshold */
                    f_dt = thresh;
                    if (f_dt) {
                        acc = &dummy;
                        acc->acc.docno = UINT_MAX;   /* shouldn't be used */
                        acc->acc.weight = 0.0;
                        /* METRIC_CONTRIB */
                        r_dt = ((((param->k1) + 1) * f_dt)       / ((param->k1) * ((1 - (param->b)) + (((param->b) * (avg_D_terms)) / avg_D_terms)) + f_dt));
                        (acc->acc.weight) += r_dt * w_t * r_qt * click_boost(acc->acc.docno);

                        results->v_t = acc->acc.weight;
                        acc = *prevptr;
                    } else {
                        results->v_t = FLT_MIN;
                    }
                }

                rethresh_dist *= 2;
                rethresh = rethresh_dist;
            }
        }

        /* need to read more data, preserving bytes that we already have */
        if ((ret = src->read(src, VEC_LEN(&v),
            (void **) &v.pos, &bytes)) == SEARCH_OK) {

            v.end = v.pos + bytes;
        } else if (ret == SEARCH_FINISH) {
            /* finished, estimate total results count */
            assert(postings == decoded);

            results->total_results += (int) (results->accs - initial_accs);

            /* list entries now divide up into three portions:
             *   - matching an entry in the acc list (hit)
             *   - missed
             *   - added
             *
             * cooccurrance rate is the percentage of list items hit */
            cooc_rate = hit/(double)decoded;

            /* now have sampled co-occurrance rate, use this to estimate 
             * population co-occurrance rate (assuming unbiased sampling) 
             * and then number of results from unrestricted evaluation */
            assert(results->total_results >= results->accs);
            cooc_rate *= results->total_results/(double)results->accs; 
            assert(cooc_rate >= 0.0);
            if (cooc_rate > 1.0) {
                cooc_rate = 1.0;
            }

            /* add number of things we think would have been added from the
             * things that were missed */
            results->total_results += (1 - cooc_rate) * missed;

            /* note that the total results are not an estimate if either there
             * were no accumulators in the list when we started (in which case
             * missed records exactly the number, uh, missing from the
             * accumulators) or there were none missed, in which case the
             * accumulators have fully accounted for everything in this list.
             * In either case, the (1 - cooc_rate) * missed maths above handles
             * it exactly (modulo doubleing point errors of course). */
            if (initial_accs && missed) {
                results->estimated |= 1;
            }

            if (!VEC_LEN(&v)) {
                if (!infinite) {
                    /* continue threshold evaluation */
                    return SEARCH_OK;
                } else {
                    /* switch to AND processing */
                    return SEARCH_FINISH;
                }
            } else {
                return SEARCH_EINVAL;
            }
        } else {
            param = NULL;   /* avoid 'param not used' warning */
            return ret;
        }
    }
}

/* Declare a function named the same as the metric that returns a structure 
 * containing function pointers */
const struct search_metric * /* METRIC_NAME */ okapi 
  (struct search_metric *sm, int offsets) {
    sm->pre = pre;
    sm->post = post;
    sm->name = /* METRIC_QUOTED_NAME */ "okapi";
    sm->parse_params = parse;
    if (offsets) {
        sm->or_decode = or_decode_offsets;
        sm->and_decode = and_decode_offsets;
        sm->thresh_decode = thresh_decode_offsets;
    } else {
        sm->or_decode = or_decode;
        sm->and_decode = and_decode;
        sm->thresh_decode = thresh_decode;
    }

    return sm;
}

