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
#include <stdio.h>
#include <stdlib.h>
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
    /* Multiplicative click factor: score *= 1 + alpha * log(1 + clicks).
     * Multiplicative was tried earlier as additive (click_addend in post),
     * but additive lifted globally-popular articles by a fixed margin
     * regardless of query relevance — so a query like "sundar pichai"
     * had Google beat Sundar because Google's global clicks dominated.
     * Multiplicative scales with the BM25 base, so the boost is bounded
     * by how relevant the doc already is. */
    if (g_click_prior && docno < g_click_prior_len && g_click_prior[docno] > 0.0f)
        return 1.0 + g_click_alpha * log(1.0 + (double)g_click_prior[docno]);
    return 1.0;
}

static inline double click_addend(unsigned long int docno) {
    /* Disabled — kept as a no-op to avoid touching the post() path. */
    (void)docno;
    return 0.0;
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
            READ_OFFSETS_WEIGHTED(src, &v, f_dt, weighted_f_dt);
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

