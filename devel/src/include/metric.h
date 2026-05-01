/* metric.h declares the search-metric entry points.
 *
 * Currently okapi BM25 is the only supported metric.
 * Older variants (dirichlet, pcosine, cosine, hawkapi) were removed.
 */

#ifndef METRIC_H
#define METRIC_H

struct search_metric;

/* okapi BM25 metric */
const struct search_metric *okapi(struct search_metric *sm, int offsets);

#endif
