/* psettings.h declares an interface to an object that encapsulates parser
 * settings.  
 *
 * A useful improvement might be to improve the parsing to handle tree
 * structured documents properly (particularly with implicit end-tags).  
 * Ultimately this has to be done as part of the HTML parser, since it requires
 * a stack and other things.  However, some stuff could be done using 
 * an additive/subtractive model of psettings.
 *
 * written nml 2004-08-09
 *
 */

#ifndef PSETTINGS_H
#define PSETTINGS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <errno.h>
#include <limits.h>

#include "mime.h"

/* attributes and indexes */

enum psettings_attr {
    /* note that these attributes are all mutually exclusive */
    PSETTINGS_ATTR_DOCEND = (1 << 1),   /* tag denotes the start/end of a 
                                         * document.  This attribute is
                                         * different than the others, because
                                         * its information about the tag itself,
                                         * not the text following the tags. */
    PSETTINGS_ATTR_INDEX = (1 << 0),    /* tag content should be indexed */
    PSETTINGS_ATTR_ID = (1 << 2),       /* tag content should be treated as 
                                         * the document identifier */
    PSETTINGS_ATTR_CHECKBIN = (1 << 3), /* tag content should be hueristically 
                                         * checked to see if it is binary 
                                         * content, and indexed if not.  If so,
                                         * it should be skipped.  This is really
                                         * just here so we can skip some of the
                                         * crap in the TREC collections, so it's
                                         * pretty hacky by nature. */
    PSETTINGS_ATTR_POP = (1 << 4),      /* return to the state before the 
                                         * previous change */
    PSETTINGS_ATTR_NOOP = (1 << 5),     /* don't alter the state */
    PSETTINGS_ATTR_OFF_END = (1 << 6),  /* don't index anything until the 
                                         * corresponding end tag has been 
                                         * processed (this is pretty much a 
                                         * hack to get backward compatibility 
                                         * with the previous parsing setup) */
    PSETTINGS_ATTR_FLOW = (1 << 7),     /* this attribute indicates that text 
                                         * can continue to flow 'through' the 
                                         * tag */
    PSETTINGS_ATTR_TITLE = (1 << 8),    /* indicates the start of a title */
    PSETTINGS_ATTR_CAPTION = (1 << 9),  /* (reserved) image caption field */
    PSETTINGS_ATTR_CATEGORY = (1 << 10),/* (reserved) category list field */
    PSETTINGS_ATTR_SEEALSO = (1 << 11), /* (reserved) see-also field */
    PSETTINGS_ATTR_INFOBOX = (1 << 12)  /* (reserved) infobox content field */
};

/* PRD-017: per-occurrence field tagging for field-weighted retrieval.
 *
 * Each tag-attribute bit listed below maps to a small integer "field_id"
 * stored in the low bits of every encoded posting offset. The BM25 scorer
 * decodes the field_id and applies a per-field boost (ZET_BOOST_TITLE etc).
 *
 * Field 0 is implicit (anything not inside a recognised field tag = body).
 * Up to 16 field IDs are supported (POSTINGS_FIELD_BITS in postings.h).
 *
 * Adding a new field is: define PSETTINGS_ATTR_X above, add a case here,
 * register the tag in psettings_default.c, emit it from wiki2trec.py, and
 * ship a ZET_BOOST_X env var. No format change needed — IDs 2-15 are
 * reserved in the encoding from day one. */
static inline unsigned int psettings_field_id(unsigned int attr) {
    if (attr & PSETTINGS_ATTR_TITLE)    return 1;
    if (attr & PSETTINGS_ATTR_CAPTION)  return 2;
    if (attr & PSETTINGS_ATTR_CATEGORY) return 3;
    if (attr & PSETTINGS_ATTR_SEEALSO)  return 4;
    if (attr & PSETTINGS_ATTR_INFOBOX)  return 5;
    return 0;  /* body */
}

enum psettings_ret {
    PSETTINGS_OK = 0,
    PSETTINGS_ENOMEM = -ENOMEM,
    PSETTINGS_EEXIST = -EEXIST,
    PSETTINGS_EINVAL = -EINVAL,
    PSETTINGS_ETOOBIG = INT_MIN
};

struct psettings;
struct psettings_type;

/* create a new psettings object */
struct psettings *psettings_new(enum psettings_attr def);

/* read a new psettings object from a file */
struct psettings *psettings_read(FILE *file, enum psettings_attr def);

/* delete a psettings object */
void psettings_delete(struct psettings *pset);

/* set the attributes associated with a tag */
enum psettings_ret psettings_set(struct psettings *pset, enum mime_types type,
  const char *tag, enum psettings_attr attr);

/* set the default associated with a mime type */
enum psettings_ret psettings_type_default(struct psettings *pset, 
  enum mime_types type, enum psettings_attr def);

/* get the attributes for a tag in a given mime type.  Defaults are returned if
 * the tag isn't found. */
enum psettings_attr psettings_find(struct psettings *pset, 
  enum mime_types type, const char *tag);
enum psettings_attr psettings_type_find(struct psettings *pset, 
  struct psettings_type *type, const char *tag);

/* get the set of tags for a given mime type */
enum psettings_ret psettings_type_tags(struct psettings *pset, 
  enum mime_types type, struct psettings_type **ptype);

/* output c code to create a function psettings_new_default based on the
 * contents of the current psettings object */
enum psettings_ret psettings_write_code(struct psettings *pset, FILE *output);

/* create a psettings object with default settings */
struct psettings *psettings_new_default(enum psettings_attr def);

/* returns the overall default setting from the given settings */
enum psettings_attr psettings_default(struct psettings *pset);

/* returns a boolean indication of whether the given type contains a tag that
 * identifies each document (i.e. whether a tag in that type has 
 * PSETTINGS_ATTR_ID).  Returns 0 for unknown types */
int psettings_self_id(struct psettings *pset, enum mime_types type);

#ifdef __cplusplus
}
#endif

#endif

