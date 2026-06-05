#!/usr/bin/env python3
"""
Fast Wikipedia XML → TREC converter for Zettair.
Also outputs snippets and images flat-store sidecar files.

Usage:
  python3 wiki2trec.py input.xml output.trec
  python3 wiki2trec.py input.xml.bz2 output.trec              # streams bz2 directly
  python3 wiki2trec.py input.xml.bz2 output.trec --titles top_titles.txt
"""
import argparse, bz2, re, json, hashlib, xml.etree.ElementTree as ET

NS = 'http://www.mediawiki.org/xml/export-0.11/'

# Image filenames containing these strings are decorative — skip them
IMAGE_SKIP = ('flag', 'icon', 'logo', 'stub', 'wikidata', 'commons-logo',
              'portal', 'question', 'replace', 'edittools', 'button')
IMAGE_SKIP_EXT = ('.svg', '.ogg', '.ogv', '.webm', '.mid', '.midi', '.pdf')

def wiki_image_url(filename):
    fn = filename.strip().replace(' ', '_')
    md5 = hashlib.md5(fn.encode()).hexdigest()
    return f"https://upload.wikimedia.org/wikipedia/commons/thumb/{md5[0]}/{md5[0:2]}/{fn}/300px-{fn}"

def _is_usable_image(fn):
    """Filter out decorative / non-photo images."""
    fn_lower = fn.lower()
    if any(skip in fn_lower for skip in IMAGE_SKIP):
        return False
    if any(fn_lower.endswith(ext) for ext in IMAGE_SKIP_EXT):
        return False
    if not any(fn_lower.endswith(ext) for ext in ('.jpg', '.jpeg', '.png', '.gif', '.webp')):
        return False
    return True


def extract_image(raw):
    """Extract the article's lead image.

    Prefer infobox `image = Foo.jpg` (or `image1 = ...` / `image_caption =`-
    adjacent fields) — that's where Wikipedia puts the canonical lead image
    for people, places, products, etc. Falls back to the first usable
    [[File:...]] / [[Image:...]] reference in the body for articles that
    don't use an infobox.

    Without this, the Morrissey article (and many others) returned
    Queen's_Square,_Hulme.png — the first body image in the wikitext —
    instead of the singer's portrait that lives in the infobox.
    """
    # Infobox image fields. Wikipedia infoboxes look like:
    #   {{Infobox musical artist
    #   | image             = Morrissey crop tie.jpg <!-- ... -->
    #   | caption           = ...
    # so each parameter line typically starts with `|` and then the
    # parameter name. Permissive about whitespace and ordering.
    for m in re.finditer(
        r'^\s*\|?\s*image[0-9_a-z]*\s*=\s*(?:File:|Image:)?\s*'
        r'([^\|\}\n<\[]+\.(?:jpg|jpeg|png|gif|webp))',
        raw, flags=re.I | re.M
    ):
        fn = m.group(1).strip()
        if _is_usable_image(fn):
            return wiki_image_url(fn)

    # Fallback: first [[File:...]] in the body.
    for m in re.finditer(r'\[\[(?:File|Image):([^\|\]\n]+)', raw, flags=re.I):
        fn = m.group(1).strip()
        if _is_usable_image(fn):
            return wiki_image_url(fn)
    return None

def extract_snippet(text):
    """Extract first 2-3 clean sentences, 300-500 chars, never cut mid-sentence."""
    sentences = re.split(r'(?<=[.!?]) (?=[A-Z])', text)
    snippet = ''
    for s in sentences:
        s = s.strip()
        if not s:
            continue
        if s.endswith(']]') or s.startswith('|') or len(s) < 20:
            continue
        if '{{' in s or '}}' in s or '[[' in s:
            continue
        candidate = (snippet + ' ' + s).strip() if snippet else s
        if len(candidate) >= 300:
            snippet = candidate
            break
        snippet = candidate
        if len(snippet) >= 500:
            break
    return snippet[:600] if snippet else text[:300]

def clean(text):
    """Strip wikitext markup from article text."""
    for _ in range(5):
        t = re.sub(r'\{\{[^{}]*\}\}', '', text)
        if t == text: break
        text = t
    text = re.sub(r'\[\[(?:File|Image):[^\]]*\]\]', '', text, flags=re.I)
    text = re.sub(r'\[\[(Category):[^\]]*\]\]', '', text, flags=re.I)
    text = re.sub(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]', r'\1', text)
    text = re.sub(r'\[https?://\S+\s+([^\]]+)\]', r'\1', text)
    text = re.sub(r'\[https?://\S+\]', '', text)
    text = re.sub(r"'{2,3}", '', text)
    text = re.sub(r'={2,6}[^=]+=+', ' ', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&[a-z#]+;', ' ', text)
    text = re.sub(r'\]\]|\[\[', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def safe_id(title):
    return re.sub(r'[^\w\-]', '_', title)[:80]

def title_to_dbkey(title):
    """Convert display title to dbkey form for allowlist lookup."""
    return title.replace(' ', '_')

def load_titles(path):
    """Load allowlist from file, return as a set of dbkey strings."""
    with open(path, encoding='utf-8') as f:
        titles = {line.strip() for line in f if line.strip()}
    print(f'Loaded {len(titles):,} titles from allowlist', flush=True)
    return titles

URL_PREFIX = 'https://en.wikipedia.org/wiki/'

# PRD-031 followup: strip the Wikipedia disambiguator suffix
# ("Mercury (planet)" -> "Mercury") from the indexed TITLE only. The
# display sidecar and TEXT body keep the full title. Matches
# trailing "(...)" with no nested parens, leading whitespace
# tolerated.
_DISAMB_PAREN_RE = re.compile(r'\s*\([^()]+\)\s*$')


def convert(xml_path, trec_path, titles=None):
    base = trec_path.replace('.trec', '')
    snip_store_path  = base + '_snippets.store'
    snip_map_path    = base + '_snippets.map'
    img_store_path   = base + '_images.store'
    img_map_path     = base + '_images.map'
    url_store_path   = base + '_urls.store'
    url_map_path     = base + '_urls.map'
    # PRD-031: canonical Wikipedia display title per docno. Covers
    # 100% of the corpus (unlike the URL sidecar which only stores
    # dbkey-mismatched docnos) so server-side title rendering is one
    # FlatStore lookup, never a URL parse.
    title_store_path = base + '_titles.store'
    title_map_path   = base + '_titles.map'

    count = skipped = filtered = img_count = url_count = 0
    snip_map  = {}
    img_map   = {}
    url_map   = {}
    title_map = {}
    snip_offset  = 0
    img_offset   = 0
    url_offset   = 0
    title_offset = 0

    # Stream bz2 or plain XML
    if xml_path.endswith('.bz2'):
        fh = bz2.open(xml_path, 'rb')
    else:
        fh = open(xml_path, 'rb')

    with fh, \
         open(trec_path, 'w', encoding='utf-8') as out, \
         open(snip_store_path,  'wb') as snip_store, \
         open(img_store_path,   'wb') as img_store, \
         open(url_store_path,   'wb') as url_store, \
         open(title_store_path, 'wb') as title_store:

        for event, elem in ET.iterparse(fh, events=('end',)):
            if elem.tag != f'{{{NS}}}page':
                continue
            ns_el = elem.find(f'{{{NS}}}ns')
            if ns_el is None or ns_el.text != '0':
                elem.clear(); continue
            title_el = elem.find(f'{{{NS}}}title')
            rev = elem.find(f'{{{NS}}}revision')
            if rev is None: elem.clear(); skipped += 1; continue
            text_el = rev.find(f'{{{NS}}}text')
            if title_el is None or text_el is None or not text_el.text:
                elem.clear(); skipped += 1; continue

            title = title_el.text.strip()
            raw = text_el.text

            # Skip redirects
            if raw.lstrip().lower().startswith('#redirect'):
                elem.clear(); skipped += 1; continue

            # Apply title allowlist if provided
            if titles is not None and title_to_dbkey(title) not in titles:
                elem.clear(); filtered += 1; continue

            img_url = extract_image(raw)

            text = clean(raw)
            if len(text) < 100:
                elem.clear(); skipped += 1; continue

            docno = safe_id(title)
            dbkey = title_to_dbkey(title)
            # Only store the URL when the dbkey form differs from the safe_id —
            # for the 77% of articles where they match, server.py constructs
            # the URL on the fly from the docno.
            if dbkey != docno:
                encoded_url = (URL_PREFIX + dbkey).encode('utf-8')
                url_map[docno] = [url_offset, len(encoded_url)]
                url_store.write(encoded_url)
                url_offset += len(encoded_url)
                url_count += 1

            snippet = extract_snippet(text)
            encoded = snippet.encode('utf-8')
            snip_map[docno] = [snip_offset, len(encoded)]
            snip_store.write(encoded)
            snip_offset += len(encoded)

            if img_url:
                encoded_img = img_url.encode('utf-8')
                img_map[docno] = [img_offset, len(encoded_img)]
                img_store.write(encoded_img)
                img_offset += len(encoded_img)
                img_count += 1

            # PRD-031: canonical display title sidecar. Every docno
            # gets an entry. server.py reads this directly instead of
            # parsing it back out of the URL (which was a hack). This
            # is the *full* title including the disambiguator
            # ("Mercury (planet)"); display-only.
            encoded_title = title.encode('utf-8')
            title_map[docno] = [title_offset, len(encoded_title)]
            title_store.write(encoded_title)
            title_offset += len(encoded_title)

            # PRD-031 followup: the indexed TITLE drops the
            # disambiguator. Wikipedia's "(planet)" / "(film)" /
            # "(album)" suffix is metadata that disambiguates a
            # docno, not content about the page. Indexing it inflates
            # the title's effective length under BM25F b=1.0, which
            # currently makes "Mercury (planet)" tie on title-tf
            # against any other 2-word title containing "mercury"
            # (Freddie_Mercury, Mercury_Cougar, ...). With the
            # disambiguator stripped from the indexed title, the
            # planet's title is just "Mercury" — a 1-word exact match
            # for the bare query, on par with the disambig page.
            # The disambiguator survives in <TEXT> (one paragraph
            # below) so queries like "Mercury planet" still match it
            # via the body field.
            index_title = _DISAMB_PAREN_RE.sub('', title).strip()
            # Emit the indexed (stripped) title in <TITLE> for the
            # BM25F scorer, and the full (unstripped) title in a
            # sibling <DISPLAY_TITLE> tag that bootstrap consumers
            # (build_titles_sidecar.py) read to populate the PRD-031
            # display sidecar. <DISPLAY_TITLE> is not indexed by zet
            # because its tokeniser only knows about TITLE/TEXT.
            out.write(
                f'<DOC>\n'
                f'<DOCNO>{docno}</DOCNO>\n'
                f'<TITLE>{index_title}</TITLE>\n'
                f'<DISPLAY_TITLE>{title}</DISPLAY_TITLE>\n'
                f'<TEXT>\n{title}. {text}\n</TEXT>\n'
                f'</DOC>\n'
            )
            count += 1

            if count % 10000 == 0:
                out.flush()
                snip_store.flush()
                img_store.flush()
                print(f'  {count:,} articles written ({img_count:,} images, {filtered:,} filtered out)...', flush=True)

            elem.clear()

    print(f'Writing {snip_map_path}...', flush=True)
    with open(snip_map_path, 'w', encoding='utf-8') as f:
        json.dump(snip_map, f, separators=(',', ':'))

    print(f'Writing {img_map_path}...', flush=True)
    with open(img_map_path, 'w', encoding='utf-8') as f:
        json.dump(img_map, f, separators=(',', ':'))

    print(f'Writing {url_map_path}...', flush=True)
    with open(url_map_path, 'w', encoding='utf-8') as f:
        json.dump(url_map, f, separators=(',', ':'))

    print(f'Writing {title_map_path}...', flush=True)
    with open(title_map_path, 'w', encoding='utf-8') as f:
        json.dump(title_map, f, separators=(',', ':'))

    if titles is not None:
        matched_pct = 100 * count / len(titles) if titles else 0
        print(f'Allowlist match: {count:,} of {len(titles):,} titles found in dump ({matched_pct:.1f}%)', flush=True)
        if count < len(titles) * 0.8:
            print(f'WARNING: fewer than 80% of allowlist titles matched — check title normalisation', flush=True)

    print(f'Done: {count:,} articles written, {skipped:,} skipped, {filtered:,} filtered, {img_count:,} images, {url_count:,} URLs, {len(title_map):,} titles.')
    return count, count, img_count

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('xml_in',  nargs='?', default='enwiki.xml')
    parser.add_argument('trec_out', nargs='?', default='enwiki.trec')
    parser.add_argument('--titles', default=None,
                        help='Path to allowlist file (one title per line, dbkey form)')
    args = parser.parse_args()

    titles = load_titles(args.titles) if args.titles else None
    print(f'Converting {args.xml_in} → {args.trec_out}', flush=True)
    if titles:
        print(f'Allowlist: {len(titles):,} titles', flush=True)
    convert(args.xml_in, args.trec_out, titles)
