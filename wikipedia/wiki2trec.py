#!/usr/bin/env python3
"""
Fast Wikipedia XML → TREC converter for Zettair.
Also outputs snippets.json and images.json sidecar files.
"""
import sys, re, json, hashlib, xml.etree.ElementTree as ET

NS = 'http://www.mediawiki.org/xml/export-0.11/'

# Image filenames containing these strings are decorative — skip them
IMAGE_SKIP = ('flag', 'icon', 'logo', 'stub', 'wikidata', 'commons-logo',
              'portal', 'question', 'replace', 'edittools', 'button')
IMAGE_SKIP_EXT = ('.svg', '.ogg', '.ogv', '.webm', '.mid', '.midi', '.pdf')

def wiki_image_url(filename):
    fn = filename.strip().replace(' ', '_')
    md5 = hashlib.md5(fn.encode()).hexdigest()
    return f"https://upload.wikimedia.org/wikipedia/commons/thumb/{md5[0]}/{md5[0:2]}/{fn}/300px-{fn}"

def extract_image(raw):
    """Extract first non-decorative image filename from raw wikitext."""
    for m in re.finditer(r'\[\[(?:File|Image):([^\|\]\n]+)', raw, flags=re.I):
        fn = m.group(1).strip()
        fn_lower = fn.lower()
        # Skip decorative images
        if any(skip in fn_lower for skip in IMAGE_SKIP):
            continue
        if any(fn_lower.endswith(ext) for ext in IMAGE_SKIP_EXT):
            continue
        # Must end in an image extension
        if not any(fn_lower.endswith(ext) for ext in ('.jpg', '.jpeg', '.png', '.gif', '.webp')):
            continue
        return wiki_image_url(fn)
    return None

def extract_snippet(text):
    """Extract first 2-3 clean sentences, 300-500 chars, never cut mid-sentence."""
    # Split on sentence boundaries
    sentences = re.split(r'(?<=[.!?]) (?=[A-Z])', text)
    snippet = ''
    for s in sentences:
        s = s.strip()
        if not s:
            continue
        # Skip sentences that look like image captions (short, end with ]])
        if s.endswith(']]') or s.startswith('|') or len(s) < 20:
            continue
        # Skip sentences that still have template artifacts
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
    # Remove File/Image blocks including caption text before the closing ]]
    text = re.sub(r'\[\[(?:File|Image):[^\]]*\]\]', '', text, flags=re.I)
    text = re.sub(r'\[\[(Category):[^\]]*\]\]', '', text, flags=re.I)
    text = re.sub(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]', r'\1', text)
    text = re.sub(r'\[https?://\S+\s+([^\]]+)\]', r'\1', text)
    text = re.sub(r'\[https?://\S+\]', '', text)
    text = re.sub(r"'{2,3}", '', text)
    text = re.sub(r'={2,6}[^=]+=+', ' ', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&[a-z#]+;', ' ', text)
    # Remove any remaining ]] or [[ fragments
    text = re.sub(r'\]\]|\[\[', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def safe_id(title):
    return re.sub(r'[^\w\-]', '_', title)[:80]

def convert(xml_path, trec_path):
    snippets = {}
    images = {}
    count = skipped = img_count = 0

    with open(trec_path, 'w', encoding='utf-8') as out:
        for event, elem in ET.iterparse(xml_path, events=('end',)):
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

            # Extract image from raw wikitext (before cleaning)
            img_url = extract_image(raw)

            text = clean(raw)
            if len(text) < 100:
                elem.clear(); skipped += 1; continue

            docno = safe_id(title)

            # Extract snippet from cleaned text
            snippet = extract_snippet(text)
            snippets[docno] = snippet

            # Store image if found
            if img_url:
                images[docno] = img_url
                img_count += 1

            out.write(f'<DOC>\n<DOCNO>{docno}</DOCNO>\n<TEXT>\n{title}. {text}\n</TEXT>\n</DOC>\n')
            count += 1

            if count % 5000 == 0:
                out.flush()
                print(f'  {count:,} articles... ({img_count:,} images, {count:,} snippets)', flush=True)

            elem.clear()

    # Write sidecar files
    snippets_path = trec_path.replace('.trec', '') + '_snippets.json'
    images_path = trec_path.replace('.trec', '') + '_images.json'

    print(f'Writing {snippets_path}...', flush=True)
    with open(snippets_path, 'w', encoding='utf-8') as f:
        json.dump(snippets, f, ensure_ascii=False)

    print(f'Writing {images_path}...', flush=True)
    with open(images_path, 'w', encoding='utf-8') as f:
        json.dump(images, f, ensure_ascii=False)

    print(f'Done: {count:,} articles, {skipped:,} skipped, {img_count:,} images extracted.')
    return count, len(snippets), img_count

if __name__ == '__main__':
    xml_in  = sys.argv[1] if len(sys.argv) > 1 else 'simplewiki.xml'
    trec_out = sys.argv[2] if len(sys.argv) > 2 else 'simplewiki.trec'
    print(f'Converting {xml_in} → {trec_out}', flush=True)
    convert(xml_in, trec_out)
