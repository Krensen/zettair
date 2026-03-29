#!/usr/bin/env python3
"""
Fast Wikipedia XML → TREC converter for Zettair.
Uses a single-pass regex approach instead of nested loops.
"""
import sys, re, xml.etree.ElementTree as ET

NS = 'http://www.mediawiki.org/xml/export-0.11/'

def clean(text):
    # Remove templates in one shot (non-greedy won't work on nested, so use a fixed-point pass but cap it)
    for _ in range(5):
        t = re.sub(r'\{\{[^{}]*\}\}', '', text)
        if t == text: break
        text = t
    text = re.sub(r'\[\[(File|Image|Category):[^\]]*\]\]', '', text, flags=re.I)
    text = re.sub(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]', r'\1', text)
    text = re.sub(r'\[https?://\S+\s+([^\]]+)\]', r'\1', text)
    text = re.sub(r'\[https?://\S+\]', '', text)
    text = re.sub(r"'{2,3}", '', text)
    text = re.sub(r'={2,6}[^=]+=+', ' ', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&[a-z]+;', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def safe_id(title):
    return re.sub(r'[^\w\-]', '_', title)[:80]

def convert(xml_path, trec_path):
    count = skipped = 0
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

            # Skip redirects quickly
            if raw.lstrip().lower().startswith('#redirect'):
                elem.clear(); skipped += 1; continue

            text = clean(raw)
            if len(text) < 100:
                elem.clear(); skipped += 1; continue

            docno = safe_id(title)
            out.write(f'<DOC>\n<DOCNO>{docno}</DOCNO>\n<TEXT>\n{title}. {text}\n</TEXT>\n</DOC>\n')
            count += 1
            if count % 5000 == 0:
                out.flush()
                print(f'  {count:,} articles...', flush=True)
            elem.clear()

    print(f'Done: {count:,} articles, {skipped:,} skipped.')

if __name__ == '__main__':
    xml_in  = sys.argv[1] if len(sys.argv) > 1 else 'simplewiki.xml'
    trec_out = sys.argv[2] if len(sys.argv) > 2 else 'simplewiki.trec'
    print(f'Converting {xml_in} → {trec_out}', flush=True)
    convert(xml_in, trec_out)
