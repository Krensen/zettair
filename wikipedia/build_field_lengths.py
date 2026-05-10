#!/usr/bin/env python3
"""
build_field_lengths.py — Compute per-doc field lengths and per-field corpus
stats from a TREC file, in the same docno order that Zettair will assign
when it indexes the file.

Outputs (in cwd):
  field_lengths.bin  — N_docs * NUM_FIELDS * uint32, field_id 0=body, 1=title, etc.
  field_stats.bin    — header + (avg_len, n_docs_with_field) per field

For now we only count body and title; other fields are reserved (lengths=0).

Usage:
  python3 build_field_lengths.py corpus.trec
"""
import re
import struct
import sys

NUM_FIELDS = 16  # matches POSTINGS_MAX_FIELDS

DOC_RE = re.compile(r"<DOC>\s*(.*?)\s*</DOC>", re.DOTALL)
DOCNO_RE = re.compile(r"<DOCNO>(.*?)</DOCNO>", re.DOTALL)
TITLE_RE = re.compile(r"<TITLE>(.*?)</TITLE>", re.DOTALL)
TEXT_RE = re.compile(r"<TEXT>(.*?)</TEXT>", re.DOTALL)
WORD_RE = re.compile(r"[A-Za-z0-9]+")


def count_words(s: str) -> int:
    return sum(1 for _ in WORD_RE.finditer(s))


def main():
    if len(sys.argv) < 2:
        print("usage: build_field_lengths.py corpus.trec", file=sys.stderr)
        sys.exit(1)
    trec_path = sys.argv[1]

    with open(trec_path, encoding="utf-8") as f:
        content = f.read()

    docs = DOC_RE.findall(content)
    print(f"Found {len(docs)} docs")

    # field_lengths[doc_idx * NUM_FIELDS + field_id] = word_count
    field_lengths = [0] * (len(docs) * NUM_FIELDS)
    sum_len = [0] * NUM_FIELDS
    n_with = [0] * NUM_FIELDS

    for i, doc in enumerate(docs):
        title_m = TITLE_RE.search(doc)
        text_m = TEXT_RE.search(doc)
        title = title_m.group(1) if title_m else ""
        text = text_m.group(1) if text_m else ""

        # Body length: <TEXT> minus the title prefix. wiki2trec emits
        # `{title}. {text}` inside <TEXT> so we want to count title-words
        # ONCE — as a title field length — not double-count them as body.
        # Simplest: body length = total <TEXT> word count. Title length
        # is the count of words in <TITLE>. That mirrors the index: each
        # word's posting carries the field_id where it occurred, and the
        # title words appear twice in the index (once as title, once as
        # body — because wiki2trec leaves the title at the start of <TEXT>).
        # Length-norm should reflect that doubling, since BM25 sees both.
        title_len = count_words(title)
        body_len = count_words(text)  # includes the title-prefix words

        title_len_field = title_len
        body_len_field = body_len  # all words in <TEXT> are body postings

        field_lengths[i * NUM_FIELDS + 0] = body_len_field
        field_lengths[i * NUM_FIELDS + 1] = title_len_field
        if body_len_field > 0:
            n_with[0] += 1
            sum_len[0] += body_len_field
        if title_len_field > 0:
            n_with[1] += 1
            sum_len[1] += title_len_field

    # Write field_lengths.bin
    with open("field_lengths.bin", "wb") as f:
        f.write(struct.pack(f"{len(field_lengths)}I", *field_lengths))
    print(f"Wrote field_lengths.bin: {len(field_lengths)} entries ({len(field_lengths)*4} bytes)")

    # Write field_stats.bin: header (n_docs, num_fields) + per-field (avg_len, n_with)
    with open("field_stats.bin", "wb") as f:
        f.write(struct.pack("II", len(docs), NUM_FIELDS))
        for fld in range(NUM_FIELDS):
            avg = (sum_len[fld] / n_with[fld]) if n_with[fld] > 0 else 0.0
            f.write(struct.pack("dI", avg, n_with[fld]))
    print(f"Wrote field_stats.bin: avg body={sum_len[0]/max(n_with[0],1):.1f}, "
          f"avg title={sum_len[1]/max(n_with[1],1):.1f} "
          f"({n_with[1]}/{len(docs)} docs have title)")


if __name__ == "__main__":
    main()
