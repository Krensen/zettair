#!/usr/bin/env python3
"""
build_field_lengths.py — Compute per-doc field lengths and per-field corpus
stats from a TREC file, in the same docno order that Zettair will assign
when it indexes the file.

Outputs (in cwd):
  field_lengths.bin  — N_docs * NUM_FIELDS * uint32, field_id 0=body, 1=title, etc.
  field_stats.bin    — header + (avg_len, n_docs_with_field) per field

Streams the TREC file line-by-line so it works on the 14 GB production
file inside an 8 GB box.

For now we only count body and title; other fields are reserved (lengths=0).

Usage:
  python3 build_field_lengths.py corpus.trec
"""
import re
import struct
import sys

NUM_FIELDS = 16  # matches POSTINGS_MAX_FIELDS
WORD_RE = re.compile(r"[A-Za-z0-9]+")


def count_words(s: str) -> int:
    return sum(1 for _ in WORD_RE.finditer(s))


def main():
    if len(sys.argv) < 2:
        print("usage: build_field_lengths.py corpus.trec", file=sys.stderr)
        sys.exit(1)
    trec_path = sys.argv[1]

    # Streaming state machine. We track whether we're currently inside a
    # <TITLE> or <TEXT> tag and accumulate the word count for each per
    # document. Tags are assumed to be opened/closed on their own lines —
    # which matches what wiki2trec.py emits.
    in_title = False
    in_text = False
    title_words = 0
    text_words = 0
    n_docs = 0
    sum_body = 0
    sum_title = 0
    n_with_body = 0
    n_with_title = 0

    f_out = open("field_lengths.bin", "wb")
    pack_uint = struct.Struct("I").pack
    zero_field = pack_uint(0)

    def emit_doc(body_len, title_len):
        # Write one row of NUM_FIELDS uint32s. Body=field 0, title=field 1,
        # everything else zero (reserved).
        row = [pack_uint(0)] * NUM_FIELDS
        row[0] = pack_uint(body_len)
        row[1] = pack_uint(title_len)
        f_out.write(b"".join(row))

    with open(trec_path, "rb") as f:
        for raw in f:
            # Decode permissively — corpus is mostly UTF-8 but some pages
            # have stray bytes. We only do regex word matching, so latin-1
            # is safe and never raises.
            line = raw.decode("utf-8", errors="replace")
            stripped = line.strip()

            if stripped.startswith("<TITLE>"):
                # could be one-line: "<TITLE>foo</TITLE>" or multi-line
                inner = stripped[len("<TITLE>"):]
                end = inner.find("</TITLE>")
                if end >= 0:
                    title_words += count_words(inner[:end])
                else:
                    title_words += count_words(inner)
                    in_title = True
                continue
            if in_title:
                end = stripped.find("</TITLE>")
                if end >= 0:
                    title_words += count_words(stripped[:end])
                    in_title = False
                else:
                    title_words += count_words(stripped)
                continue

            if stripped == "<TEXT>":
                in_text = True
                continue
            if in_text:
                if stripped == "</TEXT>":
                    in_text = False
                    continue
                text_words += count_words(line)
                continue

            if stripped == "</DOC>":
                # finalise this doc
                emit_doc(text_words, title_words)
                if text_words > 0:
                    sum_body += text_words
                    n_with_body += 1
                if title_words > 0:
                    sum_title += title_words
                    n_with_title += 1
                n_docs += 1
                if n_docs % 100000 == 0:
                    print(f"  {n_docs:,} docs processed", flush=True)
                title_words = 0
                text_words = 0
                continue

    f_out.close()
    print(f"Total docs: {n_docs:,}")
    print(f"Wrote field_lengths.bin: {n_docs * NUM_FIELDS} entries ({n_docs * NUM_FIELDS * 4} bytes)")

    # Write field_stats.bin: header (n_docs, num_fields) + per-field (avg_len, n_with)
    with open("field_stats.bin", "wb") as f:
        f.write(struct.pack("II", n_docs, NUM_FIELDS))
        for fld in range(NUM_FIELDS):
            if fld == 0:
                avg = (sum_body / n_with_body) if n_with_body > 0 else 0.0
                nw = n_with_body
            elif fld == 1:
                avg = (sum_title / n_with_title) if n_with_title > 0 else 0.0
                nw = n_with_title
            else:
                avg = 0.0
                nw = 0
            f.write(struct.pack("dI", avg, nw))
    avg_body = sum_body / max(n_with_body, 1)
    avg_title = sum_title / max(n_with_title, 1)
    print(f"Wrote field_stats.bin: avg body={avg_body:.1f}, "
          f"avg title={avg_title:.1f} "
          f"({n_with_title}/{n_docs} docs have title)")


if __name__ == "__main__":
    main()
