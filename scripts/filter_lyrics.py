#!/usr/bin/env python3
"""
lrclib フィルタリングスクリプト (1パス・メモリ安全・高速化版)

最適化:
  - SQLプレフィルタ: duration >= 60
  - 品質チェックを最初に (最も軽い処理)
  - 言語判定: 1パス走査 + 早期リターン
  - MD5 ハッシュ (SHA256の2-3倍速)
  - ストリーミング処理 (全件メモリに載せない)
  - 全正規表現プリコンパイル

Usage:
    python3 -u filter_lyrics.py <input.db> <output.db>
"""

import hashlib
import re
import sqlite3
import sys
import time

# ============================================================
# プリコンパイル済み正規表現
# ============================================================

_RE_TS = re.compile(r'\[[\d:.]+\]')
_RE_PAREN = re.compile(r'[\(\（].*?[\)\）]')
_RE_NON_WORD = re.compile(r'[^\w]')
_RE_BRACKET = re.compile(r'\s*[\(\[（【].+?[\)\]）】]')
_RE_FEAT = re.compile(r'\s*(feat|ft|with)\s+.*$', re.I)
_RE_SYMBOL = re.compile(r'[^\w\s]')


# ============================================================
# 正規化・判定関数
# ============================================================

def normalize_name(name: str) -> str:
    n = _RE_SYMBOL.sub('', name.lower())
    n = _RE_BRACKET.sub('', n)
    n = _RE_FEAT.sub('', n)
    return ' '.join(n.split())


def lyrics_fingerprint(synced_lyrics: str) -> str:
    text = _RE_TS.sub('', synced_lyrics)
    text = _RE_PAREN.sub('', text)
    text = _RE_NON_WORD.sub('', text).lower()
    if not text:
        return ''
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def classify_lang(text_no_ts: str) -> str | None:
    """1パス走査 + 早期リターンの言語判定"""
    if len(text_no_ts) < 30:
        return None

    ja = ko = latin = exclude = 0
    for c in text_no_ts:
        cp = ord(c)
        if 0x3040 <= cp <= 0x309F or 0x30A0 <= cp <= 0x30FF:
            ja += 1
            if ja >= 10:
                return 'ja'
        elif 0xAC00 <= cp <= 0xD7AF:
            ko += 1
            if ko >= 10:
                return 'ko'
        elif (0x41 <= cp <= 0x5A or 0x61 <= cp <= 0x7A
              or 0x00C0 <= cp <= 0x024F):
            latin += 1
        elif (0x0600 <= cp <= 0x06FF or 0x0400 <= cp <= 0x04FF
              or 0x0900 <= cp <= 0x097F or 0x0E00 <= cp <= 0x0E7F):
            exclude += 1
            if exclude > 50:
                return None
    if exclude > 50 and exclude > latin:
        return None
    if latin >= 30:
        return 'en'
    return None


# ============================================================
# メイン: 1パスストリーミング処理
# ============================================================

def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.db> <output.db>", file=sys.stderr)
        sys.exit(1)

    input_db, output_db = sys.argv[1], sys.argv[2]
    print(f"Input:  {input_db}")
    print(f"Output: {output_db}")

    # --- 入力DB ---
    src = sqlite3.connect(input_db)
    src.execute('PRAGMA mmap_size=4294967296;')
    src.execute('PRAGMA cache_size=-1000000;')
    total = src.execute('SELECT COUNT(*) FROM lyrics').fetchone()[0]
    print(f"Total records: {total:,}")

    # --- 出力DB ---
    dst = sqlite3.connect(output_db)
    dst.execute('PRAGMA synchronous=OFF;')
    dst.execute('PRAGMA journal_mode=OFF;')
    dst.execute('PRAGMA cache_size=-500000;')
    dst.execute('''CREATE TABLE lyrics (
        id INTEGER PRIMARY KEY, track_name TEXT NOT NULL,
        artist_name TEXT NOT NULL, album_name TEXT,
        duration REAL, synced_lyrics TEXT NOT NULL, lang TEXT NOT NULL
    )''')

    # --- 1パス処理 ---
    meta_seen: dict[tuple, tuple[int, int]] = {}  # key → (id, line_count)
    fp_seen: set[str] = set()

    stats = {'quality': 0, 'lang': 0, 'fp_dedup': 0, 'meta_dedup': 0, 'kept': 0}
    t0 = time.time()

    # SQLプレフィルタ: duration < 60 を除外
    cursor = src.execute(
        'SELECT id, track_name, artist_name, album_name, duration, synced_lyrics '
        'FROM lyrics WHERE duration IS NULL OR duration >= 60'
    )

    dst.execute('BEGIN')
    for i, (rid, track, artist, album, dur, lyrics) in enumerate(cursor):
        if (i + 1) % 500_000 == 0:
            elapsed = time.time() - t0
            print(f"  {i+1:>10,} / {total:,}  {i+1/elapsed:.0f}/s  kept={stats['kept']:,}")

        # 1. 品質チェック (最も軽い処理を先に)
        if lyrics.count('\n') < 9:
            stats['quality'] += 1
            continue
        text_no_ts = _RE_TS.sub('', lyrics)
        if len(text_no_ts.strip()) < 100:
            stats['quality'] += 1
            continue

        # 2. 言語判定 (早期リターンで高速)
        lang = classify_lang(text_no_ts)
        if lang is None:
            stats['lang'] += 1
            continue

        # 3. 歌詞フィンガープリント dedup (グローバル)
        fp = lyrics_fingerprint(lyrics)
        if fp:
            if fp in fp_seen:
                stats['fp_dedup'] += 1
                continue
            fp_seen.add(fp)

        # 4. メタデータ dedup
        meta_key = (normalize_name(artist), normalize_name(track), round((dur or 0) / 30))
        line_count = lyrics.count('\n') + 1

        if meta_key in meta_seen:
            prev_id, prev_lines = meta_seen[meta_key]
            if line_count <= prev_lines:
                stats['meta_dedup'] += 1
                continue
            dst.execute('DELETE FROM lyrics WHERE id=?', (prev_id,))
            stats['meta_dedup'] += 1
            stats['kept'] -= 1

        meta_seen[meta_key] = (rid, line_count)
        dst.execute('INSERT INTO lyrics VALUES (?,?,?,?,?,?,?)',
                    (rid, track, artist, album, dur, lyrics, lang))
        stats['kept'] += 1

        # 定期コミット (メモリ節約)
        if stats['kept'] % 100_000 == 0:
            dst.execute('COMMIT')
            dst.execute('BEGIN')

    dst.execute('COMMIT')
    elapsed = time.time() - t0

    print(f"\nFiltering done in {elapsed:.1f}s")
    for k, v in stats.items():
        print(f"  {k:>12}: {v:,}")

    # --- インデックス ---
    print("Building indexes...")
    t1 = time.time()
    dst.execute('CREATE INDEX idx_artist_track ON lyrics(artist_name, track_name)')
    dst.execute('CREATE INDEX idx_lang ON lyrics(lang)')
    dst.execute('ANALYZE')
    dst.commit()
    print(f"Indexes in {time.time()-t1:.1f}s")

    for lc in ('ja', 'ko', 'en'):
        cnt = dst.execute('SELECT COUNT(*) FROM lyrics WHERE lang=?', (lc,)).fetchone()[0]
        print(f"  {lc}: {cnt:,}")

    src.close()
    dst.close()
    print("Done.")


if __name__ == '__main__':
    main()
