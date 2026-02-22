#!/usr/bin/env python3
"""
lrclib フィルタリングスクリプト
34GB raw DB → 重複排除 + 言語フィルタ + 品質フィルタ → 軽量DB

Usage:
    python filter_lyrics.py <input.db> <output.db>
"""

import hashlib
import re
import sqlite3
import sys
import time


# ============================================================
# Stage 1: 重複排除
# ============================================================

def normalize_name(name: str) -> str:
    """曲名/アーティスト名の正規化"""
    name = name.lower()
    name = re.sub(r'[^\w\s]', '', name)                # 記号除去
    name = re.sub(r'\s*[\(\[（【].+?[\)\]）】]', '', name)  # 括弧内除去
    name = re.sub(r'\s*(feat|ft|with)\s+.*$', '', name, flags=re.I)
    return ' '.join(name.split())


def lyrics_fingerprint(synced_lyrics: str) -> str:
    """歌詞テキストのフィンガープリント（合いの手・改行差異を吸収）"""
    text = re.sub(r'\[[\d:.]+\]', '', synced_lyrics)    # タイムスタンプ除去
    text = re.sub(r'[\(\（].*?[\)\）]', '', text)        # 括弧内除去
    text = re.sub(r'[^\w]', '', text).lower()            # 記号除去 + 小文字化
    if not text:
        return ''
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


# ============================================================
# Stage 2: 言語フィルタ
# ============================================================

_RE_JA = re.compile(r'[\u3040-\u309F\u30A0-\u30FF]')
_RE_KO = re.compile(r'[\uAC00-\uD7AF]')
_RE_LATIN = re.compile(r'[a-zA-Z\u00C0-\u024F]')
_RE_EXCLUDE = re.compile(r'[\u0600-\u06FF\u0400-\u04FF\u0900-\u097F\u0E00-\u0E7F]')


def classify_lang(lyrics: str) -> str | None:
    """歌詞テキストから言語分類。None = 除外対象"""
    text = re.sub(r'\[[\d:.]+\]', '', lyrics)

    ja_count = len(_RE_JA.findall(text))
    ko_count = len(_RE_KO.findall(text))
    latin_count = len(_RE_LATIN.findall(text))
    exclude_count = len(_RE_EXCLUDE.findall(text))

    if exclude_count > 50 and exclude_count > latin_count:
        return None
    if ja_count >= 10:
        return 'ja'
    if ko_count >= 10:
        return 'ko'
    if latin_count >= 30:
        return 'en'
    return None


# ============================================================
# Stage 3: 品質フィルタ
# ============================================================

def passes_quality(synced_lyrics: str, duration: float | None) -> bool:
    """品質フィルタ: 低品質データを除去"""
    lines = [l for l in synced_lyrics.strip().split('\n') if l.strip()]
    if len(lines) < 10:
        return False
    text = re.sub(r'\[[\d:.]+\]', '', synced_lyrics)
    if len(text.strip()) < 100:
        return False
    if duration is not None and duration < 60:
        return False
    return True


# ============================================================
# メインパイプライン
# ============================================================

def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.db> <output.db>", file=sys.stderr)
        sys.exit(1)

    input_db = sys.argv[1]
    output_db = sys.argv[2]

    print(f"Input:  {input_db}")
    print(f"Output: {output_db}")

    # --- 入力DB接続 ---
    src = sqlite3.connect(input_db)
    src.execute('PRAGMA mmap_size=4294967296;')
    src.execute('PRAGMA cache_size=-1000000;')

    total = src.execute('SELECT COUNT(*) FROM lyrics').fetchone()[0]
    print(f"Total input records: {total:,}")

    # --- 出力DB作成 ---
    dst = sqlite3.connect(output_db)
    dst.execute('PRAGMA synchronous=OFF;')
    dst.execute('PRAGMA journal_mode=OFF;')
    dst.execute('PRAGMA cache_size=-500000;')
    dst.execute('''
        CREATE TABLE lyrics (
            id INTEGER PRIMARY KEY,
            track_name TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            album_name TEXT,
            duration REAL,
            synced_lyrics TEXT NOT NULL,
            lang TEXT NOT NULL
        )
    ''')

    # --- フィルタリング ---
    # Layer 1: メタデータ dedup → (key) → (id, lyrics_line_count)
    # Layer 2: 歌詞フィンガープリント dedup → fingerprint → id
    meta_seen: dict[tuple, tuple[int, int]] = {}   # key → (id, line_count)
    fp_seen: dict[str, int] = {}                    # fingerprint → id

    stats = {
        'lang_excluded': 0,
        'quality_excluded': 0,
        'meta_dedup': 0,
        'fp_dedup': 0,
        'kept': 0,
    }

    t0 = time.time()
    cursor = src.execute(
        'SELECT id, track_name, artist_name, album_name, duration, synced_lyrics FROM lyrics'
    )

    batch = []
    BATCH_SIZE = 50_000

    for i, (rid, track, artist, album, dur, lyrics) in enumerate(cursor):
        if (i + 1) % 500_000 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            print(f"  {i + 1:>12,} / {total:,}  ({rate:.0f} rows/s)  kept={stats['kept']:,}")

        # Stage 2: 言語フィルタ
        lang = classify_lang(lyrics)
        if lang is None:
            stats['lang_excluded'] += 1
            continue

        # Stage 3: 品質フィルタ
        if not passes_quality(lyrics, dur):
            stats['quality_excluded'] += 1
            continue

        # Stage 1 Layer 2: 歌詞フィンガープリント dedup (グローバル)
        fp = lyrics_fingerprint(lyrics)
        if fp and fp in fp_seen:
            stats['fp_dedup'] += 1
            continue
        if fp:
            fp_seen[fp] = rid

        # Stage 1 Layer 1: メタデータ dedup
        meta_key = (
            normalize_name(artist),
            normalize_name(track),
            round((dur or 0) / 30),
        )
        line_count = lyrics.count('\n') + 1

        if meta_key in meta_seen:
            prev_id, prev_lines = meta_seen[meta_key]
            if line_count <= prev_lines:
                stats['meta_dedup'] += 1
                continue
            # 新しい方が行数多い → 古い方を削除して差し替え
            batch.append(('DELETE', prev_id))
            stats['meta_dedup'] += 1
            stats['kept'] -= 1

        meta_seen[meta_key] = (rid, line_count)
        batch.append(('INSERT', (rid, track, artist, album, dur, lyrics, lang)))
        stats['kept'] += 1

        # バッチ書き込み
        if len(batch) >= BATCH_SIZE:
            _flush_batch(dst, batch)
            batch.clear()

    # 残りフラッシュ
    if batch:
        _flush_batch(dst, batch)
        batch.clear()

    elapsed = time.time() - t0
    print(f"\nFiltering completed in {elapsed:.1f}s")
    print(f"  Total input:      {total:,}")
    print(f"  Lang excluded:    {stats['lang_excluded']:,}")
    print(f"  Quality excluded: {stats['quality_excluded']:,}")
    print(f"  Meta dedup:       {stats['meta_dedup']:,}")
    print(f"  FP dedup:         {stats['fp_dedup']:,}")
    print(f"  Kept:             {stats['kept']:,}")

    # --- インデックス ---
    print("Building indexes...")
    t1 = time.time()
    dst.execute('CREATE INDEX idx_artist_track ON lyrics(artist_name, track_name)')
    dst.execute('CREATE INDEX idx_lang ON lyrics(lang)')
    dst.execute('ANALYZE')
    dst.commit()
    print(f"Indexes built in {time.time() - t1:.1f}s")

    # --- 統計出力 ---
    for lang_code in ('ja', 'ko', 'en'):
        cnt = dst.execute('SELECT COUNT(*) FROM lyrics WHERE lang=?', (lang_code,)).fetchone()[0]
        print(f"  {lang_code}: {cnt:,}")

    src.close()
    dst.close()
    print("Done.")


def _flush_batch(dst: sqlite3.Connection, batch: list):
    """バッチをDBに書き込む"""
    dst.execute('BEGIN')
    for op in batch:
        if op[0] == 'DELETE':
            dst.execute('DELETE FROM lyrics WHERE id=?', (op[1],))
        else:
            dst.execute(
                'INSERT INTO lyrics (id, track_name, artist_name, album_name, duration, synced_lyrics, lang)'
                ' VALUES (?,?,?,?,?,?,?)',
                op[1],
            )
    dst.execute('COMMIT')


if __name__ == '__main__':
    main()
