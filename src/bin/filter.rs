// lrclib フィルタ: 34GB raw DB → 重複排除 + 言語フィルタ + 品質フィルタ → 軽量 DB
//
// 1パス・ストリーミング処理:
//   1. 品質チェック (行数 >= 10, 文字数 >= 100)
//   2. 言語判定 (ja/ko/en のみ, 1パス走査 + 早期リターン)
//   3. 歌詞フィンガープリント dedup (MD5, グローバル)
//   4. メタデータ dedup (正規化済み artist+track+duration)
//
// Usage: filter-lrclib <input.db> <output.db>

use md5::{Digest, Md5};
use regex::Regex;
use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};
use std::env;
use std::time::Instant;

// ============================================================
// 言語判定
// ============================================================

fn classify_lang(text_no_ts: &str) -> Option<&'static str> {
    // NOTE: len() はバイト長。CJKテキストでは1文字3バイトなので
    // 30バイト ≈ 10文字。Python版の30文字より緩いが、短すぎるテキストの
    // 除外が目的なので問題ない。
    if text_no_ts.len() < 30 {
        return None;
    }
    let (mut ja, mut ko, mut latin, mut exclude) = (0u32, 0u32, 0u32, 0u32);
    for c in text_no_ts.chars() {
        let cp = c as u32;
        if (0x3040..=0x309F).contains(&cp) || (0x30A0..=0x30FF).contains(&cp) {
            ja += 1;
            if ja >= 10 { return Some("ja"); }
        } else if (0xAC00..=0xD7AF).contains(&cp) {
            ko += 1;
            if ko >= 10 { return Some("ko"); }
        } else if (0x41..=0x5A).contains(&cp) || (0x61..=0x7A).contains(&cp)
                || (0xC0..=0x24F).contains(&cp) {
            latin += 1;
        } else if (0x600..=0x6FF).contains(&cp) || (0x400..=0x4FF).contains(&cp)
                || (0x900..=0x97F).contains(&cp) || (0xE00..=0xE7F).contains(&cp) {
            exclude += 1;
            if exclude > 50 { return None; }
        }
    }
    if exclude > 50 && exclude > latin { return None; }
    if latin >= 30 { return Some("en"); }
    None
}

// ============================================================
// 正規化
// ============================================================

fn normalize_name(name: &str, re_symbol: &Regex, re_bracket: &Regex, re_feat: &Regex) -> String {
    let n = name.to_lowercase();
    // bracket除去を先に (re_symbolが括弧を消す前に処理する)
    let n = re_bracket.replace_all(&n, "");
    let n = re_feat.replace_all(&n, "");
    let n = re_symbol.replace_all(&n, "");
    n.split_whitespace().collect::<Vec<_>>().join(" ")
}

// ============================================================
// 歌詞フィンガープリント
// ============================================================

/// text_no_ts: タイムスタンプ除去済みのテキスト（品質チェックで既に計算済み）
fn lyrics_fingerprint(text_no_ts: &str, re_paren: &Regex, re_non_word: &Regex) -> Option<[u8; 16]> {
    let text = re_paren.replace_all(text_no_ts, "");
    let text = re_non_word.replace_all(&text, "");
    let text = text.to_lowercase();
    if text.is_empty() { return None; }
    let mut hasher = Md5::new();
    hasher.update(text.as_bytes());
    Some(hasher.finalize().into())
}

// ============================================================
// メイン
// ============================================================

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <input.db> <output.db>", args[0]);
        std::process::exit(1);
    }
    let input_db = &args[1];
    let output_db = &args[2];

    eprintln!("Input:  {}", input_db);
    eprintln!("Output: {}", output_db);

    // プリコンパイル正規表現
    let re_ts = Regex::new(r"\[[\d:.]+\]").unwrap();
    let re_paren = Regex::new(r"[(（][^()（）]*[)）]").unwrap();
    let re_non_word = Regex::new(r"[^\w]").unwrap();
    let re_symbol = Regex::new(r"[^\w\s]").unwrap();
    let re_bracket = Regex::new(r"\s*[(（\[【].+?[)）\]】]").unwrap();
    let re_feat = Regex::new(r"(?i)\s*(feat|ft|with)\s+.*$").unwrap();

    // 入力DB
    let src = Connection::open(input_db).expect("Failed to open input DB");
    src.execute_batch("PRAGMA mmap_size=4294967296; PRAGMA cache_size=-1000000;").unwrap();
    let total: i64 = src.query_row("SELECT COUNT(*) FROM lyrics", [], |r| r.get(0)).unwrap();
    eprintln!("Total records: {}", total);

    // 出力DB
    let dst = Connection::open(output_db).expect("Failed to open output DB");
    dst.execute_batch(
        "PRAGMA synchronous=OFF; PRAGMA journal_mode=OFF; PRAGMA cache_size=-500000;
         CREATE TABLE lyrics (
             id INTEGER PRIMARY KEY, track_name TEXT NOT NULL,
             artist_name TEXT NOT NULL, album_name TEXT,
             duration REAL, synced_lyrics TEXT NOT NULL, lang TEXT NOT NULL
         );"
    ).unwrap();

    // dedup 用データ構造
    let mut fp_seen: HashSet<[u8; 16]> = HashSet::with_capacity(5_000_000);
    // meta_key → (id, line_count)
    let mut meta_seen: HashMap<String, (i64, usize)> = HashMap::with_capacity(5_000_000);

    let mut stats = [0u64; 5]; // quality, lang, fp_dedup, meta_dedup, kept
    let t0 = Instant::now();

    // SQL プレフィルタ (duration >= 60)
    let mut stmt = src.prepare(
        "SELECT id, track_name, artist_name, album_name, duration, synced_lyrics \
         FROM lyrics WHERE duration IS NULL OR duration >= 60"
    ).unwrap();

    let mut rows = stmt.query([]).unwrap();
    let mut i: u64 = 0;

    dst.execute_batch("BEGIN").unwrap();

    while let Some(row) = rows.next().unwrap() {
        let rid: i64 = row.get(0).unwrap();
        let track: String = row.get(1).unwrap();
        let artist: String = row.get(2).unwrap();
        let album: Option<String> = row.get(3).unwrap();
        let dur: Option<f64> = row.get(4).unwrap();
        let lyrics: String = row.get(5).unwrap();

        i += 1;
        if i % 500_000 == 0 {
            let elapsed = t0.elapsed().as_secs_f64();
            eprintln!("  {:>10} / {}  {:.0}/s  kept={}", i, total, i as f64 / elapsed, stats[4]);
        }

        // 1. 品質チェック
        let line_count = lyrics.matches('\n').count() + 1;
        if line_count < 10 {
            stats[0] += 1; continue;
        }
        let text_no_ts = re_ts.replace_all(&lyrics, "");
        if text_no_ts.trim().len() < 100 {
            stats[0] += 1; continue;
        }

        // 2. 言語判定
        let lang = match classify_lang(&text_no_ts) {
            Some(l) => l,
            None => { stats[1] += 1; continue; }
        };

        // 3. 歌詞フィンガープリント dedup (text_no_ts を再利用して re_ts 二重実行を回避)
        if let Some(fp) = lyrics_fingerprint(&text_no_ts, &re_paren, &re_non_word) {
            if !fp_seen.insert(fp) {
                stats[2] += 1; continue;
            }
        }

        // 4. メタデータ dedup
        // duration バケット: 10秒単位 (±5秒以内を同一曲とみなす)
        let meta_key = format!("{}\t{}\t{}",
            normalize_name(&artist, &re_symbol, &re_bracket, &re_feat),
            normalize_name(&track, &re_symbol, &re_bracket, &re_feat),
            ((dur.unwrap_or(0.0) / 10.0).round() as i64),
        );

        if let Some((prev_id, prev_lines)) = meta_seen.get(&meta_key) {
            if line_count <= *prev_lines {
                stats[3] += 1; continue;
            }
            dst.execute("DELETE FROM lyrics WHERE id=?", params![*prev_id]).unwrap();
            stats[3] += 1;
            stats[4] -= 1;
        }
        meta_seen.insert(meta_key, (rid, line_count));

        dst.execute(
            "INSERT INTO lyrics VALUES (?,?,?,?,?,?,?)",
            params![rid, track, artist, album, dur, lyrics, lang],
        ).unwrap();
        stats[4] += 1;

        // 定期コミット
        if stats[4] % 100_000 == 0 {
            dst.execute_batch("COMMIT; BEGIN").unwrap();
        }
    }

    dst.execute_batch("COMMIT").unwrap();
    let elapsed = t0.elapsed().as_secs_f64();

    eprintln!("\nFiltering done in {:.1}s", elapsed);
    eprintln!("  quality:    {}", stats[0]);
    eprintln!("  lang:       {}", stats[1]);
    eprintln!("  fp_dedup:   {}", stats[2]);
    eprintln!("  meta_dedup: {}", stats[3]);
    eprintln!("  kept:       {}", stats[4]);

    // インデックス
    eprintln!("Building indexes...");
    let t1 = Instant::now();
    dst.execute_batch(
        "CREATE INDEX idx_artist_track ON lyrics(artist_name, track_name);
         CREATE INDEX idx_lang ON lyrics(lang);
         ANALYZE;"
    ).unwrap();
    eprintln!("Indexes in {:.1}s", t1.elapsed().as_secs_f64());

    for lc in ["ja", "ko", "en"] {
        let cnt: i64 = dst.query_row(
            "SELECT COUNT(*) FROM lyrics WHERE lang=?", params![lc], |r| r.get(0)
        ).unwrap();
        eprintln!("  {}: {}", lc, cnt);
    }

    eprintln!("Done.");
}
