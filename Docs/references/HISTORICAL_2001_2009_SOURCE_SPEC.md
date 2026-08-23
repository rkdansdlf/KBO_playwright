# 2001–2009 Historical Data Source Specification & Manifest Contract

Last updated: 2026-08-24
Issue reference: [#3 Acquire archive source for 2001-2009 detail and PBP backfill](https://github.com/rkdansdlf/KBO_playwright/issues/3)

---

## 1. Background & Problem Statement

The parent schedule rows for 2001–2009 seasons were backfilled into `game` (504–544 games/season, 4,688 games total). However, boxscore detail, player game stats, and play-by-play (`game_events`) remain incomplete or missing:

- **2001–2007**: Only 126–166 boxscores per season are populated (~25–30% coverage); `game_events` count is 0.
- **2008–2009**: ~231–246 boxscores populated; `game_events` count is 0.
- Public web live scraping endpoints for 2001–2009 are deprecated, return 404, or time out.

To complete backfills safely without corrupting existing databases, external archives (partner export, community curated sets, HTML boxscore dumps) must adhere to this strict manifest and provenance verification contract.

---

## 2. Source Availability Matrix

| Season | Schedule (Games) | Boxscore Target | PBP Source Candidate | Primary Archive Format | Status |
|:---:|:---:|:---:|:---:|:---:|:---:|
| **2001** | 544 | 544 | Legacy Text Relay / News Archive | Structured JSON / HTML Dump | Manifest Spec Defined |
| **2002** | 532 | 532 | Legacy Text Relay / News Archive | Structured JSON / HTML Dump | Manifest Spec Defined |
| **2003** | 532 | 532 | Legacy Text Relay / News Archive | Structured JSON / HTML Dump | Manifest Spec Defined |
| **2004** | 532 | 532 | Legacy Text Relay / News Archive | Structured JSON / HTML Dump | Manifest Spec Defined |
| **2005** | 504 | 504 | Legacy Text Relay / News Archive | Structured JSON / HTML Dump | Manifest Spec Defined |
| **2006** | 504 | 504 | Legacy Text Relay / News Archive | Structured JSON / HTML Dump | Manifest Spec Defined |
| **2007** | 504 | 504 | Legacy Text Relay / News Archive | Structured JSON / HTML Dump | Manifest Spec Defined |
| **2008** | 504 | 504 | Naver / KBO Legacy Boxscore | Structured JSON / HTML Dump | Manifest Spec Defined |
| **2009** | 532 | 532 | Naver / KBO Legacy Boxscore | Structured JSON / HTML Dump | Manifest Spec Defined |

---

## 3. Historical Manifest Specification (`manifest.json`)

All imported historical archive files must be cataloged in a `manifest.json` or `.csv` file.

### Schema:
```json
{
  "manifest_version": "1.0",
  "season": 2001,
  "source_name": "kbo_historical_archive_2001",
  "provenance": {
    "publisher": "KBO / Verified Community Archive",
    "capture_timestamp": "2026-08-24T00:00:00Z",
    "curator": "data-integrity-team"
  },
  "entries": [
    {
      "game_id": "20010405LTHU0",
      "game_date": "2001-04-05",
      "home_team": "HU",
      "away_team": "LT",
      "payload_path": "payloads/20010405LTHU0.json",
      "payload_type": "json_boxscore",
      "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      "status": "ok"
    }
  ]
}
```

### Required Fields:
- `game_id`: Standard 13-character KBO Game ID format (`YYYYMMDD{away}{home}{dh}`).
- `season`: 4-digit integer matching the `game_id` prefix.
- `payload_path`: Relative path from the manifest location to the payload file.
- `payload_type`: One of `json_boxscore`, `html_boxscore`, `normalized_events_json`, `pbp_text`.
- `sha256`: Lowercase hexadecimal SHA-256 hash of the exact payload file.

---

## 4. Backfill Acceptance Gates

Before any historical payload is imported into operational databases:
1. **Checksum & Provenance Gate**: `historical_boxscore_import --dry-run` must verify SHA-256 match and provenance validity (0 checksum failures).
2. **Completeness Gate**: Boxscore must contain both away/home batter rows and away/home pitcher rows.
3. **Score Consistency Gate**: Boxscore batting/pitching sums must reconcile with the recorded final score in the schedule.
4. **Idempotency Gate**: All writes must use UPSERT and produce zero duplicate records.
