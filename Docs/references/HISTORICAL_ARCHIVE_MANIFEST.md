# Historical Archive Manifest

Historical archive ingestion for seasons 1982 through 2000 requires a JSON
manifest. The manifest binds the archive file to its source, authorization
record, season, and SHA-256 digest.

## Required Fields

```json
{
  "source_name": "kbo_official_archive",
  "source_url": "https://approved.example/archive/1982.json",
  "authorization_ref": "approval-ticket-or-license-reference",
  "sha256": "64-character-lowercase-or-uppercase-hex-digest",
  "season": 1982
}
```

`authorization_ref` must identify a recorded permission or license decision.
Do not use placeholders such as `unknown`, and do not commit credentials or
private access tokens.

## Validation

The CLI verifies:

- The manifest is a JSON object with every required field.
- The manifest season matches `--season`.
- The archive SHA-256 matches `sha256`.
- Every archive game has a unique `game_id` beginning with the target season.
- A 1982-2000 write includes verified provenance.

Use dry-run before any write:

```bash
python3 -m src.cli.ingest_historical_archive \
  --file data/archives/kbo_1982.json \
  --manifest data/archives/kbo_1982.manifest.json \
  --season 1982 \
  --dry-run --json
```

Only run without `--dry-run` after the source authorization and checksum have
been reviewed. The ingestor stores the verified manifest fields in each
ingested game's `game_metadata.source_payload.provenance` object.
