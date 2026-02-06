# Notion Database Duplicate

Notion 데이터베이스를 로컬 dump 파일로 저장한 뒤, 다른 API key(대상 워크스페이스)로 업로드하는 CLI 입니다.

핵심 방식:
- `dump`: source key로 데이터베이스/데이터를 로컬 JSON 파일로 저장
- `upload`: dump 파일을 destination key로 업로드
- `run`: dump + upload를 한 번에 실행(중간에 로컬 파일 생성됨)

## Requirements

- Python 3.9+
- source Notion API key
- destination Notion API key
- destination parent page ID (새 DB 생성 위치)

```bash
python3 -m pip install -r requirements.txt
```

## Dump Format

`--dump-dir` 아래에 다음 구조로 생성됩니다.

- `manifest.json`
- `databases/<source_database_id>.json`

`manifest.json`에는 dump 메타정보와 파일 인덱스가 들어갑니다.

## Commands

### 1) dump

특정 DB 목록 dump:

```bash
python3 src/notion_db_duplicate.py dump \
  --src-key "secret_source_key" \
  --database-id "db_id_1" \
  --database-id "db_id_2" \
  --copy-data yes \
  --dump-dir "./notion-dump"
```

source key로 접근 가능한 모든 DB dump:

```bash
python3 src/notion_db_duplicate.py dump \
  --src-key "secret_source_key" \
  --src-all-databases yes \
  --copy-data yes \
  --dump-dir "./notion-dump"
```

### 2) upload

로컬 dump를 destination으로 업로드:

```bash
python3 src/notion_db_duplicate.py upload \
  --dst-key "secret_destination_key" \
  --dst-parent-page-id "destination_parent_page_id" \
  --copy-data yes \
  --dump-dir "./notion-dump"
```

### 3) run

dump + upload를 한 번에 실행(중간에 로컬 dump 파일 생성):

```bash
python3 src/notion_db_duplicate.py run \
  --src-key "secret_source_key" \
  --dst-key "secret_destination_key" \
  --database-id "db_id_1,db_id_2" \
  --copy-data yes \
  --dst-parent-page-id "destination_parent_page_id" \
  --dump-dir "./notion-dump"
```

전체 DB 자동 발견 + run:

```bash
python3 src/notion_db_duplicate.py run \
  --src-key "secret_source_key" \
  --dst-key "secret_destination_key" \
  --src-all-databases yes \
  --copy-data yes \
  --dst-parent-page-id "destination_parent_page_id" \
  --dump-dir "./notion-dump"
```

## Notes / Limitations

- `--copy-data no`면 스키마만 dump/upload 됩니다.
- system 속성(생성일/생성자 등)은 Notion 제약상 그대로 복원되지 않습니다.
- 파일 속성은 external URL만 복사합니다(Notion-hosted 파일은 제외).
- relation은 페이지 생성 후 2차 업데이트로 연결합니다.
- 복잡한 formula/relation/rollup 일부는 권한/Notion 제약으로 경고가 날 수 있습니다.
