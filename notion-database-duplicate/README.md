# Notion Database Duplicate

Notion 데이터베이스를 로컬 dump 파일로 저장한 뒤, 다른 API key(대상 워크스페이스)로 업로드하는 CLI 입니다.

## 핵심 기능

- `dump`: source key로 데이터베이스/데이터를 로컬 JSON 파일로 저장
- `upload`: dump 파일을 destination key로 업로드
- `repair`: 업로드 후 중복 릴레이션 속성 정리
- `run`: dump + upload + repair를 한 번에 실행 (중간에 로컬 파일 생성됨)

## Requirements

- Python 3.9+
- source Notion API key
- destination Notion API key
- destination parent page ID (새 DB 생성 위치)

```bash
pip install -r requirements.txt
```

## Dump Format

`--dump-dir` 아래에 다음 구조로 생성됩니다.

```
dump-dir/
├── manifest.json
└── databases/
    ├── <source_database_id_1>.json
    └── <source_database_id_2>.json
```

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

### 3) repair

업로드 후 중복 릴레이션 속성 정리:

```bash
python3 src/notion_db_duplicate.py repair \
  --api-key "secret_destination_key" \
  --parent-page-id "destination_parent_page_id" \
  --dump-dir "./notion-dump"
```

**사용 시점**: `upload` 후 자동 생성된 "Related to X (Y)" 형태의 중복 릴레이션 속성을 삭제합니다. 원본 속성명이 이미 존재하면 자동 생성된 중복을 제거합니다.

### 4) run

dump + upload + repair를 한 번에 실행:

```bash
python3 src/notion_db_duplicate.py run \
  --src-key "secret_source_key" \
  --dst-key "secret_destination_key" \
  --src-all-databases yes \
  --copy-data no \
  --dst-parent-page-id "destination_parent_page_id" \
  --dump-dir "./notion-dump" \
  --auto-repair yes
```

`--auto-repair yes` (기본값)이면 업로드 후 자동으로 중복 릴레이션을 정리합니다.

## Options

### Common Options

| Option | Description |
|--------|-------------|
| `--timeout` | HTTP timeout in seconds (default: 30) |

### dump Options

| Option | Description |
|--------|-------------|
| `--src-key` | Source Notion API key |
| `--database-id` | Source database ID (repeatable) |
| `--src-all-databases` | Discover all accessible databases (yes/no) |
| `--copy-data` | Include row data in dump (yes/no) |
| `--dump-dir` | Local directory for dump files |

### upload Options

| Option | Description |
|--------|-------------|
| `--dst-key` | Destination Notion API key |
| `--dst-parent-page-id` | Parent page ID for new databases |
| `--copy-data` | Upload row data from dump (yes/no) |
| `--dump-dir` | Local dump directory |

### repair Options

| Option | Description |
|--------|-------------|
| `--api-key` | Notion API key for workspace to repair |
| `--parent-page-id` | Parent page ID containing databases |
| `--dump-dir` | Dump directory for reference |

### run Options

| Option | Description |
|--------|-------------|
| `--auto-repair` | Auto-repair duplicate relations (yes/no, default: yes) |

## Notes / Limitations

- `--copy-data no`면 스키마만 dump/upload 됩니다.
- system 속성(생성일/생성자 등)은 Notion 제약상 그대로 복원되지 않습니다.
- 파일 속성은 external URL만 복사합니다 (Notion-hosted 파일은 제외).
- relation은 페이지 생성 후 2차 업데이트로 연결합니다.
- select/multi_select 옵션은 100개까지만 복사됩니다 (Notion API 제한).
- rollup/formula는 일부 복잡한 경우 경고가 날 수 있습니다.
- 릴레이션은 single_property로 생성하여 자동 역방향 생성을 방지합니다.

## Troubleshooting

### 중복 "Related to..." 속성이 생긴 경우

`repair` 명령어로 정리할 수 있습니다:

```bash
python3 src/notion_db_duplicate.py repair \
  --api-key "your_api_key" \
  --parent-page-id "parent_page_id" \
  --dump-dir "./notion-dump"
```

### multi_select 옵션이 100개 초과인 경우

Notion API는 select/multi_select 옵션을 100개까지만 허용합니다. 초과 옵션은 자동으로 잘립니다.

## Security

⚠️ **API 키를 소스코드에 포함하지 마세요!** 항상 커맨드라인 인자로 전달하세요.
