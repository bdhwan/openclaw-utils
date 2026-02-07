#!/usr/bin/env python3
"""
Dump and upload Notion databases between integrations.

Features:
- Dump: Export database schemas and data to local JSON files
- Upload: Import dump files to another workspace
- Repair: Fix duplicate relation properties after upload
- Run: Dump + Upload in one command
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
MAX_RETRIES = 5
DUMP_FORMAT_VERSION = 1


class NotionAPIError(RuntimeError):
    pass


class DumpFormatError(RuntimeError):
    pass


class NotionClient:
    def __init__(self, api_key: str, notion_version: str = NOTION_VERSION, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Notion-Version": notion_version,
                "Content-Type": "application/json",
            }
        )

    def request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{NOTION_API_BASE}{path}"
        for attempt in range(1, MAX_RETRIES + 1):
            response = self.session.request(
                method=method,
                url=url,
                json=payload,
                params=params,
                timeout=self.timeout,
            )
            if response.status_code == 429 and attempt < MAX_RETRIES:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else attempt
                time.sleep(wait_seconds)
                continue

            if response.status_code >= 400:
                try:
                    error_body = response.json()
                except ValueError:
                    error_body = {"error": response.text}
                raise NotionAPIError(
                    f"{method} {path} failed ({response.status_code}): {json.dumps(error_body)}"
                )

            if response.status_code == 204:
                return {}

            return response.json()

        raise NotionAPIError(f"{method} {path} exceeded retry attempts")

    def get_database(self, database_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/databases/{database_id}")

    def create_database(
        self,
        parent_page_id: str,
        title: List[Dict[str, Any]],
        properties: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        payload = {
            "parent": {"type": "page_id", "page_id": parent_page_id},
            "title": title,
            "properties": properties,
        }
        return self.request("POST", "/databases", payload=payload)

    def update_database(self, database_id: str, properties: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        return self.request("PATCH", f"/databases/{database_id}", payload={"properties": properties})

    def query_database(self, database_id: str) -> Iterable[Dict[str, Any]]:
        cursor: Optional[str] = None
        while True:
            payload: Dict[str, Any] = {"page_size": 100}
            if cursor:
                payload["start_cursor"] = cursor
            response = self.request("POST", f"/databases/{database_id}/query", payload=payload)
            for result in response.get("results", []):
                yield result
            if not response.get("has_more"):
                break
            cursor = response.get("next_cursor")

    def search_databases(self) -> Iterable[Dict[str, Any]]:
        cursor: Optional[str] = None
        while True:
            payload: Dict[str, Any] = {
                "page_size": 100,
                "filter": {"value": "database", "property": "object"},
            }
            if cursor:
                payload["start_cursor"] = cursor
            response = self.request("POST", "/search", payload=payload)
            for result in response.get("results", []):
                if result.get("object") == "database":
                    yield result
            if not response.get("has_more"):
                break
            cursor = response.get("next_cursor")

    def get_block_children(self, block_id: str) -> Iterable[Dict[str, Any]]:
        """Get all children blocks of a page/block."""
        cursor: Optional[str] = None
        while True:
            params: Dict[str, Any] = {"page_size": 100}
            if cursor:
                params["start_cursor"] = cursor
            response = self.request("GET", f"/blocks/{block_id}/children", params=params)
            for result in response.get("results", []):
                yield result
            if not response.get("has_more"):
                break
            cursor = response.get("next_cursor")

    def create_page(
        self,
        database_id: str,
        properties: Dict[str, Any],
        icon: Optional[Dict[str, Any]] = None,
        cover: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "parent": {"database_id": database_id},
            "properties": properties,
        }
        if icon:
            payload["icon"] = icon
        if cover:
            payload["cover"] = cover
        return self.request("POST", "/pages", payload=payload)

    def update_page(self, page_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        return self.request("PATCH", f"/pages/{page_id}", payload={"properties": properties})


def parse_database_ids(raw_items: List[str]) -> List[str]:
    ids: List[str] = []
    for raw in raw_items:
        for part in raw.split(","):
            database_id = part.strip()
            if database_id:
                ids.append(database_id)
    unique_ids = list(dict.fromkeys(ids))
    if not unique_ids:
        raise ValueError("At least one database ID must be provided.")
    return unique_ids


def resolve_source_database_ids(
    source_client: NotionClient, manual_database_ids: List[str], include_all_accessible: bool
) -> List[str]:
    resolved_ids = list(manual_database_ids)

    if include_all_accessible:
        print("Discovering source databases accessible by source API key...")
        discovered_ids: List[str] = []
        for database in source_client.search_databases():
            database_id = database.get("id")
            if database_id:
                discovered_ids.append(database_id)
        print(f"  Discovered {len(discovered_ids)} databases")
        resolved_ids.extend(discovered_ids)

    unique_ids = list(dict.fromkeys(resolved_ids))
    if not unique_ids:
        raise ValueError("Provide --database-id or set --src-all-databases yes.")
    return unique_ids


def rich_text_from_plain_text(text: str) -> List[Dict[str, Any]]:
    return [{"type": "text", "text": {"content": text}}]


def sanitize_rich_text_item(item: Dict[str, Any]) -> Dict[str, Any]:
    item_type = item.get("type")
    if item_type == "text":
        text_obj = item.get("text", {})
        safe_text: Dict[str, Any] = {"content": text_obj.get("content", item.get("plain_text", ""))}
        if text_obj.get("link"):
            safe_text["link"] = text_obj["link"]
        sanitized = {"type": "text", "text": safe_text}
    elif item_type == "equation":
        expression = item.get("equation", {}).get("expression", "")
        sanitized = {"type": "equation", "equation": {"expression": expression}}
    else:
        sanitized = {"type": "text", "text": {"content": item.get("plain_text", "")}}

    if "annotations" in item:
        sanitized["annotations"] = item["annotations"]
    return sanitized


def sanitize_rich_text_array(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [sanitize_rich_text_item(item) for item in items]


def sanitize_database_title(source_db: Dict[str, Any]) -> List[Dict[str, Any]]:
    source_title = source_db.get("title", [])
    clean_title = sanitize_rich_text_array(source_title)
    if clean_title:
        return clean_title
    fallback = source_db.get("id", "Duplicated Database")
    return rich_text_from_plain_text(f"Duplicated {fallback}")


def get_database_title_plain(db: Dict[str, Any]) -> str:
    """Extract plain text title from database object."""
    title_arr = db.get("title", [])
    if title_arr and len(title_arr) > 0:
        return title_arr[0].get("plain_text", "")
    return ""


def sanitize_select_options(options: List[Dict[str, Any]], max_options: int = 100) -> List[Dict[str, str]]:
    """Sanitize select options, limiting to max_options to avoid Notion API limit."""
    sanitized: List[Dict[str, str]] = []
    for option in options[:max_options]:  # Limit to max_options
        name = option.get("name")
        if not name:
            continue
        sanitized.append({"name": name, "color": option.get("color", "default")})
    return sanitized


def property_schema_config(
    source_property: Dict[str, Any], source_to_dest_db_map: Dict[str, str]
) -> Optional[Tuple[str, Dict[str, Any], Optional[str]]]:
    prop_type = source_property.get("type")
    if not prop_type:
        return None

    if prop_type in {"title", "rich_text", "date", "people", "files", "checkbox", "url", "email", "phone_number"}:
        return prop_type, {}, None

    if prop_type == "number":
        config = {"format": source_property.get("number", {}).get("format", "number")}
        return prop_type, config, None

    if prop_type == "select":
        options = sanitize_select_options(source_property.get("select", {}).get("options", []))
        return prop_type, {"options": options}, None

    if prop_type == "multi_select":
        options = sanitize_select_options(source_property.get("multi_select", {}).get("options", []))
        return prop_type, {"options": options}, None

    if prop_type == "status":
        return prop_type, {}, None

    if prop_type == "formula":
        expression = source_property.get("formula", {}).get("expression")
        if not expression:
            return None
        return prop_type, {"expression": expression}, "formula"

    if prop_type == "relation":
        relation = source_property.get("relation", {})
        source_relation_db_id = relation.get("database_id")
        if not source_relation_db_id:
            return None
        destination_relation_db_id = source_to_dest_db_map.get(source_relation_db_id, source_relation_db_id)
        relation_type = relation.get("type", "single_property")
        relation_config: Dict[str, Any] = {"database_id": destination_relation_db_id}
        
        # Use single_property to avoid auto-generated reverse relations
        # We'll create the reverse relation explicitly with the correct name
        relation_config["single_property"] = {}
        
        return prop_type, relation_config, "relation"

    if prop_type == "rollup":
        rollup = source_property.get("rollup", {})
        relation_property_name = rollup.get("relation_property_name")
        rollup_property_name = rollup.get("rollup_property_name")
        function = rollup.get("function")
        if not relation_property_name or not rollup_property_name or not function:
            return None
        config = {
            "relation_property_name": relation_property_name,
            "rollup_property_name": rollup_property_name,
            "function": function,
        }
        return prop_type, config, "rollup"

    return None


def build_database_properties(
    source_properties: Dict[str, Dict[str, Any]],
    source_to_dest_db_map: Dict[str, str],
    defer_complex: bool,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], List[str]]:
    immediate_properties: Dict[str, Dict[str, Any]] = {}
    deferred_properties: Dict[str, Dict[str, Any]] = {}
    warnings: List[str] = []

    for property_name, source_property in source_properties.items():
        result = property_schema_config(source_property, source_to_dest_db_map)
        if not result:
            warnings.append(f"Skipped unsupported schema property '{property_name}' ({source_property.get('type')}).")
            continue

        prop_type, config, marker = result
        payload = {prop_type: config}
        if defer_complex and marker in {"formula", "relation", "rollup"}:
            deferred_properties[property_name] = payload
        else:
            immediate_properties[property_name] = payload

    has_title = any("title" in prop for prop in immediate_properties.values()) or any(
        "title" in prop for prop in deferred_properties.values()
    )
    if not has_title:
        immediate_properties["Name"] = {"title": {}}
        warnings.append("Source database had no valid title property; added fallback 'Name'.")

    return immediate_properties, deferred_properties, warnings


def sanitize_icon(icon_obj: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not icon_obj:
        return None
    icon_type = icon_obj.get("type")
    if icon_type == "emoji":
        return {"type": "emoji", "emoji": icon_obj.get("emoji")}
    if icon_type == "external":
        external = icon_obj.get("external", {})
        if external.get("url"):
            return {"type": "external", "external": {"url": external["url"]}}
    return None


def sanitize_cover(cover_obj: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not cover_obj:
        return None
    cover_type = cover_obj.get("type")
    if cover_type == "external":
        external = cover_obj.get("external", {})
        if external.get("url"):
            return {"type": "external", "external": {"url": external["url"]}}
    return None


def sanitize_page_files(files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sanitized: List[Dict[str, Any]] = []
    for file_obj in files:
        if file_obj.get("type") != "external":
            continue
        external = file_obj.get("external", {})
        url = external.get("url")
        if not url:
            continue
        sanitized.append(
            {
                "name": file_obj.get("name", "file"),
                "type": "external",
                "external": {"url": url},
            }
        )
    return sanitized


def build_page_properties_for_create(source_page_properties: Dict[str, Any]) -> Dict[str, Any]:
    writable: Dict[str, Any] = {}
    for property_name, source_property in source_page_properties.items():
        prop_type = source_property.get("type")
        if prop_type == "title":
            rich_text = sanitize_rich_text_array(source_property.get("title", []))
            if not rich_text:
                rich_text = rich_text_from_plain_text("Untitled")
            writable[property_name] = {"title": rich_text}
            continue
        if prop_type == "rich_text":
            writable[property_name] = {"rich_text": sanitize_rich_text_array(source_property.get("rich_text", []))}
            continue
        if prop_type == "number":
            writable[property_name] = {"number": source_property.get("number")}
            continue
        if prop_type == "select":
            value = source_property.get("select")
            writable[property_name] = {"select": {"name": value.get("name")} if value else None}
            continue
        if prop_type == "multi_select":
            values = source_property.get("multi_select", [])
            writable[property_name] = {"multi_select": [{"name": value.get("name")} for value in values if value.get("name")]}
            continue
        if prop_type == "date":
            writable[property_name] = {"date": copy.deepcopy(source_property.get("date"))}
            continue
        if prop_type == "people":
            people = source_property.get("people", [])
            writable[property_name] = {"people": [{"id": person.get("id")} for person in people if person.get("id")]}
            continue
        if prop_type == "files":
            writable[property_name] = {"files": sanitize_page_files(source_property.get("files", []))}
            continue
        if prop_type == "checkbox":
            writable[property_name] = {"checkbox": source_property.get("checkbox", False)}
            continue
        if prop_type == "url":
            writable[property_name] = {"url": source_property.get("url")}
            continue
        if prop_type == "email":
            writable[property_name] = {"email": source_property.get("email")}
            continue
        if prop_type == "phone_number":
            writable[property_name] = {"phone_number": source_property.get("phone_number")}
            continue
        if prop_type == "status":
            status_obj = source_property.get("status")
            writable[property_name] = {"status": {"name": status_obj.get("name")} if status_obj else None}
            continue
    return writable


def extract_relation_properties_for_update(source_page_properties: Dict[str, Any]) -> Dict[str, Any]:
    relation_updates: Dict[str, Any] = {}
    for property_name, source_property in source_page_properties.items():
        if source_property.get("type") != "relation":
            continue
        related_pages = source_property.get("relation", [])
        source_relations = []
        for page_ref in related_pages:
            source_related_page_id = page_ref.get("id")
            if source_related_page_id:
                source_relations.append({"id": source_related_page_id})
        relation_updates[property_name] = {"relation": source_relations}
    return relation_updates


def remap_relation_update(
    relation_update: Dict[str, Any], source_to_destination_page_map: Dict[str, str]
) -> Dict[str, Any]:
    remapped: Dict[str, Any] = {}
    for property_name, property_payload in relation_update.items():
        relation_values = property_payload.get("relation", [])
        mapped_relations = []
        for relation_value in relation_values:
            source_related_id = relation_value.get("id")
            if not source_related_id:
                continue
            destination_related_id = source_to_destination_page_map.get(source_related_id)
            if destination_related_id:
                mapped_relations.append({"id": destination_related_id})
        remapped[property_name] = {"relation": mapped_relations}
    return remapped


def safe_database_filename(database_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", database_id) + ".json"


def ensure_dump_directory(dump_dir: Path) -> Tuple[Path, Path]:
    dump_dir.mkdir(parents=True, exist_ok=True)
    databases_dir = dump_dir / "databases"
    databases_dir.mkdir(parents=True, exist_ok=True)
    return dump_dir, databases_dir


def dump_databases_to_files(
    source_client: NotionClient,
    source_database_ids: List[str],
    include_data: bool,
    dump_dir: Path,
) -> None:
    dump_dir, databases_dir = ensure_dump_directory(dump_dir)
    manifest_path = dump_dir / "manifest.json"
    manifest: Dict[str, Any] = {
        "format_version": DUMP_FORMAT_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "notion_version": NOTION_VERSION,
        "include_data": include_data,
        "databases": [],
    }

    print(f"Writing dump to: {dump_dir}")
    for source_database_id in source_database_ids:
        print(f"  Dumping database schema: {source_database_id}")
        source_db = source_client.get_database(source_database_id)

        pages: List[Dict[str, Any]] = []
        if include_data:
            page_count = 0
            for source_page in source_client.query_database(source_database_id):
                pages.append(
                    {
                        "id": source_page.get("id"),
                        "properties": source_page.get("properties", {}),
                        "icon": source_page.get("icon"),
                        "cover": source_page.get("cover"),
                    }
                )
                page_count += 1
            print(f"    Dumped rows: {page_count}")

        file_name = safe_database_filename(source_database_id)
        file_path = databases_dir / file_name
        payload = {
            "source_database_id": source_database_id,
            "database": source_db,
            "pages": pages,
        }
        file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        manifest["databases"].append(
            {
                "source_database_id": source_database_id,
                "file": f"databases/{file_name}",
                "page_count": len(pages),
            }
        )

    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Dump manifest written: {manifest_path}")


def load_dump(dump_dir: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    manifest_path = dump_dir / "manifest.json"
    if not manifest_path.exists():
        raise DumpFormatError(f"manifest.json not found in dump directory: {dump_dir}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format_version") != DUMP_FORMAT_VERSION:
        raise DumpFormatError(
            f"Unsupported dump format version: {manifest.get('format_version')}, "
            f"expected {DUMP_FORMAT_VERSION}"
        )

    records: List[Dict[str, Any]] = []
    for db_item in manifest.get("databases", []):
        relative_file = db_item.get("file")
        if not relative_file:
            raise DumpFormatError("Invalid manifest: missing databases[].file")
        record_path = dump_dir / relative_file
        if not record_path.exists():
            raise DumpFormatError(f"Database dump file missing: {record_path}")
        records.append(json.loads(record_path.read_text(encoding="utf-8")))

    return manifest, records


def upload_dump_to_destination(
    destination_client: NotionClient,
    destination_parent_page_id: str,
    dump_dir: Path,
    include_data: bool,
) -> None:
    manifest, records = load_dump(dump_dir)
    warnings: List[str] = []

    source_to_destination_db_map: Dict[str, str] = {}
    source_database_ids: List[str] = [record.get("source_database_id") for record in records if record.get("source_database_id")]
    source_databases: Dict[str, Dict[str, Any]] = {}

    for record in records:
        source_database_id = record.get("source_database_id")
        source_db = record.get("database")
        if not source_database_id or not isinstance(source_db, dict):
            warnings.append("Skipped invalid dump record (missing source_database_id/database).")
            continue
        source_databases[source_database_id] = source_db

    print("Creating destination databases from dump...")
    for source_database_id in source_database_ids:
        source_db = source_databases.get(source_database_id)
        if not source_db:
            continue
        immediate_properties, _, property_warnings = build_database_properties(
            source_properties=source_db.get("properties", {}),
            source_to_dest_db_map=source_to_destination_db_map,
            defer_complex=True,
        )
        warnings.extend([f"{source_database_id}: {warning}" for warning in property_warnings])

        destination_db = destination_client.create_database(
            parent_page_id=destination_parent_page_id,
            title=sanitize_database_title(source_db),
            properties=immediate_properties,
        )
        destination_db_id = destination_db.get("id")
        source_to_destination_db_map[source_database_id] = destination_db_id
        print(f"  {source_database_id} -> {destination_db_id}")

    print("Applying deferred complex schema properties...")
    for source_database_id in source_database_ids:
        source_db = source_databases.get(source_database_id)
        destination_db_id = source_to_destination_db_map.get(source_database_id)
        if not source_db or not destination_db_id:
            continue
        _, deferred_properties, _ = build_database_properties(
            source_properties=source_db.get("properties", {}),
            source_to_dest_db_map=source_to_destination_db_map,
            defer_complex=True,
        )
        if not deferred_properties:
            continue
        try:
            destination_client.update_database(destination_db_id, deferred_properties)
            print(f"  Updated complex properties for {destination_db_id}")
        except NotionAPIError as error:
            warnings.append(f"{source_database_id}: Could not apply all complex properties: {error}")

    dump_has_data = bool(manifest.get("include_data"))
    if include_data and not dump_has_data:
        warnings.append("Upload requested data copy, but dump was created without data.")

    if include_data and dump_has_data:
        print("Uploading page data from dump...")
        source_to_destination_page_map: Dict[str, str] = {}
        deferred_page_relations: List[Tuple[str, Dict[str, Any]]] = []

        for record in records:
            source_database_id = record.get("source_database_id")
            destination_database_id = source_to_destination_db_map.get(source_database_id)
            if not source_database_id or not destination_database_id:
                continue
            pages = record.get("pages", [])
            copied_count = 0

            for source_page in pages:
                source_page_properties = source_page.get("properties", {})
                page_properties = build_page_properties_for_create(source_page_properties)
                page_icon = sanitize_icon(source_page.get("icon"))
                page_cover = sanitize_cover(source_page.get("cover"))

                try:
                    destination_page = destination_client.create_page(
                        database_id=destination_database_id,
                        properties=page_properties,
                        icon=page_icon,
                        cover=page_cover,
                    )
                except NotionAPIError as error:
                    warnings.append(f"Page create failed in source DB {source_database_id}: {error}")
                    continue

                source_page_id = source_page.get("id")
                destination_page_id = destination_page.get("id")
                if source_page_id and destination_page_id:
                    source_to_destination_page_map[source_page_id] = destination_page_id
                    relation_update = extract_relation_properties_for_update(source_page_properties)
                    if relation_update:
                        deferred_page_relations.append((destination_page_id, relation_update))
                copied_count += 1

            print(f"  Copied {copied_count}/{len(pages)} pages for {source_database_id}")

        print("Updating relation properties...")
        relation_updates_applied = 0
        for destination_page_id, relation_update in deferred_page_relations:
            remapped_update = remap_relation_update(relation_update, source_to_destination_page_map)
            if not remapped_update:
                continue
            try:
                destination_client.update_page(destination_page_id, remapped_update)
                relation_updates_applied += 1
            except NotionAPIError as error:
                warnings.append(f"Relation update failed for destination page {destination_page_id}: {error}")
        print(f"  Applied relation updates to {relation_updates_applied} pages")

    print("Upload done.")
    print("Database mapping:")
    for source_database_id, destination_db_id in source_to_destination_db_map.items():
        print(f"  {source_database_id} -> {destination_db_id}")
    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"  - {warning}")


def repair_duplicate_relations(
    client: NotionClient,
    parent_page_id: str,
    dump_dir: Path,
) -> None:
    """
    Repair duplicate relation properties created by dual_property auto-generation.
    
    When Notion creates dual_property relations, it auto-generates reverse relations
    with names like "Related to X (Y)". This function removes those duplicates
    if the original property name already exists.
    """
    print("Loading dump for reference...")
    manifest, records = load_dump(dump_dir)
    
    # Build original property names per database title
    original_props_by_title: Dict[str, set] = {}
    for record in records:
        source_db = record.get("database", {})
        title = get_database_title_plain(source_db)
        if title:
            original_props_by_title[title] = set(source_db.get("properties", {}).keys())
    
    print(f"  Loaded {len(original_props_by_title)} database schemas from dump")
    
    # Get all databases under the parent page
    print("Fetching destination databases...")
    dest_dbs: Dict[str, Dict[str, Any]] = {}
    
    for block in client.get_block_children(parent_page_id):
        if block.get("type") == "child_database":
            db_id = block["id"]
            db_data = client.get_database(db_id)
            title = get_database_title_plain(db_data)
            dest_dbs[title] = {
                "id": db_id,
                "properties": db_data.get("properties", {})
            }
    
    print(f"  Found {len(dest_dbs)} destination databases")
    
    # Find and remove duplicate "Related to..." properties
    print("\nRemoving duplicate 'Related to...' properties...")
    total_deleted = 0
    
    for title, dest_info in dest_dbs.items():
        if title not in original_props_by_title:
            continue
        
        original_names = original_props_by_title[title]
        dest_props = dest_info["properties"]
        db_id = dest_info["id"]
        
        # Find "Related to..." properties that are duplicates
        to_delete = []
        for prop_name, prop_val in dest_props.items():
            if not prop_name.startswith("Related to "):
                continue
            if prop_val.get("type") != "relation":
                continue
            
            # Get target database ID
            target_id = prop_val.get("relation", {}).get("database_id", "")
            
            # Check if any original property also points to this target
            for orig_name in original_names:
                if orig_name not in dest_props:
                    continue
                orig_prop = dest_props[orig_name]
                if orig_prop.get("type") != "relation":
                    continue
                orig_target = orig_prop.get("relation", {}).get("database_id", "")
                if orig_target == target_id:
                    to_delete.append(prop_name)
                    break
        
        if to_delete:
            # Delete properties by setting them to None
            updates = {name: None for name in to_delete}
            try:
                client.update_database(db_id, updates)
                print(f"  ✅ [{title}] Deleted {len(to_delete)} duplicate properties:")
                for name in to_delete:
                    print(f"      - {name}")
                total_deleted += len(to_delete)
            except NotionAPIError as error:
                print(f"  ❌ [{title}] Error: {error}")
            
            time.sleep(0.3)  # Rate limiting
    
    print(f"\nRepair complete: Deleted {total_deleted} duplicate properties")


def command_dump(args: argparse.Namespace) -> int:
    source_client = NotionClient(api_key=args.src_key, timeout=args.timeout)

    manual_database_ids: List[str] = []
    if args.database_id:
        try:
            manual_database_ids = parse_database_ids(args.database_id)
        except ValueError as error:
            print(f"Error: {error}")
            return 2

    try:
        database_ids = resolve_source_database_ids(
            source_client=source_client,
            manual_database_ids=manual_database_ids,
            include_all_accessible=(args.src_all_databases == "yes"),
        )
        dump_databases_to_files(
            source_client=source_client,
            source_database_ids=database_ids,
            include_data=(args.copy_data == "yes"),
            dump_dir=Path(args.dump_dir),
        )
        return 0
    except (NotionAPIError, DumpFormatError, requests.RequestException) as error:
        print(f"Error: {error}")
        return 1


def command_upload(args: argparse.Namespace) -> int:
    destination_client = NotionClient(api_key=args.dst_key, timeout=args.timeout)
    try:
        upload_dump_to_destination(
            destination_client=destination_client,
            destination_parent_page_id=args.dst_parent_page_id,
            dump_dir=Path(args.dump_dir),
            include_data=(args.copy_data == "yes"),
        )
        return 0
    except (NotionAPIError, DumpFormatError, requests.RequestException) as error:
        print(f"Error: {error}")
        return 1


def command_repair(args: argparse.Namespace) -> int:
    """Repair duplicate relation properties after upload."""
    client = NotionClient(api_key=args.api_key, timeout=args.timeout)
    try:
        repair_duplicate_relations(
            client=client,
            parent_page_id=args.parent_page_id,
            dump_dir=Path(args.dump_dir),
        )
        return 0
    except (NotionAPIError, DumpFormatError, requests.RequestException) as error:
        print(f"Error: {error}")
        return 1


def command_run(args: argparse.Namespace) -> int:
    source_client = NotionClient(api_key=args.src_key, timeout=args.timeout)
    destination_client = NotionClient(api_key=args.dst_key, timeout=args.timeout)

    manual_database_ids: List[str] = []
    if args.database_id:
        try:
            manual_database_ids = parse_database_ids(args.database_id)
        except ValueError as error:
            print(f"Error: {error}")
            return 2

    try:
        database_ids = resolve_source_database_ids(
            source_client=source_client,
            manual_database_ids=manual_database_ids,
            include_all_accessible=(args.src_all_databases == "yes"),
        )
        dump_dir = Path(args.dump_dir)
        dump_databases_to_files(
            source_client=source_client,
            source_database_ids=database_ids,
            include_data=(args.copy_data == "yes"),
            dump_dir=dump_dir,
        )
        upload_dump_to_destination(
            destination_client=destination_client,
            destination_parent_page_id=args.dst_parent_page_id,
            dump_dir=dump_dir,
            include_data=(args.copy_data == "yes"),
        )
        
        # Auto-repair duplicate relations after upload
        if args.auto_repair == "yes":
            print("\nAuto-repairing duplicate relations...")
            repair_duplicate_relations(
                client=destination_client,
                parent_page_id=args.dst_parent_page_id,
                dump_dir=dump_dir,
            )
        
        return 0
    except (NotionAPIError, DumpFormatError, requests.RequestException) as error:
        print(f"Error: {error}")
        return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dump Notion databases to local files and upload them to another workspace."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_source_db_flags(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--database-id",
            action="append",
            default=[],
            help="Source Notion database ID. Repeat this flag or pass comma-separated values.",
        )
        subparser.add_argument(
            "--src-all-databases",
            choices=("yes", "no"),
            default="no",
            help="Use 'yes' to discover all source databases accessible by source API key.",
        )

    dump_parser = subparsers.add_parser("dump", help="Download source databases into local dump files.")
    dump_parser.add_argument("--src-key", required=True, help="Notion API key for source workspace.")
    add_source_db_flags(dump_parser)
    dump_parser.add_argument("--copy-data", choices=("yes", "no"), default="no", help="Include row data in dump.")
    dump_parser.add_argument("--dump-dir", required=True, help="Local directory for dump files.")
    dump_parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds.")

    upload_parser = subparsers.add_parser("upload", help="Upload local dump files into destination workspace.")
    upload_parser.add_argument("--dst-key", required=True, help="Notion API key for destination workspace.")
    upload_parser.add_argument(
        "--dst-parent-page-id",
        required=True,
        help="Destination parent page ID where databases will be created.",
    )
    upload_parser.add_argument(
        "--copy-data",
        choices=("yes", "no"),
        default="yes",
        help="Upload row data from dump if available.",
    )
    upload_parser.add_argument("--dump-dir", required=True, help="Local dump directory (contains manifest.json).")
    upload_parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds.")

    repair_parser = subparsers.add_parser("repair", help="Repair duplicate relation properties after upload.")
    repair_parser.add_argument("--api-key", required=True, help="Notion API key for the workspace to repair.")
    repair_parser.add_argument(
        "--parent-page-id",
        required=True,
        help="Parent page ID containing the databases to repair.",
    )
    repair_parser.add_argument("--dump-dir", required=True, help="Local dump directory for reference.")
    repair_parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds.")

    run_parser = subparsers.add_parser("run", help="Run dump then upload in one command (still uses local dump files).")
    run_parser.add_argument("--src-key", required=True, help="Notion API key for source workspace.")
    run_parser.add_argument("--dst-key", required=True, help="Notion API key for destination workspace.")
    add_source_db_flags(run_parser)
    run_parser.add_argument("--copy-data", choices=("yes", "no"), default="no", help="Copy row data.")
    run_parser.add_argument(
        "--dst-parent-page-id",
        required=True,
        help="Destination parent page ID where databases will be created.",
    )
    run_parser.add_argument("--dump-dir", required=True, help="Local dump directory to write/read.")
    run_parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds.")
    run_parser.add_argument(
        "--auto-repair",
        choices=("yes", "no"),
        default="yes",
        help="Automatically repair duplicate relation properties after upload.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "dump":
        return command_dump(args)
    if args.command == "upload":
        return command_upload(args)
    if args.command == "repair":
        return command_repair(args)
    if args.command == "run":
        return command_run(args)
    print(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
