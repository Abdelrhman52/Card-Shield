"""
CardShield — HBase Backup Helper
==================================
Used by the Airflow DAG task `hbase_table_backup`.

Strategy:
  - Uses the HBase ExportSnapshot tool via subprocess (runs inside HBase container)
    OR happybase to scan + serialize table data to Avro files on HDFS.
  - Creates a dated directory under HDFS_HBASE_BACKUP_BASE.
  - Backs up: Blacklist, UserProfiles, FraudAlerts

For production: use HBase's built-in `ExportSnapshot` or `distcp` approach.
For local/Docker: we use happybase scan + fastavro to serialize to HDFS.
"""

import io
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

import fastavro
import requests

logger = logging.getLogger("cardshield.hbase_backup")

HBASE_HOST = os.environ.get("HBASE_HOST", "hbase")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))   # Thrift port
HDFS_WEBHDFS_HOST = os.environ.get("HDFS_WEBHDFS_HOST", "namenode")
HDFS_WEBHDFS_PORT = int(os.environ.get("HDFS_WEBHDFS_PORT", "9870"))
HDFS_BACKUP_BASE = os.environ.get("HDFS_HBASE_BACKUP_BASE", "/cardshield/hbase-backups")
HDFS_WEBHDFS_USER = os.environ.get("HDFS_WEBHDFS_USER", "root")

TABLES_TO_BACKUP = ["Blacklist", "UserProfiles", "FraudAlerts"]

# Generic row schema for backup (key + column map serialized as Avro)
BACKUP_AVRO_SCHEMA = fastavro.parse_schema({
    "type": "record",
    "name": "HBaseRow",
    "namespace": "com.cardshield.hbase.backup",
    "fields": [
        {"name": "row_key",  "type": "string"},
        {"name": "columns",  "type": {"type": "map", "values": "string"}},
        {"name": "backup_ts","type": "long", "logicalType": "timestamp-millis"},
    ],
})


def _webhdfs_url(path: str) -> str:
    return f"http://{HDFS_WEBHDFS_HOST}:{HDFS_WEBHDFS_PORT}/webhdfs/v1{path}?user.name={HDFS_WEBHDFS_USER}"


def _hdfs_mkdirs(path: str):
    r = requests.put(_webhdfs_url(path) + "&op=MKDIRS", timeout=15)
    r.raise_for_status()


def _hdfs_write(path: str, data: bytes):
    r1 = requests.put(_webhdfs_url(path) + "&op=CREATE&overwrite=true&replication=1",
                      allow_redirects=False, timeout=15)
    r1.raise_for_status()
    redirect = r1.headers["Location"]
    r2 = requests.put(redirect, data=data,
                      headers={"Content-Type": "application/octet-stream"}, timeout=120)
    r2.raise_for_status()


def _backup_table(conn, table_name: str, backup_dir: str, backup_ts: int) -> int:
    """
    Scan all rows of a single HBase table and write an Avro container file to HDFS.
    Returns the number of rows backed up.
    """

    table = conn.table(table_name)
    records = []

    for row_key, columns in table.scan():
        row_dict = {
            col.decode(errors="replace"): val.decode(errors="replace")
            for col, val in columns.items()
        }
        records.append({
            "row_key": row_key.decode(errors="replace"),
            "columns": row_dict,
            "backup_ts": backup_ts,
        })

    if not records:
        logger.info("Table '%s' is empty — skipping backup file creation.", table_name)
        return 0

    # Serialize to Avro
    buf = io.BytesIO()
    fastavro.writer(buf, BACKUP_AVRO_SCHEMA, records, codec="snappy")
    avro_bytes = buf.getvalue()

    # Write to HDFS
    hdfs_path = f"{backup_dir}/{table_name.lower()}_backup.avro"
    _hdfs_write(hdfs_path, avro_bytes)
    logger.info("Backed up table '%s': %d rows → %s", table_name, len(records), hdfs_path)
    return len(records)


def backup_hbase_tables() -> Dict[str, Any]:
    """
    Backup all CardShield HBase tables to HDFS.

    Returns:
        dict with keys: success (bool), reason (str), details (dict)
    """
    import happybase

    result = {"success": False, "reason": "Unknown", "details": {}}
    backup_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    backup_dir = f"{HDFS_BACKUP_BASE}/{date_str}"

    try:
        _hdfs_mkdirs(backup_dir)
        logger.info("Created HDFS backup directory: %s", backup_dir)
    except Exception as exc:  # noqa: BLE001
        result["reason"] = f"HDFS mkdir failed: {exc}"
        logger.error(result["reason"])
        return result

    try:
        conn = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, autoconnect=True)
    except Exception as exc:  # noqa: BLE001
        result["reason"] = f"HBase connection failed: {exc}"
        logger.error(result["reason"])
        return result

    total_rows = 0
    table_results = {}
    try:
        for table_name in TABLES_TO_BACKUP:
            try:
                row_count = _backup_table(conn, table_name, backup_dir, backup_ts)
                table_results[table_name] = {"rows": row_count, "status": "OK"}
                total_rows += row_count
            except Exception as exc:  # noqa: BLE001
                logger.error("Backup failed for table '%s': %s", table_name, exc)
                table_results[table_name] = {"rows": 0, "status": f"FAILED: {exc}"}
    finally:
        conn.close()

    result["success"] = all(
        v["status"] == "OK" for v in table_results.values()
    )
    result["reason"] = "OK" if result["success"] else "One or more table backups failed"
    result["details"] = {
        "backup_dir": backup_dir,
        "total_rows": total_rows,
        "tables": table_results,
        "backup_ts": backup_ts,
    }
    return result
