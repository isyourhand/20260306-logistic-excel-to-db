from __future__ import annotations

from typing import Any

from psycopg import connect


def collect_publish_metrics(dsn: str, batch_id: str) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "raw_workbook_count": 0,
        "raw_sheet_count": 0,
        "raw_workbooks": [],
        "channels_total_for_batch_workbooks": 0,
        "tiers_total_for_batch_workbooks": 0,
        "channels_by_workbook": {},
    }
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT workbook_name, COUNT(*) AS sheet_count
                FROM raw_sheet_meta
                WHERE import_batch_id = %s
                GROUP BY workbook_name
                ORDER BY workbook_name
                """,
                (batch_id,),
            )
            rows = cur.fetchall()
            metrics["raw_workbook_count"] = len(rows)
            metrics["raw_workbooks"] = [{"workbook_name": x[0], "sheet_count": int(x[1])} for x in rows]
            metrics["raw_sheet_count"] = sum(int(x[1]) for x in rows)

            workbooks = [x[0] for x in rows]
            if workbooks:
                cur.execute(
                    """
                    SELECT
                      c.source_workbook,
                      COUNT(DISTINCT c.id) AS channels,
                      COALESCE(COUNT(t.id), 0) AS tiers
                    FROM pricing_channels c
                    LEFT JOIN pricing_rate_tiers t ON t.channel_id = c.id AND t.active = TRUE
                    WHERE c.active = TRUE
                      AND c.source_workbook = ANY(%s)
                    GROUP BY c.source_workbook
                    ORDER BY c.source_workbook
                    """,
                    (workbooks,),
                )
                crows = cur.fetchall()
                channels_by_workbook: dict[str, dict[str, int]] = {}
                for wb, channels, tiers in crows:
                    channels_by_workbook[str(wb)] = {
                        "channels": int(channels),
                        "tiers": int(tiers),
                    }
                metrics["channels_by_workbook"] = channels_by_workbook
                metrics["channels_total_for_batch_workbooks"] = sum(v["channels"] for v in channels_by_workbook.values())
                metrics["tiers_total_for_batch_workbooks"] = sum(v["tiers"] for v in channels_by_workbook.values())
    return metrics


__all__ = ["collect_publish_metrics"]
