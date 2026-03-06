from __future__ import annotations

from logistics_ingest.domain.models import RateRecord, SurchargeRuleRecord

from logistics_ingest.infra.db.schema import ensure_pricing_schema


def ensure_engine_tables(conn) -> None:
    ensure_pricing_schema(conn, include_indexes=True)


def truncate_engine_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE pricing_constraints, pricing_rate_tiers, pricing_surcharge_rules, "
            "pricing_channels RESTART IDENTITY CASCADE"
        )
    conn.commit()


def upsert_rates(conn, records: list[RateRecord], surcharge_rules: list[SurchargeRuleRecord]) -> tuple[int, int, int]:
    if not records:
        return 0, 0, 0

    channels_map: dict[str, int] = {}
    inserted_channels = 0
    inserted_tiers = 0
    inserted_surcharges = 0

    with conn.cursor() as cur:
        for rec in records:
            if rec.channel_code not in channels_map:
                cur.execute(
                    """
                    INSERT INTO pricing_channels (
                        channel_code, channel_name, transport_mode, cargo_natures,
                        destination_country, destination_scope, tax_included, destination_keyword,
                        source_workbook, source_company,
                        transit_days_min, transit_days_max, active, note
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                    ON CONFLICT (channel_code)
                    DO UPDATE SET
                        channel_name = EXCLUDED.channel_name,
                        transport_mode = EXCLUDED.transport_mode,
                        cargo_natures = EXCLUDED.cargo_natures,
                        destination_country = EXCLUDED.destination_country,
                        destination_scope = EXCLUDED.destination_scope,
                        tax_included = EXCLUDED.tax_included,
                        destination_keyword = EXCLUDED.destination_keyword,
                        source_workbook = EXCLUDED.source_workbook,
                        source_company = EXCLUDED.source_company,
                        transit_days_min = EXCLUDED.transit_days_min,
                        transit_days_max = EXCLUDED.transit_days_max,
                        note = EXCLUDED.note,
                        active = TRUE
                    RETURNING id
                    """,
                    (
                        rec.channel_code,
                        rec.channel_name,
                        rec.transport_mode,
                        rec.cargo_natures,
                        rec.destination_country,
                        rec.destination_scope,
                        rec.tax_included,
                        rec.destination_keyword,
                        rec.source_workbook,
                        rec.source_company,
                        rec.transit_days_min,
                        rec.transit_days_max,
                        rec.channel_note,
                    ),
                )
                channel_id = cur.fetchone()[0]
                channels_map[rec.channel_code] = channel_id
                inserted_channels += 1

        touched_ids = sorted(set(channels_map.values()))
        cur.execute("DELETE FROM pricing_rate_tiers WHERE channel_id = ANY(%s)", (touched_ids,))
        cur.execute("DELETE FROM pricing_surcharge_rules WHERE channel_id = ANY(%s)", (touched_ids,))

        tier_rows = [
            (
                channels_map[r.channel_code],
                r.min_weight,
                r.max_weight,
                r.unit_price,
                r.currency,
                r.volumetric_divisor,
                r.min_charge,
            )
            for r in records
        ]
        cur.executemany(
            """
            INSERT INTO pricing_rate_tiers (
                channel_id, min_weight, max_weight, unit_price, currency,
                volumetric_divisor, min_charge, active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
            """,
            tier_rows,
        )
        inserted_tiers = len(tier_rows)

        surcharge_rows = [
            (
                channels_map[r.channel_code],
                r.rule_name,
                r.trigger_type,
                r.trigger_value,
                r.calc_method,
                r.amount,
                r.currency,
                r.weight_basis,
                r.min_charge,
                r.max_charge,
                r.requires_fuel_multiplier,
                r.stack_mode,
                r.priority,
                r.note,
                r.source_excerpt,
            )
            for r in surcharge_rules
            if r.channel_code in channels_map
        ]
        if surcharge_rows:
            cur.executemany(
                """
                INSERT INTO pricing_surcharge_rules (
                    channel_id, rule_name, trigger_type, trigger_value, calc_method,
                    amount, currency, weight_basis, min_charge, max_charge, requires_fuel_multiplier,
                    stack_mode, priority, active, note, source_excerpt
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s)
                """,
                surcharge_rows,
            )
            inserted_surcharges = len(surcharge_rows)

    conn.commit()
    return inserted_channels, inserted_tiers, inserted_surcharges


__all__ = ["ensure_engine_tables", "truncate_engine_tables", "upsert_rates"]
