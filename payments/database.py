from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from .config import ROOT, database_coordinates, write_json
from .seed import COUNTS, dataset

TABLES = tuple(COUNTS)


def connect(config):
    import psycopg

    return psycopg.connect(config["DATABASE_URL"], connect_timeout=20, options="-c timezone=UTC")


def bootstrap(config) -> None:
    from psycopg import sql

    database_coordinates(config)
    with connect(config) as conn:
        wal = conn.execute("SHOW wal_level").fetchone()[0]
        if wal != "logical":
            raise ValueError("Habilite logical replication no Neon antes do setup")
        conn.execute((ROOT / "database/001_schema.sql").read_text(encoding="utf-8"))
        if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname='cdc_user'").fetchone():
            conn.execute("CREATE ROLE cdc_user LOGIN REPLICATION")
        conn.execute(
            sql.SQL("ALTER ROLE cdc_user WITH LOGIN REPLICATION PASSWORD {}").format(
                sql.Literal(config["CDC_PASSWORD"])
            )
        )
        conn.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO cdc_user").format(
                sql.Identifier(conn.info.dbname)
            )
        )
        conn.execute("GRANT USAGE ON SCHEMA public TO cdc_user")
        for table in TABLES:
            conn.execute(
                sql.SQL("GRANT SELECT ON TABLE public.{} TO cdc_user").format(sql.Identifier(table))
            )
        if not conn.execute(
            "SELECT 1 FROM pg_publication WHERE pubname='desafio_publication'"
        ).fetchone():
            conn.execute(
                "CREATE PUBLICATION desafio_publication FOR TABLE "
                + ", ".join("public." + table for table in TABLES)
            )
        data = dataset(int(config.get("SEED", 42)))
        # Reexecutar setup nao reescreve registros nem dispara milhares de updates no CDC.
        for table, rows in data.items():
            columns = list(rows[0])
            query = sql.SQL(
                "INSERT INTO public.{} ({}) VALUES ({}) ON CONFLICT (id) DO NOTHING"
            ).format(
                sql.Identifier(table),
                sql.SQL(",").join(map(sql.Identifier, columns)),
                sql.SQL(",").join(sql.Placeholder() for _ in columns),
            )
            with conn.cursor() as cursor:
                cursor.executemany(query, [tuple(row[c] for c in columns) for row in rows])
        counts = {
            t: conn.execute(
                sql.SQL("SELECT count(*) FROM public.{}").format(sql.Identifier(t))
            ).fetchone()[0]
            for t in TABLES
        }
        if counts != COUNTS:
            raise ValueError(
                f"Contagens diferentes do seed esperado: {counts}. Use banco exclusivo."
            )
    write_json(
        ROOT / "evidencias/00-seed.json", {"source": "postgres-live", "seed": 42, "counts": counts}
    )


def cdc_changes(config, identifier: int) -> None:
    # Tres commits separados: CDC registra estados intermediarios de forma verificavel.
    with connect(config) as conn:
        conn.execute(
            "INSERT INTO public.merchants VALUES (%s,%s,%s)",
            (identifier, "Loja para evidencia CDC", "TEST"),
        )
    with connect(config) as conn:
        conn.execute(
            "UPDATE public.merchants SET name=%s WHERE id=%s",
            ("Loja atualizada para evidencia CDC", identifier),
        )
    with connect(config) as conn:
        conn.execute("DELETE FROM public.merchants WHERE id=%s", (identifier,))


def fraud_changes(config) -> list[dict]:
    import secrets

    start = 10_000_000 + secrets.randbelow(1_000_000_000)
    now = datetime.now(UTC) - timedelta(seconds=10)
    rows = [
        {
            "id": start + i,
            "account_id": 1,
            "card_id": 1,
            "merchant_id": 1,
            "amount": Decimal("19.90") + i,
            "currency": "BRL",
            "status": "APPROVED",
            "occurred_at": now + timedelta(seconds=i),
        }
        for i in range(3)
    ]
    with connect(config) as conn:
        for row in rows:
            conn.execute(
                "INSERT INTO public.transactions "
                "(id,account_id,card_id,merchant_id,amount,currency,status,occurred_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                tuple(row.values()),
            )
        # Outro cartao avanca o horario do fluxo para liberar a janela do Flink.
        sentinel = dict(
            rows[-1], id=start + 3, account_id=2, card_id=2, occurred_at=now + timedelta(seconds=10)
        )
        conn.execute(
            "INSERT INTO public.transactions "
            "(id,account_id,card_id,merchant_id,amount,currency,status,occurred_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            tuple(sentinel.values()),
        )
    write_json(ROOT / "evidencias/02-fraud-input.json", {"source": "postgres-live", "rows": rows})
    return rows


def cleanup_replication(config) -> None:
    # Nao apaga tabelas, dados, role nem projeto Neon. Elimina somente objetos nomeados do CDC.
    with connect(config) as conn:
        slot = conn.execute(
            "SELECT active FROM pg_replication_slots WHERE slot_name=%s", ("desafio_slot",)
        ).fetchone()
        if slot and slot[0]:
            raise ValueError("Slot ainda ativo. Aguarde o conector terminar de ser removido")
        if slot:
            conn.execute("SELECT pg_drop_replication_slot(%s)", ("desafio_slot",))
        conn.execute("DROP PUBLICATION IF EXISTS desafio_publication")
