"""Dados sinteticos e repetiveis. Dinheiro nunca passa por float."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal

COUNTS = {"customers": 200, "accounts": 240, "cards": 320, "merchants": 60, "transactions": 2000}
BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def dataset(seed: int = 42) -> dict[str, list[dict]]:
    rng = random.Random(seed)
    customers = [
        {
            "id": i,
            "name": f"Cliente ficticio {i:03}",
            "email": f"cliente{i:03}@example.invalid",
            "document": f"TEST-{i:011}",
        }
        for i in range(1, 201)
    ]
    accounts = [
        {
            "id": i,
            "customer_id": (i - 1) % 200 + 1,
            "status": "ACTIVE",
            "balance": Decimal(rng.randrange(10000, 5000000)) / 100,
            "updated_at": BASE_TIME,
        }
        for i in range(1, 241)
    ]
    cards = [
        {
            "id": i,
            "account_id": (i - 1) % 240 + 1,
            "token": f"tok_test_{i:06}",
            "last4": f"{i:04}",
            "status": "ACTIVE",
        }
        for i in range(1, 321)
    ]
    merchants = [
        {"id": i, "name": f"Loja ficticia {i:02}", "category": "RETAIL"} for i in range(1, 61)
    ]
    transactions = []
    for i in range(1, 2001):
        card = cards[(i - 1) % 320]
        transactions.append(
            {
                "id": i,
                "account_id": card["account_id"],
                "card_id": card["id"],
                "merchant_id": rng.randrange(1, 61),
                "amount": Decimal(rng.randrange(100, 200000)) / 100,
                "currency": "BRL",
                "status": "APPROVED",
                "occurred_at": BASE_TIME + timedelta(seconds=i * 70),
            }
        )
    return {
        "customers": customers,
        "accounts": accounts,
        "cards": cards,
        "merchants": merchants,
        "transactions": transactions,
    }
