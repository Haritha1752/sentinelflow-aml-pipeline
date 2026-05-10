"""
AML Compliance Pipeline - Transaction Generator
------------------------------------------------
Generates realistic fake ISO 20022 bank payment messages
and streams them into Kafka in real time.

Think of this as your fake bank - it produces thousands
of transactions per minute, with a small % of intentionally
suspicious ones to test your pipeline.
"""

import json
import time
import random
import uuid
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

# ── Setup ──────────────────────────────────────────────────
fake = Faker()

KAFKA_TOPIC   = "aml-transactions"
KAFKA_BROKER  = "localhost:9092"

# ── Known sanctioned names (from OFAC SDN list samples) ────
# These will be injected into ~5% of transactions to simulate
# real hits against the sanctions watchlist
SANCTIONED_NAMES = [
    "Viktor Bout",
    "Semion Mogilevich",
    "Joaquin Guzman Loera",
    "Alisher Usmanov",
    "Ramzan Kadyrov",
    "Ali Khamenei",
    "Kim Jong Un",
    "Robert Mugabe",
]

# ── High-risk countries (FATF grey/black list) ──────────────
HIGH_RISK_COUNTRIES = ["KP", "IR", "MM", "RU", "BY", "CU", "SY", "YE"]
NORMAL_COUNTRIES    = ["US", "GB", "DE", "FR", "JP", "CA", "AU", "SG", "NL", "CH"]

# ── Currency codes ──────────────────────────────────────────
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "SGD"]

# ── Payment purpose codes (ISO 20022 standard) ─────────────
PURPOSE_CODES = [
    "SALA",  # Salary payment
    "SUPP",  # Supplier payment
    "TRAD",  # Trade settlement
    "LOAN",  # Loan repayment
    "INSU",  # Insurance premium
    "INVS",  # Investment
    "GDDS",  # Purchase of goods
    "SVCS",  # Purchase of services
]


def generate_account_number():
    """Generate a realistic IBAN-style account number."""
    country = random.choice(["DE", "GB", "FR", "NL", "US"])
    number  = "".join([str(random.randint(0, 9)) for _ in range(18)])
    return f"{country}{number}"


def generate_swift_code():
    """Generate a realistic SWIFT/BIC bank code."""
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return (
        "".join(random.choices(letters, k=4))  # Bank code
        + random.choice(NORMAL_COUNTRIES)       # Country code
        + "".join(random.choices(letters, k=2)) # Location
    )


def generate_transaction(inject_suspicious=False):
    """
    Generate a single ISO 20022-style payment message.

    inject_suspicious=True forces a bad transaction:
      - sanctioned sender or receiver name, OR
      - missing Travel Rule fields, OR
      - unusually large amount from a high-risk country
    """
    txn_type = random.choice(["normal", "sanctioned", "missing_fields", "large_amount"])

    if inject_suspicious:
        txn_type = random.choice(["sanctioned", "missing_fields", "large_amount"])

    # ── Sender details ──────────────────────────────────────
    if txn_type == "sanctioned":
        # Use a known sanctioned name
        sender_name = random.choice(SANCTIONED_NAMES)
    else:
        sender_name = fake.name()

    sender_country = (
        random.choice(HIGH_RISK_COUNTRIES)
        if txn_type == "large_amount"
        else random.choice(NORMAL_COUNTRIES)
    )

    # ── Receiver details ────────────────────────────────────
    receiver_name    = fake.name()
    receiver_country = random.choice(NORMAL_COUNTRIES)

    # ── Amount ──────────────────────────────────────────────
    if txn_type == "large_amount":
        amount = round(random.uniform(500_000, 5_000_000), 2)  # Suspiciously large
    else:
        amount = round(random.uniform(100, 50_000), 2)          # Normal range

    # ── Travel Rule fields (sometimes missing) ───────────────
    # FATF Travel Rule requires sender address — we omit it
    # for missing_fields transactions to simulate a violation
    sender_address = (
        "" if txn_type == "missing_fields"
        else fake.address().replace("\n", ", ")
    )

    # ── Build the ISO 20022-style message ───────────────────
    transaction = {
        # Unique transaction identifier
        "transaction_id":   str(uuid.uuid4()),
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "message_type":     "pacs.008",  # ISO 20022 credit transfer

        # Sender (Originator)
        "sender_name":      sender_name,
        "sender_account":   generate_account_number(),
        "sender_bank_swift": generate_swift_code(),
        "sender_country":   sender_country,
        "sender_address":   sender_address,       # Empty = Travel Rule violation

        # Receiver (Beneficiary)
        "receiver_name":    receiver_name,
        "receiver_account": generate_account_number(),
        "receiver_bank_swift": generate_swift_code(),
        "receiver_country": receiver_country,

        # Payment details
        "amount":           amount,
        "currency":         random.choice(CURRENCIES),
        "purpose_code":     random.choice(PURPOSE_CODES),

        # Metadata
        "transaction_type": txn_type,  # Label for testing — not in real ISO 20022
    }

    return transaction


def create_producer():
    """Connect to Kafka and return a producer."""
    print("Connecting to Kafka...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Connected! Streaming to topic: '{KAFKA_TOPIC}'")
    return producer


def stream_transactions(rate_per_second=2):
    """
    Continuously generate and send transactions to Kafka.
    Every 20th transaction is intentionally suspicious.

    rate_per_second: how many transactions to send per second
                     (keep at 2 for development, up to 100 for load testing)
    """
    producer  = create_producer()
    count     = 0
    interval  = 1.0 / rate_per_second

    print(f"\nStreaming {rate_per_second} transactions/second...")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            count += 1

            # Every 20th transaction is suspicious (~5% bad rate)
            is_suspicious = (count % 20 == 0)
            txn = generate_transaction(inject_suspicious=is_suspicious)

            # Send to Kafka
            producer.send(KAFKA_TOPIC, value=txn)

            # Print to terminal so you can see what's being sent
            flag = "🚨 SUSPICIOUS" if is_suspicious else "✅ normal    "
            print(
                f"[{count:>5}] {flag} | "
                f"{txn['sender_name']:<25} → "
                f"{txn['receiver_name']:<25} | "
                f"{txn['currency']} {txn['amount']:>12,.2f} | "
                f"{txn['sender_country']} → {txn['receiver_country']}"
            )

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\nStopped. Total transactions sent: {count}")
        producer.flush()
        producer.close()


# ── Run ─────────────────────────────────────────────────────
if __name__ == "__main__":
    stream_transactions(rate_per_second=2)