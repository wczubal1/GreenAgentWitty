import argparse
import asyncio
import csv
import json
import random
import re
from datetime import datetime
from pathlib import Path
import os
from uuid import uuid4

import httpx
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.types import Message, Part, Role, TextPart

DEFAULT_SP500_CSV = str(Path(__file__).resolve().parent / "SP500symbols.csv")


def _normalize_windows_path(path_str: str) -> str:
    match = re.match(r"^[A-Za-z]:[\\\\/]", path_str)
    if not match:
        return path_str
    drive = path_str[0].lower()
    rest = path_str[2:].replace("\\", "/").lstrip("/")
    return f"/mnt/{drive}/{rest}"


def _load_symbols(path_str: str) -> list[str]:
    path = Path(_normalize_windows_path(path_str)).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Symbols CSV not found: {path}")

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            return []
        header_lower = [cell.strip().lower() for cell in header]
        symbol_index = None
        for key in ("symbol", "ticker"):
            if key in header_lower:
                symbol_index = header_lower.index(key)
                break
        rows = [header] if symbol_index is None else []
        rows.extend(reader)

    symbols: list[str] = []
    for row in rows:
        if not row:
            continue
        cell = row[0] if symbol_index is None else row[symbol_index]
        symbol = str(cell).strip().upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _sample_symbols(
    symbols: list[str],
    sample_size: int,
    seed: int | None,
) -> list[str]:
    if sample_size <= 0:
        raise ValueError("sample-size must be positive")
    if len(symbols) < sample_size:
        raise ValueError(
            f"Not enough symbols ({len(symbols)}) to sample {sample_size} entries"
        )
    rng = random.Random(seed)
    return rng.sample(symbols, sample_size)


def _normalize_date(date_str: str) -> str:
    trimmed = date_str.strip()
    if re.match(r"^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$", trimmed):
        parsed = datetime.strptime(trimmed, "%m/%d/%Y")
        return parsed.strftime("%Y-%m-%d")
    return trimmed

def _build_payload(
    args: argparse.Namespace,
    symbols: list[str] | None,
) -> dict[str, object]:
    config: dict[str, object] = {}
    if args.settlement_date:
        config["settlement_date"] = args.settlement_date
    if args.target_month is not None:
        config["target_month"] = args.target_month
    if args.random_seed is not None:
        config["random_seed"] = args.random_seed
    if symbols:
        config["symbols"] = symbols
    else:
        config["symbol"] = args.symbol
    if args.issue_name:
        config["issue_name"] = args.issue_name
    if args.question:
        config["question"] = args.question
    if args.dataset_group_eval:
        config["dataset_group_eval"] = args.dataset_group_eval
    if args.dataset_name_eval:
        config["dataset_name_eval"] = args.dataset_name_eval
    finra_client_id = args.finra_client_id or os.environ.get("FINRA_CLIENT_ID")
    finra_client_secret = args.finra_client_secret or os.environ.get(
        "FINRA_CLIENT_SECRET"
    )
    if finra_client_id:
        config["finra_client_id"] = finra_client_id
    if finra_client_secret:
        config["finra_client_secret"] = finra_client_secret
    if args.timeout is not None:
        config["timeout"] = args.timeout

    return {
        "participants": {"purple": args.purple_url},
        "config": config,
    }


def _serialize_event(event: object) -> dict[str, object]:
    if isinstance(event, Message):
        return {"kind": "message", "message": event.model_dump()}
    if isinstance(event, tuple) and len(event) == 2:
        task, update = event
        payload: dict[str, object] = {"kind": "task-update", "task": task.model_dump()}
        if update is not None:
            payload["update"] = update.model_dump()
        return payload
    return {"kind": "unknown", "repr": repr(event)}


async def _run(args: argparse.Namespace) -> None:
    async with httpx.AsyncClient(timeout=args.http_timeout) as httpx_client:
        resolver = A2ACardResolver(httpx_client=httpx_client, base_url=args.green_url)
        agent_card = await resolver.get_agent_card()
        client = ClientFactory(
            ClientConfig(httpx_client=httpx_client, streaming=args.streaming)
        ).create(agent_card)

        payload = _build_payload(args, args.symbols_list)
        msg = Message(
            kind="message",
            role=Role.user,
            parts=[Part(TextPart(text=json.dumps(payload)))],
            message_id=uuid4().hex,
        )

        events = [event async for event in client.send_message(msg)]

    serialized = [_serialize_event(event) for event in events]
    print(json.dumps(serialized, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send a short-interest assessment request to the green agent."
    )
    parser.add_argument(
        "--green-url",
        default="http://127.0.0.1:9009",
        help="Green agent base URL.",
    )
    parser.add_argument(
        "--purple-url",
        default="http://127.0.0.1:9010",
        help="Purple agent base URL.",
    )
    parser.add_argument("--symbol", help="Symbol to query.")
    parser.add_argument(
        "--symbols",
        help="Comma separated list of symbols to evaluate.",
    )
    parser.add_argument(
        "--symbols-csv",
        default=DEFAULT_SP500_CSV,
        help="Path to a CSV file containing SP500 symbols.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="How many symbols to sample from the CSV.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        help="Seed for reproducible sampling.",
    )
    parser.add_argument(
        "--settlement-date",
        help="Settlement date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--target-month",
        type=int,
        help="Target month (1-12) for random day selection in 2025.",
    )
    parser.add_argument("--issue-name", help="Issue name filter.")
    parser.add_argument("--question", help="Question to drive dataset selection.")
    parser.add_argument("--dataset-group-eval", help="Dataset group for evaluation only.")
    parser.add_argument("--dataset-name-eval", help="Dataset name for evaluation only.")
    parser.add_argument("--finra-client-id", help="FINRA client id.")
    parser.add_argument("--finra-client-secret", help="FINRA client secret.")
    parser.add_argument(
        "--timeout",
        type=int,
        help="Timeout (seconds) to pass to the purple agent call.",
    )
    parser.add_argument(
        "--http-timeout",
        type=int,
        default=30,
        help="HTTP timeout (seconds) for A2A calls.",
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Enable streaming responses.",
    )

    args = parser.parse_args()
    if args.settlement_date:
        args.settlement_date = _normalize_date(args.settlement_date)

    if args.symbol and args.symbols:
        parser.error("Use --symbol or --symbols, not both.")

    symbols_list: list[str] | None = None
    if args.symbols:
        symbols_list = [part.strip().upper() for part in args.symbols.split(",") if part.strip()]
        if not symbols_list:
            parser.error("--symbols must include at least one symbol.")
    elif args.symbol:
        symbols_list = None
    else:
        symbols = _load_symbols(args.symbols_csv)
        symbols_list = _sample_symbols(symbols, args.sample_size, args.random_seed)

    args.symbols_list = symbols_list
    if not args.symbols_list and not args.symbol:
        parser.error("Provide --symbol or symbols via --symbols/--symbols-csv.")

    if not args.settlement_date and args.target_month is None:
        parser.error("Provide --settlement-date or --target-month.")

    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
