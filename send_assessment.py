import argparse
import asyncio
import json
from uuid import uuid4

import httpx
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.types import Message, Part, Role, TextPart


def _build_payload(args: argparse.Namespace) -> dict[str, object]:
    config: dict[str, object] = {
        "symbol": args.symbol,
        "settlement_date": args.settlement_date,
    }
    if args.issue_name:
        config["issue_name"] = args.issue_name
    if args.finra_client_id:
        config["finra_client_id"] = args.finra_client_id
    if args.finra_client_secret:
        config["finra_client_secret"] = args.finra_client_secret
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

        payload = _build_payload(args)
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
    parser.add_argument("--symbol", required=True, help="Symbol to query.")
    parser.add_argument(
        "--settlement-date",
        required=True,
        help="Settlement date (YYYY-MM-DD).",
    )
    parser.add_argument("--issue-name", help="Issue name filter.")
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
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
