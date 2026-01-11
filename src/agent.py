from typing import Any
import json
from pydantic import BaseModel, HttpUrl, ValidationError
from a2a.server.tasks import TaskUpdater
from a2a.types import Message, TaskState, Part, TextPart, DataPart
from a2a.utils import get_message_text, new_agent_text_message

from messenger import Messenger


class EvalRequest(BaseModel):
    """Request format sent by the AgentBeats platform to green agents."""
    participants: dict[str, HttpUrl] # role -> agent URL
    config: dict[str, Any]

CLIENT_SHORT_PATH = "/home/wczubal1/projects/tau2/brokercheck/client_short.py"


def _normalize_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "rows", "results", "result", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _extract_short_position(
    payload: Any,
    symbol: str,
    settlement_date: str,
) -> tuple[Any | None, dict[str, Any] | None]:
    target_symbol = symbol.upper()
    for record in _normalize_records(payload):
        record_symbol = record.get("symbolCode")
        if not isinstance(record_symbol, str) or record_symbol.upper() != target_symbol:
            continue
        record_date = record.get("settlementDate")
        if not isinstance(record_date, str) or not record_date.startswith(settlement_date):
            continue
        return record.get("currentShortPositionQuantity"), record
    return None, None


def _build_purple_request(config: dict[str, Any]) -> str:
    payload: dict[str, Any] = {
        "task": "fetch_short_interest",
        "client_short_path": CLIENT_SHORT_PATH,
        "args": {
            "symbol": str(config.get("symbol", "")).strip(),
            "settlement_date": str(config.get("settlement_date", "")).strip(),
        },
        "expected_response": {
            "symbol": "string",
            "settlement_date": "YYYY-MM-DD",
            "currentShortPositionQuantity": "number",
            "record": "object (raw dataset row)",
        },
        "notes": "Run client_short.py and return only JSON (no markdown).",
    }
    issue_name = str(config.get("issue_name", "")).strip()
    if issue_name:
        payload["args"]["issue_name"] = issue_name
    client_id = config.get("finra_client_id")
    client_secret = config.get("finra_client_secret")
    if client_id:
        payload["finra_client_id"] = str(client_id)
    if client_secret:
        payload["finra_client_secret"] = str(client_secret)
    timeout = config.get("timeout")
    if timeout is not None:
        payload["timeout"] = timeout
    return json.dumps(payload)


def _load_response_json(response_text: str) -> Any:
    candidate = response_text.strip()
    if not candidate:
        raise ValueError("Empty response from purple agent")
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(candidate[start : end + 1])
        list_start = candidate.find("[")
        list_end = candidate.rfind("]")
        if list_start == -1 or list_end == -1 or list_end <= list_start:
            raise
        return json.loads(candidate[list_start : list_end + 1])


def _extract_quantity(
    payload: Any,
    symbol: str,
    settlement_date: str,
) -> tuple[Any | None, dict[str, Any] | None]:
    if isinstance(payload, dict):
        direct_value = payload.get("currentShortPositionQuantity")
        record = payload.get("record") if isinstance(payload.get("record"), dict) else None
        if direct_value is not None:
            return direct_value, record or payload
        if record:
            record_value = record.get("currentShortPositionQuantity")
            if record_value is not None:
                return record_value, record
    return _extract_short_position(payload, symbol, settlement_date)


class Agent:
    # Fill in: list of required participant roles, e.g. ["pro_debater", "con_debater"]
    required_roles: list[str] = ["purple"]
    # Fill in: list of required config keys, e.g. ["topic", "num_rounds"]
    required_config_keys: list[str] = ["symbol", "settlement_date"]

    def __init__(self):
        self.messenger = Messenger()
        # Initialize other state here

    def validate_request(self, request: EvalRequest) -> tuple[bool, str]:
        missing_roles = set(self.required_roles) - set(request.participants.keys())
        if missing_roles:
            return False, f"Missing roles: {missing_roles}"

        missing_config_keys = set(self.required_config_keys) - set(request.config.keys())
        if missing_config_keys:
            return False, f"Missing config keys: {missing_config_keys}"

        empty_keys = [
            key
            for key in self.required_config_keys
            if not str(request.config.get(key, "")).strip()
        ]
        if empty_keys:
            return False, f"Config keys must be non-empty: {empty_keys}"

        # Add additional request validation here

        return True, "ok"

    async def run(self, message: Message, updater: TaskUpdater) -> None:
        """Implement your agent logic here.

        Args:
            message: The incoming message
            updater: Report progress (update_status) and results (add_artifact)

        Use self.messenger.talk_to_agent(message, url) to call other agents.
        """
        input_text = get_message_text(message)

        try:
            request: EvalRequest = EvalRequest.model_validate_json(input_text)
            ok, msg = self.validate_request(request)
            if not ok:
                await updater.reject(new_agent_text_message(msg))
                return
        except ValidationError as e:
            await updater.reject(new_agent_text_message(f"Invalid request: {e}"))
            return

        # Replace example code below with your agent logic
        # Use request.participants to get participant agent URLs by role
        # Use request.config for assessment parameters

        await updater.update_status(
            TaskState.working, new_agent_text_message("Contacting purple agent...")
        )

        purple_url = request.participants.get("purple")
        if not purple_url:
            await updater.reject(new_agent_text_message("Missing purple agent endpoint."))
            return

        request_payload = _build_purple_request(request.config)
        try:
            purple_response = await self.messenger.talk_to_agent(
                request_payload,
                str(purple_url),
                new_conversation=True,
            )
        except Exception as exc:
            await updater.failed(new_agent_text_message(f"Purple agent call failed: {exc}"))
            return

        await updater.update_status(
            TaskState.working, new_agent_text_message("Evaluating purple response...")
        )

        errors: list[str] = []
        parsed: Any | None = None
        quantity: Any | None = None
        record: dict[str, Any] | None = None
        try:
            parsed = _load_response_json(purple_response)
            quantity, record = _extract_quantity(
                parsed,
                str(request.config.get("symbol", "")).strip(),
                str(request.config.get("settlement_date", "")).strip(),
            )
        except Exception as exc:
            errors.append(f"Failed to parse JSON response: {exc}")

        if quantity is None:
            errors.append("Missing currentShortPositionQuantity for the requested symbol/date.")

        expected_symbol = str(request.config.get("symbol", "")).strip().upper()
        expected_date = str(request.config.get("settlement_date", "")).strip()
        record_symbol = None
        record_date = None
        if record:
            record_symbol = record.get("symbolCode") or record.get("symbol")
            record_date = record.get("settlementDate") or record.get("settlement_date")
        if record_symbol and isinstance(record_symbol, str):
            if record_symbol.upper() != expected_symbol:
                errors.append(
                    f"Symbol mismatch: expected {expected_symbol}, got {record_symbol}."
                )
        if record_date and isinstance(record_date, str):
            if not record_date.startswith(expected_date):
                errors.append(
                    f"Settlement date mismatch: expected {expected_date}, got {record_date}."
                )

        if quantity is not None and not isinstance(quantity, (int, float)):
            try:
                float(str(quantity))
            except ValueError:
                errors.append("currentShortPositionQuantity is not numeric.")

        status = "pass" if not errors else "fail"
        summary = (
            f"Short interest lookup {status} for {expected_symbol} on {expected_date}."
        )

        await updater.add_artifact(
            parts=[
                Part(root=TextPart(text=summary)),
                Part(
                    root=DataPart(
                        data={
                            "status": status,
                            "errors": errors,
                            "symbol": expected_symbol,
                            "settlement_date": expected_date,
                            "currentShortPositionQuantity": quantity,
                            "record": record,
                            "purple_response": parsed,
                        }
                    )
                ),
            ],
            name="Result",
        )
