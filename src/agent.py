from typing import Any
import calendar
from datetime import date, datetime
import random
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
RANDOM_YEAR = 2025
MIN_ATTEMPTS = 3


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


def _normalize_symbols(value: Any) -> list[str] | None:
    if not value:
        return None
    if isinstance(value, str):
        symbols = [part.strip().upper() for part in value.split(",") if part.strip()]
    elif isinstance(value, list):
        symbols = [str(item).strip().upper() for item in value if str(item).strip()]
    else:
        return None
    return symbols or None


def _parse_date(value: str) -> date | None:
    trimmed = value.strip()
    if not trimmed:
        return None
    try:
        return datetime.strptime(trimmed, "%Y-%m-%d").date()
    except ValueError:
        return None


QUESTION_WEEKLY_KEYWORDS = ("weekly", "week", "weeklysummary", "weekly summary")
QUESTION_SHARE_KEYWORDS = ("share", "shares", "totalweeklysharequantity", "total weekly share")
QUESTION_SHORT_KEYWORDS = ("short interest", "short position", "current short")


def _normalize_question(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _is_weekly_question(question: str | None) -> bool:
    if not question:
        return False
    lowered = question.lower()
    return any(key in lowered for key in QUESTION_WEEKLY_KEYWORDS) and any(
        key in lowered for key in QUESTION_SHARE_KEYWORDS
    )


def _extract_weekly_share(
    payload: Any,
    symbol: str,
    settlement_date: str,
) -> tuple[Any | None, dict[str, Any] | None]:
    target_symbol = symbol.upper()
    for record in _normalize_records(payload):
        record_symbol = (
            record.get("issueSymbolIdentifier")
            or record.get("symbolCode")
            or record.get("symbol")
        )
        if not isinstance(record_symbol, str) or record_symbol.upper() != target_symbol:
            continue
        record_date = record.get("weekStartDate") or record.get("summaryStartDate")
        if not isinstance(record_date, str) or not record_date.startswith(settlement_date):
            continue
        return record.get("totalWeeklyShareQuantity"), record
    return None, None


def _pick_requested_date(config: dict[str, Any]) -> tuple[str, str]:
    settlement_date = str(config.get("settlement_date", "")).strip()
    if settlement_date:
        return settlement_date, "provided"

    month_value = config.get("target_month") or config.get("month")
    if month_value is None:
        raise ValueError("target_month is required when settlement_date is omitted")
    try:
        month = int(month_value)
    except (TypeError, ValueError) as exc:
        raise ValueError("target_month must be an integer from 1 to 12") from exc
    if month < 1 or month > 12:
        raise ValueError("target_month must be between 1 and 12")

    seed = config.get("random_seed")
    rng = random.Random(seed) if seed is not None else random.SystemRandom()
    last_day = calendar.monthrange(RANDOM_YEAR, month)[1]
    day = rng.choice([15, last_day])
    return date(RANDOM_YEAR, month, day).strftime("%Y-%m-%d"), f"random-day-{day}"


def _coerce_number(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if value is None:
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _build_purple_request(config: dict[str, Any]) -> str:
    symbols = _normalize_symbols(config.get("symbols"))
    question = _normalize_question(config.get("question"))
    dataset_group = config.get("dataset_group") or config.get("datasetGroup")
    dataset_name = config.get("dataset_name") or config.get("datasetName")
    dataset_group_eval = config.get("dataset_group_eval") or config.get("datasetGroupEval")
    dataset_name_eval = config.get("dataset_name_eval") or config.get("datasetNameEval")
    explicit_eval = bool(dataset_group_eval or dataset_name_eval)
    if dataset_group_eval is None:
        dataset_group_eval = dataset_group
    if dataset_name_eval is None:
        dataset_name_eval = dataset_name
    dataset_name_value = str(dataset_name_eval).strip() if dataset_name_eval else ""
    is_weekly = "weeklysummary" in dataset_name_value.lower()
    if not is_weekly and question and _is_weekly_question(question):
        is_weekly = True

    if symbols:
        expected_response: dict[str, Any] = {
            "best_symbol": "string",
            "best_quantity": "number",
            "results": "array",
        }
        notes = (
            "Run client_short.py for each symbol. Use dataset_group/dataset_name if provided. "
            "Try multiple dates to find the closest available settlement date and include attempts in the response. "
            "Return JSON only."
        )
        if is_weekly:
            expected_response = {
                "best_symbol": "string",
                "best_quantity": "number (totalWeeklyShareQuantity)",
                "results": "array",
            }
            notes = (
                "Run client_short.py for each symbol. Use dataset_group/dataset_name if provided. "
                "For weeklySummary, use totalWeeklyShareQuantity and pick the closest available "
                "weekStartDate/summaryStartDate; include attempts in the response. Return JSON only."
            )
        payload: dict[str, Any] = {
            "task": "max_short_interest",
            "client_short_path": CLIENT_SHORT_PATH,
            "args": {
                "symbols": symbols,
                "settlement_date": "",
            },
            "requested_settlement_date": "",
            "expected_response": expected_response,
            "notes": notes,
        }
    else:
        expected_response = {
            "symbol": "string",
            "settlement_date": "YYYY-MM-DD",
            "currentShortPositionQuantity": "number",
            "record": "object (raw dataset row)",
        }
        notes = (
            "Run client_short.py with dataset_group/dataset_name if provided and return only JSON (no markdown)."
        )
        if is_weekly:
            expected_response = {
                "symbol": "string",
                "weekStartDate": "YYYY-MM-DD",
                "totalWeeklyShareQuantity": "number",
                "record": "object (raw dataset row)",
            }
            notes = (
                "Run client_short.py with dataset_group/dataset_name if provided. For weeklySummary, "
                "return totalWeeklyShareQuantity and the closest available weekStartDate/summaryStartDate. "
                "Return only JSON (no markdown)."
            )
        payload = {
            "task": "fetch_short_interest",
            "client_short_path": CLIENT_SHORT_PATH,
            "args": {
                "symbol": str(config.get("symbol", "")).strip(),
                "settlement_date": "",
            },
            "requested_settlement_date": "",
            "expected_response": expected_response,
            "notes": notes,
        }
    issue_name = str(config.get("issue_name", "")).strip()
    if issue_name and not symbols:
        payload["args"]["issue_name"] = issue_name
    if question:
        payload["question"] = question
    if dataset_group and not explicit_eval:
        payload["dataset_group"] = str(dataset_group)
    if dataset_name and not explicit_eval:
        payload["dataset_name"] = str(dataset_name)
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
    is_weekly: bool,
) -> tuple[Any | None, dict[str, Any] | None]:
    if isinstance(payload, dict):
        if is_weekly:
            direct_value = payload.get("totalWeeklyShareQuantity")
            record = payload.get("record") if isinstance(payload.get("record"), dict) else None
            if direct_value is not None:
                return direct_value, record or payload
            if record:
                record_value = record.get("totalWeeklyShareQuantity")
                if record_value is not None:
                    return record_value, record
        else:
            direct_value = payload.get("currentShortPositionQuantity")
            record = payload.get("record") if isinstance(payload.get("record"), dict) else None
            if direct_value is not None:
                return direct_value, record or payload
            if record:
                record_value = record.get("currentShortPositionQuantity")
                if record_value is not None:
                    return record_value, record
    if is_weekly:
        return _extract_weekly_share(payload, symbol, settlement_date)
    return _extract_short_position(payload, symbol, settlement_date)


def _build_single_result_data(
    *,
    status: str,
    errors: list[str],
    symbol: str,
    settlement_date: str,
    requested_date_reason: str,
    quantity: Any | None,
    record: dict[str, Any] | None,
    purple_response: Any,
    is_weekly: bool,
) -> dict[str, Any]:
    data: dict[str, Any] = {
        "status": status,
        "errors": errors,
        "symbol": symbol,
        "settlement_date": settlement_date,
        "requested_date_reason": requested_date_reason,
        "record": record,
        "purple_response": purple_response,
    }
    if is_weekly:
        data["totalWeeklyShareQuantity"] = quantity
    else:
        data["currentShortPositionQuantity"] = quantity
    return data


def _extract_results(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        results = payload.get("results")
        if isinstance(results, list):
            return [item for item in results if isinstance(item, dict)]
    return []


class Agent:
    # Fill in: list of required participant roles, e.g. ["pro_debater", "con_debater"]
    required_roles: list[str] = ["purple"]
    # Fill in: list of required config keys, e.g. ["topic", "num_rounds"]
    required_config_keys: list[str] = []

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

        settlement_date = str(request.config.get("settlement_date", "")).strip()
        target_month = request.config.get("target_month") or request.config.get("month")
        if not settlement_date and target_month is None:
            return False, "Provide settlement_date or target_month in config."
        if settlement_date and _parse_date(settlement_date) is None:
            return False, "settlement_date must be in YYYY-MM-DD format"
        if target_month is not None:
            try:
                month_value = int(target_month)
            except (TypeError, ValueError):
                return False, "target_month must be an integer from 1 to 12"
            if month_value < 1 or month_value > 12:
                return False, "target_month must be between 1 and 12"

        symbols = _normalize_symbols(request.config.get("symbols"))
        symbol = str(request.config.get("symbol", "")).strip()
        if symbols and symbol:
            return False, "Provide either symbol or symbols, not both."
        if not symbols and not symbol:
            return False, "Provide symbol or symbols in config."

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
            TaskState.working, new_agent_text_message("Selecting target date...")
        )

        try:
            requested_date, requested_reason = _pick_requested_date(request.config)
        except ValueError as exc:
            await updater.reject(new_agent_text_message(str(exc)))
            return

        await updater.update_status(
            TaskState.working,
            new_agent_text_message(
                f"Contacting purple agent (requested date: {requested_date})..."
            ),
        )

        purple_url = request.participants.get("purple")
        if not purple_url:
            await updater.reject(new_agent_text_message("Missing purple agent endpoint."))
            return

        request_payload = _build_purple_request(request.config)
        request_payload_obj = json.loads(request_payload)
        request_payload_obj["requested_settlement_date"] = requested_date
        request_payload_obj["args"]["settlement_date"] = requested_date
        if "min_attempts" not in request_payload_obj:
            request_payload_obj["min_attempts"] = MIN_ATTEMPTS
        request_payload = json.dumps(request_payload_obj)
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
        symbols = _normalize_symbols(request.config.get("symbols"))
        expected_date = requested_date
        question = _normalize_question(request.config.get("question"))
        requested_dataset_name = (
            request.config.get("dataset_name_eval")
            or request.config.get("datasetNameEval")
            or request.config.get("dataset_name")
            or request.config.get("datasetName")
        )
        if requested_dataset_name:
            requested_is_weekly = "weeklysummary" in str(requested_dataset_name).lower()
        else:
            requested_is_weekly = _is_weekly_question(question)


        try:
            parsed = _load_response_json(purple_response)
        except Exception as exc:
            errors.append(f"Failed to parse JSON response: {exc}")

        response_dataset_name = None
        response_is_weekly = None
        if isinstance(parsed, dict):
            response_dataset_name = parsed.get("dataset_name") or parsed.get("datasetName")
            if response_dataset_name:
                response_is_weekly = "weeklysummary" in str(response_dataset_name).lower()

        if question or requested_dataset_name:
            if not response_dataset_name:
                errors.append("Purple response missing dataset_name.")
            elif response_is_weekly != requested_is_weekly:
                expected_dataset = "weeklySummary" if requested_is_weekly else "consolidatedShortInterest"
                errors.append(
                    f"Dataset mismatch: expected {expected_dataset}, got {response_dataset_name}."
                )

        is_weekly = requested_is_weekly if (question or requested_dataset_name) else bool(response_is_weekly)
        metric_label = "weekly share" if is_weekly else "short interest"

        if symbols:
            results = _extract_results(parsed)
            if not results:
                errors.append("Purple response missing results list.")

            expected_set = {symbol.upper() for symbol in symbols}
            result_symbols: set[str] = set()
            max_symbol: str | None = None
            max_quantity: float | None = None
            expected_date_obj = _parse_date(expected_date)

            for result in results:
                symbol_value = (
                    result.get("symbol")
                    or result.get("symbolCode")
                    or result.get("issueSymbolIdentifier")
                )
                symbol_text = str(symbol_value).strip().upper() if symbol_value else ""
                if not symbol_text:
                    continue
                result_symbols.add(symbol_text)

                attempts = result.get("attempts")
                if not isinstance(attempts, list) or len(attempts) < MIN_ATTEMPTS:
                    errors.append(
                        f"{symbol_text}: expected at least {MIN_ATTEMPTS} attempts."
                    )

                if is_weekly:
                    chosen_date = (
                        result.get("chosen_date")
                        or result.get("weekStartDate")
                        or result.get("summaryStartDate")
                        or (result.get("record") or {}).get("weekStartDate")
                        or (result.get("record") or {}).get("summaryStartDate")
                    )
                else:
                    chosen_date = (
                        result.get("chosen_date")
                        or result.get("settlement_date")
                        or (result.get("record") or {}).get("settlementDate")
                    )
                chosen_date_str = str(chosen_date).strip() if chosen_date else ""
                chosen_date_obj = _parse_date(chosen_date_str) if chosen_date_str else None

                closest_date = None
                closest_quantity = None
                closest_diff = None

                if isinstance(attempts, list) and expected_date_obj:
                    for attempt in attempts:
                        if not isinstance(attempt, dict):
                            continue
                        attempt_date = str(attempt.get("settlement_date", "")).strip()
                        attempt_date_obj = _parse_date(attempt_date)
                        if not attempt_date_obj:
                            continue
                        if is_weekly:
                            attempt_quantity = _coerce_number(
                                attempt.get("quantity")
                                or attempt.get("totalWeeklyShareQuantity")
                            )
                        else:
                            attempt_quantity = _coerce_number(
                                attempt.get("quantity")
                                or attempt.get("currentShortPositionQuantity")
                            )
                        if attempt_quantity is None:
                            continue
                        diff = abs((attempt_date_obj - expected_date_obj).days)
                        if closest_diff is None or diff < closest_diff:
                            closest_diff = diff
                            closest_date = attempt_date
                            closest_quantity = attempt_quantity

                if closest_date is None or closest_quantity is None:
                    errors.append(
                        f"{symbol_text}: no numeric quantity found in attempts."
                    )
                    continue

                if chosen_date_obj is None:
                    errors.append(f"{symbol_text}: missing chosen_date.")
                else:
                    closest_date_obj = _parse_date(closest_date) if closest_date else None
                    if closest_date_obj and chosen_date_obj != closest_date_obj:
                        errors.append(
                            f"{symbol_text}: chosen_date {chosen_date_str} is not closest to {expected_date}."
                        )

                if max_quantity is None or closest_quantity > max_quantity:
                    max_quantity = closest_quantity
                    max_symbol = symbol_text

            missing_symbols = sorted(expected_set - result_symbols)
            if missing_symbols:
                errors.append(f"Missing results for symbols: {', '.join(missing_symbols)}")

            best_symbol = None
            best_quantity = None
            if isinstance(parsed, dict):
                best_symbol = parsed.get("best_symbol") or parsed.get("bestSymbol")
                best_quantity = parsed.get("best_quantity") or parsed.get("bestQuantity")

            if max_symbol is None or max_quantity is None:
                errors.append(
                    "No numeric weekly share values returned."
                    if is_weekly
                    else "No numeric short interest values returned."
                )
            else:
                if best_symbol and str(best_symbol).strip().upper() != max_symbol:
                    errors.append(
                        f"Best symbol mismatch: expected {max_symbol}, got {best_symbol}."
                    )
                if best_quantity is not None:
                    parsed_quantity = _coerce_number(best_quantity)
                    if parsed_quantity is None or abs(parsed_quantity - max_quantity) > 0.0001:
                        errors.append(
                            f"Best quantity mismatch: expected {max_quantity}, got {best_quantity}."
                        )

            status = "pass" if not errors else "fail"
            summary = (
                f"Max {metric_label} lookup {status} for {len(symbols)} symbols "
                f"(requested date {expected_date})."
            )

            await updater.add_artifact(
                parts=[
                    Part(root=TextPart(text=summary)),
                    Part(
                        root=DataPart(
                            data={
                                "status": status,
                                "errors": errors,
                                "symbols": symbols,
                                "settlement_date": expected_date,
                                "requested_date_reason": requested_reason,
                                "best_symbol": max_symbol,
                                "best_quantity": max_quantity,
                                "results": results,
                                "purple_response": parsed,
                            }
                        )
                    ),
                ],
                name="Result",
            )
            return

        quantity: Any | None = None
        record: dict[str, Any] | None = None
        try:
            quantity, record = _extract_quantity(
                parsed,
                str(request.config.get("symbol", "")).strip(),
                expected_date,
                is_weekly,
            )
        except Exception as exc:
            errors.append(f"Failed to parse JSON response: {exc}")

        if quantity is None:
            errors.append(
                "Missing totalWeeklyShareQuantity for the requested symbol/date."
                if is_weekly
                else "Missing currentShortPositionQuantity for the requested symbol/date."
            )

        expected_symbol = str(request.config.get("symbol", "")).strip().upper()
        record_symbol = None
        record_date = None
        if record:
            if is_weekly:
                record_symbol = (
                    record.get("issueSymbolIdentifier")
                    or record.get("symbolCode")
                    or record.get("symbol")
                )
                record_date = (
                    record.get("weekStartDate")
                    or record.get("summaryStartDate")
                    or record.get("settlementDate")
                    or record.get("settlement_date")
                )
            else:
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
                    f"Date mismatch: expected {expected_date}, got {record_date}."
                )

        if quantity is not None and not isinstance(quantity, (int, float)):
            if _coerce_number(quantity) is None:
                errors.append(
                    "totalWeeklyShareQuantity is not numeric."
                    if is_weekly
                    else "currentShortPositionQuantity is not numeric."
                )

        status = "pass" if not errors else "fail"
        summary = (
            f"{metric_label.title()} lookup {status} for {expected_symbol} on {expected_date}."
        )

        await updater.add_artifact(
            parts=[
                Part(root=TextPart(text=summary)),
                Part(
                    root=DataPart(
                        data=_build_single_result_data(
                            status=status,
                            errors=errors,
                            symbol=expected_symbol,
                            settlement_date=expected_date,
                            requested_date_reason=requested_reason,
                            quantity=quantity,
                            record=record,
                            purple_response=parsed,
                            is_weekly=is_weekly,
                        )
                    )
                ),
            ],
            name="Result",
        )
