from typing import Any
import re
import calendar
from datetime import date, datetime, timedelta
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
QUESTION_TREASURY_KEYWORDS = ("treasury", "dealer customer volume", "on-the-run")
TREASURY_UPPER_BOUND_BUCKETS = {
    2: "<= 2 years",
    3: "> 2 years and <= 3 years",
    5: "> 3 years and <= 5 years",
    7: "> 5 years and <= 7 years",
    10: "> 7 years and <= 10 years",
}


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


def _is_treasury_question(question: str | None) -> bool:
    if not question:
        return False
    lowered = question.lower()
    return any(key in lowered for key in QUESTION_TREASURY_KEYWORDS)


def _is_treasury_max_question(question: str | None) -> bool:
    if not question:
        return False
    lowered = question.lower()
    return ("highest" in lowered or "max" in lowered) and "dealer customer volume" in lowered


def _is_treasury_delta_question(question: str | None) -> bool:
    if not question:
        return False
    lowered = question.lower()
    return "dealer customer volume" in lowered and (
        "last year" in lowered
        or "over the last year" in lowered
        or "year over year" in lowered
        or "year-over-year" in lowered
        or "yoy" in lowered
    )


def _has_treasury_bucket(question: str | None) -> bool:
    if not question:
        return False
    lowered = question.lower()
    if re.search(r">\\s*\\d+\\s*years\\s*and\\s*<=\\s*\\d+\\s*years", lowered):
        return True
    if re.search(r"(?:<=|up to)\\s*\\d+\\s*years", lowered):
        return True
    return False


def _parse_treasury_bucket(question: str | None) -> tuple[str, str]:
    if not question:
        return "<= 2 years", "On-the-run"
    lowered = question.lower()
    benchmark = "On-the-run"
    if "off-the-run" in lowered or "off the run" in lowered:
        benchmark = "Off-the-run"
    elif "on-the-run" in lowered or "on the run" in lowered:
        benchmark = "On-the-run"

    explicit = re.search(r">\\s*\\d+\\s*years\\s*and\\s*<=\\s*\\d+\\s*years", lowered)
    if explicit:
        return explicit.group(0).replace("  ", " "), benchmark
    bound_match = re.search(r"(?:<=|up to)\\s*(\\d+)\\s*years", lowered)
    if bound_match:
        bound = int(bound_match.group(1))
        bucket = TREASURY_UPPER_BOUND_BUCKETS.get(bound)
        if bucket:
            return bucket, benchmark
        return f"<= {bound} years", benchmark
    return "<= 2 years", benchmark


def _shift_year(value: str, years: int) -> str | None:
    parsed = _parse_date(value)
    if not parsed:
        return None
    try:
        return parsed.replace(year=parsed.year + years).strftime("%Y-%m-%d")
    except ValueError:
        adjusted = parsed - timedelta(days=1)
        return adjusted.replace(year=adjusted.year + years).strftime("%Y-%m-%d")


def _extract_attempts(payload: dict[str, Any], key: str | None) -> list[dict[str, Any]]:
    attempts = payload.get("attempts")
    if isinstance(attempts, dict) and key:
        value = attempts.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        return []
    if isinstance(attempts, list) and key is None:
        return [item for item in attempts if isinstance(item, dict)]
    return []


def _closest_attempt_date(
    target_date: str,
    attempts: list[dict[str, Any]],
) -> str | None:
    target = _parse_date(target_date)
    if not target:
        return None
    closest = None
    closest_delta = None
    for attempt in attempts:
        attempt_date = attempt.get("tradeDate") or attempt.get("date")
        if not isinstance(attempt_date, str):
            continue
        if not _coerce_bool(attempt.get("has_data")):
            continue
        parsed = _parse_date(attempt_date)
        if not parsed:
            continue
        delta = abs((parsed - target).days)
        if closest_delta is None or delta < closest_delta:
            closest_delta = delta
            closest = attempt_date
    return closest


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


def _extract_treasury_record(
    payload: Any,
    trade_date: str,
    years_to_maturity: str,
    benchmark: str,
) -> dict[str, Any] | None:
    for record in _normalize_records(payload):
        record_date = record.get("tradeDate")
        if not isinstance(record_date, str) or not record_date.startswith(trade_date):
            continue
        years = str(record.get("yearsToMaturity") or "").strip()
        record_benchmark = str(record.get("benchmark") or "").strip()
        if years == years_to_maturity and record_benchmark.lower() == benchmark.lower():
            return record
    return None


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


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "t"}
    return False


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
    dataset_name_lower = dataset_name_value.lower()
    is_weekly = "weeklysummary" in dataset_name_lower
    is_treasury = "treasurydailyaggregates" in dataset_name_lower
    if not is_weekly and question and _is_weekly_question(question):
        is_weekly = True
    if not is_treasury and question and _is_treasury_question(question):
        is_treasury = True
    notes = (
        "Run the FINRA data API and extract the answer based on the question; this includes "
        "deciding which FINRA dataset to use. Use MCP tools if available; otherwise run client_short.py. "
        "Pick the dataset based on the question: "
        "equity consolidatedShortInterest provides currentShortPositionQuantity for OTC short interest "
        "submissions across exchanges (rolling year by settlement date). "
        "equity weeklySummary provides totalWeeklyShareQuantity and weekStartDate/summaryStartDate for "
        "weekly OTC aggregate trade data. "
        "fixedIncomeMarket/treasuryDailyAggregates provides daily US Treasury volumes from TRACE; "
        "select the matching yearsToMaturity bucket (e.g., '> 5 years and <= 7 years') and "
        "benchmark ('On-the-run' or 'Off-the-run'). "
        "Data definitions: examples/finra/consolidatedShortInterestDescription.json, "
        "examples/finra/weeklySummaryDescription.json, "
        "examples/finra/treasuryDailyAggregatesDescription.json. "
        "Sample payloads: examples/finra/consolidatedShortInterest.sample.json, "
        "examples/finra/weeklySummary.sample.json, "
        "examples/finra/treasuryDailyAggregates.sample.json. "
        "Use dataset_group/dataset_name if provided. "
        "For symbol lists, return best_symbol/best_quantity across results. "
        "For Treasury max-volume questions, return best_years_to_maturity, "
        "best_dealer_customer_volume, and a candidates list of rows considered. "
        "If a trade date is a weekend/holiday, use the closest available tradeDate "
        "and include attempts with has_data flags. "
        "Try nearby dates where applicable and include attempts. Return JSON only."
    )

    if symbols:
        expected_response: dict[str, Any] = {
            "best_symbol": "string",
            "best_quantity": "number",
            "results": "array",
        }
        if is_weekly:
            expected_response = {
                "best_symbol": "string",
                "best_quantity": "number (totalWeeklyShareQuantity)",
                "results": "array",
            }
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
        if is_treasury:
            is_treasury_delta = bool(question and _is_treasury_delta_question(question))
            is_treasury_max = bool(question and _is_treasury_max_question(question))
            if is_treasury_delta:
                expected_response = {
                    "tradeDate": "YYYY-MM-DD",
                    "previous_trade_date": "YYYY-MM-DD",
                    "benchmark": "On-the-run|Off-the-run",
                    "best_years_to_maturity": "yearsToMaturity bucket string",
                    "best_dealer_customer_volume_delta": "number",
                    "record_current": "object (best row)",
                    "record_previous": "object (best row)",
                    "candidates_current": "array (rows considered)",
                    "candidates_previous": "array (rows considered)",
                    "attempts": "object (current/previous attempts)",
                }
            elif is_treasury_max:
                expected_response = {
                    "tradeDate": "YYYY-MM-DD",
                    "benchmark": "On-the-run|Off-the-run",
                    "best_years_to_maturity": "yearsToMaturity bucket string",
                    "best_dealer_customer_volume": "number",
                    "record": "object (best row)",
                    "candidates": "array (rows considered)",
                    "attempts": "array (date attempts)",
                }
            else:
                expected_response = {
                    "tradeDate": "YYYY-MM-DD",
                    "dealerCustomerVolume": "number",
                    "yearsToMaturity": "<= 2 years",
                    "benchmark": "On-the-run",
                    "record": "object (raw dataset row)",
                    "attempts": "array (date attempts)",
                }
            payload = {
                "task": "treasury_daily_aggregate",
                "client_short_path": CLIENT_SHORT_PATH,
                "args": {
                    "trade_date": "",
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
            if is_weekly:
                expected_response = {
                    "symbol": "string",
                    "weekStartDate": "YYYY-MM-DD",
                    "totalWeeklyShareQuantity": "number",
                    "record": "object (raw dataset row)",
                }
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
        question = _normalize_question(request.config.get("question"))
        dataset_name_eval = (
            request.config.get("dataset_name_eval")
            or request.config.get("datasetNameEval")
            or request.config.get("dataset_name")
            or request.config.get("datasetName")
        )
        is_treasury = bool(
            dataset_name_eval
            and "treasurydailyaggregates" in str(dataset_name_eval).lower()
        )
        if not is_treasury and _is_treasury_question(question):
            is_treasury = True
        if symbols and symbol:
            return False, "Provide either symbol or symbols, not both."
        if not symbols and not symbol and not is_treasury:
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
        if "trade_date" in request_payload_obj.get("args", {}):
            request_payload_obj["args"]["trade_date"] = requested_date
        else:
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
        requested_dataset_group = (
            request.config.get("dataset_group_eval")
            or request.config.get("datasetGroupEval")
            or request.config.get("dataset_group")
            or request.config.get("datasetGroup")
        )
        requested_dataset_name = (
            request.config.get("dataset_name_eval")
            or request.config.get("datasetNameEval")
            or request.config.get("dataset_name")
            or request.config.get("datasetName")
        )
        requested_dataset_name_value = (
            str(requested_dataset_name).strip() if requested_dataset_name else ""
        )
        requested_is_treasury = "treasurydailyaggregates" in requested_dataset_name_value.lower()
        if not requested_is_treasury and _is_treasury_question(question):
            requested_is_treasury = True
        if requested_dataset_name:
            requested_is_weekly = "weeklysummary" in requested_dataset_name_value.lower()
        else:
            requested_is_weekly = _is_weekly_question(question)
        if requested_is_treasury:
            requested_is_weekly = False


        try:
            parsed = _load_response_json(purple_response)
        except Exception as exc:
            errors.append(f"Failed to parse JSON response: {exc}")

        if (
            isinstance(parsed, dict)
            and isinstance(parsed.get("treasury_daily_aggregate"), dict)
        ):
            treasury_payload = parsed["treasury_daily_aggregate"]
            outer_dataset_name = parsed.get("dataset_name") or parsed.get("datasetName")
            outer_dataset_group = parsed.get("dataset_group") or parsed.get("datasetGroup")
            if outer_dataset_name and not (
                treasury_payload.get("dataset_name") or treasury_payload.get("datasetName")
            ):
                treasury_payload["dataset_name"] = outer_dataset_name
            if outer_dataset_group and not (
                treasury_payload.get("dataset_group") or treasury_payload.get("datasetGroup")
            ):
                treasury_payload["dataset_group"] = outer_dataset_group
            parsed = treasury_payload

        response_dataset_name = None
        response_dataset_group = None
        response_is_weekly = None
        if isinstance(parsed, dict):
            response_dataset_name = parsed.get("dataset_name") or parsed.get("datasetName")
            response_dataset_group = parsed.get("dataset_group") or parsed.get("datasetGroup")
            if response_dataset_name:
                response_is_weekly = "weeklysummary" in str(response_dataset_name).lower()

        if question or requested_dataset_name or requested_dataset_group:
            if not response_dataset_name:
                errors.append("Purple response missing dataset_name.")
            else:
                if requested_is_treasury:
                    expected_dataset = "treasuryDailyAggregates"
                else:
                    expected_dataset = (
                        "weeklySummary" if requested_is_weekly else "consolidatedShortInterest"
                    )
                if str(response_dataset_name).lower() != expected_dataset.lower():
                    errors.append(
                        f"Dataset mismatch: expected {expected_dataset}, got {response_dataset_name}."
                    )
            if requested_dataset_group:
                if not response_dataset_group:
                    errors.append("Purple response missing dataset_group.")
                elif str(response_dataset_group).lower() != str(requested_dataset_group).lower():
                    errors.append(
                        f"Dataset group mismatch: expected {requested_dataset_group}, got {response_dataset_group}."
                    )

        is_treasury = requested_is_treasury
        is_weekly = (
            requested_is_weekly if (question or requested_dataset_name) else bool(response_is_weekly)
        )
        metric_label = (
            "dealer customer volume"
            if is_treasury
            else ("weekly share" if is_weekly else "short interest")
        )

        if is_treasury:
            is_delta = _is_treasury_delta_question(question)
            is_max = _is_treasury_max_question(question) and not is_delta
            bucket_explicit = _has_treasury_bucket(question)
            expected_years, expected_benchmark = _parse_treasury_bucket(question)
            record = None
            if isinstance(parsed, dict):
                record = parsed.get("record")

            if is_delta:
                if not isinstance(parsed, dict):
                    errors.append("Purple response missing JSON object for treasury delta.")
                attempts_current = (
                    _extract_attempts(parsed, "current") if isinstance(parsed, dict) else []
                )
                attempts_previous = (
                    _extract_attempts(parsed, "previous") if isinstance(parsed, dict) else []
                )
                expected_previous_date = _shift_year(expected_date, -1) or ""
                resolved_current_date = (
                    parsed.get("tradeDate") if isinstance(parsed, dict) else None
                )
                resolved_previous_date = (
                    parsed.get("previous_trade_date")
                    if isinstance(parsed, dict)
                    else None
                )
                closest_current = (
                    _closest_attempt_date(expected_date, attempts_current)
                    if attempts_current
                    else None
                )
                closest_previous = (
                    _closest_attempt_date(expected_previous_date, attempts_previous)
                    if attempts_previous
                    else None
                )
                if closest_current and resolved_current_date != closest_current:
                    errors.append(
                        "Current trade date is not the closest available date."
                    )
                if closest_previous and resolved_previous_date != closest_previous:
                    errors.append(
                        "Previous trade date is not the closest available date."
                    )

                candidates_current = (
                    _normalize_records(parsed.get("candidates_current"))
                    if isinstance(parsed, dict)
                    else []
                )
                candidates_previous = (
                    _normalize_records(parsed.get("candidates_previous"))
                    if isinstance(parsed, dict)
                    else []
                )
                if not candidates_current or not candidates_previous:
                    errors.append("Treasury candidates missing for delta evaluation.")

                def _bucket_map(records: list[dict[str, Any]]) -> dict[str, float]:
                    buckets: dict[str, float] = {}
                    for item in records:
                        benchmark = str(item.get("benchmark") or "").strip()
                        if benchmark.lower() != expected_benchmark.lower():
                            continue
                        years = str(item.get("yearsToMaturity") or "").strip()
                        if bucket_explicit and years != expected_years:
                            continue
                        volume = _coerce_number(item.get("dealerCustomerVolume"))
                        if volume is None:
                            continue
                        current = buckets.get(years)
                        if current is None or volume > current:
                            buckets[years] = volume
                    return buckets

                current_map = _bucket_map(candidates_current)
                previous_map = _bucket_map(candidates_previous)
                shared = {key for key in current_map if key in previous_map}
                if not shared:
                    errors.append("No overlapping maturity buckets for delta evaluation.")

                expected_best_years = ""
                expected_delta = None
                if shared:
                    expected_best_years = max(
                        shared, key=lambda key: current_map[key] - previous_map[key]
                    )
                    expected_delta = current_map[expected_best_years] - previous_map[
                        expected_best_years
                    ]

                response_best_years = str(
                    parsed.get("best_years_to_maturity")
                    or parsed.get("bestYearsToMaturity")
                    or ""
                ).strip() if isinstance(parsed, dict) else ""
                response_delta = (
                    _coerce_number(
                        parsed.get("best_dealer_customer_volume_delta")
                        or parsed.get("bestDealerCustomerVolumeDelta")
                    )
                    if isinstance(parsed, dict)
                    else None
                )
                if expected_best_years and response_best_years != expected_best_years:
                    errors.append(
                        "Best yearsToMaturity mismatch: "
                        f"expected '{expected_best_years}', got {response_best_years}."
                    )
                if expected_delta is not None and response_delta is not None:
                    if abs(response_delta - expected_delta) > 1e-6:
                        errors.append(
                            "Best dealerCustomerVolume delta mismatch: "
                            f"expected {expected_delta}, got {response_delta}."
                        )
                if response_best_years == "":
                    errors.append("Missing best_years_to_maturity for delta question.")
                if response_delta is None:
                    errors.append(
                        "Missing best_dealer_customer_volume_delta for delta question."
                    )

                record_current = (
                    parsed.get("record_current") if isinstance(parsed, dict) else None
                )
                record_previous = (
                    parsed.get("record_previous") if isinstance(parsed, dict) else None
                )

                status = "pass" if not errors else "fail"
                summary = (
                    "Treasury dealer customer volume delta "
                    f"{status} for trade date {expected_date}."
                )
                await updater.add_artifact(
                    parts=[
                        Part(root=TextPart(text=summary)),
                        Part(
                            root=DataPart(
                                data={
                                    "status": status,
                                    "errors": errors,
                                    "trade_date": expected_date,
                                    "previous_trade_date": expected_previous_date,
                                    "best_years_to_maturity": response_best_years,
                                    "best_dealer_customer_volume_delta": response_delta,
                                    "record_current": record_current,
                                    "record_previous": record_previous,
                                    "candidates_current": candidates_current,
                                    "candidates_previous": candidates_previous,
                                    "purple_response": parsed,
                                }
                            )
                        ),
                    ],
                    name="Result",
                )
                return

            if is_max:
                candidates: list[dict[str, Any]] = []
                if isinstance(parsed, dict):
                    candidate_payload = (
                        parsed.get("candidates")
                        or parsed.get("records")
                        or parsed.get("rows")
                        or parsed.get("data")
                        or parsed.get("results")
                    )
                    candidates = _normalize_records(candidate_payload)
                if not candidates:
                    errors.append("Purple response missing treasury candidates list.")

                matching: list[dict[str, Any]] = []
                for candidate in candidates:
                    candidate_date = candidate.get("tradeDate")
                    if not isinstance(candidate_date, str) or not candidate_date.startswith(
                        expected_date
                    ):
                        continue
                    candidate_benchmark = str(candidate.get("benchmark") or "").strip()
                    if candidate_benchmark.lower() != expected_benchmark.lower():
                        continue
                    candidate_years = str(candidate.get("yearsToMaturity") or "").strip()
                    if bucket_explicit and candidate_years != expected_years:
                        continue
                    matching.append(candidate)

                if not matching:
                    errors.append(
                        "No treasury candidates matched the requested trade date/benchmark."
                    )

                best_record = None
                best_volume = None
                if matching:
                    best_record = max(
                        matching,
                        key=lambda item: _coerce_number(item.get("dealerCustomerVolume"))
                        or float("-inf"),
                    )
                    best_volume = _coerce_number(best_record.get("dealerCustomerVolume"))
                    if best_volume is None:
                        errors.append(
                            "No dealerCustomerVolume values found in treasury candidates."
                        )

                response_best_years = ""
                response_best_volume = None
                if isinstance(parsed, dict):
                    response_best_years = str(
                        parsed.get("best_years_to_maturity")
                        or parsed.get("bestYearsToMaturity")
                        or parsed.get("yearsToMaturity")
                        or ""
                    ).strip()
                    response_best_volume = _coerce_number(
                        parsed.get("best_dealer_customer_volume")
                        or parsed.get("bestDealerCustomerVolume")
                        or parsed.get("best_quantity")
                        or parsed.get("dealerCustomerVolume")
                    )
                if not response_best_years and isinstance(record, dict):
                    response_best_years = str(
                        record.get("yearsToMaturity") or ""
                    ).strip()
                if response_best_volume is None and isinstance(record, dict):
                    response_best_volume = _coerce_number(
                        record.get("dealerCustomerVolume")
                    )
                if not response_best_years:
                    errors.append(
                        "Missing best_years_to_maturity for max-volume question."
                    )
                if response_best_volume is None:
                    errors.append(
                        "Missing best_dealer_customer_volume for max-volume question."
                    )

                if record is None and best_record is not None:
                    record = best_record

                if best_record is not None:
                    expected_best_years = str(best_record.get("yearsToMaturity") or "").strip()
                    if response_best_years and response_best_years != expected_best_years:
                        errors.append(
                            "Best yearsToMaturity mismatch: "
                            f"expected '{expected_best_years}', got {response_best_years}."
                        )
                    if (
                        best_volume is not None
                        and response_best_volume is not None
                        and abs(response_best_volume - best_volume) > 1e-6
                    ):
                        errors.append(
                            "Best dealerCustomerVolume mismatch: "
                            f"expected {best_volume}, got {response_best_volume}."
                        )

                if record is None:
                    errors.append("Missing treasury record for the requested trade date.")
                else:
                    record_date = record.get("tradeDate")
                    if not isinstance(record_date, str):
                        errors.append("Missing tradeDate on treasury record.")
                    else:
                        attempts = (
                            _extract_attempts(parsed, None) if isinstance(parsed, dict) else []
                        )
                        closest = (
                            _closest_attempt_date(expected_date, attempts)
                            if attempts
                            else None
                        )
                        if closest and record_date != closest:
                            errors.append(
                                "Trade date is not the closest available date."
                            )
                        elif not record_date.startswith(expected_date) and not closest:
                            errors.append(
                                f"Trade date mismatch: expected {expected_date}, got {record_date}."
                            )
                    record_benchmark = str(record.get("benchmark") or "").strip()
                    if record_benchmark.lower() != expected_benchmark.lower():
                        errors.append(
                            f"benchmark mismatch: expected '{expected_benchmark}', got {record_benchmark}."
                        )
                    if bucket_explicit:
                        record_years = str(record.get("yearsToMaturity") or "").strip()
                        if record_years != expected_years:
                            errors.append(
                                "yearsToMaturity mismatch: "
                                f"expected '{expected_years}', got {record_years}."
                            )

                status = "pass" if not errors else "fail"
                summary = (
                    f"Treasury max dealer customer volume {status} for trade date {expected_date}."
                )
                await updater.add_artifact(
                    parts=[
                        Part(root=TextPart(text=summary)),
                        Part(
                            root=DataPart(
                                data={
                                    "status": status,
                                    "errors": errors,
                                    "trade_date": expected_date,
                                    "best_years_to_maturity": response_best_years,
                                    "best_dealer_customer_volume": response_best_volume,
                                    "record": record,
                                    "candidates": candidates,
                                    "purple_response": parsed,
                                }
                            )
                        ),
                    ],
                    name="Result",
                )
                return

            record = None
            if isinstance(parsed, dict):
                record = parsed.get("record")
            if record is None:
                record = _extract_treasury_record(
                    parsed, expected_date, expected_years, expected_benchmark
                )
            volume = None
            if isinstance(parsed, dict):
                volume = parsed.get("dealerCustomerVolume")
            if volume is None and record:
                volume = record.get("dealerCustomerVolume")

            if record is None:
                errors.append("Missing treasury record for the requested trade date.")
            else:
                record_date = record.get("tradeDate")
                if not isinstance(record_date, str):
                    errors.append("Missing tradeDate on treasury record.")
                else:
                    attempts = (
                        _extract_attempts(parsed, None) if isinstance(parsed, dict) else []
                    )
                    closest = (
                        _closest_attempt_date(expected_date, attempts)
                        if attempts
                        else None
                    )
                    if closest and record_date != closest:
                        errors.append(
                            "Trade date is not the closest available date."
                        )
                    elif not record_date.startswith(expected_date) and not closest:
                        errors.append(
                            f"Trade date mismatch: expected {expected_date}, got {record_date}."
                        )
                years = str(record.get("yearsToMaturity") or "").strip()
                if years != expected_years:
                    errors.append(
                        f"yearsToMaturity mismatch: expected '{expected_years}', got {years}."
                    )
                benchmark = str(record.get("benchmark") or "").strip()
                if benchmark.lower() != expected_benchmark.lower():
                    errors.append(
                        f"benchmark mismatch: expected '{expected_benchmark}', got {benchmark}."
                    )

            if volume is None or _coerce_number(volume) is None:
                errors.append("Missing dealerCustomerVolume for the requested trade date.")

            status = "pass" if not errors else "fail"
            summary = (
                f"Treasury dealer customer volume {status} for trade date {expected_date}."
            )

            await updater.add_artifact(
                parts=[
                    Part(root=TextPart(text=summary)),
                    Part(
                        root=DataPart(
                            data={
                                "status": status,
                                "errors": errors,
                                "trade_date": expected_date,
                                "dealerCustomerVolume": _coerce_number(volume),
                                "record": record,
                                "purple_response": parsed,
                            }
                        )
                    ),
                ],
                name="Result",
            )
            return

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
