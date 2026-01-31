# A2A Agent Template

A minimal template for building [A2A (Agent-to-Agent)](https://a2a-protocol.org/latest/) green agents compatible with the [AgentBeats](https://agentbeats.dev) platform.

## Project Structure

```
src/
├─ server.py      # Server setup and agent card configuration
├─ executor.py    # A2A request handling
├─ agent.py       # Your agent implementation goes here
└─ messenger.py   # A2A messaging utilities
tests/
└─ test_agent.py  # Agent tests
Dockerfile        # Docker configuration
pyproject.toml    # Python dependencies
.github/
└─ workflows/
   └─ test-and-publish.yml # CI workflow
```

## Getting Started

1. **Create your repository** - Click "Use this template" to create your own repository from this template

2. **Implement your agent** - Add your agent logic to [`src/agent.py`](src/agent.py)

3. **Configure your agent card** - Fill in your agent's metadata (name, skills, description) in [`src/server.py`](src/server.py)

4. **Write your tests** - Add custom tests for your agent in [`tests/test_agent.py`](tests/test_agent.py)

For a concrete example of implementing a green agent using this template, see this [draft PR](https://github.com/RDI-Foundation/green-agent-template/pull/3).

## Running Locally

```bash
# Install dependencies
uv sync

# Run the server
uv run src/server.py
```

## Assessment Runner

This repo includes `send_assessment.py` to trigger the green → purple evaluation.

```bash
export FINRA_CLIENT_ID="..."
export FINRA_CLIENT_SECRET="..."

python send_assessment.py \
  --green-url http://127.0.0.1:9009 \
  --purple-url http://127.0.0.1:9010 \
  --target-month 5 \
  --sample-size 10 \
  --http-timeout 180
```

By default, `send_assessment.py` reads symbols from `SP500symbols.csv` in the repo
root. Override with `--symbols-csv` or pass `--symbols`.

Note: the purple agent can optionally use an MCP server for FINRA lookups. If so,
set `MCP_SERVER_COMMAND` on the purple runtime (see PurpleAgentWitty README).

## Benchmark Information

Purple connects to FINRA data via the API (available through the MCP server) and
answers a set of 10 questions. The first step is deciding which dataset to use:
`consolidatedShortInterest`, `weeklySummary`, or `treasuryDailyAggregates`.

The datasets map to question types:
- **consolidatedShortInterest:** short interest for multiple S&P 500 symbols, then
  select the symbol with the highest short interest. If the requested date is a
  weekend/holiday, the agent must pick the closest available date with data.
- **weeklySummary:** weekly trading volumes for specific symbols.
- **treasuryDailyAggregates:** US Treasury trading activity, including identifying
  the maturity bucket (e.g., `> 5 years and <= 7 years`) with the highest activity.

This results in analytical answers such as which symbol has the highest short
interest on a given date, or which Treasury maturity bucket has the highest
dealer customer volume on a given date.

Implementation details:
The environment exposes FINRA data through an MCP tool (or the client API). The
goal is to answer each question by selecting the correct dataset, retrieving the
required records, and computing the requested value (for example, the max short
interest or dealer customer volume). The state includes the question, requested
date, symbol list (if any), and any data retrieved so far. Purple actions include
choosing a dataset, calling the FINRA API/MCP tool with parameters, and deciding
whether to make additional calls (such as nearby dates) before returning a final
JSON response. A task ends when a complete, correctly formatted response is
returned or when the maximum allowed attempts are exhausted.

## Evaluation

Each case is evaluated by the green agent and returns `pass` or `fail`.

Evaluation is based on correctness and completeness. Metrics include selecting
the correct dataset, returning all required records/fields, meeting minimum
attempt counts, choosing the closest available date when the requested date has
no data, and computing the correct best symbol/quantity or treasury maturity
bucket. Treasury questions also validate benchmark matching and year-over-year
delta calculations where required. A case passes only if all checks succeed.

Checks performed per case:
- **Dataset selection:** if `question` or `dataset_name_eval` is provided, the purple
  response must include `dataset_name` and it must match the expected dataset
  (`consolidatedShortInterest` vs `weeklySummary` vs `treasuryDailyAggregates`).
- **Results present:** for multi-symbol cases, a `results` list must be returned and
  cover every requested symbol.
- **Attempts count:** each symbol must have at least `MIN_ATTEMPTS` attempts.
- **Closest date:** the chosen date must be the closest available date to the
  requested date (based on attempted dates).
- **Numeric metric:** short-interest cases require `currentShortPositionQuantity`,
  weekly cases require `totalWeeklyShareQuantity`, treasury cases require
  `dealerCustomerVolume`.
- **Best symbol/quantity:** the reported best symbol/quantity must match the max
  computed from the valid attempts.

Example failure reasons:
We designed test cases to trigger common failure modes and confirmed green flags
them. Example: if a weekly-summary question is answered with
`consolidatedShortInterest`, green fails with a dataset-mismatch error. If the
purple agent returns a non-closest trade date (e.g., using `2024-01-27` instead
of `2024-01-26`), green fails the closest-date check. If required fields like
`totalWeeklyShareQuantity` or `dealerCustomerVolume` are missing, green fails
the case.
- **Dataset mismatch:** weekly question answered with `consolidatedShortInterest`
  instead of `weeklySummary`, so evaluation fails with a dataset mismatch.
- **Treasury record missing:** no matching `treasuryDailyAggregates` record found
  for the requested trade date (or closest available date), so the required
  `dealerCustomerVolume` and record are missing.
- **Delta date not closest:** for year-over-year treasury deltas, the `previous_trade_date`
  must be the closest available date to the requested prior year; if the response
  reports a non-closest date (e.g., 2024-01-27 when data exists for 2024-01-26),
  the case fails.

Example trajectories:
- **Correct dataset + closest date + correct max:** passes.
- **Wrong dataset:** fails with a dataset mismatch.
- **Closest date not used:** fails the closest-date check.
- **Treasury delta mismatch:** fails if `previous_trade_date` is not the closest
  available date.
- **Missing required fields:** fails if `totalWeeklyShareQuantity` or
  `dealerCustomerVolume` is missing where required.

Dataset guidance:
The benchmark data and test cases span three FINRA datasets via the MCP server,
covering short-interest ranking across randomly sampled S&P 500 symbols (3/5/10),
weekly share volume comparisons, and fixed-income treasury maturity questions
including max volume and year-over-year deltas. These cases are designed to
exercise dataset selection, closest-date handling, required fields, and max/summary
computations in the green evaluation.
- **Equity consolidatedShortInterest:** OTC short interest submissions across exchanges.
  Use `currentShortPositionQuantity` (current cycle) and settlement dates.
- **Equity weeklySummary:** weekly OTC aggregate trade data with `totalWeeklyShareQuantity`
  and `weekStartDate`/`summaryStartDate`.
- **Fixed income treasuryDailyAggregates:** TRACE daily US Treasury volume. Select the
  matching `yearsToMaturity` bucket (e.g., `<= 2 years`, `> 5 years and <= 7 years`)
  and `benchmark` (`On-the-run` or `Off-the-run`) and return `dealerCustomerVolume`.
  The evaluator parses maturity phrases like `<= 2 years` or `up to 7 years` into
  these buckets:
  - `<= 2 years`
  - `> 2 years and <= 3 years`
  - `> 3 years and <= 5 years`
  - `> 5 years and <= 7 years`
  - `> 7 years and <= 10 years`

Scoring:
- Each case is marked `pass` if no errors are found, otherwise `fail`.
- Leaderboard runs count `passed` vs `total` and report overall `pass` only when
  all cases pass.

## Evaluation Internals

Evaluation happens in `/home/wczubal1/projects/GreenAgentWitty/src/agent.py` and
follows these steps:

- **Normalize config:** `_normalize_question`, `_normalize_symbols`, `_is_weekly_question`,
  `_is_treasury_question` to infer dataset intent when not explicitly provided.
- **Build purple request:** `_build_purple_request` assembles the request payload,
  including dataset expectations and response shape.
- **Parse purple response:** `_load_response_json` parses the JSON response and
  dataset selection is validated against `dataset_name_eval`/`dataset_group_eval`
  (or question-based inference).
- **Multi-symbol checks:** `_extract_results` ensures all symbols are present,
  `MIN_ATTEMPTS` are met, the chosen date is the closest to the requested date,
  and `best_symbol`/`best_quantity` match the computed max from valid attempts.
- **Single-symbol checks:** `_extract_quantity` validates symbol/date alignment and
  required metrics (`currentShortPositionQuantity` or `totalWeeklyShareQuantity`).
- **Treasury checks:** `_extract_treasury_record` finds the row for the requested
  trade date and verifies `yearsToMaturity`, `benchmark`, and `dealerCustomerVolume`.

## Examples

Sample payloads (returned data shapes):
- `examples/finra/consolidatedShortInterest.sample.json`
- `examples/finra/weeklySummary.sample.json`
- `examples/finra/treasuryDailyAggregates.sample.json`

Dataset descriptions:
- `examples/finra/consolidatedShortInterestDescription.json`
- `examples/finra/weeklySummaryDescription.json`
- `examples/finra/treasuryDailyAggregatesDescription.json`

## Make Targets

```bash
make run
make send
make docker-build
make docker-run
```

## Running with Docker

```bash
# Build the image
docker build -t my-agent .

# Run the container
docker run -p 9009:9009 my-agent --host 0.0.0.0 --port 9009
```

## Testing

Run A2A conformance tests against your agent.

```bash
# Install test dependencies
uv sync --extra test

# Start your agent (uv or docker; see above)

# Run tests against your running agent URL
uv run pytest --agent-url http://localhost:9009
```

## Publishing

The repository includes a GitHub Actions workflow that automatically builds, tests, and publishes a Docker image of your agent to GitHub Container Registry.

If your agent needs API keys or other secrets, add them in Settings → Secrets and variables → Actions → Repository secrets. They'll be available as environment variables during CI tests.

- **Push to `main`** → publishes `latest` tag:
```
ghcr.io/<your-username>/<your-repo-name>:latest
```

- **Create a git tag** (e.g. `git tag v1.0.0 && git push origin v1.0.0`) → publishes version tags:
```
ghcr.io/<your-username>/<your-repo-name>:1.0.0
ghcr.io/<your-username>/<your-repo-name>:1
```

Once the workflow completes, find your Docker image in the Packages section (right sidebar of your repository). Configure the package visibility in package settings.

> **Note:** Organization repositories may need package write permissions enabled manually (Settings → Actions → General). Version tags must follow [semantic versioning](https://semver.org/) (e.g., `v1.0.0`).
