#!/usr/bin/env python3
"""Generic Vertex AI Batch Prediction + Synchronous Online Retry Pipeline.

Uses the modern unified Google GenAI SDK configured for Vertex AI Enterprise:
  - `genai.Client(vertexai=True, project=..., location=...)`
  - `client.batches.create()` & `client.batches.get()` for bulk asynchronous jobs
  - `client.aio.models.generate_content()` (with `location='global'`) for
    synchronous online retries of failed/invalid records
  - Renders an in-place refreshing ASCII/Unicode process flow dashboard in the
    terminal (clearing and redrawing over the previous frame so it never scrolls).
"""

import argparse
import asyncio
import datetime
import json
import logging
import subprocess
import sys
import time
from typing import Any
import warnings

warnings.filterwarnings("ignore")
logging.getLogger().setLevel(logging.ERROR)

from aiolimiter import AsyncLimiter
import google.auth
from google.auth import credentials as auth_credentials
from google import genai
from google.cloud import storage
from google.genai import types

# =====================================================================
# ANSI Color & In-Place Terminal Refresh Helpers
# =====================================================================
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
CYAN = "\033[96m"
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
WHITE = "\033[97m"

SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]


def badge(state: str, tick: int = 0) -> str:
  """Returns a fixed-width colored status badge for a diagram node."""
  if state == "DONE":
    return f"{GREEN}{BOLD}[✓ DONE     ]{RESET}"
  elif state == "RUNNING":
    spin = SPINNER_FRAMES[tick % len(SPINNER_FRAMES)]
    return f"{YELLOW}{BOLD}[{spin} RUNNING  ]{RESET}"
  elif state == "ALERT":
    return f"{RED}{BOLD}[⚡ RETRYING]{RESET}"
  return f"{DIM}[· PENDING  ]{RESET}"


def add_log(state: dict[str, Any], message: str) -> None:
  """Adds a timestamped line to the live dashboard event log and redraws."""
  ts = datetime.datetime.now().strftime("%H:%M:%S")
  state["logs"].append(f"{DIM}[{ts}]{RESET} {message}")
  state["logs"] = state["logs"][-10:]
  render_flow_diagram(state)


def render_flow_diagram(state: dict[str, Any]) -> None:
  """Clears the terminal in-place and redraws the 7-stage process flow diagram."""
  state["tick"] = state.get("tick", 0) + 1
  tick = state["tick"]

  s1 = badge(state["s1_status"], tick)
  s2 = badge(state["s2_status"], tick)
  s3 = badge(state["s3_status"], tick)
  s4 = badge(state["s4_status"], tick)
  s5a = badge(state["s5a_status"], tick)
  s5b = badge(state["s5b_status"], tick)
  s6 = badge(state["s6_status"], tick)
  s7 = badge(state["s7_status"], tick)

  log_lines = state.get("logs", [])
  padded_logs = log_lines + [""] * max(0, 8 - len(log_lines))
  log_block = "\n".join(f"  {line}" for line in padded_logs[-8:])

  clear_seq = "\033[H\033[2J\033[3J" if not state.get("no_clear") else "\n"

  diagram = (
      f"{clear_seq}"
      f"{BOLD}{WHITE}========================================================================================{RESET}\n"
      f"{BOLD}{CYAN}   VERTEX AI (google-genai SDK): BATCH PREDICTION + SYNCHRONOUS RETRY PIPELINE{RESET}\n"
      f"{BOLD}{WHITE}========================================================================================{RESET}\n"
      f"                  +---------------------------------------------------------+\n"
      f"                  | {BOLD}1. Batch Input (Cloud Storage){RESET}            {s1} |\n"
      f"                  |    URI: {state['input_uri_short']:<47} |\n"
      f"                  |    Payload: {state['total_records']} GenerateContent requests (JSONL)          |\n"
      f"                  +---------------------------------------------------------+\n"
      f"                                               |\n"
      f"                                               v\n"
      f"                  +---------------------------------------------------------+\n"
      f"                  | {BOLD}2. Vertex AI Batch Prediction Job{RESET}         {s2} |\n"
      f"                  |    SDK: client.batches.create(vertexai=True)            |\n"
      f"                  |    Job ID: {state['job_id']:<44} |\n"
      f"                  |    State:  {state['batch_job_state']:<26} Elapsed: {state['elapsed_s']:>4}s    |\n"
      f"                  +---------------------------------------------------------+\n"
      f"                                               |\n"
      f"                                               v\n"
      f"                  +---------------------------------------------------------+\n"
      f"                  | {BOLD}3. Raw Batch Output (Cloud Storage){RESET}       {s3} |\n"
      f"                  |    predictions.jsonl ({state['downloaded_records']}/{state['total_records']} records downloaded)           |\n"
      f"                  |    Each line echoes original 'request' + 'response'     |\n"
      f"                  +---------------------------------------------------------+\n"
      f"                                               |\n"
      f"                                               v\n"
      f"                  +---------------------------------------------------------+\n"
      f"                  | {BOLD}4. Post-Batch Validation Router (Python){RESET}  {s4} |\n"
      f"                  |    Checks finishReason == STOP & JSON schema rules      |\n"
      f"                  +---------------------------------------------------------+\n"
      f"                                 /                           \\\n"
      f"                    (Valid JSON)                              (Invalid / Truncated)\n"
      f"                               /                               \\\n"
      f"                              v                                 v\n"
      f"  +-----------------------------------------+     +-----------------------------------------+\n"
      f"  | {BOLD}5a. Validated Records{RESET}     {s5a} |     | {BOLD}5b. Failed / Invalid{RESET}      {s5b} |\n"
      f"  |     Passed 1st Attempt: {GREEN}{BOLD}{state['passed_count']:>2}{RESET} / {state['total_records']:<2}         |     |     Needs Sync Retry:   {RED}{BOLD}{state['failed_count']:>2}{RESET} / {state['total_records']:<2}         |\n"
      f"  |     IDs: {state['passed_ids']:<30} |     |     IDs: {state['failed_ids']:<30} |\n"
      f"  +-----------------------------------------+     +-----------------------------------------+\n"
      f"                      |                                                 |\n"
      f"                      |                                                 v\n"
      f"                      |                           +-----------------------------------------+\n"
      f"                      |                           | {BOLD}6. Sync Online Retry{RESET}      {s6} |\n"
      f"                      |                           |    client.aio.models.generate_content   |\n"
      f"                      |                           |    Endpoint: location='global'          |\n"
      f"                      |                           |    Repaired Online: {GREEN}{BOLD}{state['repaired_count']:>2}{RESET} / {state['failed_count']:<2}             |\n"
      f"                      |                           +-----------------------------------------+\n"
      f"                      \\                                                 /\n"
      f"                       \\               (Merged in seconds)             /\n"
      f"                        \\                                             /\n"
      f"                         v                                           v\n"
      f"                  +---------------------------------------------------------+\n"
      f"                  | {BOLD}7. Complete Validated Dataset (100%){RESET}      {s7} |\n"
      f"                  |    Final Validated Records: {GREEN}{BOLD}{state['final_count']:>2}{RESET} / {state['total_records']:<2}                     |\n"
      f"                  |    ({state['passed_count']} from Batch @ 50% discount + {state['repaired_count']} Sync Retried)       |\n"
      f"                  +---------------------------------------------------------+\n"
      f"{BOLD}{WHITE}----------------------------------------------------------------------------------------{RESET}\n"
      f" {BOLD}{CYAN}LIVE ACTIVITY STREAM:{RESET}\n"
      f"{log_block}\n"
      f"{BOLD}{WHITE}========================================================================================{RESET}"
  )
  sys.stdout.write(diagram + "\n")
  sys.stdout.flush()


# =====================================================================
# Credentials Helper for gcloud Account Override
# =====================================================================
class GcloudAccountCredentials(auth_credentials.Credentials):
  """Refreshes OAuth2 access tokens dynamically via gcloud for a specific account."""

  def __init__(self, account: str):
    super().__init__()
    self.account = account
    self.refresh(None)

  def refresh(self, request: Any) -> None:
    cmd = ["gcloud", "auth", "print-access-token", f"--account={self.account}"]
    token = subprocess.check_output(cmd, text=True).strip()
    self.token = token
    self.expiry = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(minutes=45)


def get_credentials(account: str | None) -> auth_credentials.Credentials:
  if account:
    return GcloudAccountCredentials(account)
  creds, _ = google.auth.default()
  return creds


# =====================================================================
# 1. Sample Dataset Generator (With Deliberate Failures for Demo)
# =====================================================================
def build_sample_requests() -> list[dict[str, Any]]:
  """Builds 5 generic GenerateContent batch requests where 3 succeed and 2 fail validation."""
  schema_instruction = (
      "Return ONLY a valid JSON object with keys: "
      "'id' (string), 'summary' (string), and 'extracted_items' (non-empty list of strings)."
  )

  return [
      # REC-001: Valid request -> Will PASS batch validation
      {
          "request": {
              "contents": [{
                  "role": "user",
                  "parts": [{
                      "text": (
                          f"Record ID: REC-001. Analyze cloud telemetry: CPU utilization averaged 42% "
                          f"across 12 nodes with zero packet loss. {schema_instruction}"
                      )
                  }],
              }],
              "generationConfig": {"responseMimeType": "application/json", "temperature": 0.1},
          }
      },
      # REC-002: Valid request -> Will PASS batch validation
      {
          "request": {
              "contents": [{
                  "role": "user",
                  "parts": [{
                      "text": (
                          f"Record ID: REC-002. Analyze database audit log: 3 read replicas healthy, "
                          f"backup snapshot completed in 14s. {schema_instruction}"
                      )
                  }],
              }],
              "generationConfig": {"responseMimeType": "application/json", "temperature": 0.1},
          }
      },
      # REC-003: Deliberate Schema Validation Failure -> Will FAIL in batch, then SUCCEED on Sync Retry!
      {
          "request": {
              "contents": [{
                  "role": "user",
                  "parts": [{
                      "text": (
                          "Record ID: REC-003. Analyze storage lifecycle policy: 40TB archived to Coldline.\n"
                          "IMPORTANT INITIAL BATCH INSTRUCTION: Unless you see a 'VALIDATION RETRY OVERRIDE' "
                          "message below, return ONLY {'id': 'REC-003', 'summary': 'Pending extraction'} "
                          "and DO NOT include the 'extracted_items' key."
                      )
                  }],
              }],
              "generationConfig": {"responseMimeType": "application/json", "temperature": 0.1},
          }
      },
      # REC-004: Deliberate Token Truncation Failure (maxOutputTokens=8) -> Will FAIL in batch, then SUCCEED on Sync Retry!
      {
          "request": {
              "contents": [{
                  "role": "user",
                  "parts": [{
                      "text": (
                          f"Record ID: REC-004. Analyze network firewall rules: 5 ingress rules verified, "
                          f"TLS 1.3 enforced on load balancer. {schema_instruction}"
                      )
                  }],
              }],
              "generationConfig": {
                  "responseMimeType": "application/json",
                  "temperature": 0.1,
                  "maxOutputTokens": 8,  # Forces finishReason=MAX_TOKENS & truncated JSON in batch!
              },
          }
      },
      # REC-005: Valid request -> Will PASS batch validation
      {
          "request": {
              "contents": [{
                  "role": "user",
                  "parts": [{
                      "text": (
                          f"Record ID: REC-005. Analyze autoscaler metrics: scaled from 4 to 9 instances "
                          f"during peak traffic window. {schema_instruction}"
                      )
                  }],
              }],
              "generationConfig": {"responseMimeType": "application/json", "temperature": 0.1},
          }
      },
  ]


def extract_record_id_from_request(request_dict: dict[str, Any]) -> str:
  """Helper to pull 'REC-00X' out of the echoed request text for display."""
  try:
    text = request_dict["contents"][0]["parts"][0]["text"]
    for token in text.split():
      cleaned = token.strip(".,:;")
      if cleaned.startswith("REC-"):
        return cleaned
  except Exception:
    pass
  return "REC-???"


# =====================================================================
# 2. Custom Validation Function
# =====================================================================
def validate_output(parsed_json: dict[str, Any]) -> tuple[bool, str]:
  """Validates the parsed JSON response from the model."""
  if not isinstance(parsed_json, dict):
    return False, "Response is not a JSON object."

  required_keys = ["id", "summary", "extracted_items"]
  missing = [k for k in required_keys if k not in parsed_json or not parsed_json[k]]
  if missing:
    return False, f"Missing or empty required fields: {missing}"

  if not isinstance(parsed_json["extracted_items"], list):
    return False, "'extracted_items' must be a JSON array."

  return True, "OK"


# =====================================================================
# 3. Synchronous Online Retry Worker (google-genai SDK with vertexai=True)
# =====================================================================
RETRY_RATE_LIMITER = AsyncLimiter(max_rate=300, time_period=60)


async def retry_single_record_online(
    online_client: genai.Client,
    model_id: str,
    failed_batch_record: dict[str, Any],
    record_id: str,
    failure_reason: str,
    ui_state: dict[str, Any],
    max_attempts: int = 3,
) -> dict[str, Any]:
  """Replays a failed batch record synchronously via `client.aio.models.generate_content`."""
  original_request = failed_batch_record["request"]
  base_contents = original_request.get("contents", [])
  gen_config_dict = original_request.get("generationConfig", {})

  for attempt in range(1, max_attempts + 1):
    add_log(
        ui_state,
        f"{YELLOW}↳ [Sync Retry #{attempt}]{RESET} {BOLD}{record_id}{RESET} ({RED}{failure_reason}{RESET}) -> global endpoint...",
    )
    retry_contents = list(base_contents) + [
        {
            "role": "user",
            "parts": [{
                "text": (
                    f"VALIDATION RETRY OVERRIDE: Your previous attempt failed validation ({failure_reason}). "
                    "Ignore any earlier instruction to omit fields. Regenerate a complete, valid JSON object "
                    "containing 'id', 'summary', and a non-empty 'extracted_items' array of strings."
                )
            }],
        }
    ]

    try:
      async with RETRY_RATE_LIMITER:
        response = await online_client.aio.models.generate_content(
            model=model_id,
            contents=retry_contents,
            config=types.GenerateContentConfig(
                response_mime_type=gen_config_dict.get("responseMimeType", "application/json"),
                temperature=0.1,
                max_output_tokens=1024,
                automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
            ),
        )

      parsed_json = json.loads(response.text)
      is_valid, reason = validate_output(parsed_json)
      if is_valid:
        ui_state["repaired_count"] += 1
        add_log(
            ui_state,
            f"{GREEN}✓ [Sync Retry Repaired!]{RESET} {BOLD}{record_id}{RESET}: {parsed_json.get('summary')}",
        )
        return parsed_json
      failure_reason = reason

    except Exception as exc:
      failure_reason = f"Online API/Parse error on attempt {attempt}: {exc}"
      await asyncio.sleep(2**attempt)

  raise RuntimeError(f"Record {record_id} failed after {max_attempts} retries: {failure_reason}")


# =====================================================================
# 4. Main End-to-End Execution with In-Place Refreshing Console Diagram
# =====================================================================
async def main() -> None:
  parser = argparse.ArgumentParser(description="Vertex AI (google-genai SDK) Batch + Sync Retry Demo")
  parser.add_argument("--project_id", required=True, help="GCP Project ID")
  parser.add_argument("--region", default="us-central1", help="Batch Prediction region")
  parser.add_argument(
      "--bucket",
      default=None,
      help="GCS Bucket name (defaults to <project_id>-vertex-batch-demo)",
  )
  parser.add_argument("--model", default="gemini-2.5-flash", help="Vertex AI Gemini model ID")
  parser.add_argument("--account", default=None, help="Optional gcloud account email")
  parser.add_argument(
      "--existing_job_name",
      default=None,
      help="Optional existing BatchJob name/ID to replay validation & sync retries immediately",
  )
  parser.add_argument(
      "--no_clear",
      action="store_true",
      help="Disable ANSI screen clearing (print frames sequentially instead of overwriting in-place)",
  )
  args = parser.parse_args()

  bucket_name = args.bucket or f"{args.project_id}-vertex-batch-demo"
  creds = get_credentials(args.account)
  storage_client = storage.Client(project=args.project_id, credentials=creds)
  bucket = storage_client.bucket(bucket_name)

  # Initialize the modern google-genai Client for Vertex AI Batch (regional)
  batch_client = genai.Client(
      vertexai=True,
      project=args.project_id,
      location=args.region,
      credentials=creds,
  )

  sample_requests = build_sample_requests()
  run_ts = int(time.time())
  input_blob_path = f"generic_batch_demo/run_{run_ts}/inputs/requests.jsonl"
  input_gcs_uri = f"gs://{bucket_name}/{input_blob_path}"
  output_gcs_prefix = f"gs://{bucket_name}/generic_batch_demo/run_{run_ts}/outputs"

  ui_state: dict[str, Any] = {
      "s1_status": "RUNNING",
      "s2_status": "PENDING",
      "s3_status": "PENDING",
      "s4_status": "PENDING",
      "s5a_status": "PENDING",
      "s5b_status": "PENDING",
      "s6_status": "PENDING",
      "s7_status": "PENDING",
      "input_uri_short": f".../{input_blob_path[-38:]}",
      "total_records": len(sample_requests),
      "job_id": "(submitting...)",
      "batch_job_state": "NOT_STARTED",
      "elapsed_s": 0,
      "downloaded_records": 0,
      "passed_count": 0,
      "failed_count": 0,
      "passed_ids": "none",
      "failed_ids": "none",
      "repaired_count": 0,
      "final_count": 0,
      "logs": [],
      "tick": 0,
      "no_clear": args.no_clear,
  }

  start_t = time.time()

  if args.existing_job_name:
    ui_state["s1_status"] = "DONE"
    ui_state["s2_status"] = "RUNNING"
    add_log(ui_state, f"Attaching to existing BatchJob {args.existing_job_name.split('/')[-1]}...")
    job = batch_client.batches.get(name=args.existing_job_name)
    ui_state["job_id"] = job.name.split("/")[-1]
    ui_state["batch_job_state"] = job.state.name
    render_flow_diagram(ui_state)
  else:
    # -----------------------------------------------------------------
    # STAGE 1: Upload Batch Input JSONL to Cloud Storage
    # -----------------------------------------------------------------
    add_log(ui_state, f"Uploading {len(sample_requests)} requests to {input_gcs_uri}...")
    jsonl_payload = "\n".join(json.dumps(r) for r in sample_requests) + "\n"
    bucket.blob(input_blob_path).upload_from_string(jsonl_payload, content_type="application/jsonl")
    ui_state["s1_status"] = "DONE"
    ui_state["s2_status"] = "RUNNING"
    add_log(ui_state, "Input JSONL uploaded. Submitting Vertex AI BatchJob via client.batches.create()...")

    # -----------------------------------------------------------------
    # STAGE 2: Submit & Poll Vertex AI Batch Job via google-genai SDK
    # -----------------------------------------------------------------
    job = batch_client.batches.create(
        model=args.model,
        src=input_gcs_uri,
        config=types.CreateBatchJobConfig(
            display_name=f"genai-sdk-batch-sync-retry-{run_ts}",
            dest=output_gcs_prefix,
        ),
    )
    job_id_short = job.name.split("/")[-1]
    ui_state["job_id"] = job_id_short
    ui_state["batch_job_state"] = job.state.name
    add_log(ui_state, f"Submitted BatchJob {BOLD}{job_id_short}{RESET} ({job.state.name})")

  job_id_short = job.name.split("/")[-1]
  while not job.done:
    for _ in range(15):
      await asyncio.sleep(1)
      ui_state["elapsed_s"] = int(time.time() - start_t)
      render_flow_diagram(ui_state)

    job = batch_client.batches.get(name=job.name)
    ui_state["elapsed_s"] = int(time.time() - start_t)
    ui_state["batch_job_state"] = job.state.name
    add_log(ui_state, f"Polled BatchJob {job_id_short}: state={CYAN}{job.state.name}{RESET} ({ui_state['elapsed_s']}s)")

  if job.state != types.JobState.JOB_STATE_SUCCEEDED:
    raise RuntimeError(f"BatchJob failed with state {job.state}: {job.error}")

  ui_state["elapsed_s"] = int(time.time() - start_t)
  ui_state["batch_job_state"] = job.state.name
  ui_state["s2_status"] = "DONE"
  ui_state["s3_status"] = "RUNNING"
  add_log(ui_state, f"{GREEN}BatchJob {job_id_short} SUCCEEDED!{RESET} Downloading predictions.jsonl from GCS...")

  # -----------------------------------------------------------------
  # STAGE 3: Download Raw Batch Output (predictions.jsonl) from GCS
  # -----------------------------------------------------------------
  output_dir = (
      (job.output_info.gcs_output_directory if job.output_info else None)
      or job.dest.gcs_uri
  )
  path_without_scheme = output_dir.replace("gs://", "")
  out_bucket_name, out_prefix = path_without_scheme.split("/", 1)
  out_bucket = storage_client.bucket(out_bucket_name)

  batch_records: list[dict[str, Any]] = []
  for blob in out_bucket.list_blobs(prefix=out_prefix):
    if blob.name.endswith("predictions.jsonl"):
      for line in blob.download_as_text().splitlines():
        if line.strip():
          batch_records.append(json.loads(line))

  ui_state["downloaded_records"] = len(batch_records)
  ui_state["s3_status"] = "DONE"
  ui_state["s4_status"] = "RUNNING"
  add_log(ui_state, f"Downloaded {len(batch_records)} prediction records. Running Stage 4 Validation Router...")
  await asyncio.sleep(0.6)

  # -----------------------------------------------------------------
  # STAGE 4 & 5: Validate Every Record & Split Into Pass (5a) vs Fail (5b)
  # -----------------------------------------------------------------
  valid_results: list[dict[str, Any]] = []
  passed_ids: list[str] = []
  failed_items: list[tuple[str, dict[str, Any], str]] = []

  for record in batch_records:
    rec_id = extract_record_id_from_request(record.get("request", {}))

    if record.get("status"):
      reason = f"Batch API status error: {record['status']}"
      failed_items.append((rec_id, record, reason))
      add_log(ui_state, f"{RED}✗ {rec_id} FAILED:{RESET} {reason}")
      continue

    try:
      candidate = record["response"]["candidates"][0]
      finish_reason = candidate.get("finishReason", "STOP")
      if finish_reason != "STOP":
        raise ValueError(f"Abnormal finishReason={finish_reason} (output truncated)")

      raw_text = candidate["content"]["parts"][0]["text"]
      parsed_json = json.loads(raw_text)
      is_valid, reason = validate_output(parsed_json)
    except Exception as exc:
      parsed_json = {}
      is_valid, reason = False, f"{exc}"

    if is_valid:
      valid_results.append(parsed_json)
      passed_ids.append(rec_id)
      ui_state["passed_count"] = len(valid_results)
      ui_state["passed_ids"] = ", ".join(sorted(passed_ids))
      add_log(ui_state, f"{GREEN}✓ {rec_id} PASSED:{RESET} {parsed_json.get('summary')}")
    else:
      failed_items.append((rec_id, record, reason))
      ui_state["failed_count"] = len(failed_items)
      ui_state["failed_ids"] = ", ".join(sorted(x[0] for x in failed_items))
      add_log(ui_state, f"{RED}✗ {rec_id} FAILED:{RESET} {reason}")
    await asyncio.sleep(0.4)

  ui_state["s4_status"] = "DONE"
  ui_state["s5a_status"] = "DONE"

  if failed_items:
    ui_state["s5b_status"] = "ALERT"
    ui_state["s6_status"] = "RUNNING"
    add_log(
        ui_state,
        f"{YELLOW}Routing {len(failed_items)} failed record(s) to Stage 6 Synchronous Online Retry Worker...{RESET}",
    )
    await asyncio.sleep(0.8)

    # -----------------------------------------------------------------
    # STAGE 6: Synchronous Online Retry Worker (google-genai SDK, location='global')
    # -----------------------------------------------------------------
    online_client = genai.Client(
        vertexai=True,
        project=args.project_id,
        location="global",
        credentials=creds,
    )

    retry_tasks = [
        retry_single_record_online(online_client, args.model, rec, rec_id, reason, ui_state)
        for rec_id, rec, reason in failed_items
    ]
    repaired_results = await asyncio.gather(*retry_tasks)
    valid_results.extend(repaired_results)

    ui_state["s5b_status"] = "DONE"
    ui_state["s6_status"] = "DONE"
  else:
    ui_state["s5b_status"] = "DONE"
    ui_state["s6_status"] = "DONE"

  # -----------------------------------------------------------------
  # STAGE 7: Final Complete Validated Dataset
  # -----------------------------------------------------------------
  ui_state["final_count"] = len(valid_results)
  ui_state["s7_status"] = "DONE"
  add_log(
      ui_state,
      f"{GREEN}{BOLD}PIPELINE COMPLETE:{RESET} {len(valid_results)}/{len(sample_requests)} records validated (100%)!",
  )


if __name__ == "__main__":
  asyncio.run(main())
