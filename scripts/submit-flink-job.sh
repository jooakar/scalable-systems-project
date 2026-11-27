#!/bin/sh
set -e

JOBMANAGER_URL="${JOBMANAGER_URL:-http://jobmanager:8081}"
JAR_PATH="/app/flink-job.jar"
ENTRY_CLASS="trading.flink.TrendJob"
MAX_RETRIES=60
RETRY_INTERVAL=5

echo "Waiting for Flink JobManager and TaskManager at ${JOBMANAGER_URL} ..."

retries=0
until curl -sf "${JOBMANAGER_URL}/overview" | grep -q '"taskmanagers":[1-9]'; do
  retries=$((retries + 1))
  if [ "$retries" -ge "$MAX_RETRIES" ]; then
    echo "ERROR: Flink cluster (TaskManager) did not become available after $((MAX_RETRIES * RETRY_INTERVAL))s"
    exit 1
  fi
  echo "  attempt ${retries}/${MAX_RETRIES} – retrying in ${RETRY_INTERVAL}s ..."
  sleep "$RETRY_INTERVAL"
done
echo "Flink cluster is ready (TaskManager connected)."

# Upload the jar
echo "Uploading ${JAR_PATH} ..."
UPLOAD_RESPONSE=$(curl -sf -X POST "${JOBMANAGER_URL}/jars/upload" \
  -H "Expect:" \
  -F "jarfile=@${JAR_PATH};filename=flink-job.jar")

JAR_ID=$(echo "$UPLOAD_RESPONSE" | sed -n 's/.*"filename"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
JAR_ID=$(basename "$JAR_ID")

if [ -z "$JAR_ID" ]; then
  echo "ERROR: Failed to extract jar ID from upload response:"
  echo "$UPLOAD_RESPONSE"
  exit 1
fi
echo "Uploaded jar: ${JAR_ID}"

# Write the request body to a temp file to avoid shell quoting issues
REQUEST_BODY=/tmp/run-request.json
printf '{"entryClass":"%s","parallelism":%s}' "$ENTRY_CLASS" "${PARALLELISM:-1}" > "$REQUEST_BODY"

# Submit the job
echo "Submitting job (entry class: ${ENTRY_CLASS}) ..."
RUN_RESPONSE=$(curl -sf -X POST \
  "${JOBMANAGER_URL}/jars/${JAR_ID}/run" \
  -H "Content-Type: application/json" \
  -d @"$REQUEST_BODY")

rm -f "$REQUEST_BODY"

JOB_ID=$(echo "$RUN_RESPONSE" | sed -n 's/.*"jobid"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')

if [ -z "$JOB_ID" ]; then
  echo "ERROR: Failed to extract job ID from run response:"
  echo "$RUN_RESPONSE"
  exit 1
fi

echo "Flink job submitted successfully: ${JOB_ID}"
