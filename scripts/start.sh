#!/bin/bash
set -e

echo "========================================="
echo "  Market Simulator — Starting Up"
echo "========================================="

# ── Environment ───────────────────────────────────────────────────────────────
KAFKA_LOG_DIR="/var/lib/kafka/data"
CLUSTER_ID="MkU3OEVBNTcwNTJENDM2Qk"
APP_PORT="${PORT:-8000}"
MCP_PORT="${MCP_PORT:-8001}"

echo "  App port: $APP_PORT"
echo "  MCP port: $MCP_PORT"
echo ""

# ── Kafka JVM tuning — keep under 256MB on free tier ─────────────────────────
export KAFKA_HEAP_OPTS="-Xmx256m -Xms128m"
export KAFKA_JVM_PERFORMANCE_OPTS="-client -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:+ExplicitGCInvokesConcurrent"
export KAFKA_OPTS="-Djava.net.preferIPv4Stack=true"

# ── Step 1: Format Kafka storage ──────────────────────────────────────────────
echo "[1/5] Formatting Kafka storage (KRaft)..."
if [ ! -f "$KAFKA_LOG_DIR/meta.properties" ]; then
    $KAFKA_HOME/bin/kafka-storage.sh format \
        -t $CLUSTER_ID \
        -c $KAFKA_HOME/config/kraft/server.properties \
        --ignore-formatted > /dev/null 2>&1
    echo "      Done."
else
    echo "      Already formatted, skipping."
fi

# ── Step 2: Start Kafka broker ────────────────────────────────────────────────
echo "[2/5] Starting Kafka broker..."
$KAFKA_HOME/bin/kafka-server-start.sh \
    -daemon \
    $KAFKA_HOME/config/kraft/server.properties

# ── Step 3: Wait for Kafka ────────────────────────────────────────────────────
echo "[3/5] Waiting for Kafka to be ready..."
MAX_RETRIES=40
RETRY=0
until $KAFKA_HOME/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --list > /dev/null 2>&1; do
    RETRY=$((RETRY + 1))
    if [ $RETRY -ge $MAX_RETRIES ]; then
        echo "ERROR: Kafka failed to start after ${MAX_RETRIES} retries."
        exit 1
    fi
    echo "      Waiting... ($RETRY/$MAX_RETRIES)"
    sleep 3
done
echo "      Kafka is ready."

# ── Step 4: Start MCP server in background ────────────────────────────────────
echo "[4/5] Starting MCP server (background)..."
MCP_PORT=$MCP_PORT uv run python api/mcp_server.py > /tmp/mcp.log 2>&1 &
MCP_PID=$!
echo "      MCP PID: $MCP_PID"

# ── Step 5: Start simulation + API (foreground) ───────────────────────────────
echo "[5/5] Starting simulation + FastAPI..."
echo ""
echo "  API:      http://0.0.0.0:${APP_PORT}"
echo "  Docs:     http://0.0.0.0:${APP_PORT}/docs"
echo "  Health:   http://0.0.0.0:${APP_PORT}/system/health"
echo "  MCP:      http://0.0.0.0:${MCP_PORT}/mcp"
echo ""

exec uv run python main.py api \
    --host 0.0.0.0 \
    --port $APP_PORT