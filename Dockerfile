# ── Base image ────────────────────────────────────────────────────────────────
# Python 3.12 slim — we add Java manually for Kafka
FROM python:3.12-slim

# ── Build args ────────────────────────────────────────────────────────────────
ARG KAFKA_VERSION=3.7.0
ARG SCALA_VERSION=2.13

# ── System dependencies ───────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    wget \
    bash \
    procps \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Java env ──────────────────────────────────────────────────────────────────
ENV java-21=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# ── Kafka install ─────────────────────────────────────────────────────────────
ENV KAFKA_HOME=/opt/kafka
ENV PATH="$KAFKA_HOME/bin:$PATH"

RUN wget -q \
    https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    -O /tmp/kafka.tgz \
    && mkdir -p ${KAFKA_HOME} \
    && tar -xzf /tmp/kafka.tgz \
        -C ${KAFKA_HOME} \
        --strip-components=1 \
    && rm /tmp/kafka.tgz

# ── Kafka data + log directories ──────────────────────────────────────────────
RUN mkdir -p /var/lib/kafka/data \
    && mkdir -p /opt/kafka/logs

# ── Kafka KRaft config ────────────────────────────────────────────────────────
COPY scripts/kafka-kraft.properties \
     /opt/kafka/config/kraft/server.properties

# ── Python env ────────────────────────────────────────────────────────────────
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# ── UV install ────────────────────────────────────────────────────────────────
RUN pip install uv --quiet

# ── Working directory ─────────────────────────────────────────────────────────
WORKDIR /app

# ── Install Python dependencies ───────────────────────────────────────────────
# copy only dependency files first — better Docker layer caching
# if source changes but deps don't, this layer is reused
COPY pyproject.toml .
COPY README.md .
RUN uv sync --no-dev

# ── Copy application source ───────────────────────────────────────────────────
COPY . .

# ── Startup script ────────────────────────────────────────────────────────────
RUN chmod +x /app/scripts/start.sh

# ── Environment defaults ──────────────────────────────────────────────────────
ENV BOOTSTRAP_SERVERS=localhost:9092
ENV ENV=production
ENV PORT=8000
ENV MCP_PORT=8001

# ── Exposed ports ─────────────────────────────────────────────────────────────
# Render only exposes one port publicly (PORT env var)
# MCP runs on 8001 internally
EXPOSE 8000
EXPOSE 8001

# ── Health check ──────────────────────────────────────────────────────────────
# Docker will mark container unhealthy if this fails
HEALTHCHECK \
    --interval=30s \
    --timeout=10s \
    --start-period=60s \
    --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/system/health || exit 1

# ── Entry point ───────────────────────────────────────────────────────────────
CMD ["/app/scripts/start.sh"]