FROM python:3.12-slim

# ── System deps ───────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# ── UV ────────────────────────────────────────────────────────────────────────
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# ── Dependencies (cached layer) ───────────────────────────────────────────────
COPY pyproject.toml .
RUN uv pip install --system --no-cache -e .

# ── Source ────────────────────────────────────────────────────────────────────
COPY . .

# ── Runtime ───────────────────────────────────────────────────────────────────
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# SERVICE env var selects which pod role this container plays.
# Set in each Deployment's env: engine | ledger | price-feed |
# sentiment | candles | circuit | bots | api
CMD ["python", "services/entrypoint.py"]