import logging
import uvicorn
from core.kafka_client import ensure_topics

logging.basicConfig(level=logging.INFO)


def main():
    ensure_topics()
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        log_level="warning",
        reload=False,
    )


if __name__ == "__main__":
    main()