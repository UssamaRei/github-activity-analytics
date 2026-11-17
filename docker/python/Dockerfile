FROM python:3.11-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestion ingestion
COPY scripts scripts
COPY data/samples data/samples
CMD ["python", "-m", "ingestion.producer", "--input", "data/samples/github_events.jsonl"]
