FROM python:3.12.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY src src
COPY scripts scripts

RUN useradd --create-home --uid 10001 app \
    && mkdir -p /app/data /app/models /app/state/cache /app/state/events /app/state/profiles /app/state/worker \
    && chown -R app:app /app

USER app

EXPOSE 8082

CMD ["python", "-m", "uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8082"]
