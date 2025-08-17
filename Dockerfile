# syntax=docker/dockerfile:1
FROM python:3.11-slim
ENV PYTHONPATH=/app

# Keep CA store current
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates build-essential gcc librdkafka-dev \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Make Requests use the system CA bundle
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x /app/entrypoint.sh

VOLUME ["/app/data"]
ENV PYTHONUNBUFFERED=1
CMD ["/app/entrypoint.sh"]