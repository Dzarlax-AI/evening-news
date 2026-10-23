# Python web/worker image; collection and browser operations are owned by Go.
FROM python:3.12-slim

# Build arguments for metadata
ARG BUILDTIME
ARG VERSION
ARG REVISION

# Add metadata labels
LABEL org.opencontainers.image.created=$BUILDTIME \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$REVISION \
      org.opencontainers.image.title="Evening News" \
      org.opencontainers.image.description="AI-powered news aggregation system with web interface" \
      org.opencontainers.image.source="https://github.com/dzarlax/evening-news" \
      org.opencontainers.image.vendor="Dzarlax" \
      org.opencontainers.image.licenses="MIT"

WORKDIR /app

# Install additional system dependencies (PostgreSQL client, libmagic for MIME detection)
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    curl \
    libmagic-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy requirements first (changes less often than app code)
COPY requirements.txt .

# Install Python dependencies
# No browser automation or Python extraction engine is installed in this image.
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# Create directories with proper permissions
RUN mkdir -p /app/logs /app/data /app/cache /app/backups /app/exports && \
    chmod 755 /app/logs /app/data /app/cache /app/backups /app/exports

# Copy static files that rarely change first
COPY db/ ./db/
COPY scripts/ ./scripts/

# Copy web assets (change less frequently than main code)
COPY web/ ./web/

# Copy main application code LAST (most frequently changed)
COPY news_aggregator/ ./news_aggregator/

EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "news_aggregator.main:app", "--host", "0.0.0.0", "--port", "8000"]