# Private domain executor and migration image; public HTTP/UI has its own image.
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

EXPOSE 8100

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import json,os,urllib.request; r=urllib.request.Request('http://127.0.0.1:8100/internal/v1/status',headers={'Authorization':'Bearer '+os.environ['JOB_EXECUTOR_TOKEN']}); assert json.load(urllib.request.urlopen(r,timeout=5))['ready']" || exit 1

CMD ["python", "-m", "news_aggregator.executor"]
