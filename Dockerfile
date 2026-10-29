FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    PORT=10000 \
    IS_CLOUD_DEPLOYMENT=true

WORKDIR /app

# Install system dependencies for Playwright, Chromium, FFmpeg and PyAV
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    ffmpeg \
    ca-certificates \
    libgl1 \
    libglib2.0-0 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libasound2 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency requirements
COPY requirements.txt .

# Install Python packages and Playwright Chromium browser
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    playwright install --with-deps chromium || playwright install chromium

# Copy all application assets and code
COPY . .

# Expose target cloud port
EXPOSE 10000

# Periodic healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -f http://localhost:${PORT}/api/health || exit 1

# Launch application daemon
CMD ["python", "main.py", "--host", "0.0.0.0"]
