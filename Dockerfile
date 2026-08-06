FROM python:3.11-slim

# Install system dependencies required by lxml, curl_cffi, and camoufox (Firefox runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    libssl-dev \
    curl \
    # GTK3 and Firefox runtime dependencies for Camoufox
    libgtk-3-0 \
    libdbus-glib-1-2 \
    libxt6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    libasound2 \
    libpangocairo-1.0-0 \
    libatk1.0-0 \
    libcairo-gobject2 \
    libgdk-pixbuf-2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# CRITICAL FIX: Pre-download Camoufox browser binary and fingerprint data into the image.
# Prevents GitHub Actions from timing out while fetching browser binaries on container launch.
RUN python -m camoufox fetch

# Copy project source code
COPY . .

# Environment variables
ENV STORAGE_BACKEND=motherduck

ENTRYPOINT ["python", "main.py", "--use-crawler"]