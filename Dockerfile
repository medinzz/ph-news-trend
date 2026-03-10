FROM python:3.11-slim

# Install system dependencies required by lxml, curl_cffi, and camoufox
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    libssl-dev \
    curl \
    # GTK3 and Firefox runtime dependencies for camoufox
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

# Install Python dependencies first (cached layer unless requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY . .

# MotherDuck token and table config are injected at runtime via environment
# variables — never baked into the image.
# ENV MOTHERDUCK_TOKEN is set by GitHub Actions secrets at run time.
# ENV TABLE_NAME is set by GitHub Actions secrets at run time.
# ENV STORAGE_BACKEND defaults to motherduck.
ENV STORAGE_BACKEND=motherduck

ENTRYPOINT ["python", "main.py", "--use-crawler"]