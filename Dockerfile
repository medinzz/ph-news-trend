# Start from an official Python 3.11 image (slim = smaller size)
FROM python:3.11-slim

# Install system-level packages that your Python libraries need to compile
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libxml2-dev libxslt-dev libffi-dev libssl-dev curl \
    && rm -rf /var/lib/apt/lists/*

# Set /app as the working directory inside the container
WORKDIR /app

# Copy requirements first (Docker caches this layer — speeds up rebuilds)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your project code into the container
COPY . .

# Set default environment variable (can be overridden at runtime)
ENV STORAGE_BACKEND=motherduck

# The command that runs when the container starts
ENTRYPOINT ["python", "main.py", "--use-crawler"]