# Use Python 3.9 as the base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN addgroup --system app && adduser --system --group app

# Create necessary directories and set permissions
RUN mkdir -p /app/data_app_config /app/logs \
    && chown -R app:app /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy application code
COPY . .

# Change ownership of all files to the app user
RUN chown -R app:app /app

# Switch to non-root user
USER app

# Create volume mount points
VOLUME ["/app/data_app_config", "/app/logs"]

# Expose port
EXPOSE 22222

# Command to run the application
CMD ["uvicorn", "main_gateway:app", "--host", "0.0.0.0", "--port", "22222"]