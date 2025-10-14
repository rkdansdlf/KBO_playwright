FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies (playwright is already in base image)
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create logs/data directories
RUN mkdir -p logs data

# Default command (can be overridden in docker-compose)
CMD ["python", "scheduler.py"]
