# Dockerfile for binance_collector
# Use slim Python image
FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Default execution command
CMD [ "uvicorn","binance_tick_collector:app","--host","0.0.0.0","--port","8002"]