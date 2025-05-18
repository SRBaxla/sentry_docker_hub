# Dockerfile.stocks

FROM python:3.11-slim

# Install system deps
RUN apt-get update && apt-get install -y build-essential


# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Copy app files
COPY . /app

# Expose port for FastAPI (optional)
EXPOSE 8000

# Run the FastAPI app with Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
