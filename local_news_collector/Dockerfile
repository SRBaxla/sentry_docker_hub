FROM python:3.10-slim

WORKDIR /app
RUN apt-get update && apt-get install -y \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y curl wget
RUN pip install python-dotenv hf_xet

COPY . .
CMD ["uvicorn", "collector:app", "--host", "0.0.0.0", "--port", "8001"]
