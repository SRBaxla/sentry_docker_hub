FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get install -y curl
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "main_orchestrator.py"]
