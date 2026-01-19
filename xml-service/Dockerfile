FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8080
EXPOSE 50051

CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}

