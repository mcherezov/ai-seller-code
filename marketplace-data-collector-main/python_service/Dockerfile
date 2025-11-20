FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y curl
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR /app
COPY config.json entrypoint.sh app/ tests/ ./
CMD ["/app/entrypoint.sh"]
