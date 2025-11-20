FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
CMD ["jupyter", "lab", "--port=8888", "--ip=0.0.0.0", "--allow-root"]

