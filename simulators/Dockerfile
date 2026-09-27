# Image shared by the two simulators (stream producer + batch producer)
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY simulators/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY simulators ./simulators
CMD ["python", "-m", "simulators.stream_producer"]
