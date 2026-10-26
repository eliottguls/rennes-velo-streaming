FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir gtfs-realtime-bindings==1.0.0
COPY server.py .
USER nobody
CMD ["python", "-u", "server.py"]
