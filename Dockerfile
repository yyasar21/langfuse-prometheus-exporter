FROM docker.io/library/python:3.13.9-alpine3.22
COPY langfuse_exporter.py /app/langfuse_exporter.py
USER nobody
EXPOSE 9877
ENTRYPOINT ["python", "-u", "/app/langfuse_exporter.py"]
