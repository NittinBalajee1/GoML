FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml requirements.txt README.md ./
COPY src ./src
COPY sql ./sql
COPY tests ./tests
COPY dashboard.py ./dashboard.py
COPY .streamlit ./.streamlit
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir . && useradd --create-home appuser && mkdir -p /app/data && chown -R appuser:appuser /app
USER appuser
VOLUME ["/app/data"]
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 CMD ["goml-pipeline", "health", "--root", "/app"]
CMD ["goml-pipeline", "run", "--root", "/app"]
