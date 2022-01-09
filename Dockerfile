FROM python:3.10.1-slim
WORKDIR /home
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir requests beautifulsoup4 psycopg2-binary
