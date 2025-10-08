FROM python:3.12-slim

# Optional: psql client is handy for debugging; safe to remove if you want even leaner image
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Python deps (only psycopg)
RUN pip install --no-cache-dir "psycopg[binary]==3.2.1"

# Set the default command to run the ELT script
CMD ["python", "elt.py"]

WORKDIR /app