FROM apache/airflow:3.1.0-python3.12

# Switch to root to install OS packages
USER root

# Install git (needed for dbt deps from git sources)
RUN apt-get update && apt-get install -y git \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Upgrade pip
RUN pip install --upgrade pip

# Install dbt-core and DuckDB adapter (pinned versions)
RUN pip install --no-cache-dir dbt-core==1.10.11 dbt-duckdb==1.9.6

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Install additional Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt
