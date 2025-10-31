from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess
import duckdb
import smtplib
from email.mime.text import MIMEText
import json
import sys
import requests  # <-- NEW import for MCP API call
import signal
import time

sys.path.append("/opt/airflow/MCP_Healthcare_pipeline/ingestion")
from ingest import run_ingestion
sys.path.append("/opt/airflow/MCP_Healthcare_pipeline/dbt_project")
from mcp_agent import MCPClient

DBT_PROJECT_DIR = "/opt/airflow/MCP_Healthcare_pipeline/dbt_project"
OUTPUT_DIR = "/opt/airflow/MCP_Healthcare_pipeline/output"
DUCKDB_FILE = "/opt/airflow/MCP_Healthcare_pipeline/data/bronze.duckdb"
ALERT_EMAIL = "kunal@teqfocus.com"
MCP_URL = "http://127.0.0.1:8080"  # <-- Your running MCP FastAPI service
MCP_SCRIPT = "/opt/airflow/MCP_Healthcare_pipeline/dbt_project/mcp_service.py"
MCP_PROCESS = None  # store the subprocess globally
MCP_Agent="/opt/airflow/MCP_Healthcare_pipeline/dbt_project/mcp_client.py"


# -----------------------------
# Start MCP FastAPI service
# -----------------------------

def run_mcp_sync():
    """Run MCP Context Sync after dbt run."""
    try:
        print("🔁 Running MCP Client sync...")
        client = MCPClient(config_path="/opt/airflow/MCP_Healthcare_pipeline/dbt_project/mcp_config.yaml")
        client.sync_context()
        print("✅ MCP Client sync completed successfully.")
    except Exception as e:
        print(f"❌ MCP Client sync failed: {e}")
        raise


def start_mcp_service():
    global MCP_PROCESS
    if MCP_PROCESS is not None and MCP_PROCESS.poll() is None:
        print("✅ MCP service already running.")
        return

    print("🚀 Starting MCP service...")
    MCP_PROCESS = subprocess.Popen(
        ["python", MCP_SCRIPT],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid
    )
    
    issues=[]
    # Wait until MCP responds
    for i in range(30):
        try:
            r = requests.get(f"{MCP_URL}/context", timeout=2)
            if r.status_code == 200:
                print("✅ MCP service is healthy and ready.")
                return
        except requests.exceptions.RequestException:
            pass
        print(f"⏳ Waiting for MCP to start... ({i+1}/30)")
        time.sleep(2)

    error_msg = "❌ MCP service failed to start within timeout period."
    print(error_msg)
    issues.append(error_msg)
    return issues


# -----------------------------
# Data Quality Checks
# -----------------------------
def data_quality_check():
    con = duckdb.connect(DUCKDB_FILE)
    issues = []
    tables = ['patient_table', 'encounter_table', 'dx_table', 'cpt_table']
    for table in tables:
        try:
            cols_df = con.execute(f"PRAGMA table_info('{table}')").fetchdf()
            columns = cols_df['name'].tolist()
            if not columns:
                issues.append(f"⚠ Table {table} has no columns")
                continue

            # Null check
            null_where = " OR ".join([f"{col} IS NULL" for col in columns])
            null_count = con.execute(f"""
                SELECT COUNT(*) AS cnt FROM {table} WHERE {null_where}
            """).fetchone()[0]
            if null_count > 0:
                issues.append(f"❌ {null_count} nulls found in {table}")

            # Duplicate check
            pk_col = columns[0]
            dup_count = con.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT {pk_col}) AS cnt FROM {table}
            """).fetchone()[0]
            if dup_count > 0:
                issues.append(f"❌ {dup_count} duplicates found in {table}.{pk_col}")
        except Exception as e:
            issues.append(f"⚠ Could not check table {table}: {str(e)}")

    con.close()
    return issues

# -----------------------------
# MCP Refresh
# -----------------------------
def refresh_mcp_context():
    print("🔁 Triggering MCP /refresh endpoint...")
    issues=[]
    try:
        response = requests.post(f"{MCP_URL}/refresh", timeout=30)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ MCP refresh successful: {data}")
        else:
            print(f"⚠ MCP refresh failed: {response.status_code} - {response.text}")
            raise Exception("MCP refresh endpoint returned an error")
    except Exception as e:
        print(f"❌ MCP refresh failed: {e}")
        error_msg = "❌ MCP refresh failed: {e}"
        issues.append(error_msg)
        return issues


# -----------------------------
# Monitoring + Email Alert
# -----------------------------
def send_email(to: str, subject: str, html_content: str):
    SMTP_SERVER = "sandbox.smtp.mailtrap.io"
    SMTP_PORT = 587
    SMTP_USER = "8045dcbe0c4023"
    SMTP_PASSWORD = "dfb01929227b5d"

    msg = MIMEText(html_content, "html")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = to

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, [to], msg.as_string())
    print(f"Email sent to {to}")

def monitor_pipeline():
    print("🔍 Running data quality checks on dbt-generated tables...")
    issues = data_quality_check()
    mcp_issues1= start_mcp_service()
    mcp_issues2= refresh_mcp_context()
    issues.append(mcp_issues1)
    issues.append(mcp_issues2)
    if issues:
        body = "<h3>🚨 Pipeline Issues Detected</h3><ul>"
        for i in issues:
            body += f"<li>{i}</li>"
        body += "</ul>"
        send_email(
            to=ALERT_EMAIL,
            subject="Airflow Pipeline Alert: Data Quality Issues and MCP Issues",
            html_content=body
        )
        print("❌ Issues detected — email sent.")
    else:
        print("✅ No issues detected. All dbt tables passed checks!")

# -----------------------------
# Export Gold Layer
# -----------------------------
def export_gold_to_csv():
    con = duckdb.connect(DUCKDB_FILE, read_only=True)
    df = con.execute("SELECT * FROM all_data").fetchdf()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(os.path.join(OUTPUT_DIR, "all_data.csv"), index=False)
    con.close()
    print(f"CSV exported to {OUTPUT_DIR}/all_data.csv")



# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="prism_pipeline",
    start_date=datetime(2025, 9, 18),
    schedule="@daily",
    catchup=False,
    tags=["prism", "healthcare"],
) as dag:

    ingestion_task = PythonOperator(
        task_id="run_ingestion",
        python_callable=run_ingestion
    )

    dbt_task = BashOperator(
        task_id="run_dbt",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_compile_task = BashOperator(
        task_id="dbt_compile",
        bash_command=f"dbt compile --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} || true",
    )

    dbt_docs_task = BashOperator(
        task_id="dbt_docs",
        bash_command=f"dbt docs generate --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} || true",
    )
    
    mcp_sync_task = PythonOperator(
    task_id="run_mcp_sync",
    python_callable=run_mcp_sync
    )

    start_mcp = PythonOperator(
        task_id="start_mcp_service",
        python_callable=start_mcp_service
    )

    mcp_refresh_task = PythonOperator(
        task_id="refresh_mcp_context",
        python_callable=refresh_mcp_context
    )

    monitor_task = PythonOperator(
        task_id="monitor_pipeline",
        python_callable=monitor_pipeline
    )

    export_csv_task = PythonOperator(
        task_id="export_gold_csv",
        python_callable=export_gold_to_csv,
    )

    # 🔗 DAG dependencies
    ingestion_task >> dbt_task >> dbt_compile_task >> dbt_docs_task >> mcp_sync_task >> start_mcp >> mcp_refresh_task >> monitor_task >> export_csv_task
