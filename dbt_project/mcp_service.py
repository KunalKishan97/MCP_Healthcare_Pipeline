from fastapi import FastAPI
import duckdb
import os
import re
from datetime import datetime

app = FastAPI(title="MCP Context API for dbt")
#MODELS_DIR = "D:/Project/MCP_Healthcare_pipeline/dbt_project/models"
#DB_PATH = "D:/Project/MCP_Healthcare_pipeline/data/bronze.duckdb"
MODELS_DIR = "/opt/airflow/MCP_Healthcare_pipeline/dbt_project/models"
DB_PATH = "/opt/airflow/MCP_Healthcare_pipeline/data/bronze.duckdb"


@app.get("/context")
def get_all_context():
    """
    MCP multiplexer endpoint: advertises all available APIs to the LLM.
    """
    con = duckdb.connect(DB_PATH)
    df = con.execute("SELECT * FROM dbt_context").fetchdf()
    return {
        "dbt_context": df.to_dict(orient="records"),
        "service": "MCP dbt Context API",
        "description": "Provides model lineage, metadata, and data summaries for dbt pipeline.",
        "endpoints": [
            {
                "name": "get_all_context",
                "path": "/context",
                "method": "GET",
                "description": "Returns this full list of available API endpoints."
            },
            {
                "name": "get_model_context",
                "path": "/model/{model_name}",
                "method": "GET",
                "description": "Returns dbt context entries for a specific model."
            },
            {
                "name": "get_lineage",
                "path": "/lineage/{model_name}",
                "method": "GET",
                "description": "Returns model dependencies for lineage tracing."
            },
            {
                "name": "get_metadata",
                "path": "/metadata/{model_name}",
                "method": "GET",
                "description": "Returns metadata like row count, columns, and last update timestamp for a model."
            },
            {
                "name": "get_data_summary",
                "path": "/data_summary",
                "method": "GET",
                "description": "Compares row counts between bronze, silver, and gold layers and summarizes transformations."
            },
            {
                "name": "get_column_lineage",
                "path": "/column_lineage/{column_name}",
                "method": "GET",
                "description": "Finds lineage of a specific column across all dbt models."
            },
            {
                "name": "get_dependents",
                "path": "/dependents/{model_name}",
                "method": "GET",
                "description": "Lists models that depend on a given model."
            },
            {
                "name": "get_column_info",
                "path": "/column_info/{model_name}",
                "method": "GET",
                "description": "Returns metadata like row count and columns info for a model."
            },
            {
                "name": "health_check",
                "path": "/health",
                "method": "GET",
                "description": "Health check endpoint to verify MCP API is running."
            },
            {
                "name": "refresh_context",
                "path": "/refresh",
                "method": "POST",
                "description": "Refreshes dbt documentation and context metadata."
            }
        ]
    }





@app.get("/model/{model_name}")
def get_model_context(model_name: str):
    con = duckdb.connect(DB_PATH)
    query = f"""
        SELECT * FROM dbt_context
        WHERE model LIKE '%{model_name}%'
    """
    df = con.execute(query).fetchdf()
    return df.to_dict(orient="records")

@app.get("/lineage/{model_name}")
def get_lineage(model_name: str):
    con = duckdb.connect(DB_PATH)
    result = con.execute(f"""
        SELECT model, depends_on FROM dbt_context
        WHERE model LIKE '%{model_name}%'
    """).fetchone()
    if result:
        model, depends_on = result
        return {
            "model": model,
            "depends_on": depends_on.split(",") if depends_on else []
        }
    return {"error": "Model not found"}

@app.get("/health")
def health_check():
    return {"status": "ok", "message": "MCP dbt context API is running"}

@app.post("/refresh")
def refresh_context():
    from mcp_agent import MCPClient   

    try:
        client = MCPClient(config_path="mcp_config.yaml")
        client.sync_context()
        return {"status": "success", "message": "MCP Context synced successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


    
@app.get("/search")
def search_models(q: str):
    con = duckdb.connect(DB_PATH)
    df = con.execute(f"""
        SELECT * FROM dbt_context
        WHERE model LIKE '%{q}%' OR description LIKE '%{q}%'
    """).fetchdf()
    con.close()
    return df.to_dict(orient="records")

@app.get("/metadata/{model_name}")
def get_model_metadata(model_name: str):
    con = duckdb.connect(DB_PATH)
    metadata = {"model": model_name}

    try:
        # ✅ 1️⃣ Get total row count
        row_count = con.execute(f"SELECT COUNT(*) FROM {model_name}").fetchone()[0]
        metadata["rows"] = row_count

        # ✅ 2️⃣ Check if 'load_dtm' column exists before querying
        columns_df = con.execute(f"PRAGMA table_info({model_name})").fetchdf()
        if "load_dtm" in columns_df["name"].tolist():
            last_update = con.execute(f"SELECT MAX(load_dtm) FROM {model_name}").fetchone()[0]
            metadata["last_updated"] = str(last_update)
        else:
            metadata["last_updated"] = "N/A"

        # ✅ 3️⃣ Optionally, get column count and list
        metadata["column_count"] = len(columns_df)
        metadata["columns"] = columns_df["name"].tolist()

    except Exception as e:
        metadata["error"] = str(e)
    finally:
        con.close()

    return metadata

@app.get("/dependents/{model_name}")
def get_dependents(model_name: str):
    con = duckdb.connect(DB_PATH)
    df = con.execute(f"""
        SELECT model FROM dbt_context
        WHERE depends_on LIKE '%{model_name}%'
    """).fetchdf()
    con.close()
    return df["model"].tolist()

@app.get("/column_lineage/{column_name}")
def get_column_lineage(column_name: str):
    """
    Trace lineage of a column across all dbt models by scanning SQL files.
    Example: /column_lineage/PatientName
    """
    lineage = []
    found_in_models = []
    column_name_lower = column_name.lower()

    # Step 1: Iterate over model SQLs
    for root, _, files in os.walk(MODELS_DIR):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                model_name = file.replace(".sql", "")

                with open(file_path, "r", encoding="utf-8") as f:
                    sql = f.read().lower()

                # Step 2: Search for column definition (e.g., first_name || last_name AS patientname)
                pattern = re.compile(rf"(.*)\s+as\s+{column_name_lower}\b")
                matches = pattern.findall(sql)

                if matches:
                    source_expr = matches[0].strip()
                    lineage.append({
                        "model": model_name,
                        "derived_from": extract_source_columns(source_expr),
                        "expression": source_expr
                    })
                    found_in_models.append(model_name)

    # Step 3: Try to find in upstream dbt_context (optional)
    con = duckdb.connect(DB_PATH)
    all_models = con.execute("SELECT model, depends_on FROM dbt_context").fetchall()
    con.close()

    # Step 4: Build final structured output
    if not lineage:
        return {"message": f"No lineage found for column '{column_name}'"}

    return {
        "column": column_name,
        "lineage_chain": lineage,
        "models_found": found_in_models,
        "note": "Derived using static SQL parsing. For best results, keep SQL clean (no nested CTEs)."
    }


def extract_source_columns(expression: str):
    """
    Extracts potential source columns from an SQL expression.
    e.g. "p.first_name || ' ' || p.last_name" → ["first_name", "last_name"]
    """
    # Remove literals and functions
    expr = re.sub(r"['\"].*?['\"]", "", expression)  # remove strings
    expr = re.sub(r"\bconcat\b|\|\||\+|\(|\)|,|::|\btrim\b|\bcoalesce\b", " ", expr)
    cols = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", expr)
    # Filter out common SQL keywords
    keywords = {"select", "from", "join", "on", "as", "and", "or", "case", "when", "then", "else", "end"}
    return [c for c in cols if c not in keywords and not c.isdigit()]

@app.get("/data_summary")
def get_data_summary():
    con = duckdb.connect(DB_PATH)
    summary = []

    def safe_count(table_name):
        """Return count or 0 if table not found."""
        try:
            return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        except Exception:
            return 0

    # --- Bronze Layer ---
    bronze_rows = safe_count("bronze_table_data")
    summary.append({
        "layer": "bronze",
        "model": "bronze_table_data",
        "rows": bronze_rows,
        "description": "Raw ingested data from PRISM or external sources. No transformation applied yet."
    })

    # --- Silver Layer ---
    silver_models = ["patient_table", "encounter_table", "dx_table", "cpt_table"]
    silver_total = sum(safe_count(m) for m in silver_models)
    bronze_to_silver_diff = silver_total - bronze_rows
    bronze_to_silver_pct = (
        round((bronze_to_silver_diff / bronze_rows) * 100, 2)
        if bronze_rows > 0 else 0
    )

    summary.append({
        "layer": "silver",
        "model": ", ".join(silver_models),
        "rows": silver_total,
        "description": f"Data cleaned, deduplicated, and split into domain tables. "
                       f"{'Reduced' if bronze_to_silver_pct < 0 else 'Increased'} by {abs(bronze_to_silver_pct)}% from Bronze layer."
    })

    # --- Gold Layer ---
    gold_rows = safe_count("all_data")
    silver_to_gold_diff = gold_rows - silver_total
    silver_to_gold_pct = (
        round((silver_to_gold_diff / silver_total) * 100, 2)
        if silver_total > 0 else 0
    )

    summary.append({
        "layer": "gold",
        "model": "all_data",
        "rows": gold_rows,
        "description": f"Final analytical data combined from all domain tables for reporting. "
                       f"{'Reduced' if silver_to_gold_pct < 0 else 'Increased'} by {abs(silver_to_gold_pct)}% from Silver layer."
    })

    con.close()
    return summary

@app.get("/transform_stats")
def get_transform_stats():
    return get_transform_stats_internal()


def get_transform_stats_internal():
    con = duckdb.connect(DB_PATH)


    def count_rows(model):
        try:
            return con.execute(f"SELECT COUNT(*) FROM {model}").fetchone()[0]
        except Exception:
            return 0

    bronze_count = count_rows("bronze_table_data")
    patient_count = count_rows("patient_table")
    encounter_count = count_rows("encounter_table")
    all_data_count = count_rows("all_data")

    con.close()
    return {
        "bronze_table_data": bronze_count,
        "patient_table": patient_count,
        "encounter_table": encounter_count,
        "all_data": all_data_count,
        "transformation_summary": (
            f"Raw bronze has {bronze_count} rows → "
            f"patient_table: {patient_count}, encounter_table: {encounter_count}, "
            f"and final all_data: {all_data_count} after cleansing & joins."
        ),
        "last_refreshed": str(datetime.now())
    }

@app.get("/column_info/{model_name}")
def get_column_info(model_name: str):
    """
    Returns column count, data types, and descriptions (if available) for a given model.
    Example: /column_info/all_data
    """
    import yaml

    con = duckdb.connect(DB_PATH)
    try:
        # --- 1️⃣ Get column metadata from DuckDB ---
        df = con.execute(f"PRAGMA table_info('{model_name}')").fetchdf()
        if df.empty:
            return {"error": f"Model '{model_name}' not found in database."}

        columns = []
        for _, row in df.iterrows():
            columns.append({
                "name": row["name"],
                "type": row["type"],
                "nullable": not row["notnull"]
            })

        # --- 2️⃣ Try to get YAML descriptions from dbt schema files ---
        yaml_descriptions = {}
        for root, _, files in os.walk(MODELS_DIR):
            for file in files:
                if file.endswith((".yml", ".yaml")):
                    with open(os.path.join(root, file), "r", encoding="utf-8") as f:
                        try:
                            content = yaml.safe_load(f)
                            if not content or "models" not in content:
                                continue
                            for model in content["models"]:
                                if model.get("name") == model_name and "columns" in model:
                                    for col in model["columns"]:
                                        yaml_descriptions[col["name"]] = col.get("description", "")
                        except Exception:
                            continue

        # --- 3️⃣ Combine DuckDB info with YAML descriptions ---
        for c in columns:
            c["description"] = yaml_descriptions.get(c["name"], "")

        return {
            "model": model_name,
            "column_count": len(columns),
            "columns": columns
        }

    except Exception as e:
        return {"error": str(e)}

    finally:
        con.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("mcp_service:app", host="0.0.0.0", port=8080)
