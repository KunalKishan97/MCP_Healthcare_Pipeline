import yaml
import json
import duckdb
from pathlib import Path

class MCPClient:
    

    def __init__(self, config_path='mcp_config.yaml'):
        self.config = yaml.safe_load(open(config_path))
        self.db_path = self.config["context_store"]["path"]
        self.db = duckdb.connect(self.db_path)

    def sync_context(self):
        manifest_path = Path(self.config["dbt_integration"]["manifest_path"])
        if not manifest_path.exists():
            raise FileNotFoundError(f"{manifest_path} not found. Run dbt first.")

        manifest = json.load(open(manifest_path))

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS dbt_context (
                model TEXT,
                depends_on TEXT,
                description TEXT
            )
        """)
        self.db.execute("DELETE FROM dbt_context")

        for model, details in manifest["nodes"].items():
            depends_on = ",".join(details["depends_on"]["nodes"])
            description = details.get("description", "")
            self.db.execute("INSERT INTO dbt_context VALUES (?, ?, ?)", 
                            [model, depends_on, description])

        print("✅ MCP Context synced successfully.")

if __name__ == "__main__":
    MCPClient().sync_context()
