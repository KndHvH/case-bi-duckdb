import os
import duckdb
from pipelines.bronze import BronzePipeline
from pipelines.silver import SilverPipeline
from pipelines.gold   import GoldPipeline

db_path = "data/database.duckdb"

if os.path.exists(db_path):
    os.remove(db_path)

conn = duckdb.connect(db_path)
conn.install_extension("excel")
conn.load_extension("excel")

bronze = BronzePipeline(conn)
bronze.execute()

silver = SilverPipeline(conn)
silver.execute()
silver.export()

gold = GoldPipeline(conn)
gold.execute()
gold.export()






