from dataclasses import dataclass
import duckdb


@dataclass
class ExcelConnectionModel:
    path:       str
    sheet_name: str | None = None
    header:     int = 0
    usecols:    str | None = None
    nrows:      int | None = None


class ExcelService:
    def __init__(self, conn: duckdb.DuckDBPyConnection, connectionModel: ExcelConnectionModel):
        self._conn = conn
        self._connectionModel = connectionModel

    def read_excel(self) -> duckdb.DuckDBPyRelation:
        cm = self._connectionModel

        params = [f"'{cm.path}'"]
        if cm.sheet_name:
            params.append(f"sheet='{cm.sheet_name}'")
        params.append(f"header={str(cm.header == 0).lower()}")
        if cm.usecols:
            params.append(f"range='{cm.usecols}'")
        params.append("all_varchar=true")

        query = f"SELECT * FROM read_xlsx({', '.join(params)})"
        if cm.nrows is not None:
            query += f" LIMIT {cm.nrows}"

        return self._conn.sql(query)