import duckdb
from service.excel_service import ExcelService, ExcelConnectionModel


class BronzePipeline:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def execute(self) -> list:
        recebimento = ExcelService(self.conn, ExcelConnectionModel(
            path="data/data.xlsx", 
            sheet_name="Notas Fiscais - Recebimento"
        )).read_excel()

        expedicao = ExcelService(self.conn, ExcelConnectionModel(
            path="data/data.xlsx", 
            sheet_name="Notas Fiscais - Expedição"
        )).read_excel()

        recebimento.create("raw_recebimento") 
        expedicao.create("raw_expedicao") 

     
