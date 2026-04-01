import duckdb

class SilverPipeline:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        


    def execute(self) -> None:
        self._create_macros()
        self._create_d_local()
        self._create_d_cliente()
        self._create_d_fornecedor()
        self._create_d_sku()
        self._create_f_entrada()
        self._create_f_saida()
        
    def _create_macros(self):
        
        self.conn.execute("""
            CREATE MACRO clean(n) AS
            CASE
                WHEN n IS NULL OR TRIM(n) = '' OR UPPER(TRIM(n)) = 'NULL' THEN NULL
                ELSE TRIM(n)
            END
        """)
        
        self.conn.execute("""
            CREATE MACRO excel_to_timestamp(n) AS
                CASE 
                    WHEN clean(n) IS NULL THEN NULL
                    ELSE EPOCH_MS(CAST((CAST(clean(n) AS DOUBLE) - 25569) * 86400000 AS BIGINT))::TIMESTAMP
                END
        """)

    def _create_d_local(self):
        self.conn.execute("""
            CREATE TABLE d_local AS
            SELECT
                ROW_NUMBER() OVER () AS id,
                cidade,
                estado
            FROM (
                SELECT DISTINCT
                    UPPER(clean("CIDADE")) AS cidade,
                    UPPER(clean("UF"))     AS estado
                FROM raw_expedicao
            )
        """)

    def _create_d_cliente(self):
        self.conn.execute("""
            CREATE TABLE d_cliente AS
            SELECT
                ROW_NUMBER() OVER () AS id,
                UPPER(clean(nome))   AS nome
            FROM (
                SELECT DISTINCT
                    "CLIENTE" AS nome
                FROM raw_expedicao
            )
        """)

    def _create_d_fornecedor(self):
        self.conn.execute("""
            CREATE TABLE d_fornecedor AS
            SELECT
                ROW_NUMBER() OVER ()  AS id,
                UPPER(clean(nome))    AS nome
            FROM (
                SELECT DISTINCT "FORNECEDOR" AS nome
                FROM raw_recebimento
            )
        """)

    def _create_d_sku(self):
        self.conn.execute("""
            CREATE TABLE d_sku AS
            WITH recebimento AS (
                SELECT
                    clean("SKU")                           AS sku_original,
                    UPPER(clean("DESCRIÇÃO"))              AS descricao,
                    excel_to_timestamp("DATA RECEBIMENTO") AS data_ref
                FROM raw_recebimento
            ),
            expedicao AS (
                SELECT
                    clean("SKU")                           AS sku_original,
                    UPPER(clean("DESCRIÇÃO"))              AS descricao,
                    excel_to_timestamp("DATA VENDA")       AS data_ref
                FROM raw_expedicao
            ),
            todos AS (
                SELECT * FROM recebimento
                UNION ALL
                SELECT * FROM expedicao
            ),
            mais_recente AS (
                SELECT DISTINCT ON (sku_original)
                    sku_original,
                    descricao
                FROM todos
                ORDER BY sku_original, data_ref DESC
            )
            SELECT
                ROW_NUMBER() OVER () AS id,
                sku_original,
                descricao
            FROM mais_recente
        """)

    def _create_f_entrada(self):
        self.conn.execute("""
            CREATE TABLE f_entrada AS
            SELECT
                ROW_NUMBER() OVER ()                     AS id,
                clean(r."NOTA FISCAL")                   AS nf,
                clean(r."SERIE")                         AS serie,
                s.id                                     AS id_sku,
                f.id                                     AS id_fornecedor,
                excel_to_timestamp(r."DATA RECEBIMENTO") AS data_recebimento,
                excel_to_timestamp(r."VALIDADE")         AS data_vencimento,
                CAST(
                    REPLACE(
                        REPLACE(clean(r."VALOR"), 'R$ ', ''),
                    ',', '.')
                AS FLOAT)                                AS valor_unitario,
                CAST(clean(r."QUANTIDADE") AS INT)       AS quantidade
            FROM raw_recebimento r
            JOIN d_sku s
                ON clean(r."SKU") = s.sku_original
            JOIN d_fornecedor f
                ON UPPER(clean(r."FORNECEDOR")) = f.nome
        """)
        
    def _create_f_saida(self):
        self.conn.execute("""
            CREATE TABLE f_saida AS
            SELECT
                ROW_NUMBER() OVER ()               AS id,
                clean(e."NOTA FISCAL")             AS nf,
                clean(e."SERIE")                   AS serie,
                s.id                               AS id_sku,
                c.id                               AS id_cliente,
                l.id                               AS id_local,
                excel_to_timestamp(e."DATA VENDA") AS data_venda,
                CAST(clean(e."QUANTIDADE") AS INT) AS quantidade,
                s.sku_original IN (
                    SELECT DISTINCT clean("SKU")
                    FROM raw_recebimento
                )                                  AS entrada_rastreavel
            FROM raw_expedicao e
            JOIN d_sku s
                ON clean(e."SKU") = s.sku_original
            JOIN d_cliente c
                ON UPPER(clean(e."CLIENTE")) = c.nome
            JOIN d_local l
                ON UPPER(clean(e."CIDADE")) = l.cidade
                AND UPPER(clean(e."UF")) = l.estado
        """)
        
    def export(self) -> None:
        tables = [
            "d_local",
            "d_cliente", 
            "d_fornecedor",
            "d_sku",
            # "f_entrada",
            # "f_saida",
        ]
        for table in tables:
            self.conn.execute(f"""
                COPY {table} TO 'data/silver/{table}.parquet' (FORMAT PARQUET)
            """)
