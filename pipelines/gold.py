import duckdb

class GoldPipeline:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        
    def execute(self) -> None:
        self._create_vw_fefo()
        self._create_vw_lote()

    def export(self) -> None:
        tables = [
            "vw_fefo",
            "vw_lote",
        ]
        for table in tables:
            self.conn.execute(f"""
                COPY {table} TO 'data/gold/{table}.parquet' (FORMAT PARQUET)
            """)


    def _create_vw_fefo(self):
        self.conn.execute("""
            CREATE TABLE vw_fefo AS
            WITH acum_entrada AS (
                SELECT
                    id              AS id_lote,
                    id_sku,
                    quantidade,
                    valor_unitario,
                    SUM(quantidade) OVER (
                        PARTITION BY id_sku
                        ORDER BY data_vencimento ASC
                        ROWS UNBOUNDED PRECEDING
                    )               AS faixa_fim
                FROM f_entrada
            ),
            acum_saida AS (
                SELECT
                    id              AS id_saida,
                    id_sku,
                    id_cliente,
                    id_local,
                    nf,
                    data_venda,
                    quantidade,
                    SUM(quantidade) OVER (
                        PARTITION BY id_sku
                        ORDER BY data_venda ASC
                        ROWS UNBOUNDED PRECEDING
                    )               AS faixa_fim
                FROM f_saida
                WHERE entrada_rastreavel = TRUE
            )
            SELECT
                s.id_saida,
                s.id_cliente,
                s.id_local,
                s.nf,
                s.data_venda::DATE  AS data_venda,
                s.data_venda        AS datahora_venda,
                e.id_lote,
                LEAST(s.faixa_fim, e.faixa_fim) -
                GREATEST(s.faixa_fim - s.quantidade, e.faixa_fim - e.quantidade) AS qtd_consumida,
                (LEAST(s.faixa_fim, e.faixa_fim) -
                GREATEST(s.faixa_fim - s.quantidade, e.faixa_fim - e.quantidade))
                * e.valor_unitario                                                AS custo_total,
                e.valor_unitario
            FROM acum_saida s
            JOIN acum_entrada e
                ON s.id_sku = e.id_sku
                AND (s.faixa_fim - s.quantidade) < e.faixa_fim
                AND s.faixa_fim                  > (e.faixa_fim - e.quantidade)
            WHERE qtd_consumida > 0
        """)

    def _create_vw_lote(self):
        self.conn.execute("""
            CREATE TABLE vw_lote AS
            WITH vendido_por_lote AS (
                SELECT
                    id_lote,
                    SUM(qtd_consumida)              AS quantidade_vendida,
                    SUM(custo_total)                AS valor_vendido
                FROM vw_fefo
                GROUP BY id_lote
            )
            SELECT
                e.id                                AS id_lote,
                e.id_sku,
                e.nf,
                e.id_fornecedor,
                e.data_recebimento as datahora_recebimento,
                e.data_recebimento::DATE as data_recebimento,
                e.data_vencimento::DATE as data_vencimento,
                e.valor_unitario,
                e.quantidade                        AS quantidade_entrada,
                COALESCE(v.quantidade_vendida, 0)   AS quantidade_vendida,
                COALESCE(v.valor_vendido, 0)        AS valor_vendido,
                GREATEST(
                    e.quantidade - COALESCE(v.quantidade_vendida, 0),
                    0
                )                                   AS quantidade_expirada,
                GREATEST(
                    e.quantidade - COALESCE(v.quantidade_vendida, 0),
                    0
                ) * e.valor_unitario                AS valor_expirado,
                CASE
                    WHEN e.data_vencimento < CURRENT_DATE
                    AND (e.quantidade - COALESCE(v.quantidade_vendida, 0)) > 0
                        THEN 'EXPIRADO'
                    WHEN COALESCE(v.quantidade_vendida, 0) >= e.quantidade
                        THEN 'ESGOTADO'
                    ELSE 'ATIVO'
                END                                 AS status
            FROM f_entrada e
            LEFT JOIN vendido_por_lote v
                ON e.id = v.id_lote
        """)