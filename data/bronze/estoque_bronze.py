# Databricks notebook source
from datetime import datetime, timedelta, date
from typing import Optional, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pytz import timezone

# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================

# Nome da tabela de destino na camada Bronze
TABELA_BRONZE_ESTOQUE_LOJA: str = "databox.bcg_comum.supply_bronze_estoque_lojas_on_off"

TABELA_BRONZE_ESTOQUE_CD: str = "databox.bcg_comum.supply_bronze_CDs_on_off"

# Timezone São Paulo (GMT-3)
TIMEZONE_SP = timezone('America/Sao_Paulo')

# Inicialização do Spark
spark = SparkSession.builder.appName("estoque_bronze").getOrCreate()

# Data de processamento (ontem)
hoje = datetime.now(TIMEZONE_SP) - timedelta(days=0)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

print(f"📅 Data de processamento: {hoje}")
print(f"📝 Data string: {hoje_str}")
print(f"🔢 Data int: {hoje_int}")
print(f"🌍 Timezone: {TIMEZONE_SP}")

# COMMAND ----------

def load_estoque_loja_data(spark: SparkSession) -> DataFrame:
    """
    Carrega dados de estoque das lojas ativas.
    
    Args:
        spark: Sessão do Spark
        current_year: Ano atual para filtro de partição
        
    Returns:
        DataFrame com dados de estoque das lojas, incluindo:
        - Informações da filial e SKU
        - Dados de estoque e classificação
        - Métricas de DDE e faixas
    """
    return (
        spark.read.table("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
        .filter(F.col("DtAtual") == hoje_str)
        .filter(F.col("StLoja") == "ATIVA")
        .filter(F.col("DsEstoqueLojaDeposito") == "L")
        # .select(
        #     "CdFilial", 
        #     "CdSku",
        #     "DsSku",
        #     "DsSetor",
        #     "DsCurva",
        #     "DsCurvaAbcLoja",
        #     "StLinha",
        #     "DsObrigatorio",
        #     "DsVoltagem",
        #     F.col("DsTipoEntrega").alias("TipoEntrega"),
        #     F.col("CdEstoqueFilialAbastecimento").alias("QtdEstoqueCDVinculado"),
        #     (F.col("VrTotalVv")/F.col("VrVndCmv")).alias("DDE"),
        #     F.col("QtEstoqueBoaOff").alias("EstoqueLoja"),
        #     F.col("DsFaixaDde").alias("ClassificacaoDDE"),
        #     F.col("data_ingestao"),
        #     F.date_format(F.col("data_ingestao"), "yyyy-MM-dd").alias("DtAtual")    
        # )
        # .dropDuplicates(["DtAtual", "CdSku", "CdFilial"])
    )

df_estoque_loja = load_estoque_loja_data(spark)
df_estoque_loja.display()

# COMMAND ----------

def load_estoque_CD_data(spark: SparkSession) -> DataFrame:
    """
    Carrega dados de estoque das lojas ativas.
    
    Args:
        spark: Sessão do Spark
        current_year: Ano atual para filtro de partição
        
    Returns:
        DataFrame com dados de estoque das lojas, incluindo:
        - Informações da filial e SKU
        - Dados de estoque e classificação
        - Métricas de DDE e faixas
    """
    return (
        spark.read.table("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
        .filter(F.col("DtAtual") == hoje_str)
        .filter(F.col("DsEstoqueLojaDeposito") == "D")
        # .select(
        #     "CdFilial", 
        #     "CdSku",
        #     "DsSku",
        #     "DsSetor",
        #     "DsCurva",
        #     "DsCurvaAbcLoja",
        #     "StLinha",
        #     "DsObrigatorio",
        #     "DsVoltagem",
        #     F.col("DsTipoEntrega").alias("TipoEntrega"),
        #     F.col("CdEstoqueFilialAbastecimento").alias("QtdEstoqueCDVinculado"),
        #     (F.col("VrTotalVv")/F.col("VrVndCmv")).alias("DDE"),
        #     F.col("QtEstoqueBoaOff").alias("EstoqueLoja"),
        #     F.col("DsFaixaDde").alias("ClassificacaoDDE"),
        #     F.col("data_ingestao"),
        #     F.date_format(F.col("data_ingestao"), "yyyy-MM-dd").alias("DtAtual")    
        # )
        # .dropDuplicates(["DtAtual", "CdSku", "CdFilial"])
    )

df_estoque_CD = load_estoque_loja_data(spark)
df_estoque_CD.display()

# COMMAND ----------

# MAGIC %sql select * from databox.logistica_comum.gef_visao_estoque_lojas
# MAGIC
# MAGIC -- TODO - CODIGO_ITEM = CdSku 
# MAGIC -- Filial = CdFilial com 4 ultimos digitos em inteiro
# MAGIC -- 
