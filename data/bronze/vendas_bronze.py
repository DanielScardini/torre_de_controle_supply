# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd

# Inicialização do Spark
spark = SparkSession.builder.appName("vendas_bronze").getOrCreate()
hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

print(hoje, hoje_str, hoje_int)

# COMMAND ----------

# TODO - implementar timezone gmt-3 Sao Paulo e get_data_inicio como sendo ultimos 90d

def get_data_inicio(hoje: datetime | date | None = None) -> datetime:
    """
    Retorna datetime no dia 1 do mês que está 14 meses antes de 'hoje'.
    """
    if hoje is None:
        hoje_d = date.today()
    elif isinstance(hoje, datetime):
        hoje_d = hoje.date()
    else:
        hoje_d = hoje

    total_meses = hoje_d.year * 12 + hoje_d.month - 2
    ano = total_meses // 12
    mes = total_meses % 12
    if mes == 0:
        ano -= 1
        mes = 12

    return datetime(ano, mes, 1)

# exemplo de uso
data_inicio = get_data_inicio()
data_inicio_int = int(data_inicio.strftime("%Y%m%d"))
print(data_inicio, data_inicio_int)

# COMMAND ----------

# TODO - implementar vendas ON e vendas OFFLINE
 
def get_vendas_on_off(
    spark: SparkSession,
    start_date: int = data_inicio_int,
    end_date: int = hoje_int,
) -> DataFrame:
    """
    Constrói uma visão unificada e agregada de vendas.
    
    Args:
        spark: Sessão do Spark
        start_date: Data de início no formato YYYYMMDD
        end_date: Data de fim no formato YYYYMMDD
        
    Returns:
        DataFrame com:
        - year_month (YYYYMM)
        - NmUF, NmRegiaoGeografica
        - CdSkuLoja, NmTipoNegocio
        - Receita (sum of VrOperacao)
        - QtMercadoria (sum of QtMercadoria)
        - merchandising attributes from mercadoria table
    """
    # load tables off
    df_rateada =(
        spark.table("app_venda.vendafaturadarateada")
        .filter(F.col("NmEstadoMercadoria") != '1 - SALDO')
        .filter(F.col("NmTipoNegocio") == 'LOJA FISICA')
        .filter(
            F.col("DtAprovacao").between(start_date, end_date)
            & (F.col("VrOperacao") >= 0)
            & (F.col("VrCustoContabilFilialSku") >= 0)
        )
    )  

    # TODO - exemplo de filtro de vendas ON
    #     df_rateada = spark.table("app_venda.vendafaturadarateada").filter(
        #     F.col("DtAprovacao").between(start_date, end_date)
        #     & (F.col("VrOperacao") >= 0)
        #     & (F.col("VrCustoContabilFilialSku") >= 0)
        #     & (F.col("NmEstadoMercadoria") != '1 - SALDO')                
        #     & (F.col("NmTipoNegocio") != 'LOJA FISICA')
        # )   
        
    df_nao_rateada = (
        spark.table("app_venda.vendafaturadanaorateada")
        .filter(F.col("QtMercadoria") >= 0)
        )

    # unify and filter
    df = (
        df_rateada
        .join(df_nao_rateada.select("ChaveFatos","QtMercadoria"), on="ChaveFatos")     
        .withColumn(
            "year_month",
            F.date_format(F.to_date(F.col("DtAprovacao").cast("string"), "yyyyMMdd"), "yyyyMM").cast("int")
        )
        .withColumnRenamed("CdFilialVenda", "CdFilial")
        .withColumn("DtAtual",
            F.date_format(F.to_date(F.col("DtAprovacao").cast("string"), "yyyyMMdd"), "yyyy-MM-dd"))
    )

    # aggregate
    df_agg = (
        df.groupBy(
            "DtAtual",
            "year_month",
            "CdSkuLoja",
            "CdFilial",
        )
        .agg(
            F.sum("VrOperacao").alias("Receita"),
            F.sum("QtMercadoria").alias("QtMercadoria"),
            F.sum("VrCustoContabilFilialSku").alias("Custo")
        )
    )

    # 1) Calendário diário (DateType)
    cal = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit(str(start_date)), "yyyyMMdd"),
                    F.to_date(F.lit(str(end_date)),   "yyyyMMdd"),
                    F.expr("interval 1 day")
                )
            ).alias("DtAtual_date")
        )
    )

    # 2) Conjunto de chaves (Filial x SKU) pertinentes a LOJA FISICA
    keys = (
        df
          .select("CdFilial", "CdSkuLoja")
          .dropDuplicates()
    )

    # 3) Grade completa (Dt x Filial x SKU)
    grade = cal.crossJoin(keys)

    # 4) Agregado com Dt como DateType para o join
    df_agg_d = df_agg.withColumn("DtAtual_date", F.to_date("DtAtual"))

    # 5) Left join + zeros onde não houver venda
    result = (
        grade.join(
            df_agg_d,
            on=["DtAtual_date",  "CdSkuLoja", "CdFilial"],
            how="left"
        )
        .withColumn("Receita",      F.coalesce(F.col("Receita"),      F.lit(0.0)))
        .withColumn("QtMercadoria", F.coalesce(F.col("QtMercadoria"), F.lit(0.0)))
        .withColumn("year_month", F.date_format(F.col("DtAtual_date"), "yyyyMM").cast("int"))
        .withColumn("DtAtual",    F.date_format(F.col("DtAtual_date"), "yyyy-MM-dd"))
        .withColumnRenamed("CdSkuLoja", "CdSku")
        .select("DtAtual", "year_month", "CdFilial", "CdSku",  "Receita", "QtMercadoria")
        .withColumn("TeveVenda",
                    F.when(F.col("QtMercadoria") > 0, F.lit(1))
                    .otherwise(F.lit(0)))
        )               


    return result

# Executar a função de vendas
sales_df = build_sales_view(spark)

# COMMAND ----------

# TODO - join entre vendas on e off por CdFilial, CdSku e DtAtual e com _ON ou _OFF

# COMMAND ----------


