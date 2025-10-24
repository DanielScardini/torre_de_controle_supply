# Databricks notebook source
# MAGIC %md
# MAGIC # Processamento de Vendas - Camada Bronze
# MAGIC
# MAGIC Este notebook processa dados de vendas online e offline para a camada Bronze,
# MAGIC seguindo o padrão Medallion Architecture e as melhores práticas Python.
# MAGIC
# MAGIC **Author**: Torre de Controle Supply Chain  
# MAGIC **Date**: 2024  
# MAGIC **Purpose**: Processar vendas online e offline para análise de demanda e planejamento de estoque

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports e Configuração Inicial

# COMMAND ----------

from datetime import datetime, timedelta, date
from typing import Optional, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pytz import timezone

# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================

# Nome da tabela de destino na camada Bronze
TABELA_BRONZE_VENDAS: str = "databox.bcg_comum.supply_bronze_vendas_90d_on_off"

# Timezone São Paulo (GMT-3)
TIMEZONE_SP = timezone('America/Sao_Paulo')

# =============================================================================
# CONFIGURAÇÕES DE DESENVOLVIMENTO
# =============================================================================

# Usar samples para desenvolvimento (evitar gasto de processamento)
USAR_SAMPLES: bool = True  # Alterar para False em produção
SAMPLE_SIZE: int = 10000  # Tamanho do sample para desenvolvimento

# Inicialização do Spark
spark = SparkSession.builder.appName("vendas_bronze").getOrCreate()

# Data de processamento (ontem)
hoje = datetime.now(TIMEZONE_SP) - timedelta(days=0)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

print(f"📅 Data de processamento: {hoje}")
print(f"📝 Data string: {hoje_str}")
print(f"🔢 Data int: {hoje_int}")
print(f"🌍 Timezone: {TIMEZONE_SP}")

# =============================================================================
# CONFIGURAÇÕES DE PROCESSAMENTO
# =============================================================================
print("=" * 80)
print("🔧 CONFIGURAÇÕES DE PROCESSAMENTO:")
print(f"  • Usar Samples: {USAR_SAMPLES}")
if USAR_SAMPLES:
    print(f"  • Tamanho do Sample: {SAMPLE_SIZE:,} registros")
    print("  • ⚠️  MODO DESENVOLVIMENTO - Alterar USAR_SAMPLES=False para produção")
else:
    print("  • ✅ MODO PRODUÇÃO - Processamento completo")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo da Data de Início

# COMMAND ----------

# Configuração do período de análise
dias_retrocesso = 1

# Validação do parâmetro
if dias_retrocesso < 0:
    raise ValueError("dias_retrocesso deve ser positivo")

# Cálculo da data de início
hoje_dt = datetime.now(TIMEZONE_SP)
data_inicio = hoje_dt - timedelta(days=dias_retrocesso)
data_inicio_int = int(data_inicio.strftime("%Y%m%d"))
data_inicio_str = data_inicio.strftime("%Y-%m-%d")

print(f"📊 Data de início calculada: {data_inicio}")
print(f"⏰ Dias de retrocesso: {dias_retrocesso}")
print(f"📅 Data início: {data_inicio}")
print(f"🔢 Data início int: {data_inicio_int}")

# DataFrame de exemplo para verificação
df_exemplo = spark.range(1).select(
    F.lit(data_inicio).alias("data_inicio"),
    F.lit(data_inicio_int).alias("data_inicio_int"),
    F.lit(dias_retrocesso).alias("dias_retrocesso")
)

# display(df_exemplo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento de Vendas Offline (Loja Física)

# COMMAND ----------

print(f"🏪 Processando vendas OFFLINE de {data_inicio_int} até {hoje_int}")

# 💾 Cache Strategy: Aplicamos cache nos DataFrames principais que serão
# reutilizados múltiplas vezes durante o processamento para otimizar performance
# Carregar tabela de vendas rateadas (offline)
vendas_rateadas_offline_df = (
        spark.table("app_venda.vendafaturadarateada")
        .filter(F.col("NmEstadoMercadoria") != '1 - SALDO')
        .filter(F.col("NmTipoNegocio") == 'LOJA FISICA')
        .filter(
        F.col("DtAprovacao").between(data_inicio_int, hoje_int)
            & (F.col("VrOperacao") >= 0)
            & (F.col("VrCustoContabilFilialSku") >= 0)
        )
    )  

# Aplicar sample se configurado para desenvolvimento
if USAR_SAMPLES:
    print(f"🔬 Aplicando sample de {SAMPLE_SIZE:,} registros para desenvolvimento...")
    vendas_rateadas_offline_df = vendas_rateadas_offline_df.sample(fraction=0.1, seed=42).limit(SAMPLE_SIZE)

vendas_rateadas_offline_df = vendas_rateadas_offline_df.cache()

print(f"📊 Registros rateados carregados: {vendas_rateadas_offline_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento de Quantidades (Vendas Não Rateadas)

# COMMAND ----------

# Carregar tabela de vendas não rateadas para quantidade
vendas_nao_rateadas_df = (
        spark.table("app_venda.vendafaturadanaorateada")
        .filter(F.col("QtMercadoria") >= 0)
        .filter(F.col("DtEmissaoFaturamento").between(data_inicio_int, hoje_int))
        .select("ChaveFatos", "QtMercadoria")
)

print(f"📈 Registros não rateados carregados: {vendas_nao_rateadas_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unificação dos Dados Offline

# COMMAND ----------

# Unificar dados e aplicar transformações
vendas_offline_unificadas_df = (
    vendas_rateadas_offline_df
    .join(vendas_nao_rateadas_df.select("ChaveFatos", "QtMercadoria"), on="ChaveFatos")
        .withColumn(
            "year_month",
        F.date_format(
            F.to_date(F.col("DtAprovacao").cast("string"), "yyyyMMdd"), 
            "yyyyMM"
        ).cast("int")
    )
    .withColumnRenamed("CdFilialVenda", "CdFilial")
    .withColumn(
        "DtAtual",
        F.date_format(
            F.to_date(F.col("DtAprovacao").cast("string"), "yyyyMMdd"), 
            "yyyy-MM-dd"
        )
    )
).cache()

print(f"🔗 Registros após join: {vendas_offline_unificadas_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação das Vendas Offline

# COMMAND ----------

# Agregar por filial, SKU e data
vendas_offline_agregadas_df = (
    vendas_offline_unificadas_df.groupBy(
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

print(f"📊 Registros após agregação: {vendas_offline_agregadas_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação da Grade Completa de Datas

# COMMAND ----------

# Criar grade completa de datas
calendario_df = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                F.to_date(F.lit(str(data_inicio_int)), "yyyyMMdd"),
                F.to_date(F.lit(str(hoje_int)), "yyyyMMdd"),
                    F.expr("interval 1 day")
                )
            ).alias("DtAtual_date")
        )
    )

# Conjunto de chaves (Filial x SKU) para loja física
chaves_filial_sku_df = (
    vendas_offline_unificadas_df
          .select("CdFilial", "CdSkuLoja")
          .dropDuplicates()
    )

print(f"🔑 Chaves únicas (Filial x SKU): {chaves_filial_sku_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grade Completa Offline (Data x Filial x SKU)

# COMMAND ----------

# Grade completa (Data x Filial x SKU)
grade_completa_offline_df = calendario_df.crossJoin(chaves_filial_sku_df)

# Agregado com data como DateType para join
vendas_offline_agregadas_com_data_df = vendas_offline_agregadas_df.withColumn("DtAtual_date", F.to_date("DtAtual"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicação da Grade Completa Offline

# COMMAND ----------

# Left join + zeros onde não houver venda
vendas_offline_final_df = (
    grade_completa_offline_df.join(
        vendas_offline_agregadas_com_data_df,
        on=["DtAtual_date", "CdSkuLoja", "CdFilial"],
            how="left"
    )
    .withColumn("Receita", F.coalesce(F.col("Receita"), F.lit(0.0)))
    .withColumn("QtMercadoria", F.coalesce(F.col("QtMercadoria"), F.lit(0.0)))
    .withColumn("year_month", F.date_format(F.col("DtAtual_date"), "yyyyMM").cast("int"))
    .withColumn("DtAtual", F.date_format(F.col("DtAtual_date"), "yyyy-MM-dd"))
    .withColumnRenamed("CdSkuLoja", "CdSku")
    .select("DtAtual", "year_month", "CdFilial", "CdSku", "Receita", "QtMercadoria")
    .withColumn(
        "TeveVenda",
        F.when(F.col("QtMercadoria") > 0, F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn("Canal", F.lit("OFFLINE"))
)

print(f"✅ Registros finais OFFLINE: {vendas_offline_final_df.count()}")

# Mostrar amostra dos dados
print("📋 Amostra dos dados OFFLINE:")
# display(vendas_offline_final_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento de Vendas Online (Canais Digitais)

# COMMAND ----------

print(f"🌐 Processando vendas ONLINE de {data_inicio_int} até {hoje_int}")

# 💾 Cache Strategy: Reutilizando vendas_nao_rateadas_df já em cache
# Carregar tabela de vendas rateadas (online)
vendas_rateadas_online_df = (
    spark.table("app_venda.vendafaturadarateada")
    .filter(F.col("NmEstadoMercadoria") != '1 - SALDO')
    .filter(F.col("NmTipoNegocio") != 'LOJA FISICA')  # Excluir loja física
    .filter(
        F.col("DtAprovacao").between(data_inicio_int, hoje_int)
        & (F.col("VrOperacao") >= 0)
        & (F.col("VrCustoContabilFilialSku") >= 0)
    )
)

# Aplicar sample se configurado para desenvolvimento
if USAR_SAMPLES:
    print(f"🔬 Aplicando sample de {SAMPLE_SIZE:,} registros ONLINE para desenvolvimento...")
    vendas_rateadas_online_df = vendas_rateadas_online_df.sample(fraction=0.1, seed=42).limit(SAMPLE_SIZE)

vendas_rateadas_online_df = vendas_rateadas_online_df.cache()

print(f"📊 Registros rateados ONLINE carregados: {vendas_rateadas_online_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unificação dos Dados Online

# COMMAND ----------

# Unificar dados e aplicar transformações
vendas_online_unificadas_df = (
    vendas_rateadas_online_df
    .join(vendas_nao_rateadas_df.select("ChaveFatos", "QtMercadoria"), on="ChaveFatos")
    .withColumn(
        "year_month",
        F.date_format(
            F.to_date(F.col("DtAprovacao").cast("string"), "yyyyMMdd"), 
            "yyyyMM"
        ).cast("int")
    )
    .withColumnRenamed("CdFilialEmissao", "CdFilial")
    .withColumn(
        "DtAtual",
        F.date_format(
            F.to_date(F.col("DtAprovacao").cast("string"), "yyyyMMdd"), 
            "yyyy-MM-dd"
        )
    )
).cache()

print(f"🔗 Registros após join ONLINE: {vendas_online_unificadas_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação das Vendas Online

# COMMAND ----------

# Agregar por filial, SKU e data
vendas_online_agregadas_df = (
    vendas_online_unificadas_df.groupBy(
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

print(f"📊 Registros após agregação ONLINE: {vendas_online_agregadas_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação da Grade Completa Online

# COMMAND ----------

# Conjunto de chaves (Filial x SKU) para online
chaves_filial_sku_online_df = (
    vendas_online_unificadas_df
    .select("CdFilial", "CdSkuLoja")
    .dropDuplicates()
)

print(f"🔑 Chaves únicas ONLINE (Filial x SKU): {chaves_filial_sku_online_df.count()}")

# Grade completa (Data x Filial x SKU)
grade_completa_online_df = calendario_df.crossJoin(chaves_filial_sku_online_df)

# Agregado com data como DateType para join
vendas_online_agregadas_com_data_df = vendas_online_agregadas_df.withColumn("DtAtual_date", F.to_date("DtAtual"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicação da Grade Completa Online

# COMMAND ----------

# Left join + zeros onde não houver venda
vendas_online_final_df = (
    grade_completa_online_df.join(
        vendas_online_agregadas_com_data_df,
        on=["DtAtual_date", "CdSkuLoja", "CdFilial"],
        how="left"
    )
    .withColumn("Receita", F.coalesce(F.col("Receita"), F.lit(0.0)))
        .withColumn("QtMercadoria", F.coalesce(F.col("QtMercadoria"), F.lit(0.0)))
        .withColumn("year_month", F.date_format(F.col("DtAtual_date"), "yyyyMM").cast("int"))
    .withColumn("DtAtual", F.date_format(F.col("DtAtual_date"), "yyyy-MM-dd"))
        .withColumnRenamed("CdSkuLoja", "CdSku")
    .select("DtAtual", "year_month", "CdFilial", "CdSku", "Receita", "QtMercadoria")
    .withColumn(
        "TeveVenda",
        F.when(F.col("QtMercadoria") > 0, F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn("Canal", F.lit("ONLINE"))
)

print(f"✅ Registros finais ONLINE: {vendas_online_final_df.count()}")

# Mostrar amostra dos dados
print("📋 Amostra dos dados ONLINE:")
# display(vendas_online_final_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consolidação de Vendas Online e Offline com Outer Join

# COMMAND ----------

print("🔄 Consolidando vendas ONLINE e OFFLINE com outer join...")

# 💾 Cache Strategy: DataFrame final consolidado em cache para múltiplas operações
# Validação de dados
if vendas_offline_final_df.count() == 0 and vendas_online_final_df.count() == 0:
    raise ValueError("Ambos os DataFrames de vendas estão vazios")

# Fazer outer join para garantir que todas as filiais apareçam em ambos os canais
# Primeiro, vamos preparar os DataFrames para o join
vendas_offline_preparadas_df = (
    vendas_offline_final_df
    .withColumnRenamed("Receita", "Receita_OFF")
    .withColumnRenamed("QtMercadoria", "QtMercadoria_OFF")
    .withColumnRenamed("TeveVenda", "TeveVenda_OFF")
    .drop("Canal")  # Remover coluna Canal pois não precisamos mais
)

vendas_online_preparadas_df = (
    vendas_online_final_df
    .withColumnRenamed("Receita", "Receita_ON")
    .withColumnRenamed("QtMercadoria", "QtMercadoria_ON")
    .withColumnRenamed("TeveVenda", "TeveVenda_ON")
    .drop("Canal")  # Remover coluna Canal pois não precisamos mais
)

# Outer join para garantir todas as combinações
vendas_consolidadas_temp_df = (
    vendas_offline_preparadas_df
    .join(
        vendas_online_preparadas_df,
        on=["DtAtual", "year_month", "CdFilial", "CdSku"],
        how="outer"
    )
)

# Preencher valores nulos com zeros para garantir dados completos
vendas_consolidadas_df = (
    vendas_consolidadas_temp_df
    .fillna(0, subset=[
        "Receita_OFF", "QtMercadoria_OFF", "TeveVenda_OFF",
        "Receita_ON", "QtMercadoria_ON", "TeveVenda_ON"
    ])
    # Calcular totais consolidados
    .withColumn("Receita", F.col("Receita_OFF") + F.col("Receita_ON"))
    .withColumn("QtMercadoria", F.col("QtMercadoria_OFF") + F.col("QtMercadoria_ON"))
    .withColumn("TeveVenda", F.col("TeveVenda_OFF") + F.col("TeveVenda_ON"))
    # Selecionar colunas finais
    .select(
        "DtAtual", "year_month", "CdFilial", "CdSku",
        "Receita_OFF", "QtMercadoria_OFF", "TeveVenda_OFF",
        "Receita_ON", "QtMercadoria_ON", "TeveVenda_ON",
        "Receita", "QtMercadoria", "TeveVenda"
    )
).cache()

print(f"📊 Total de registros consolidados: {vendas_consolidadas_df.count()}")

# Mostrar estatísticas de filiais por canal
print("📈 Estatísticas de filiais por canal:")
filiais_offline = vendas_offline_final_df.select("CdFilial").distinct().count()
filiais_online = vendas_online_final_df.select("CdFilial").distinct().count()
filiais_consolidadas = vendas_consolidadas_df.select("CdFilial").distinct().count()

print(f"  • Filiais com vendas OFFLINE: {filiais_offline}")
print(f"  • Filiais com vendas ONLINE: {filiais_online}")
print(f"  • Filiais consolidadas: {filiais_consolidadas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise Estatística Consolidada

# COMMAND ----------

# Mostrar estatísticas consolidadas
print("📈 Estatísticas consolidadas:")
estatisticas_consolidadas_df = vendas_consolidadas_df.agg(
    F.count("*").alias("Total_Registros"),
    F.sum("Receita").alias("Receita_Total"),
    F.sum("QtMercadoria").alias("Quantidade_Total"),
    F.sum("TeveVenda").alias("Dias_Com_Venda"),
    F.countDistinct("CdFilial").alias("Filiais_Unicas"),
    F.countDistinct("CdSku").alias("SKUs_Unicos"),
    F.sum("Receita_OFF").alias("Receita_OFF"),
    F.sum("Receita_ON").alias("Receita_ON"),
    F.sum("QtMercadoria_OFF").alias("Quantidade_OFF"),
    F.sum("QtMercadoria_ON").alias("Quantidade_ON")
)

# display(estatisticas_consolidadas_df)

# Mostrar estatísticas por filial (top 10)
print("📊 Top 10 filiais por receita total:")
top_filiais_df = (
    vendas_consolidadas_df
    .groupBy("CdFilial")
    .agg(
        F.sum("Receita").alias("Receita_Total"),
        F.sum("Receita_OFF").alias("Receita_OFF"),
        F.sum("Receita_ON").alias("Receita_ON"),
        F.sum("QtMercadoria").alias("Quantidade_Total"),
        F.sum("TeveVenda").alias("Dias_Com_Venda")
    )
    .orderBy(F.desc("Receita_Total"))
    .limit(10)
)

# display(top_filiais_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adição de Metadados de Processamento

# COMMAND ----------

# Adicionar metadados de processamento
vendas_com_metadados_df = vendas_consolidadas_df.withColumn(
    "DataHoraProcessamento", 
    F.current_timestamp()
).withColumn(
    "DataProcessamento",
    F.current_date()
).withColumn(
    "FonteDados",
    F.lit("app_venda.vendafaturadarateada + vendafaturadanaorateada")
).withColumn(
    "VersaoProcessamento",
    F.lit("1.0")
)

print(f"📊 Registros com metadados: {vendas_com_metadados_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento na Camada Bronze

# COMMAND ----------

print(f"💾 Salvando tabela {TABELA_BRONZE_VENDAS} no modo overwrite...")

try:
    # Salvar na camada Bronze
    vendas_com_metadados_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABELA_BRONZE_VENDAS)
    
    print(f"✅ Tabela {TABELA_BRONZE_VENDAS} salva com sucesso!")
    print(f"📊 Registros salvos: {vendas_com_metadados_df.count()}")

except Exception as e:
    print(f"❌ Erro ao salvar tabela {TABELA_BRONZE_VENDAS}: {str(e)}")
    raise
finally:
    # Limpar cache para liberar memória
    print("🧹 Limpando cache para liberar memória...")
    vendas_rateadas_offline_df.unpersist()
    vendas_nao_rateadas_df.unpersist()
    vendas_offline_unificadas_df.unpersist()
    vendas_rateadas_online_df.unpersist()
    vendas_online_unificadas_df.unpersist()
    vendas_consolidadas_df.unpersist()
    print("✅ Cache limpo com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação da Tabela Salva

# COMMAND ----------

# Mostrar schema da tabela salva
print("📋 Schema da tabela salva:")
spark.table(TABELA_BRONZE_VENDAS).printSchema()

# Mostrar amostra dos dados salvos
print("📋 Amostra dos dados salvos:")
# display(spark.table(TABELA_BRONZE_VENDAS).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo Final do Processamento

# COMMAND ----------

print("🎉 Processamento de vendas Bronze concluído com sucesso!")
print("=" * 80)
print("📊 RESUMO DO PROCESSAMENTO:")
print(f"  • Período processado: {data_inicio_str} até {hoje_str}")
print(f"  • Dias de histórico: {dias_retrocesso}")
print(f"  • Registros offline: {vendas_offline_final_df.count():,}")
print(f"  • Registros online: {vendas_online_final_df.count():,}")
print(f"  • Total consolidado: {vendas_consolidadas_df.count():,}")
print(f"  • Filiais únicas: {filiais_consolidadas}")
print(f"  • Tabela de destino: {TABELA_BRONZE_VENDAS}")
print("=" * 80)
print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
print("🔄 Outer join aplicado para garantir todas as combinações filial-SKU")
print("🔢 Valores nulos preenchidos com zeros")
print("📊 Estrutura: SKU x Loja x Dia com colunas separadas por canal")
print("📈 Colunas: Receita_OFF, Receita_ON, QtMercadoria_OFF, QtMercadoria_ON, etc.")
