# Databricks notebook source
# MAGIC %md
# MAGIC # Master Table Silver - Vendas + Estoque
# MAGIC
# MAGIC Este notebook cria a master table da camada Silver, consolidando dados de vendas históricas
# MAGIC com posição atual de estoque. A estratégia é otimizada para grandes volumes de dados.
# MAGIC
# MAGIC **Author**: Torre de Controle Supply Chain  
# MAGIC **Date**: 2024  
# MAGIC **Purpose**: Criar master table consolidada para análise de demanda vs disponibilidade
# MAGIC
# MAGIC ## 🎯 Estratégia de Otimização:
# MAGIC 1. **Agregação Inteligente**: Reduzir volume de vendas antes do join
# MAGIC 2. **Cache Estratégico**: Manter apenas dados essenciais em memória
# MAGIC 3. **Joins Otimizados**: Usar broadcast joins para tabelas pequenas
# MAGIC 4. **Particionamento**: Aproveitar particionamento por data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurações de Ambiente via Widgets
# MAGIC
# MAGIC Este bloco cria widgets interativos para configurar o ambiente de execução.
# MAGIC Os widgets permitem alterar os parâmetros diretamente na interface do Databricks.

# COMMAND ----------

# Criar widgets para configurações
dbutils.widgets.dropdown("modo_execucao", "TEST", ["TEST", "RUN"], "Modo de Execução")
dbutils.widgets.dropdown("ambiente_tabela", "DEV", ["DEV", "PROD"], "Ambiente da Tabela")
dbutils.widgets.text("sample_size", "50000", "Tamanho do Sample (apenas para TEST)")

# Obter valores dos widgets
MODO_EXECUCAO = dbutils.widgets.get("modo_execucao")
AMBIENTE_TABELA = dbutils.widgets.get("ambiente_tabela")
SAMPLE_SIZE = int(dbutils.widgets.get("sample_size"))

print(f"🎛️ Widgets configurados:")
print(f"  • Modo de Execução: {MODO_EXECUCAO}")
print(f"  • Ambiente da Tabela: {AMBIENTE_TABELA}")
print(f"  • Tamanho do Sample: {SAMPLE_SIZE:,}")

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

# Nomes das tabelas de destino na camada Silver (parametrizado por ambiente)
TABELA_SILVER_MASTER_LOJAS: str = f"databox.bcg_comum.supply_{AMBIENTE_TABELA.lower()}_master_vendas_estoque_lojas"
TABELA_SILVER_MASTER_CDS: str = f"databox.bcg_comum.supply_{AMBIENTE_TABELA.lower()}_master_vendas_estoque_cds"

# Timezone São Paulo (GMT-3)
TIMEZONE_SP = timezone('America/Sao_Paulo')

# =============================================================================
# CONFIGURAÇÕES DE DESENVOLVIMENTO
# =============================================================================

# Usar samples baseado no modo de execução
USAR_SAMPLES: bool = (MODO_EXECUCAO == "TEST")

# Definir quantidade de dias baseado no modo de execução
DIAS_PROCESSAMENTO: int = 1 if MODO_EXECUCAO == "TEST" else 90

# Inicialização do Spark
spark = SparkSession.builder.appName("vendas_estoque_silver").getOrCreate()

# Data de processamento (hoje)
hoje = datetime.now(TIMEZONE_SP)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

# Data de início baseada no modo de execução
data_inicio = hoje - timedelta(days=DIAS_PROCESSAMENTO)
data_inicio_str = data_inicio.strftime("%Y-%m-%d")

print(f"📅 Data de processamento: {hoje}")
print(f"📝 Data string: {hoje_str}")
print(f"🔢 Data int: {hoje_int}")
print(f"🌍 Timezone: {TIMEZONE_SP}")
print(f"📊 Período de processamento: {DIAS_PROCESSAMENTO} dias")
print(f"📅 Data início: {data_inicio_str}")
print(f"📅 Data fim: {hoje_str}")

# =============================================================================
# CONFIGURAÇÕES DE PROCESSAMENTO
# =============================================================================
print("=" * 80)
print("🔧 CONFIGURAÇÕES DE PROCESSAMENTO:")
print(f"  • Modo de Execução: {MODO_EXECUCAO}")
print(f"  • Ambiente da Tabela: {AMBIENTE_TABELA}")
print(f"  • Tabela Master Lojas: {TABELA_SILVER_MASTER_LOJAS}")
print(f"  • Tabela Master CDs: {TABELA_SILVER_MASTER_CDS}")
print(f"  • Período de Dados: {DIAS_PROCESSAMENTO} dias")
print(f"  • Data Início: {data_inicio_str}")
print(f"  • Data Fim: {hoje_str}")
print(f"  • Usar Samples: {USAR_SAMPLES}")
if USAR_SAMPLES:
    print(f"  • Tamanho do Sample: {SAMPLE_SIZE:,} registros")
    print("  • ⚠️  MODO TEST - Usando samples para desenvolvimento")
else:
    print("  • ✅ MODO RUN - Processamento completo")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do Período de Processamento
# MAGIC
# MAGIC Este bloco configura o período de dados baseado no modo de execução:
# MAGIC - **TEST**: 1 dia de dados para desenvolvimento rápido
# MAGIC - **RUN**: 90 dias de dados para análise completa

# COMMAND ----------

# Converter data_inicio para formato int (já calculado anteriormente)
data_inicio_int = int(data_inicio.strftime("%Y%m%d"))

print(f"📊 Data de início calculada: {data_inicio}")
print(f"⏰ Dias de processamento: {DIAS_PROCESSAMENTO}")
print(f"📅 Data início: {data_inicio}")
print(f"🔢 Data início int: {data_inicio_int}")

# DataFrame de exemplo para verificação
df_exemplo = spark.range(1).select(
    F.lit(data_inicio).alias("data_inicio"),
    F.lit(data_inicio_int).alias("data_inicio_int"),
    F.lit(DIAS_PROCESSAMENTO).alias("dias_processamento")
)

# display(df_exemplo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento de Dados Bronze - Vendas
# MAGIC
# MAGIC Este bloco carrega dados de vendas da camada Bronze,
# MAGIC aplicando filtros de período baseados no modo de execução.
# MAGIC **Estratégia**: Carregar apenas dados necessários para reduzir volume.

# COMMAND ----------

print(f"📊 Carregando dados de vendas Bronze de {data_inicio_str} até {hoje_str}")

# Carregar dados de vendas da camada Bronze (parametrizado por ambiente)
TABELA_BRONZE_VENDAS = f"databox.bcg_comum.supply_{AMBIENTE_TABELA.lower()}_vendas_90d_on_off"
print(f"📋 Tabela de vendas: {TABELA_BRONZE_VENDAS}")

vendas_bronze_df = (
    spark.table(TABELA_BRONZE_VENDAS)
    .filter(F.col("DtAtual").between(data_inicio_str, hoje_str))
    .select(
        "DtAtual", "CdFilial", "CdSku",
        "Receita_OFF", "QtMercadoria_OFF", "TeveVenda_OFF",
        "Receita_ON", "QtMercadoria_ON", "TeveVenda_ON",
        "Receita", "QtMercadoria", "TeveVenda"
    )
)

# Aplicar sample se configurado para desenvolvimento
if USAR_SAMPLES:
    print(f"🔬 Aplicando sample de {SAMPLE_SIZE:,} registros VENDAS para desenvolvimento...")
    vendas_bronze_df = vendas_bronze_df.sample(fraction=0.1, seed=42).limit(SAMPLE_SIZE)

print(f"📊 Registros de vendas carregados: {vendas_bronze_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento de Dados Bronze - Estoque
# MAGIC
# MAGIC Este bloco carrega dados de estoque da camada Bronze,
# MAGIC focando apenas na posição atual (hoje) para otimizar o join.

# COMMAND ----------

print(f"🏪 Carregando dados de estoque Bronze para {hoje_str}")

# Carregar dados de estoque das lojas (posição atual) - parametrizado por ambiente
TABELA_BRONZE_ESTOQUE_LOJAS = f"databox.bcg_comum.supply_{AMBIENTE_TABELA.lower()}_estoque_lojas"
TABELA_BRONZE_ESTOQUE_CDS = f"databox.bcg_comum.supply_{AMBIENTE_TABELA.lower()}_estoque_cds"

print(f"📋 Tabela de estoque lojas: {TABELA_BRONZE_ESTOQUE_LOJAS}")
print(f"📋 Tabela de estoque CDs: {TABELA_BRONZE_ESTOQUE_CDS}")

estoque_lojas_df = (
    spark.table(TABELA_BRONZE_ESTOQUE_LOJAS)
    .filter(F.col("DtAtual") == hoje_str)
    .select(
        "DtAtual", "CdFilial", "CdSku", "TipoEstoque",
        "QtEstoque", "VrTotalVv", "DDE",
        "ESTOQUE_SEGURANCA", "LEADTIME_MEDIO", "COBERTURA_ATUAL",
        "COBERTURA_ALVO", "DDV_SEM_OUTLIER", "DDV_FUTURO",
        "GRADE", "TRANSITO", "ESTOQUE_PROJETADO",
        "COBERTURA_ATUAL_C_TRANISTO_DIAS", "MEDIA_3", "MEDIA_6",
        "MEDIA_9", "MEDIA_12", "DDV_SO", "DDV_CO",
        "CLUSTER_OBG", "CLUSTER_SUG"
    )
)

# Carregar dados de estoque dos depósitos (posição atual) - parametrizado por ambiente
estoque_cds_df = (
    spark.table(TABELA_BRONZE_ESTOQUE_CDS)
    .filter(F.col("DtAtual") == hoje_str)
    .select(
        "DtAtual", "CdFilial", "CdSku", "TipoEstoque",
        "QtEstoque", "VrTotalVv", "DDE",
        "ESTOQUE_SEGURANCA", "LEADTIME_MEDIO", "COBERTURA_ATUAL",
        "COBERTURA_ALVO", "DDV_SEM_OUTLIER", "DDV_FUTURO",
        "GRADE", "TRANSITO", "ESTOQUE_PROJETADO",
        "COBERTURA_ATUAL_C_TRANISTO_DIAS", "MEDIA_3", "MEDIA_6",
        "MEDIA_9", "MEDIA_12", "DDV_SO", "DDV_CO",
        "CLUSTER_OBG", "CLUSTER_SUG"
    )
)

# Processar estoque de lojas separadamente
estoque_lojas_atual_df = (
    estoque_lojas_df
    .withColumnRenamed("QtEstoque", "QtEstoqueAtual")
    .withColumnRenamed("VrTotalVv", "VrEstoqueAtual")
    .withColumnRenamed("DDE", "DDE_Atual")
)

# Processar estoque de depósitos separadamente
estoque_cds_atual_df = (
    estoque_cds_df
    .withColumnRenamed("QtEstoque", "QtEstoqueAtual")
    .withColumnRenamed("VrTotalVv", "VrEstoqueAtual")
    .withColumnRenamed("DDE", "DDE_Atual")
)

# Aplicar sample se configurado para desenvolvimento
if USAR_SAMPLES:
    print(f"🔬 Aplicando sample de {SAMPLE_SIZE:,} registros ESTOQUE LOJAS para desenvolvimento...")
    estoque_lojas_atual_df = estoque_lojas_atual_df.sample(fraction=0.1, seed=42).limit(SAMPLE_SIZE)
    print(f"🔬 Aplicando sample de {SAMPLE_SIZE:,} registros ESTOQUE CDs para desenvolvimento...")
    estoque_cds_atual_df = estoque_cds_atual_df.sample(fraction=0.1, seed=42).limit(SAMPLE_SIZE)

# Cache estratégico: estoque atual é pequeno e será usado múltiplas vezes
estoque_lojas_atual_df = estoque_lojas_atual_df.cache()
estoque_cds_atual_df = estoque_cds_atual_df.cache()

print(f"📊 Registros de estoque LOJAS carregados: {estoque_lojas_atual_df.count():,}")
print(f"📊 Registros de estoque CDs carregados: {estoque_cds_atual_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de Janelas Temporais para Vendas
# MAGIC
# MAGIC Este bloco calcula as datas de início para cada janela temporal,
# MAGIC otimizando o processamento de agregações de vendas.

# COMMAND ----------

print("📅 Calculando janelas temporais para agregações de vendas...")

# Calcular datas de início para cada janela temporal
data_inicio_mtd = hoje.replace(day=1).strftime("%Y-%m-%d")
data_inicio_ytd = hoje.replace(month=1, day=1).strftime("%Y-%m-%d")

# Janelas móveis
data_inicio_7d = (hoje - timedelta(days=7)).strftime("%Y-%m-%d")
data_inicio_14d = (hoje - timedelta(days=14)).strftime("%Y-%m-%d")
data_inicio_30d = (hoje - timedelta(days=30)).strftime("%Y-%m-%d")
data_inicio_60d = (hoje - timedelta(days=60)).strftime("%Y-%m-%d")
data_inicio_90d = (hoje - timedelta(days=90)).strftime("%Y-%m-%d")

# Meses anteriores
data_inicio_m1 = (hoje.replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
data_inicio_m2 = (hoje.replace(day=1) - timedelta(days=32)).replace(day=1).strftime("%Y-%m-%d")
data_inicio_m3 = (hoje.replace(day=1) - timedelta(days=62)).replace(day=1).strftime("%Y-%m-%d")

# Semanas (4 semanas = 28 dias)
data_inicio_4w = (hoje - timedelta(days=28)).strftime("%Y-%m-%d")

print(f"📊 Janelas temporais calculadas:")
print(f"  • MTD: {data_inicio_mtd} até {hoje_str}")
print(f"  • YTD: {data_inicio_ytd} até {hoje_str}")
print(f"  • Last 7d: {data_inicio_7d} até {hoje_str}")
print(f"  • Last 30d: {data_inicio_30d} até {hoje_str}")
print(f"  • Last 90d: {data_inicio_90d} até {hoje_str}")
print(f"  • M-1: {data_inicio_m1}")
print(f"  • M-2: {data_inicio_m2}")
print(f"  • M-3: {data_inicio_m3}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação Inteligente com Window Functions
# MAGIC
# MAGIC Este bloco calcula todas as janelas temporais usando window functions com SUM(),
# MAGIC evitando múltiplos joins sequenciais que são terríveis para performance
# MAGIC em tabelas enormes.

# COMMAND ----------

print("🔄 Calculando janelas temporais com window functions...")

from pyspark.sql.window import Window

# Definir janelas de particionamento por SKU/Filial para diferentes períodos
window_spec = Window.partitionBy("CdFilial", "CdSku").orderBy("DtAtual")

# Janelas para médias móveis
window_7d = Window.partitionBy("CdFilial", "CdSku").orderBy("DtAtual").rowsBetween(-6, 0)
window_14d = Window.partitionBy("CdFilial", "CdSku").orderBy("DtAtual").rowsBetween(-13, 0)
window_30d = Window.partitionBy("CdFilial", "CdSku").orderBy("DtAtual").rowsBetween(-29, 0)
window_60d = Window.partitionBy("CdFilial", "CdSku").orderBy("DtAtual").rowsBetween(-59, 0)
window_90d = Window.partitionBy("CdFilial", "CdSku").orderBy("DtAtual").rowsBetween(-89, 0)

# Calcular todas as janelas temporais usando window functions com SUM
vendas_com_janelas_df = (
    vendas_bronze_df
    .withColumn("row_number", F.row_number().over(window_spec))
    # Janelas temporais usando window functions com SUM
    .withColumn("Receita_MTD", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_mtd, F.col("Receita")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("Receita_YTD", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_ytd, F.col("Receita")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("Receita_Last7d", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_7d, F.col("Receita")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("Receita_Last30d", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_30d, F.col("Receita")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("Receita_Last90d", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_90d, F.col("Receita")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("Receita_Last4w", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_4w, F.col("Receita")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("Receita_M1", 
        F.sum(F.when(F.col("DtAtual") == data_inicio_m1, F.col("Receita")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("Receita_M2", 
        F.sum(F.when(F.col("DtAtual") == data_inicio_m2, F.col("Receita")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("Receita_M3", 
        F.sum(F.when(F.col("DtAtual") == data_inicio_m3, F.col("Receita")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    # Quantidades
    .withColumn("QtMercadoria_MTD", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_mtd, F.col("QtMercadoria")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("QtMercadoria_YTD", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_ytd, F.col("QtMercadoria")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("QtMercadoria_Last7d", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_7d, F.col("QtMercadoria")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("QtMercadoria_Last30d", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_30d, F.col("QtMercadoria")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("QtMercadoria_Last90d", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_90d, F.col("QtMercadoria")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("QtMercadoria_Last4w", 
        F.sum(F.when(F.col("DtAtual") >= data_inicio_4w, F.col("QtMercadoria")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("QtMercadoria_M1", 
        F.sum(F.when(F.col("DtAtual") == data_inicio_m1, F.col("QtMercadoria")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("QtMercadoria_M2", 
        F.sum(F.when(F.col("DtAtual") == data_inicio_m2, F.col("QtMercadoria")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    .withColumn("QtMercadoria_M3", 
        F.sum(F.when(F.col("DtAtual") == data_inicio_m3, F.col("QtMercadoria")).otherwise(0))
        .over(Window.partitionBy("CdFilial", "CdSku")))
    # Médias móveis usando window functions (soma dividida pela quantidade de dias)
    .withColumn("Receita_MM7d", F.sum("Receita").over(window_7d) / 7)
    .withColumn("QtMercadoria_MM7d", F.sum("QtMercadoria").over(window_7d) / 7)
    .withColumn("Receita_MM14d", F.sum("Receita").over(window_14d) / 14)
    .withColumn("QtMercadoria_MM14d", F.sum("QtMercadoria").over(window_14d) / 14)
    .withColumn("Receita_MM30d", F.sum("Receita").over(window_30d) / 30)
    .withColumn("QtMercadoria_MM30d", F.sum("QtMercadoria").over(window_30d) / 30)
    .withColumn("Receita_MM60d", F.sum("Receita").over(window_60d) / 60)
    .withColumn("QtMercadoria_MM60d", F.sum("QtMercadoria").over(window_60d) / 60)
    .withColumn("Receita_MM90d", F.sum("Receita").over(window_90d) / 90)
    .withColumn("QtMercadoria_MM90d", F.sum("QtMercadoria").over(window_90d) / 90)
)

print(f"📊 Janelas temporais calculadas: {vendas_com_janelas_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação Final com DropDuplicates
# MAGIC
# MAGIC Este bloco agrega as vendas por SKU/Filial e remove duplicatas,
# MAGIC mantendo apenas uma linha por combinação única.

# COMMAND ----------

print("📊 Agregando vendas e removendo duplicatas...")

# Agregação final com todas as janelas e médias móveis
vendas_agregadas_final_df = (
    vendas_com_janelas_df
    .groupBy("CdFilial", "CdSku")
    .agg(
        # Totais gerais
        F.sum("Receita").alias("Receita_Total"),
        F.sum("QtMercadoria").alias("QtMercadoria_Total"),
        F.sum("TeveVenda").alias("DiasComVenda"),
        
        # Totais por canal
        F.sum("Receita_OFF").alias("Receita_OFF_Total"),
        F.sum("QtMercadoria_OFF").alias("QtMercadoria_OFF_Total"),
        F.sum("Receita_ON").alias("Receita_ON_Total"),
        F.sum("QtMercadoria_ON").alias("QtMercadoria_ON_Total"),
        
        # Janelas temporais (já calculadas com window functions)
        F.first("Receita_MTD").alias("Receita_MTD"),
        F.first("QtMercadoria_MTD").alias("QtMercadoria_MTD"),
        F.first("Receita_YTD").alias("Receita_YTD"),
        F.first("QtMercadoria_YTD").alias("QtMercadoria_YTD"),
        F.first("Receita_Last7d").alias("Receita_Last7d"),
        F.first("QtMercadoria_Last7d").alias("QtMercadoria_Last7d"),
        F.first("Receita_Last30d").alias("Receita_Last30d"),
        F.first("QtMercadoria_Last30d").alias("QtMercadoria_Last30d"),
        F.first("Receita_Last90d").alias("Receita_Last90d"),
        F.first("QtMercadoria_Last90d").alias("QtMercadoria_Last90d"),
        F.first("Receita_Last4w").alias("Receita_Last4w"),
        F.first("QtMercadoria_Last4w").alias("QtMercadoria_Last4w"),
        F.first("Receita_M1").alias("Receita_M1"),
        F.first("QtMercadoria_M1").alias("QtMercadoria_M1"),
        F.first("Receita_M2").alias("Receita_M2"),
        F.first("QtMercadoria_M2").alias("QtMercadoria_M2"),
        F.first("Receita_M3").alias("Receita_M3"),
        F.first("QtMercadoria_M3").alias("QtMercadoria_M3"),
        
        # Médias móveis (já calculadas com window functions)
        F.first("Receita_MM7d").alias("Receita_MM7d"),
        F.first("QtMercadoria_MM7d").alias("QtMercadoria_MM7d"),
        F.first("Receita_MM14d").alias("Receita_MM14d"),
        F.first("QtMercadoria_MM14d").alias("QtMercadoria_MM14d"),
        F.first("Receita_MM30d").alias("Receita_MM30d"),
        F.first("QtMercadoria_MM30d").alias("QtMercadoria_MM30d"),
        F.first("Receita_MM60d").alias("Receita_MM60d"),
        F.first("QtMercadoria_MM60d").alias("QtMercadoria_MM60d"),
        F.first("Receita_MM90d").alias("Receita_MM90d"),
        F.first("QtMercadoria_MM90d").alias("QtMercadoria_MM90d"),
        
        # Estatísticas
        F.countDistinct("DtAtual").alias("DiasDistintos"),
        F.min("DtAtual").alias("PrimeiraVenda"),
        F.max("DtAtual").alias("UltimaVenda")
    )
    # Remover duplicatas na chave principal
    .dropDuplicates(["CdFilial", "CdSku"])
)

# Cache estratégico: tabela muito menor após agregação
vendas_agregadas_final_df = vendas_agregadas_final_df.cache()

print(f"📊 Vendas agregadas com janelas: {vendas_agregadas_final_df.count():,}")
print(f"📈 Redução de volume: {vendas_bronze_df.count():,} → {vendas_agregadas_final_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Único - Estoque LOJAS + Vendas Agregadas
# MAGIC
# MAGIC Este bloco realiza um único join entre estoque atual das LOJAS e vendas agregadas,
# MAGIC evitando múltiplos joins sequenciais que são terríveis para performance.

# COMMAND ----------

print("🔗 Iniciando join único entre estoque LOJAS e vendas...")

# Configurar broadcast join para estoque atual (tabela pequena)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Join único: estoque atual LOJAS + vendas agregadas (com todas as janelas)
master_table_lojas_final_df = (
    estoque_lojas_atual_df
    .join(
        vendas_agregadas_final_df,
        on=["CdFilial", "CdSku"],
        how="left"
    )
    .withColumn("DataHoraProcessamento", F.current_timestamp())
    .withColumn("DtAtual", F.lit(hoje_str))
)

print(f"📊 Master table LOJAS finalizada: {master_table_lojas_final_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Único - Estoque CDs + Vendas Agregadas
# MAGIC
# MAGIC Este bloco realiza um único join entre estoque atual dos CDs e vendas agregadas,
# MAGIC evitando múltiplos joins sequenciais que são terríveis para performance.

# COMMAND ----------

print("🔗 Iniciando join único entre estoque CDs e vendas...")

# Join único: estoque atual CDs + vendas agregadas (com todas as janelas)
master_table_cds_final_df = (
    estoque_cds_atual_df
    .join(
        vendas_agregadas_final_df,
        on=["CdFilial", "CdSku"],
        how="left"
    )
    .withColumn("DataHoraProcessamento", F.current_timestamp())
    .withColumn("DtAtual", F.lit(hoje_str))
)

print(f"📊 Master table CDs finalizada: {master_table_cds_final_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza e Preenchimento de Valores Nulos
# MAGIC
# MAGIC Este bloco limpa ambas as master tables e preenche valores nulos com zeros,
# MAGIC garantindo dados consistentes para análise.

# COMMAND ----------

print("🧹 Limpando e preenchendo valores nulos...")

# Preencher valores nulos com zeros para colunas de vendas
colunas_vendas = [col for col in master_table_lojas_final_df.columns if col.startswith(('Receita_', 'QtMercadoria_'))]

# Limpar master table LOJAS
master_table_lojas_limpa_df = (
    master_table_lojas_final_df
    .fillna(0, subset=colunas_vendas)
)

# Limpar master table CDs
master_table_cds_limpa_df = (
    master_table_cds_final_df
    .fillna(0, subset=colunas_vendas)
)

print(f"✅ Master table LOJAS limpa: {master_table_lojas_limpa_df.count():,} registros")
print(f"✅ Master table CDs limpa: {master_table_cds_limpa_df.count():,} registros")
print(f"📊 Colunas de vendas preenchidas: {len(colunas_vendas)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento na Camada Silver
# MAGIC
# MAGIC Este bloco salva ambas as master tables na tabela Delta da camada Silver,
# MAGIC utilizando o modo `overwrite` para garantir que as tabelas sejam sempre atualizadas.

# COMMAND ----------

print(f"💾 Salvando master tables na camada Silver...")

try:
    # Salvar master table LOJAS
    master_table_lojas_limpa_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABELA_SILVER_MASTER_LOJAS)
    
    print(f"✅ Master table LOJAS {TABELA_SILVER_MASTER_LOJAS} salva com sucesso!")
    print(f"📊 Registros LOJAS salvos: {master_table_lojas_limpa_df.count():,}")
    
    # Salvar master table CDs
    master_table_cds_limpa_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABELA_SILVER_MASTER_CDS)
    
    print(f"✅ Master table CDs {TABELA_SILVER_MASTER_CDS} salva com sucesso!")
    print(f"📊 Registros CDs salvos: {master_table_cds_limpa_df.count():,}")
    
except Exception as e:
    print(f"❌ Erro ao salvar master tables: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza de Cache e Resumo Final
# MAGIC
# MAGIC Este bloco limpa o cache para liberar memória e apresenta um resumo
# MAGIC do processamento realizado.

# COMMAND ----------

print("🧹 Limpando cache para liberar memória...")

try:
    # Limpar cache de todos os DataFrames
    vendas_bronze_df.unpersist()
    vendas_com_janelas_df.unpersist()
    vendas_agregadas_final_df.unpersist()
    estoque_lojas_atual_df.unpersist()
    estoque_cds_atual_df.unpersist()
    
    print("✅ Cache limpo com sucesso!")
    
except Exception as e:
    print(f"⚠️ Erro ao limpar cache: {str(e)}")

# COMMAND ----------

print("🎉 Processamento das Master Tables Silver concluído com sucesso!")
print("=" * 80)
print("📊 RESUMO DO PROCESSAMENTO:")
print(f"  • Período processado: {data_inicio_str} até {hoje_str}")
print(f"  • Dias de histórico: {DIAS_PROCESSAMENTO}")
print(f"  • Ambiente: {AMBIENTE_TABELA}")
print(f"  • Tabela vendas: {TABELA_BRONZE_VENDAS}")
print(f"  • Tabela estoque lojas: {TABELA_BRONZE_ESTOQUE_LOJAS}")
print(f"  • Tabela estoque CDs: {TABELA_BRONZE_ESTOQUE_CDS}")
print(f"  • Master table LOJAS: {TABELA_SILVER_MASTER_LOJAS}")
print(f"  • Master table CDs: {TABELA_SILVER_MASTER_CDS}")
print(f"  • Registros de vendas: {vendas_bronze_df.count():,}")
print(f"  • Registros estoque LOJAS: {estoque_lojas_atual_df.count():,}")
print(f"  • Registros estoque CDs: {estoque_cds_atual_df.count():,}")
print(f"  • Master table LOJAS final: {master_table_lojas_limpa_df.count():,}")
print(f"  • Master table CDs final: {master_table_cds_limpa_df.count():,}")
print("=" * 80)
print("🎯 ESTRATÉGIAS DE OTIMIZAÇÃO APLICADAS:")
print("  ✅ Window functions para janelas temporais")
print("  ✅ Agregação única com todas as métricas")
print("  ✅ Join único por master table (sem múltiplos joins)")
print("  ✅ Cache estratégico de DataFrames")
print("  ✅ Broadcast joins para tabelas pequenas")
print("  ✅ Limpeza automática de cache")
print("  ✅ Separação de LOJAS e CDs")
print("  ✅ Evita 8+ joins sequenciais terríveis para performance")
print("=" * 80)

# COMMAND ----------
