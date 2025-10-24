# Databricks notebook source
# MAGIC %md
# MAGIC # Processamento de Estoque - Camada Bronze
# MAGIC
# MAGIC Este notebook processa dados de estoque de lojas e depósitos para a camada Bronze,
# MAGIC seguindo o padrão Medallion Architecture e as melhores práticas Python.
# MAGIC
# MAGIC **Author**: Torre de Controle Supply Chain  
# MAGIC **Date**: 2024  
# MAGIC **Purpose**: Processar estoque de lojas e depósitos para análise de disponibilidade e planejamento de abastecimento

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

# Nome das tabelas de destino na camada Bronze
TABELA_BRONZE_ESTOQUE_LOJA: str = "databox.bcg_comum.supply_bronze_estoque_lojas"
TABELA_BRONZE_ESTOQUE_CD: str = "databox.bcg_comum.supply_bronze_estoque_cds"

# Timezone São Paulo (GMT-3)
TIMEZONE_SP = timezone('America/Sao_Paulo')

# =============================================================================
# CONFIGURAÇÕES DE DESENVOLVIMENTO
# =============================================================================

# Usar samples para desenvolvimento (evitar gasto de processamento)
USAR_SAMPLES: bool = True  # Alterar para False em produção
SAMPLE_SIZE: int = 100000  # Tamanho do sample para desenvolvimento

# Inicialização do Spark
spark = SparkSession.builder.appName("estoque_bronze").getOrCreate()

# Data de processamento (hoje)
hoje = datetime.now(TIMEZONE_SP)
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
# MAGIC ## Carregamento de Dados de Estoque - Lojas
# MAGIC
# MAGIC Este bloco carrega dados de estoque das lojas ativas (`DsEstoqueLojaDeposito == "L"`).
# MAGIC Os dados incluem informações de disponibilidade, classificação ABC e métricas de DDE
# MAGIC para análise de qualidade do estoque.

# COMMAND ----------

print("🏪 Processando estoque de LOJAS...")

# Carregar dados de estoque das lojas
estoque_lojas_df = (
    spark.table("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
    .filter(F.col("DtAtual") >= hoje_str)
    .filter(F.col("StLoja") == "ATIVA")
    .filter(F.col("DsEstoqueLojaDeposito") == "L")
)

# Aplicar sample se configurado para desenvolvimento
if USAR_SAMPLES:
    print(f"🔬 Aplicando sample de {SAMPLE_SIZE:,} registros LOJAS para desenvolvimento...")
    estoque_lojas_df = estoque_lojas_df.sample(fraction=0.1, seed=42).limit(SAMPLE_SIZE)

estoque_lojas_df = estoque_lojas_df.cache()

print(f"📊 Registros de estoque LOJAS carregados: {estoque_lojas_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento de Dados de Estoque - Depósitos (CDs)
# MAGIC
# MAGIC Este bloco carrega dados de estoque dos depósitos (`DsEstoqueLojaDeposito == "D"`).
# MAGIC Os dados incluem informações de disponibilidade nos centros de distribuição
# MAGIC para análise de capacidade de abastecimento.

# COMMAND ----------

print("🏭 Processando estoque de DEPÓSITOS (CDs)...")

# Carregar dados de estoque dos depósitos
estoque_cds_df = (
    spark.table("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
    .filter(F.col("DtAtual") >= hoje_str)
    .filter(F.col("DsEstoqueLojaDeposito") == "D")
)

# Aplicar sample se configurado para desenvolvimento
if USAR_SAMPLES:
    print(f"🔬 Aplicando sample de {SAMPLE_SIZE:,} registros CDs para desenvolvimento...")
    estoque_cds_df = estoque_cds_df.sample(fraction=0.1, seed=42).limit(SAMPLE_SIZE)

estoque_cds_df = estoque_cds_df.cache()

print(f"📊 Registros de estoque CDs carregados: {estoque_cds_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformação e Limpeza dos Dados - Lojas
# MAGIC
# MAGIC Este bloco aplica transformações e limpeza nos dados de estoque das lojas,
# MAGIC incluindo seleção de colunas relevantes, cálculos de métricas e remoção de duplicatas.

# COMMAND ----------

print("🔄 Transformando dados de estoque LOJAS...")

# Transformar e limpar dados de estoque das lojas
estoque_lojas_processado_df = (
    estoque_lojas_df
    .select(
        "CdFilial", 
        "CdSku",
        "DsSku",
        "DsSetor",
        "DsCurva",
        "DsCurvaAbcLoja",
        "StLinha",
        "DsObrigatorio",
        "DsVoltagem",
        F.col("DsTipoEntrega").alias("TipoEntrega"),
        F.col("CdEstoqueFilialAbastecimento").alias("QtdEstoqueCDVinculado"),
        (F.col("VrTotalVv")/F.col("VrVndCmv")).alias("DDE"),
        F.col("QtEstoqueBoaOff").alias("EstoqueLoja"),
        F.col("DsFaixaDde").alias("ClassificacaoDDE"),
        F.col("data_ingestao"),
        F.date_format(F.col("data_ingestao"), "yyyy-MM-dd").alias("DtAtual")    
    )
    .dropDuplicates(["DtAtual", "CdSku", "CdFilial"])
    .withColumn("TipoEstoque", F.lit("LOJA"))
).cache()

print(f"✅ Registros de estoque LOJAS processados: {estoque_lojas_processado_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformação e Limpeza dos Dados - Depósitos
# MAGIC
# MAGIC Este bloco aplica transformações e limpeza nos dados de estoque dos depósitos,
# MAGIC incluindo seleção de colunas relevantes e cálculos de métricas.

# COMMAND ----------

print("🔄 Transformando dados de estoque DEPÓSITOS...")

# Transformar e limpar dados de estoque dos depósitos
estoque_cds_processado_df = (
    estoque_cds_df
    .select(
        "CdFilial", 
        "CdSku",
        "DsSku",
        "DsSetor",
        "DsCurva",
        "DsCurvaAbcLoja",
        "StLinha",
        "DsObrigatorio",
        "DsVoltagem",
        F.col("DsTipoEntrega").alias("TipoEntrega"),
        F.col("CdEstoqueFilialAbastecimento").alias("QtdEstoqueCDVinculado"),
        (F.col("VrTotalVv")/F.col("VrVndCmv")).alias("DDE"),
        F.col("QtEstoqueBoaOff").alias("EstoqueDeposito"),
        F.col("DsFaixaDde").alias("ClassificacaoDDE"),
        F.col("data_ingestao"),
        F.date_format(F.col("data_ingestao"), "yyyy-MM-dd").alias("DtAtual")    
    )
    .dropDuplicates(["DtAtual", "CdSku", "CdFilial"])
    .withColumn("TipoEstoque", F.lit("DEPOSITO"))
).cache()

print(f"✅ Registros de estoque DEPÓSITOS processados: {estoque_cds_processado_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise Estatística dos Dados
# MAGIC
# MAGIC Este bloco apresenta estatísticas resumidas dos dados de estoque processados,
# MAGIC incluindo totais de registros, filiais únicas, SKUs únicos e distribuição por tipo.

# COMMAND ----------

# Mostrar estatísticas das lojas
print("📈 Estatísticas de estoque LOJAS:")
estatisticas_lojas_df = estoque_lojas_processado_df.agg(
    F.count("*").alias("Total_Registros"),
    F.countDistinct("CdFilial").alias("Filiais_Unicas"),
    F.countDistinct("CdSku").alias("SKUs_Unicos"),
    F.sum("EstoqueLoja").alias("Estoque_Total"),
    F.avg("DDE").alias("DDE_Medio")
)

# display(estatisticas_lojas_df)

# Mostrar estatísticas dos depósitos
print("📈 Estatísticas de estoque DEPÓSITOS:")
estatisticas_cds_df = estoque_cds_processado_df.agg(
    F.count("*").alias("Total_Registros"),
    F.countDistinct("CdFilial").alias("Filiais_Unicas"),
    F.countDistinct("CdSku").alias("SKUs_Unicos"),
    F.sum("EstoqueDeposito").alias("Estoque_Total"),
    F.avg("DDE").alias("DDE_Medio")
)

# display(estatisticas_cds_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adição de Metadados de Processamento - Lojas
# MAGIC
# MAGIC Este bloco adiciona colunas de metadados ao DataFrame de estoque das lojas,
# MAGIC como `DataHoraProcessamento`, `DataProcessamento`, `FonteDados` e `VersaoProcessamento`.

# COMMAND ----------

print("💾 Adicionando metadados de processamento LOJAS...")

# Adicionar metadados de processamento
estoque_lojas_com_metadados_df = estoque_lojas_processado_df.withColumn(
    "DataHoraProcessamento",
    F.current_timestamp()
).withColumn(
    "DataProcessamento",
    F.current_date()
).withColumn(
    "FonteDados",
    F.lit("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
).withColumn(
    "VersaoProcessamento",
    F.lit("1.0")
)

print(f"✅ Metadados adicionados LOJAS. Total de registros: {estoque_lojas_com_metadados_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adição de Metadados de Processamento - Depósitos
# MAGIC
# MAGIC Este bloco adiciona colunas de metadados ao DataFrame de estoque dos depósitos,
# MAGIC como `DataHoraProcessamento`, `DataProcessamento`, `FonteDados` e `VersaoProcessamento`.

# COMMAND ----------

print("💾 Adicionando metadados de processamento DEPÓSITOS...")

# Adicionar metadados de processamento
estoque_cds_com_metadados_df = estoque_cds_processado_df.withColumn(
    "DataHoraProcessamento",
    F.current_timestamp()
).withColumn(
    "DataProcessamento",
    F.current_date()
).withColumn(
    "FonteDados",
    F.lit("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
).withColumn(
    "VersaoProcessamento",
    F.lit("1.0")
)

print(f"✅ Metadados adicionados DEPÓSITOS. Total de registros: {estoque_cds_com_metadados_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento na Camada Bronze - Lojas
# MAGIC
# MAGIC Este bloco salva o DataFrame de estoque das lojas na tabela Delta da camada Bronze,
# MAGIC utilizando o modo `overwrite` para garantir que a tabela seja sempre atualizada.

# COMMAND ----------

print(f"💾 Salvando tabela {TABELA_BRONZE_ESTOQUE_LOJA} no modo overwrite...")

try:
    # Salvar na camada Bronze
    estoque_lojas_com_metadados_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABELA_BRONZE_ESTOQUE_LOJA)
    
    print(f"✅ Tabela {TABELA_BRONZE_ESTOQUE_LOJA} salva com sucesso!")
    print(f"📊 Registros salvos LOJAS: {estoque_lojas_com_metadados_df.count()}")

except Exception as e:
    print(f"❌ Erro ao salvar tabela {TABELA_BRONZE_ESTOQUE_LOJA}: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento na Camada Bronze - Depósitos
# MAGIC
# MAGIC Este bloco salva o DataFrame de estoque dos depósitos na tabela Delta da camada Bronze,
# MAGIC utilizando o modo `overwrite` para garantir que a tabela seja sempre atualizada.

# COMMAND ----------

print(f"💾 Salvando tabela {TABELA_BRONZE_ESTOQUE_CD} no modo overwrite...")

try:
    # Salvar na camada Bronze
    estoque_cds_com_metadados_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABELA_BRONZE_ESTOQUE_CD)
    
    print(f"✅ Tabela {TABELA_BRONZE_ESTOQUE_CD} salva com sucesso!")
    print(f"📊 Registros salvos DEPÓSITOS: {estoque_cds_com_metadados_df.count()}")

except Exception as e:
    print(f"❌ Erro ao salvar tabela {TABELA_BRONZE_ESTOQUE_CD}: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação das Tabelas Salvas
# MAGIC
# MAGIC Este bloco realiza uma leitura das tabelas recém-salvas para verificar
# MAGIC seus schemas e exibir amostras dos dados, confirmando que o salvamento
# MAGIC foi bem-sucedido.

# COMMAND ----------

print(f"🔍 Validando tabelas salvas...")

# Validar tabela de lojas
print("📋 Schema da tabela LOJAS:")
spark.table(TABELA_BRONZE_ESTOQUE_LOJA).printSchema()

# Validar tabela de depósitos
print("📋 Schema da tabela DEPÓSITOS:")
spark.table(TABELA_BRONZE_ESTOQUE_CD).printSchema()

print("✅ Validação concluída com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza de Cache
# MAGIC
# MAGIC Este bloco limpa o cache dos DataFrames para liberar memória após o processamento.

# COMMAND ----------

print("🧹 Limpando cache para liberar memória...")

# Limpar cache
estoque_lojas_df.unpersist()
estoque_cds_df.unpersist()
estoque_lojas_processado_df.unpersist()
estoque_cds_processado_df.unpersist()

print("✅ Cache limpo com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo Final do Processamento
# MAGIC
# MAGIC Este bloco finaliza o notebook com um resumo das principais informações
# MAGIC do processamento, incluindo contagem de registros e tabelas de destino.

# COMMAND ----------

print("🎉 Processamento de estoque Bronze concluído com sucesso!")
print("=" * 80)
print("📊 RESUMO DO PROCESSAMENTO:")
print(f"  • Data de processamento: {hoje_str}")
print(f"  • Registros lojas: {estoque_lojas_com_metadados_df.count():,}")
print(f"  • Registros depósitos: {estoque_cds_com_metadados_df.count():,}")
print(f"  • Tabela lojas: {TABELA_BRONZE_ESTOQUE_LOJA}")
print(f"  • Tabela depósitos: {TABELA_BRONZE_ESTOQUE_CD}")
print("=" * 80)
print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
print("🏪 Dados de estoque de lojas processados")
print("🏭 Dados de estoque de depósitos processados")
print("📊 Estrutura: Filial x SKU x Data com métricas de estoque")