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
SAMPLE_SIZE: int = 10000  # Tamanho do sample para desenvolvimento

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
    .withColumn("TipoEstoque", F.lit("LOJA"))
    .withColumn("DDE", (F.col("VrTotalVv")/F.col("VrVndCmv")))
    .withColumn("DtAtual", F.date_format(F.col("data_ingestao"), "yyyy-MM-dd"))
    .dropDuplicates(["DtAtual", "CdSku", "CdFilial"]) # Garantir unicidade
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
    .withColumn("TipoEstoque", F.lit("CD"))
    .withColumn("DDE", (F.col("VrTotalVv")/F.col("VrVndCmv")))
    .withColumn("DtAtual", F.date_format(F.col("data_ingestao"), "yyyy-MM-dd"))
    .dropDuplicates(["DtAtual", "CdSku", "CdFilial"]) # Garantir unicidade
).cache()

print(f"✅ Registros de estoque DEPÓSITOS processados: {estoque_cds_processado_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento de Dados do GEF (Gestão de Estoque e Faturamento)
# MAGIC
# MAGIC Este bloco carrega dados do GEF que contêm informações estratégicas de estoque,
# MAGIC incluindo estoque de segurança, lead time, cobertura, demanda e projeções.
# MAGIC Estes dados serão unidos com os dados de estoque das lojas e depósitos.

# COMMAND ----------

print("📊 Carregando dados do GEF...")

# Carregar dados do GEF
gef_df = (
    spark.table("databox.logistica_comum.gef_visao_estoque_lojas")
    .select(
        F.col("CODIGO_ITEM").alias("CdSku"),
        F.col("FILIALAJ").alias("CdFilial"),
        F.date_format(F.col("DATA_ANALISE"), "yyyy-MM-dd").alias("DtAtual"),  # Converter para formato yyyy-MM-dd
        F.col("ESTOQUE_SEGURANCA"),
        F.col("LEADTIME_MEDIO"),
        F.col("COBERTURA_ES_DIAS"),
        F.col("ESTOQUE_ALVO"),
        F.col("COBERTURA_ATUAL"),
        F.col("COBERTURA_ALVO"),
        F.col("DDV_SEM_OUTLIER"),
        F.col("DDV_FUTURO"),
        F.col("GRADE"),
        F.col("TRANSITO"),
        F.col("ESTOQUE_PROJETADO"),
        F.col("COBERTURA_ATUAL_C_TRANISTO_DIAS"),
        F.col("MEDIA_3"),
        F.col("MEDIA_6"),
        F.col("MEDIA_9"),
        F.col("MEDIA_12"),
        F.col("DDV_SO"),
        F.col("DDV_CO"),
        F.col("CLUSTER_OBG"),
        F.col("CLUSTER_SUG")
    )
)

# Aplicar sample se configurado para desenvolvimento
if USAR_SAMPLES:
    print(f"🔬 Aplicando sample de {SAMPLE_SIZE:,} registros GEF para desenvolvimento...")
    gef_df = gef_df.sample(fraction=0.1, seed=42).limit(SAMPLE_SIZE)

gef_df = gef_df.cache()

print(f"📊 Registros do GEF carregados: {gef_df.count()}")

# Mostrar amostra das datas para validação
print("📅 Validação do formato de datas no GEF:")
gef_df.select("DtAtual").distinct().orderBy("DtAtual").show(10, truncate=False)

# Validação de duplicatas nas chaves de join
print("🔍 Validando chaves de join para evitar multiplicação de registros...")

# Verificar duplicatas no GEF (incluindo data)
duplicatas_gef = gef_df.groupBy("CdFilial", "CdSku", "DtAtual").count().filter(F.col("count") > 1)
total_duplicatas_gef = duplicatas_gef.count()

print(f"📊 Validação de duplicatas GEF:")
print(f"  • Chaves duplicadas no GEF (Filial+SKU+DtAtual): {total_duplicatas_gef:,}")

if total_duplicatas_gef > 0:
    print("⚠️  ATENÇÃO: GEF contém chaves duplicadas! Isso pode causar multiplicação de registros.")
    print("🔧 Solução: Remover duplicatas do GEF antes do join")
    
    # Remover duplicatas do GEF mantendo apenas o primeiro registro
    gef_df = gef_df.dropDuplicates(["CdFilial", "CdSku", "DtAtual"]).cache()
    print(f"✅ Duplicatas removidas do GEF. Novos registros: {gef_df.count():,}")
else:
    print("✅ GEF não contém chaves duplicadas")

# Verificar duplicatas no estoque das lojas (incluindo data)
duplicatas_lojas = estoque_lojas_processado_df.groupBy("CdFilial", "CdSku", "DtAtual").count().filter(F.col("count") > 1)
total_duplicatas_lojas = duplicatas_lojas.count()

print(f"📊 Validação de duplicatas Estoque Lojas:")
print(f"  • Chaves duplicadas no estoque lojas (Filial+SKU+DtAtual): {total_duplicatas_lojas:,}")

if total_duplicatas_lojas > 0:
    print("⚠️  ATENÇÃO: Estoque lojas contém chaves duplicadas!")
else:
    print("✅ Estoque lojas não contém chaves duplicadas")

# Verificar duplicatas no estoque dos depósitos (incluindo data)
duplicatas_cds = estoque_cds_processado_df.groupBy("CdFilial", "CdSku", "DtAtual").count().filter(F.col("count") > 1)
total_duplicatas_cds = duplicatas_cds.count()

print(f"📊 Validação de duplicatas Estoque Depósitos:")
print(f"  • Chaves duplicadas no estoque depósitos (Filial+SKU+DtAtual): {total_duplicatas_cds:,}")

if total_duplicatas_cds > 0:
    print("⚠️  ATENÇÃO: Estoque depósitos contém chaves duplicadas!")
else:
    print("✅ Estoque depósitos não contém chaves duplicadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join dos Dados de Estoque com GEF - Lojas
# MAGIC
# MAGIC Este bloco realiza o join entre os dados de estoque das lojas e os dados do GEF,
# MAGIC enriquecendo as informações de estoque com métricas estratégicas de gestão.

# COMMAND ----------

print("🔗 Realizando join entre estoque LOJAS e dados GEF (Filial + SKU + Data)...")

# Validação antes do join
registros_antes_join_lojas = estoque_lojas_processado_df.count()
registros_gef = gef_df.count()

print(f"📊 Validação antes do join LOJAS:")
print(f"  • Registros estoque lojas: {registros_antes_join_lojas:,}")
print(f"  • Registros GEF: {registros_gef:,}")
print(f"  • Chaves de join: CdFilial + CdSku + DtAtual")

# Join entre estoque das lojas e dados do GEF (incluindo data)
estoque_lojas_com_gef_df = (
    estoque_lojas_processado_df
    .join(
        gef_df,
        on=["CdFilial", "CdSku", "DtAtual"],
        how="left"
    )
    .withColumn("TipoEstoque", F.lit("LOJA"))
).cache()

# Validação após o join
registros_apos_join_lojas = estoque_lojas_com_gef_df.count()
registros_com_match_lojas = estoque_lojas_com_gef_df.filter(F.col("ESTOQUE_SEGURANCA").isNotNull()).count()
percentual_match_lojas = (registros_com_match_lojas / registros_apos_join_lojas) * 100

print(f"📊 Validação após join LOJAS:")
print(f"  • Registros após join: {registros_apos_join_lojas:,}")
print(f"  • Registros com match GEF: {registros_com_match_lojas:,}")
print(f"  • Percentual de match: {percentual_match_lojas:.2f}%")
print(f"  • Aumento de registros: {registros_apos_join_lojas - registros_antes_join_lojas:,}")

if registros_apos_join_lojas != registros_antes_join_lojas:
    print("⚠️  ATENÇÃO: Join gerou aumento de registros!")
else:
    print("✅ Join manteve quantidade de registros (left join correto)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join dos Dados de Estoque com GEF - Depósitos
# MAGIC
# MAGIC Este bloco realiza o join entre os dados de estoque dos depósitos e os dados do GEF,
# MAGIC enriquecendo as informações de estoque com métricas estratégicas de gestão.

# COMMAND ----------

print("🔗 Realizando join entre estoque DEPÓSITOS e dados GEF (Filial + SKU + Data)...")

# Validação antes do join
registros_antes_join_cds = estoque_cds_processado_df.count()

print(f"📊 Validação antes do join DEPÓSITOS:")
print(f"  • Registros estoque depósitos: {registros_antes_join_cds:,}")
print(f"  • Registros GEF: {registros_gef:,}")
print(f"  • Chaves de join: CdFilial + CdSku + DtAtual")

# Join entre estoque dos depósitos e dados do GEF (incluindo data)
estoque_cds_com_gef_df = (
    estoque_cds_processado_df
    .join(
        gef_df,
        on=["CdFilial", "CdSku", "DtAtual"],
        how="left"
    )
    .withColumn("TipoEstoque", F.lit("CD"))
).cache()

# Validação após o join
registros_apos_join_cds = estoque_cds_com_gef_df.count()
registros_com_match_cds = estoque_cds_com_gef_df.filter(F.col("ESTOQUE_SEGURANCA").isNotNull()).count()
percentual_match_cds = (registros_com_match_cds / registros_apos_join_cds) * 100

print(f"📊 Validação após join DEPÓSITOS:")
print(f"  • Registros após join: {registros_apos_join_cds:,}")
print(f"  • Registros com match GEF: {registros_com_match_cds:,}")
print(f"  • Percentual de match: {percentual_match_cds:.2f}%")
print(f"  • Aumento de registros: {registros_apos_join_cds - registros_antes_join_cds:,}")

if registros_apos_join_cds != registros_antes_join_cds:
    print("⚠️  ATENÇÃO: Join gerou aumento de registros!")
else:
    print("✅ Join manteve quantidade de registros (left join correto)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adição de Metadados de Processamento - Lojas Enriquecidas
# MAGIC
# MAGIC Este bloco adiciona colunas de metadados ao DataFrame de estoque das lojas enriquecido com GEF.

# COMMAND ----------

print("💾 Adicionando metadados de processamento LOJAS + GEF...")

# Adicionar metadados de processamento
estoque_lojas_gef_com_metadados_df = estoque_lojas_com_gef_df.withColumn(
    "DataHoraProcessamento",
    F.current_timestamp()
).withColumn(
    "DataProcessamento",
    F.current_date()
).withColumn(
    "FonteDados",
    F.lit("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque + databox.logistica_comum.gef_visao_estoque_lojas")
).withColumn(
    "VersaoProcessamento",
    F.lit("1.0")
)

print(f"✅ Metadados adicionados LOJAS + GEF. Total de registros: {estoque_lojas_gef_com_metadados_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adição de Metadados de Processamento - Depósitos Enriquecidos
# MAGIC
# MAGIC Este bloco adiciona colunas de metadados ao DataFrame de estoque dos depósitos enriquecido com GEF.

# COMMAND ----------

print("💾 Adicionando metadados de processamento DEPÓSITOS + GEF...")

# Adicionar metadados de processamento
estoque_cds_gef_com_metadados_df = estoque_cds_com_gef_df.withColumn(
    "DataHoraProcessamento",
    F.current_timestamp()
).withColumn(
    "DataProcessamento",
    F.current_date()
).withColumn(
    "FonteDados",
    F.lit("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque + databox.logistica_comum.gef_visao_estoque_lojas")
).withColumn(
    "VersaoProcessamento",
    F.lit("1.0")
)

print(f"✅ Metadados adicionados DEPÓSITOS + GEF. Total de registros: {estoque_cds_gef_com_metadados_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento na Camada Bronze - Lojas Enriquecidas
# MAGIC
# MAGIC Este bloco salva o DataFrame de estoque das lojas enriquecido com dados do GEF.

# COMMAND ----------

print(f"💾 Salvando tabela {TABELA_BRONZE_ESTOQUE_LOJA} (enriquecida com GEF) no modo overwrite...")

try:
    # Salvar na camada Bronze
    estoque_lojas_gef_com_metadados_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABELA_BRONZE_ESTOQUE_LOJA)
    
    print(f"✅ Tabela {TABELA_BRONZE_ESTOQUE_LOJA} salva com sucesso!")
    print(f"📊 Registros salvos LOJAS + GEF: {estoque_lojas_gef_com_metadados_df.count()}")

except Exception as e:
    print(f"❌ Erro ao salvar tabela {TABELA_BRONZE_ESTOQUE_LOJA}: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento na Camada Bronze - Depósitos Enriquecidos
# MAGIC
# MAGIC Este bloco salva o DataFrame de estoque dos depósitos enriquecido com dados do GEF.

# COMMAND ----------

print(f"💾 Salvando tabela {TABELA_BRONZE_ESTOQUE_CD} (enriquecida com GEF) no modo overwrite...")

try:
    # Salvar na camada Bronze
    estoque_cds_gef_com_metadados_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABELA_BRONZE_ESTOQUE_CD)
    
    print(f"✅ Tabela {TABELA_BRONZE_ESTOQUE_CD} salva com sucesso!")
    print(f"📊 Registros salvos DEPÓSITOS + GEF: {estoque_cds_gef_com_metadados_df.count()}")

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
gef_df.unpersist()
estoque_lojas_com_gef_df.unpersist()
estoque_cds_com_gef_df.unpersist()

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
print(f"  • Registros lojas + GEF: {estoque_lojas_gef_com_metadados_df.count():,}")
print(f"  • Registros depósitos + GEF: {estoque_cds_gef_com_metadados_df.count():,}")
print(f"  • Tabela lojas: {TABELA_BRONZE_ESTOQUE_LOJA}")
print(f"  • Tabela depósitos: {TABELA_BRONZE_ESTOQUE_CD}")
print("=" * 80)
print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
print("🏪 Dados de estoque de lojas processados e enriquecidos com GEF")
print("🏭 Dados de estoque de depósitos processados e enriquecidos com GEF")
print("📊 Estrutura: Filial x SKU x Data com métricas de estoque + dados estratégicos GEF")
print("🔗 Join realizado com dados do GEF (CdFilial + CdSku + DtAtual) para enriquecimento estratégico")
print("📅 Formato de data: yyyy-MM-dd (convertido de DATA_ANALISE do GEF)")

# COMMAND ----------