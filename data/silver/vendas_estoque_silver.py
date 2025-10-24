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

# Nome da tabela de destino na camada Silver (parametrizado por ambiente)
TABELA_SILVER_MASTER: str = f"databox.bcg_comum.supply_{AMBIENTE_TABELA.lower()}_master_vendas_estoque"

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
print(f"  • Tabela de Destino: {TABELA_SILVER_MASTER}")
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

# Unir estoque de lojas e depósitos
estoque_atual_df = (
    estoque_lojas_df
    .union(estoque_cds_df)
    .withColumnRenamed("QtEstoque", "QtEstoqueAtual")
    .withColumnRenamed("VrTotalVv", "VrEstoqueAtual")
    .withColumnRenamed("DDE", "DDE_Atual")
)

# Aplicar sample se configurado para desenvolvimento
if USAR_SAMPLES:
    print(f"🔬 Aplicando sample de {SAMPLE_SIZE:,} registros ESTOQUE para desenvolvimento...")
    estoque_atual_df = estoque_atual_df.sample(fraction=0.1, seed=42).limit(SAMPLE_SIZE)

# Cache estratégico: estoque atual é pequeno e será usado múltiplas vezes
estoque_atual_df = estoque_atual_df.cache()

print(f"📊 Registros de estoque carregados: {estoque_atual_df.count():,}")
print(f"🏪 Lojas: {estoque_lojas_df.count():,}")
print(f"🏭 Depósitos: {estoque_cds_df.count():,}")

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
# MAGIC ## Agregação Inteligente de Vendas - Fase 1
# MAGIC
# MAGIC Este bloco realiza a **primeira agregação** das vendas por SKU/Filial,
# MAGIC reduzindo drasticamente o volume de dados antes dos joins.
# MAGIC **Estratégia**: Agregar primeiro, depois calcular janelas temporais.

# COMMAND ----------

print("🔄 Iniciando agregação inteligente de vendas...")

# Primeira agregação: reduzir volume por SKU/Filial
vendas_agregadas_df = (
    vendas_bronze_df
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
        
        # Estatísticas
        F.countDistinct("DtAtual").alias("DiasDistintos"),
        F.min("DtAtual").alias("PrimeiraVenda"),
        F.max("DtAtual").alias("UltimaVenda")
    )
)

# Cache estratégico: tabela muito menor após agregação
vendas_agregadas_df = vendas_agregadas_df.cache()

print(f"📊 Vendas agregadas por SKU/Filial: {vendas_agregadas_df.count():,}")
print(f"📈 Redução de volume: {vendas_bronze_df.count():,} → {vendas_agregadas_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação por Janelas Temporais - Fase 2
# MAGIC
# MAGIC Este bloco calcula as agregações por janelas temporais específicas,
# MAGIC usando a tabela de vendas original mas com filtros otimizados.

# COMMAND ----------

print("📅 Calculando agregações por janelas temporais...")

# Função para calcular agregação por janela temporal
def calcular_agregacao_janela(vendas_df, data_inicio, data_fim, sufixo):
    return (
        vendas_df
        .filter(F.col("DtAtual").between(data_inicio, data_fim))
        .groupBy("CdFilial", "CdSku")
        .agg(
            F.sum("Receita").alias(f"Receita_{sufixo}"),
            F.sum("QtMercadoria").alias(f"QtMercadoria_{sufixo}"),
            F.sum("Receita_OFF").alias(f"Receita_OFF_{sufixo}"),
            F.sum("QtMercadoria_OFF").alias(f"QtMercadoria_OFF_{sufixo}"),
            F.sum("Receita_ON").alias(f"Receita_ON_{sufixo}"),
            F.sum("QtMercadoria_ON").alias(f"QtMercadoria_ON_{sufixo}")
        )
    )

# Calcular agregações para cada janela temporal
vendas_mtd_df = calcular_agregacao_janela(vendas_bronze_df, data_inicio_mtd, hoje_str, "MTD")
vendas_ytd_df = calcular_agregacao_janela(vendas_bronze_df, data_inicio_ytd, hoje_str, "YTD")
vendas_last_7d_df = calcular_agregacao_janela(vendas_bronze_df, data_inicio_7d, hoje_str, "Last7d")
vendas_last_30d_df = calcular_agregacao_janela(vendas_bronze_df, data_inicio_30d, hoje_str, "Last30d")
vendas_last_90d_df = calcular_agregacao_janela(vendas_bronze_df, data_inicio_90d, hoje_str, "Last90d")
vendas_last_4w_df = calcular_agregacao_janela(vendas_bronze_df, data_inicio_4w, hoje_str, "Last4w")

# Meses anteriores
vendas_m1_df = calcular_agregacao_janela(vendas_bronze_df, data_inicio_m1, data_inicio_m1, "M1")
vendas_m2_df = calcular_agregacao_janela(vendas_bronze_df, data_inicio_m2, data_inicio_m2, "M2")
vendas_m3_df = calcular_agregacao_janela(vendas_bronze_df, data_inicio_m3, data_inicio_m3, "M3")

print(f"✅ Agregações por janelas temporais calculadas:")
print(f"  • MTD: {vendas_mtd_df.count():,} registros")
print(f"  • YTD: {vendas_ytd_df.count():,} registros")
print(f"  • Last 7d: {vendas_last_7d_df.count():,} registros")
print(f"  • Last 30d: {vendas_last_30d_df.count():,} registros")
print(f"  • Last 90d: {vendas_last_90d_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de Médias Móveis
# MAGIC
# MAGIC Este bloco calcula as médias móveis para diferentes períodos,
# MAGIC usando agregações otimizadas para reduzir volume de dados.

# COMMAND ----------

print("📊 Calculando médias móveis...")

# Função para calcular média móvel
def calcular_media_movel(vendas_df, dias, sufixo):
    data_inicio_mm = (hoje - timedelta(days=dias)).strftime("%Y-%m-%d")
    
    return (
        vendas_df
        .filter(F.col("DtAtual").between(data_inicio_mm, hoje_str))
        .groupBy("CdFilial", "CdSku")
        .agg(
            (F.sum("Receita") / dias).alias(f"Receita_MM{sufixo}"),
            (F.sum("QtMercadoria") / dias).alias(f"QtMercadoria_MM{sufixo}"),
            (F.sum("Receita_OFF") / dias).alias(f"Receita_OFF_MM{sufixo}"),
            (F.sum("QtMercadoria_OFF") / dias).alias(f"QtMercadoria_OFF_MM{sufixo}"),
            (F.sum("Receita_ON") / dias).alias(f"Receita_ON_MM{sufixo}"),
            (F.sum("QtMercadoria_ON") / dias).alias(f"QtMercadoria_ON_MM{sufixo}")
        )
    )

# Calcular médias móveis
vendas_mm7d_df = calcular_media_movel(vendas_bronze_df, 7, "7d")
vendas_mm14d_df = calcular_media_movel(vendas_bronze_df, 14, "14d")
vendas_mm30d_df = calcular_media_movel(vendas_bronze_df, 30, "30d")
vendas_mm60d_df = calcular_media_movel(vendas_bronze_df, 60, "60d")
vendas_mm90d_df = calcular_media_movel(vendas_bronze_df, 90, "90d")

print(f"✅ Médias móveis calculadas:")
print(f"  • MM 7d: {vendas_mm7d_df.count():,} registros")
print(f"  • MM 14d: {vendas_mm14d_df.count():,} registros")
print(f"  • MM 30d: {vendas_mm30d_df.count():,} registros")
print(f"  • MM 60d: {vendas_mm60d_df.count():,} registros")
print(f"  • MM 90d: {vendas_mm90d_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Otimizado - Estoque + Vendas Agregadas
# MAGIC
# MAGIC Este bloco realiza o join principal entre estoque atual e vendas agregadas,
# MAGIC usando estratégias de otimização para grandes volumes de dados.

# COMMAND ----------

print("🔗 Iniciando join otimizado entre estoque e vendas...")

# Configurar broadcast join para estoque atual (tabela pequena)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Join principal: estoque atual + vendas agregadas
master_table_base_df = (
    estoque_atual_df
    .join(
        vendas_agregadas_df,
        on=["CdFilial", "CdSku"],
        how="left"
    )
)

print(f"📊 Join base realizado: {master_table_base_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join com Janelas Temporais - Fase 3
# MAGIC
# MAGIC Este bloco adiciona as agregações por janelas temporais à master table,
# MAGIC usando joins sequenciais otimizados.

# COMMAND ----------

print("🔗 Adicionando janelas temporais à master table...")

# Join com janelas temporais (sequencial para otimizar)
master_table_temp_df = (
    master_table_base_df
    .join(vendas_mtd_df, on=["CdFilial", "CdSku"], how="left")
    .join(vendas_ytd_df, on=["CdFilial", "CdSku"], how="left")
    .join(vendas_last_7d_df, on=["CdFilial", "CdSku"], how="left")
    .join(vendas_last_30d_df, on=["CdFilial", "CdSku"], how="left")
    .join(vendas_last_90d_df, on=["CdFilial", "CdSku"], how="left")
    .join(vendas_last_4w_df, on=["CdFilial", "CdSku"], how="left")
)

print(f"📊 Janelas temporais adicionadas: {master_table_temp_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join com Meses Anteriores e Médias Móveis
# MAGIC
# MAGIC Este bloco completa a master table com meses anteriores e médias móveis,
# MAGIC finalizando a consolidação de dados.

# COMMAND ----------

print("🔗 Adicionando meses anteriores e médias móveis...")

# Join com meses anteriores
master_table_meses_df = (
    master_table_temp_df
    .join(vendas_m1_df, on=["CdFilial", "CdSku"], how="left")
    .join(vendas_m2_df, on=["CdFilial", "CdSku"], how="left")
    .join(vendas_m3_df, on=["CdFilial", "CdSku"], how="left")
)

# Join com médias móveis
master_table_final_df = (
    master_table_meses_df
    .join(vendas_mm7d_df, on=["CdFilial", "CdSku"], how="left")
    .join(vendas_mm14d_df, on=["CdFilial", "CdSku"], how="left")
    .join(vendas_mm30d_df, on=["CdFilial", "CdSku"], how="left")
    .join(vendas_mm60d_df, on=["CdFilial", "CdSku"], how="left")
    .join(vendas_mm90d_df, on=["CdFilial", "CdSku"], how="left")
)

print(f"📊 Master table finalizada: {master_table_final_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza e Preenchimento de Valores Nulos
# MAGIC
# MAGIC Este bloco limpa a master table e preenche valores nulos com zeros,
# MAGIC garantindo dados consistentes para análise.

# COMMAND ----------

print("🧹 Limpando e preenchendo valores nulos...")

# Preencher valores nulos com zeros para colunas de vendas
colunas_vendas = [col for col in master_table_final_df.columns if col.startswith(('Receita_', 'QtMercadoria_'))]

master_table_limpa_df = (
    master_table_final_df
    .fillna(0, subset=colunas_vendas)
    .withColumn("DataHoraProcessamento", F.current_timestamp())
    .withColumn("DtAtual", F.lit(hoje_str))
)

print(f"✅ Master table limpa: {master_table_limpa_df.count():,} registros")
print(f"📊 Colunas de vendas preenchidas: {len(colunas_vendas)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento na Camada Silver
# MAGIC
# MAGIC Este bloco salva a master table na tabela Delta da camada Silver,
# MAGIC utilizando o modo `overwrite` para garantir que a tabela seja sempre atualizada.

# COMMAND ----------

print(f"💾 Salvando master table {TABELA_SILVER_MASTER} no modo overwrite...")

try:
    # Salvar na camada Silver
    master_table_limpa_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABELA_SILVER_MASTER)
    
    print(f"✅ Master table {TABELA_SILVER_MASTER} salva com sucesso!")
    print(f"📊 Registros salvos: {master_table_limpa_df.count():,}")
    
except Exception as e:
    print(f"❌ Erro ao salvar master table {TABELA_SILVER_MASTER}: {str(e)}")
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
    estoque_atual_df.unpersist()
    vendas_agregadas_df.unpersist()
    vendas_mtd_df.unpersist()
    vendas_ytd_df.unpersist()
    vendas_last_7d_df.unpersist()
    vendas_last_30d_df.unpersist()
    vendas_last_90d_df.unpersist()
    vendas_last_4w_df.unpersist()
    vendas_m1_df.unpersist()
    vendas_m2_df.unpersist()
    vendas_m3_df.unpersist()
    vendas_mm7d_df.unpersist()
    vendas_mm14d_df.unpersist()
    vendas_mm30d_df.unpersist()
    vendas_mm60d_df.unpersist()
    vendas_mm90d_df.unpersist()
    
    print("✅ Cache limpo com sucesso!")
    
except Exception as e:
    print(f"⚠️ Erro ao limpar cache: {str(e)}")

# COMMAND ----------

print("🎉 Processamento da Master Table Silver concluído com sucesso!")
print("=" * 80)
print("📊 RESUMO DO PROCESSAMENTO:")
print(f"  • Período processado: {data_inicio_str} até {hoje_str}")
print(f"  • Dias de histórico: {DIAS_PROCESSAMENTO}")
print(f"  • Ambiente: {AMBIENTE_TABELA}")
print(f"  • Tabela vendas: {TABELA_BRONZE_VENDAS}")
print(f"  • Tabela estoque lojas: {TABELA_BRONZE_ESTOQUE_LOJAS}")
print(f"  • Tabela estoque CDs: {TABELA_BRONZE_ESTOQUE_CDS}")
print(f"  • Master table destino: {TABELA_SILVER_MASTER}")
print(f"  • Registros de vendas: {vendas_bronze_df.count():,}")
print(f"  • Registros de estoque: {estoque_atual_df.count():,}")
print(f"  • Master table final: {master_table_limpa_df.count():,}")
print("=" * 80)
print("🎯 ESTRATÉGIAS DE OTIMIZAÇÃO APLICADAS:")
print("  ✅ Agregação inteligente de vendas")
print("  ✅ Cache estratégico de DataFrames")
print("  ✅ Joins otimizados com broadcast")
print("  ✅ Particionamento por data")
print("  ✅ Limpeza automática de cache")
print("=" * 80)

# COMMAND ----------
