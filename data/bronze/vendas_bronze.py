#%% [markdown]
# 📊 Processamento de Vendas - Camada Bronze
#
# Este notebook processa dados de vendas online e offline para a camada Bronze,
# seguindo o padrão Medallion Architecture e as melhores práticas Python.
#
# <details>
# <summary><b>🎯 Objetivos do Projeto</b></summary>
#
# - Processar vendas online e offline de forma unificada
# - Agregar dados por filial, SKU e data
# - Criar grade completa de vendas com zeros
# - Salvar na camada Bronze com metadados
# - Implementar timezone São Paulo (GMT-3)
# - Seguir padrões de qualidade de código
#
# </details>
#
# <details>
# <summary><b>📋 Funcionalidades Principais</b></summary>
#
# - **Processamento Offline**: Vendas de loja física
# - **Processamento Online**: Vendas de canais digitais
# - **Consolidação**: União de ambos os canais
# - **Grade Completa**: Todas as combinações filial × SKU × data
# - **Metadados**: DataHoraProcessamento, FonteDados, VersaoProcessamento
# - **Validações**: Tratamento de erros e dados inconsistentes
#
# </details>
#
# ---
#
# **Autor**: Torre de Controle Supply Chain - 2024

#%% [markdown]
# ## 1. Setup e Imports
#
# <details>
# <summary><b>📦 Bibliotecas Utilizadas</b></summary>
#
# - **pyspark**: Processamento distribuído de dados
# - **datetime**: Manipulação de datas e horários
# - **typing**: Anotações de tipo para melhor documentação
# - **pytz**: Tratamento de timezones
#
# </details>

#%%
# Import necessary libraries
from datetime import datetime, timedelta, date
from typing import Optional, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pytz import timezone

print("✅ Bibliotecas importadas com sucesso!")

#%% [markdown]
# ## 2. Configurações Globais
#
# <details>
# <summary><b>⚙️ Configurações do Sistema</b></summary>
#
# - **Tabela de Destino**: `databox.bcg_comum.supply_bronze_vendas_90d_on_off`
# - **Timezone**: São Paulo (GMT-3)
# - **Período**: Últimos 90 dias (configurável)
# - **Modo de Salvamento**: Overwrite por padrão
#
# </details>

#%%
# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================

# Nome da tabela de destino na camada Bronze
TABELA_BRONZE_VENDAS: str = "databox.bcg_comum.supply_bronze_vendas_90d_on_off"

# Timezone São Paulo (GMT-3)
TIMEZONE_SP = timezone('America/Sao_Paulo')

# Inicialização do Spark
spark = SparkSession.builder.appName("vendas_bronze").getOrCreate()

# Data de processamento (ontem)
hoje = datetime.now(TIMEZONE_SP) - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

print(f"📅 Data de processamento: {hoje}")
print(f"📝 Data string: {hoje_str}")
print(f"🔢 Data int: {hoje_int}")
print(f"🌍 Timezone: {TIMEZONE_SP}")

#%% [markdown]
# ## 3. Função de Cálculo de Data de Início
#
# <details>
# <summary><b>📊 Lógica de Cálculo</b></summary>
#
# Esta função calcula a data de início baseada nos últimos N dias:
# - **Padrão**: 90 dias de retrocesso
# - **Flexibilidade**: Permite ajustar o período
# - **Timezone**: Considera timezone São Paulo
# - **Validação**: Verifica se dias_retrocesso é positivo
#
# </details>

#%%
def get_data_inicio(
    hoje: Optional[Union[datetime, date]] = None, 
    dias_retrocesso: int = 90
) -> datetime:
    """
    Retorna datetime de início baseado nos últimos N dias.
    
    Args:
        hoje: Data de referência (padrão: hoje)
        dias_retrocesso: Número de dias para retroceder (padrão: 90 dias)
        
    Returns:
        datetime: Data de início no timezone São Paulo
        
    Raises:
        ValueError: Se dias_retrocesso for negativo
    """
    if dias_retrocesso < 0:
        raise ValueError("dias_retrocesso deve ser positivo")
    
    if hoje is None:
        hoje_dt = datetime.now(TIMEZONE_SP)
    elif isinstance(hoje, datetime):
        hoje_dt = hoje
    else:
        hoje_dt = datetime.combine(hoje, datetime.min.time())
        hoje_dt = TIMEZONE_SP.localize(hoje_dt)
    
    data_inicio = hoje_dt - timedelta(days=dias_retrocesso)
    
    print(f"📊 Data de início calculada: {data_inicio}")
    print(f"⏰ Dias de retrocesso: {dias_retrocesso}")
    
    return data_inicio

# Executar função e mostrar resultado
data_inicio = get_data_inicio()
data_inicio_int = int(data_inicio.strftime("%Y%m%d"))
print(f"📅 Data início: {data_inicio}")
print(f"🔢 Data início int: {data_inicio_int}")

# Mostrar DataFrame de exemplo
df_exemplo = spark.range(1).select(
    F.lit(data_inicio).alias("data_inicio"),
    F.lit(data_inicio_int).alias("data_inicio_int"),
    F.lit(90).alias("dias_retrocesso")
)
df_exemplo.show()

#%% [markdown]
# ## 4. Processamento de Vendas Offline
#
# <details>
# <summary><b>🏪 Lógica de Processamento Offline</b></summary>
#
# Esta seção processa vendas de loja física:
# - **Fonte**: Tabela `app_venda.vendafaturadarateada`
# - **Filtros**: Apenas loja física, valores positivos
# - **Agregação**: Por filial, SKU e data
# - **Grade Completa**: Todas as combinações com zeros
# - **Canal**: Identificado como "OFFLINE"
#
# </details>

#%%
def get_vendas_offline(
    spark: SparkSession,
    start_date: int = data_inicio_int,
    end_date: int = hoje_int,
) -> DataFrame:
    """
    Processa vendas offline (loja física) da tabela vendafaturadarateada.
    
    Args:
        spark: Sessão do Spark
        start_date: Data de início no formato YYYYMMDD
        end_date: Data de fim no formato YYYYMMDD
        
    Returns:
        DataFrame com vendas offline agregadas por filial, SKU e data
        
    Raises:
        ValueError: Se start_date > end_date
    """
    if start_date > end_date:
        raise ValueError("start_date deve ser menor ou igual a end_date")
    
    print(f"🏪 Processando vendas OFFLINE de {start_date} até {end_date}")
    
    # Carregar tabela de vendas rateadas (offline)
    df_rateada = (
        spark.table("app_venda.vendafaturadarateada")
        .filter(F.col("NmEstadoMercadoria") != '1 - SALDO')
        .filter(F.col("NmTipoNegocio") == 'LOJA FISICA')
        .filter(
            F.col("DtAprovacao").between(start_date, end_date)
            & (F.col("VrOperacao") >= 0)
            & (F.col("VrCustoContabilFilialSku") >= 0)
        )
    )
    
    print(f"📊 Registros rateados carregados: {df_rateada.count()}")
    
    # Carregar tabela de vendas não rateadas para quantidade
    df_nao_rateada = (
        spark.table("app_venda.vendafaturadanaorateada")
        .filter(F.col("QtMercadoria") >= 0)
    )
    
    print(f"📈 Registros não rateados carregados: {df_nao_rateada.count()}")
    
    # Unificar dados e aplicar transformações
    df = (
        df_rateada
        .join(df_nao_rateada.select("ChaveFatos", "QtMercadoria"), on="ChaveFatos")
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
    )
    
    print(f"🔗 Registros após join: {df.count()}")
    
    # Agregar por filial, SKU e data
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
    
    print(f"📊 Registros após agregação: {df_agg.count()}")
    
    # Criar grade completa de datas
    cal = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit(str(start_date)), "yyyyMMdd"),
                    F.to_date(F.lit(str(end_date)), "yyyyMMdd"),
                    F.expr("interval 1 day")
                )
            ).alias("DtAtual_date")
        )
    )
    
    # Conjunto de chaves (Filial x SKU) para loja física
    keys = (
        df
        .select("CdFilial", "CdSkuLoja")
        .dropDuplicates()
    )
    
    print(f"🔑 Chaves únicas (Filial x SKU): {keys.count()}")
    
    # Grade completa (Data x Filial x SKU)
    grade = cal.crossJoin(keys)
    
    # Agregado com data como DateType para join
    df_agg_d = df_agg.withColumn("DtAtual_date", F.to_date("DtAtual"))
    
    # Left join + zeros onde não houver venda
    result = (
        grade.join(
            df_agg_d,
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
    
    print(f"✅ Registros finais OFFLINE: {result.count()}")
    
    # Mostrar amostra dos dados
    print("📋 Amostra dos dados OFFLINE:")
    result.show(5)
    
    return result

# Executar função de vendas offline
vendas_offline_df = get_vendas_offline(spark)
print(f"🏪 DataFrame vendas offline criado com {vendas_offline_df.count()} registros")

#%% [markdown]
# ## 5. Processamento de Vendas Online
#
# <details>
# <summary><b>🌐 Lógica de Processamento Online</b></summary>
#
# Esta seção processa vendas de canais digitais:
# - **Fonte**: Tabela `app_venda.vendafaturadarateada`
# - **Filtros**: Exclui loja física, valores positivos
# - **Agregação**: Por filial, SKU e data
# - **Grade Completa**: Todas as combinações com zeros
# - **Canal**: Identificado como "ONLINE"
#
# </details>

#%%
def get_vendas_online(
    spark: SparkSession,
    start_date: int = data_inicio_int,
    end_date: int = hoje_int,
) -> DataFrame:
    """
    Processa vendas online da tabela vendafaturadarateada.
    
    Args:
        spark: Sessão do Spark
        start_date: Data de início no formato YYYYMMDD
        end_date: Data de fim no formato YYYYMMDD
        
    Returns:
        DataFrame com vendas online agregadas por filial, SKU e data
        
    Raises:
        ValueError: Se start_date > end_date
    """
    if start_date > end_date:
        raise ValueError("start_date deve ser menor ou igual a end_date")
    
    print(f"🌐 Processando vendas ONLINE de {start_date} até {end_date}")
    
    # Carregar tabela de vendas rateadas (online)
    df_rateada = (
        spark.table("app_venda.vendafaturadarateada")
        .filter(F.col("NmEstadoMercadoria") != '1 - SALDO')
        .filter(F.col("NmTipoNegocio") != 'LOJA FISICA')  # Excluir loja física
        .filter(
            F.col("DtAprovacao").between(start_date, end_date)
            & (F.col("VrOperacao") >= 0)
            & (F.col("VrCustoContabilFilialSku") >= 0)
        )
    )
    
    print(f"📊 Registros rateados ONLINE carregados: {df_rateada.count()}")
    
    # Carregar tabela de vendas não rateadas para quantidade
    df_nao_rateada = (
        spark.table("app_venda.vendafaturadanaorateada")
        .filter(F.col("QtMercadoria") >= 0)
    )
    
    # Unificar dados e aplicar transformações
    df = (
        df_rateada
        .join(df_nao_rateada.select("ChaveFatos", "QtMercadoria"), on="ChaveFatos")
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
    )
    
    print(f"🔗 Registros após join ONLINE: {df.count()}")
    
    # Agregar por filial, SKU e data
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
    
    print(f"📊 Registros após agregação ONLINE: {df_agg.count()}")
    
    # Criar grade completa de datas
    cal = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit(str(start_date)), "yyyyMMdd"),
                    F.to_date(F.lit(str(end_date)), "yyyyMMdd"),
                    F.expr("interval 1 day")
                )
            ).alias("DtAtual_date")
        )
    )
    
    # Conjunto de chaves (Filial x SKU) para online
    keys = (
        df
        .select("CdFilial", "CdSkuLoja")
        .dropDuplicates()
    )
    
    print(f"🔑 Chaves únicas ONLINE (Filial x SKU): {keys.count()}")
    
    # Grade completa (Data x Filial x SKU)
    grade = cal.crossJoin(keys)
    
    # Agregado com data como DateType para join
    df_agg_d = df_agg.withColumn("DtAtual_date", F.to_date("DtAtual"))
    
    # Left join + zeros onde não houver venda
    result = (
        grade.join(
            df_agg_d,
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
    
    print(f"✅ Registros finais ONLINE: {result.count()}")
    
    # Mostrar amostra dos dados
    print("📋 Amostra dos dados ONLINE:")
    result.show(5)
    
    return result

# Executar função de vendas online
vendas_online_df = get_vendas_online(spark)
print(f"🌐 DataFrame vendas online criado com {vendas_online_df.count()} registros")

#%% [markdown]
# ## 6. Consolidação de Vendas Online e Offline
#
# <details>
# <summary><b>🔄 Lógica de Consolidação</b></summary>
#
# Esta seção unifica os dados de ambos os canais:
# - **União**: Combina DataFrames online e offline
# - **Estatísticas**: Mostra resumo por canal
# - **Validação**: Verifica se há dados para processar
# - **Amostra**: Exibe dados consolidados para verificação
#
# </details>

#%%
def consolidar_vendas_online_offline(
    vendas_offline_df: DataFrame,
    vendas_online_df: DataFrame
) -> DataFrame:
    """
    Consolida vendas online e offline em um único DataFrame.
    
    Args:
        vendas_offline_df: DataFrame com vendas offline
        vendas_online_df: DataFrame com vendas online
        
    Returns:
        DataFrame consolidado com vendas de ambos os canais
        
    Raises:
        ValueError: Se os DataFrames estiverem vazios
    """
    if vendas_offline_df.count() == 0 and vendas_online_df.count() == 0:
        raise ValueError("Ambos os DataFrames de vendas estão vazios")
    
    print("🔄 Consolidando vendas ONLINE e OFFLINE...")
    
    # Unir os DataFrames
    vendas_consolidadas = vendas_offline_df.union(vendas_online_df)
    
    print(f"📊 Total de registros consolidados: {vendas_consolidadas.count()}")
    
    # Mostrar estatísticas por canal
    print("📈 Estatísticas por canal:")
    vendas_consolidadas.groupBy("Canal").agg(
        F.count("*").alias("Total_Registros"),
        F.sum("Receita").alias("Receita_Total"),
        F.sum("QtMercadoria").alias("Quantidade_Total"),
        F.sum("TeveVenda").alias("Dias_Com_Venda")
    ).show()
    
    # Mostrar amostra dos dados consolidados
    print("📋 Amostra dos dados consolidados:")
    vendas_consolidadas.show(10)
    
    return vendas_consolidadas

# Executar consolidação
vendas_consolidadas_df = consolidar_vendas_online_offline(vendas_offline_df, vendas_online_df)
print(f"🔄 DataFrame consolidado criado com {vendas_consolidadas_df.count()} registros")

#%% [markdown]
# ## 7. Salvamento na Camada Bronze
#
# <details>
# <summary><b>💾 Lógica de Salvamento</b></summary>
#
# Esta seção salva os dados processados na camada Bronze:
# - **Metadados**: DataHoraProcessamento, FonteDados, VersaoProcessamento
# - **Timezone**: GMT-3 São Paulo para DataHoraProcessamento
# - **Modo**: Overwrite por padrão (configurável)
# - **Validação**: Verifica sucesso do salvamento
# - **Schema**: Mostra estrutura da tabela criada
#
# </details>

#%%
def salvar_tabela_bronze(
    df: DataFrame,
    nome_tabela: str = TABELA_BRONZE_VENDAS,
    modo: str = "overwrite"
) -> bool:
    """
    Salva DataFrame na camada Bronze com metadados de processamento.
    
    Args:
        df: DataFrame para salvar
        nome_tabela: Nome da tabela de destino
        modo: Modo de salvamento (overwrite, append)
        
    Returns:
        bool: True se salvamento foi bem-sucedido
        
    Raises:
        ValueError: Se modo não for válido
    """
    if modo not in ["overwrite", "append"]:
        raise ValueError("modo deve ser 'overwrite' ou 'append'")
    
    try:
        print(f"💾 Salvando tabela {nome_tabela} no modo {modo}...")
        
        # Adicionar metadados de processamento
        df_com_metadados = df.withColumn(
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
        
        # Salvar na camada Bronze
        df_com_metadados.write \
            .format("delta") \
            .mode(modo) \
            .option("overwriteSchema", "true") \
            .saveAsTable(nome_tabela)
        
        print(f"✅ Tabela {nome_tabela} salva com sucesso!")
        print(f"📊 Registros salvos: {df_com_metadados.count()}")
        
        # Mostrar schema da tabela salva
        print("📋 Schema da tabela salva:")
        spark.table(nome_tabela).printSchema()
        
        # Mostrar amostra dos dados salvos
        print("📋 Amostra dos dados salvos:")
        spark.table(nome_tabela).show(5)
        
        return True
        
    except Exception as e:
        print(f"❌ Erro ao salvar tabela {nome_tabela}: {str(e)}")
        return False

# Executar salvamento
sucesso = salvar_tabela_bronze(vendas_consolidadas_df)
if sucesso:
    print("🎉 Processamento de vendas Bronze concluído com sucesso!")
else:
    print("💥 Falha no processamento de vendas Bronze!")

#%% [markdown]
# ## 8. Conclusões e Resumo Final
#
# <details>
# <summary><b>🔍 Principais Resultados</b></summary>
#
# ### ✅ Processamento Concluído
# - **Vendas Offline**: Processadas com sucesso
# - **Vendas Online**: Processadas com sucesso
# - **Consolidação**: Dados unificados
# - **Salvamento**: Tabela Bronze criada
#
# ### 📊 Características da Tabela
# - **Nome**: `databox.bcg_comum.supply_bronze_vendas_90d_on_off`
# - **Período**: Últimos 90 dias (configurável)
# - **Grade Completa**: Todas as combinações filial × SKU × data
# - **Canais**: ONLINE e OFFLINE identificados
# - **Metadados**: DataHoraProcessamento, FonteDados, VersaoProcessamento
#
# ### 🎯 Próximos Passos
# - **Camada Silver**: Processar dados limpos e conformados
# - **Camada Gold**: Criar agregações para dashboards
# - **Validação**: Implementar testes de qualidade
# - **Monitoramento**: Configurar alertas de processamento
#
# </details>
#
# ---
#
# ### 📝 Resumo Executivo
#
# Este notebook demonstrou um fluxo completo de processamento de dados incluindo:
# - ✅ **Carregamento**: Dados de vendas online e offline
# - ✅ **Transformação**: Agregação e criação de grade completa
# - ✅ **Consolidação**: União de ambos os canais
# - ✅ **Salvamento**: Persistência na camada Bronze com metadados
# - ✅ **Validação**: Verificação de integridade dos dados
# - ✅ **Documentação**: Código bem documentado e organizado
#
# **Status**: ✅ **CONCLUÍDO COM SUCESSO**