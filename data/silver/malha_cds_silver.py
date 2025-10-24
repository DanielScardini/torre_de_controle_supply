# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC # Análise da Complexidade da Malha Logística de CDs
# MAGIC
# MAGIC Este notebook analisa a complexidade da malha logística de Centros de Distribuição (CDs),
# MAGIC identificando ciclos, hierarquias e estruturas de fluxo entre depósitos.
# MAGIC
# MAGIC ## Objetivos:
# MAGIC - Construir grafo dirigido CD→CD
# MAGIC - Detectar componentes fortemente conectadas (SCCs) → ciclos
# MAGIC - Calcular níveis hierárquicos de cada CD
# MAGIC - Visualizar estrutura com gráfico interativo Plotly

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurações e Imports

# COMMAND ----------

# Instalar dependências necessárias
%pip install plotly networkx

# COMMAND ----------

# Imports necessários
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import networkx as nx
import pandas as pd
import json
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Set
import numpy as np

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurações de Ambiente

# COMMAND ----------

# Widgets para configuração
dbutils.widgets.dropdown("ambiente_tabela", "DEV", ["DEV", "PROD"], "Ambiente da Tabela")
dbutils.widgets.dropdown("modo_execucao", "TEST", ["TEST", "RUN"], "Modo de Execução")
dbutils.widgets.text("sample_size", "1000", "Tamanho do Sample (apenas para TEST)")

# Obter valores dos widgets
AMBIENTE_TABELA = dbutils.widgets.get("ambiente_tabela")
MODO_EXECUCAO = dbutils.widgets.get("modo_execucao")
SAMPLE_SIZE = int(dbutils.widgets.get("sample_size"))

print(f"🔧 Configurações:")
print(f"  • Ambiente: {AMBIENTE_TABELA}")
print(f"  • Modo: {MODO_EXECUCAO}")
print(f"  • Sample Size: {SAMPLE_SIZE}")

# Configurações de desenvolvimento
USAR_SAMPLES = (MODO_EXECUCAO == "TEST")

# Nome da tabela (ajustar conforme necessário)
TABELA_PLANO_ABASTECIMENTO = f"data_engineering_{AMBIENTE_TABELA.lower()}.context_logistica.planoabastecimento"

print(f"📊 Tabela de origem: {TABELA_PLANO_ABASTECIMENTO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploração da Tabela PlanoAbastecimento - Hierarquia e Malha

# COMMAND ----------

print("📥 Carregando dados da tabela PlanoAbastecimento...")

# Carregar dados da tabela
plano_df = spark.table(TABELA_PLANO_ABASTECIMENTO)

# Verificar colunas disponíveis
print(f"📋 Colunas disponíveis: {plano_df.columns}")

# Mostrar schema detalhado
print(f"\n📊 Schema da tabela:")
plano_df.printSchema()

# Mostrar algumas linhas para entender a estrutura
print(f"\n🔍 Primeiras 5 linhas:")
plano_df.show(5, truncate=False)

# Contar registros totais
total_registros = plano_df.count()
print(f"\n📊 Total de registros: {total_registros:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identificação das Colunas de Hierarquia e Malha

# COMMAND ----------

print("🔍 Identificando colunas para hierarquia e malha CD→Loja...")

# Procurar colunas relacionadas a filiais, CDs, lojas
colunas_filial = [col for col in plano_df.columns if any(palavra in col.upper() for palavra in ['FILIAL', 'CD', 'LOJA', 'DEPOSITO', 'CENTRO'])]
colunas_tipo = [col for col in plano_df.columns if any(palavra in col.upper() for palavra in ['TIPO', 'CATEGORIA', 'CLASSIFICACAO'])]
colunas_hierarquia = [col for col in plano_df.columns if any(palavra in col.upper() for palavra in ['HIERARQUIA', 'NIVEL', 'LEVEL', 'PARENT', 'PAI', 'FILHO'])]

print(f"📋 Colunas relacionadas a FILIAIS: {colunas_filial}")
print(f"📋 Colunas relacionadas a TIPO: {colunas_tipo}")
print(f"📋 Colunas relacionadas a HIERARQUIA: {colunas_hierarquia}")

# Mostrar todas as colunas para análise manual
print(f"\n📋 Todas as colunas disponíveis:")
for i, col in enumerate(plano_df.columns):
    print(f"  {i+1:2d}. {col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise dos Tipos de Filiais (CDs vs Lojas)

# COMMAND ----------

print("🏢 Analisando tipos de filiais para identificar CDs e Lojas...")

# Procurar coluna que identifica tipo de filial (CD vs Loja)
# Possíveis nomes: TipoFilial, CategoriaFilial, ClassificacaoFilial, etc.
colunas_tipo_filial = [col for col in plano_df.columns if any(palavra in col.upper() for palavra in ['TIPO', 'CATEGORIA', 'CLASSIFICACAO']) and 'FILIAL' in col.upper()]

if colunas_tipo_filial:
    tipo_filial_col = colunas_tipo_filial[0]
    print(f"📊 Usando coluna de tipo: {tipo_filial_col}")
    
    # Mostrar valores únicos de tipo
    print(f"\n📊 Valores únicos de tipo de filial:")
    tipos_unicos = plano_df.select(tipo_filial_col).distinct().collect()
    for row in tipos_unicos:
        print(f"  • {row[0]}")
    
    # Contar por tipo
    print(f"\n📈 Contagem por tipo de filial:")
    contagem_tipos = plano_df.groupBy(tipo_filial_col).count().orderBy(F.desc("count")).collect()
    for row in contagem_tipos:
        print(f"  • {row[0]}: {row[1]:,} registros")
        
else:
    print("⚠️ Não encontrou coluna de tipo de filial")
    print("💡 Será necessário identificar manualmente CDs vs Lojas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identificação das Colunas de Conexão CD→Loja

# COMMAND ----------

print("🔗 Identificando colunas de conexão CD→Loja...")

# Procurar colunas que podem representar origem (CD) e destino (Loja)
colunas_possiveis_origem = [col for col in plano_df.columns if any(palavra in col.upper() for palavra in ['ATENDE', 'ORIGEM', 'DE', 'FROM', 'FILIAL_ATENDE', 'CD_ATENDE'])]
colunas_possiveis_destino = [col for col in plano_df.columns if any(palavra in col.upper() for palavra in ['ENTREGA', 'DESTINO', 'PARA', 'TO', 'FILIAL_ENTREGA', 'LOJA_ENTREGA'])]

print(f"📋 Colunas possíveis para ORIGEM (CD): {colunas_possiveis_origem}")
print(f"📋 Colunas possíveis para DESTINO (Loja): {colunas_possiveis_destino}")

# Se não encontrar automaticamente, mostrar colunas relacionadas a filiais
if not colunas_possiveis_origem or not colunas_possiveis_destino:
    print(f"\n⚠️ Não foi possível identificar automaticamente as colunas de conexão.")
    print(f"📋 Colunas relacionadas a filiais:")
    for col in colunas_filial:
        print(f"  • {col}")
    
    print(f"\n💡 Será necessário identificar manualmente:")
    print(f"  • Coluna de ORIGEM (CD que atende): ?")
    print(f"  • Coluna de DESTINO (Loja atendida): ?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Manual das Colunas de Hierarquia

# COMMAND ----------

# Configurar manualmente as colunas baseado na exploração acima
# AJUSTAR ESTAS VARIÁVEIS CONFORME A ESTRUTURA REAL DA TABELA

# Colunas da tabela planoabastecimento (estrutura real)
COLUNA_CD_ATENDE = "CdFilialAtende"  # CD que atende (origem) - SEMPRE CD
COLUNA_CD_ENTREGA = "CdFilialEntrega"  # CD que entrega (destino) - SEMPRE CD
COLUNA_LOJA = "CdLoja"  # Loja específica atendida - SEMPRE LOJA

print(f"🔧 Configuração das colunas:")
print(f"  • Coluna CD ATENDE: {COLUNA_CD_ATENDE} (SEMPRE CD)")
print(f"  • Coluna CD ENTREGA: {COLUNA_CD_ENTREGA} (SEMPRE CD)")
print(f"  • Coluna LOJA: {COLUNA_LOJA} (SEMPRE LOJA)")

# Verificar se as colunas existem
colunas_obrigatorias = [COLUNA_CD_ATENDE, COLUNA_CD_ENTREGA, COLUNA_LOJA]

print(f"\n✅ Verificação de colunas obrigatórias:")
for col in colunas_obrigatorias:
    if col in plano_df.columns:
        print(f"  ✅ {col}: Encontrada")
    else:
        print(f"  ❌ {col}: NÃO encontrada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicação de Sample para Desenvolvimento

# COMMAND ----------

# Aplicar sample se configurado para desenvolvimento
if USAR_SAMPLES:
    print(f"🔬 Modo TEST: Aplicando sample de {SAMPLE_SIZE} registros...")
    plano_df = plano_df.sample(0.1).limit(SAMPLE_SIZE)
    print(f"✅ Sample aplicado: {plano_df.count():,} registros")
else:
    print(f"🚀 Modo RUN: Processando todos os registros ({plano_df.count():,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise da Estrutura CD→CD e CD→Loja

# COMMAND ----------

print("🏗️ Analisando estrutura CD→CD e CD→Loja...")
print("📋 IMPORTANTE: CdFilialAtende e CdFilialEntrega são SEMPRE CDs, CdLoja é SEMPRE LOJA")

# Verificar se temos as colunas necessárias
if COLUNA_CD_ATENDE not in plano_df.columns or COLUNA_CD_ENTREGA not in plano_df.columns or COLUNA_LOJA not in plano_df.columns:
    print("❌ ERRO: Colunas necessárias não encontradas!")
    print(f"📋 Colunas disponíveis: {plano_df.columns}")
    raise ValueError("Não é possível construir a análise sem as colunas necessárias!")

# Análise 1: Conexões CD→CD (quando CdFilialAtende != CdFilialEntrega)
# Ambos são CDs, então temos um link real entre CDs
conexoes_cd_cd_df = (
    plano_df
    .select(
        F.col(COLUNA_CD_ATENDE).alias("cd_atende"),
        F.col(COLUNA_CD_ENTREGA).alias("cd_entrega")
    )
    .filter(
        F.col("cd_atende").isNotNull() & 
        F.col("cd_entrega").isNotNull() &
        (F.col("cd_atende") != F.col("cd_entrega"))  # Remove auto-loops
    )
    .dropDuplicates()
)

print(f"📊 Conexões CD→CD encontradas: {conexoes_cd_cd_df.count():,}")

# Análise 2: Conexões CD→Loja (quando CdFilialAtende == CdFilialEntrega)
# CdFilialAtende é CD, CdLoja é Loja, então temos CD atendendo suas lojas
conexoes_cd_loja_df = (
    plano_df
    .select(
        F.col(COLUNA_CD_ATENDE).alias("cd_atende"),
        F.col(COLUNA_LOJA).alias("loja_atendida")
    )
    .filter(
        F.col("cd_atende").isNotNull() & 
        F.col("loja_atendida").isNotNull() &
        (F.col(COLUNA_CD_ATENDE) == F.col(COLUNA_CD_ENTREGA))  # Mesmo CD atendendo suas lojas
    )
    .dropDuplicates()
)

print(f"📊 Conexões CD→Loja encontradas: {conexoes_cd_loja_df.count():,}")

# Mostrar algumas conexões de cada tipo
print(f"\n🔍 Primeiras 5 conexões CD→CD:")
conexoes_cd_cd_df.show(5, truncate=False)

print(f"\n🔍 Primeiras 5 conexões CD→Loja:")
conexoes_cd_loja_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise Detalhada das Conexões

# COMMAND ----------

print("📊 Analisando conexões CD→CD e CD→Loja...")

# Coletar dados para análise em Python
conexoes_cd_cd_pandas = conexoes_cd_cd_df.toPandas()
conexoes_cd_loja_pandas = conexoes_cd_loja_df.toPandas()

print(f"\n📈 ESTATÍSTICAS GERAIS:")
print(f"  • Conexões CD→CD: {len(conexoes_cd_cd_pandas):,}")
print(f"  • Conexões CD→Loja: {len(conexoes_cd_loja_pandas):,}")

# Análise CD→CD
if len(conexoes_cd_cd_pandas) > 0:
    print(f"\n🏗️ ANÁLISE CD→CD (Links entre CDs):")
    print(f"  • CDs únicos que atendem outros CDs: {conexoes_cd_cd_pandas['cd_atende'].nunique():,}")
    print(f"  • CDs únicos que são atendidos: {conexoes_cd_cd_pandas['cd_entrega'].nunique():,}")
    print(f"  📋 NOTA: Ambos CdFilialAtende e CdFilialEntrega são SEMPRE CDs")
    
    # Mostrar top CDs por número de outros CDs atendidos
    print(f"\n🏆 TOP 10 CDs que atendem mais outros CDs:")
    top_cds_atendem = conexoes_cd_cd_pandas['cd_atende'].value_counts().head(10)
    for cd, count in top_cds_atendem.items():
        print(f"  • {cd}: atende {count} outros CDs")
    
    # Mostrar top CDs por número de vezes que são atendidos
    print(f"\n🏆 TOP 10 CDs mais atendidos por outros CDs:")
    top_cds_atendidos = conexoes_cd_cd_pandas['cd_entrega'].value_counts().head(10)
    for cd, count in top_cds_atendidos.items():
        print(f"  • {cd}: atendido por {count} outros CDs")

# Análise CD→Loja
if len(conexoes_cd_loja_pandas) > 0:
    print(f"\n🏪 ANÁLISE CD→Loja (CDs atendendo suas lojas):")
    print(f"  • CDs únicos que atendem lojas: {conexoes_cd_loja_pandas['cd_atende'].nunique():,}")
    print(f"  • Lojas únicas atendidas: {conexoes_cd_loja_pandas['loja_atendida'].nunique():,}")
    print(f"  📋 NOTA: CdFilialAtende é SEMPRE CD, CdLoja é SEMPRE LOJA")
    
    # Mostrar top CDs por número de lojas atendidas
    print(f"\n🏆 TOP 10 CDs que atendem mais lojas:")
    top_cds_lojas = conexoes_cd_loja_pandas['cd_atende'].value_counts().head(10)
    for cd, count in top_cds_lojas.items():
        print(f"  • {cd}: atende {count} lojas")

# Análise combinada
print(f"\n🔄 ANÁLISE COMBINADA:")
if len(conexoes_cd_cd_pandas) > 0 and len(conexoes_cd_loja_pandas) > 0:
    cds_que_atendem_outros = set(conexoes_cd_cd_pandas['cd_atende'].unique())
    cds_que_atendem_lojas = set(conexoes_cd_loja_pandas['cd_atende'].unique())
    cds_hibridos = cds_que_atendem_outros.intersection(cds_que_atendem_lojas)
    
    print(f"  • CDs que atendem outros CDs: {len(cds_que_atendem_outros):,}")
    print(f"  • CDs que atendem lojas: {len(cds_que_atendem_lojas):,}")
    print(f"  • CDs híbridos (fazem ambos): {len(cds_hibridos):,}")
    print(f"  📋 NOTA: Todos os valores são CDs (CdFilialAtende)")
    
    if cds_hibridos:
        print(f"  • CDs híbridos: {list(cds_hibridos)[:10]}")  # Mostrar apenas os primeiros 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação dos Dados Antes da Análise de Grafos

# COMMAND ----------

# Verificar se temos dados suficientes para análise de grafos CD→CD
if len(conexoes_cd_cd_pandas) == 0:
    print("⚠️ AVISO: Nenhuma conexão CD→CD encontrada!")
    print("🔍 Isso significa que não há links entre CDs - cada CD atende apenas suas próprias lojas")
    print("📊 A análise de grafos será focada apenas na estrutura CD→Loja")
    print("📋 NOTA: CdFilialAtende e CdFilialEntrega são SEMPRE CDs")
    usar_grafo_cd_cd = False
else:
    print(f"✅ Dados CD→CD validados! {len(conexoes_cd_cd_pandas):,} conexões CD→CD encontradas")
    print("🚀 Prosseguindo com análise de grafos CD→CD...")
    print("📋 NOTA: CdFilialAtende e CdFilialEntrega são SEMPRE CDs")
    usar_grafo_cd_cd = True

# Verificar dados CD→Loja
if len(conexoes_cd_loja_pandas) == 0:
    print("⚠️ AVISO: Nenhuma conexão CD→Loja encontrada!")
    print("🔍 Verifique se a estrutura da tabela está correta")
    print("📋 NOTA: CdFilialAtende é SEMPRE CD, CdLoja é SEMPRE LOJA")
    usar_grafo_cd_loja = False
else:
    print(f"✅ Dados CD→Loja validados! {len(conexoes_cd_loja_pandas):,} conexões CD→Loja encontradas")
    print("📋 NOTA: CdFilialAtende é SEMPRE CD, CdLoja é SEMPRE LOJA")
    usar_grafo_cd_loja = True

if not usar_grafo_cd_cd and not usar_grafo_cd_loja:
    print("❌ ERRO: Nenhum tipo de conexão válida encontrada!")
    raise ValueError("Não é possível prosseguir sem conexões válidas!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Grafos com NetworkX

# COMMAND ----------

print("🕸️ Criando grafos NetworkX...")

# Criar grafo dirigido para CD→CD
if usar_grafo_cd_cd:
    print("🏗️ Criando grafo CD→CD...")
    G_cd_cd = nx.DiGraph()
    
    # Adicionar arestas CD→CD
    for _, row in conexoes_cd_cd_pandas.iterrows():
        G_cd_cd.add_edge(row['cd_atende'], row['cd_entrega'])
    
    # Estatísticas do grafo CD→CD
    num_nos_cd_cd = G_cd_cd.number_of_nodes()
    num_arestas_cd_cd = G_cd_cd.number_of_edges()
    
    print(f"📊 Estatísticas do Grafo CD→CD:")
    print(f"  • Nós (CDs): {num_nos_cd_cd:,}")
    print(f"  • Arestas (fluxos CD→CD): {num_arestas_cd_cd:,}")
    print(f"  • Densidade: {nx.density(G_cd_cd):.4f}")
    
    # Verificar se o grafo CD→CD é válido
    if num_nos_cd_cd == 0:
        print("❌ ERRO: Grafo CD→CD vazio!")
        usar_grafo_cd_cd = False
    elif num_nos_cd_cd == 1:
        print("⚠️ AVISO: Grafo CD→CD com apenas 1 nó - análise limitada")
    else:
        print("✅ Grafo CD→CD criado com sucesso!")

# Criar grafo dirigido para CD→Loja
if usar_grafo_cd_loja:
    print("\n🏪 Criando grafo CD→Loja...")
    G_cd_loja = nx.DiGraph()
    
    # Adicionar arestas CD→Loja
    for _, row in conexoes_cd_loja_pandas.iterrows():
        G_cd_loja.add_edge(row['cd_atende'], row['loja_atendida'])
    
    # Estatísticas do grafo CD→Loja
    num_nos_cd_loja = G_cd_loja.number_of_nodes()
    num_arestas_cd_loja = G_cd_loja.number_of_edges()
    
    print(f"📊 Estatísticas do Grafo CD→Loja:")
    print(f"  • Nós (CDs + Lojas): {num_nos_cd_loja:,}")
    print(f"  • Arestas (CD→Loja): {num_arestas_cd_loja:,}")
    print(f"  • Densidade: {nx.density(G_cd_loja):.4f}")
    
    # Identificar CDs e Lojas no grafo
    cds_no_grafo = set(conexoes_cd_loja_pandas['cd_atende'].unique())
    lojas_no_grafo = set(conexoes_cd_loja_pandas['loja_atendida'].unique())
    
    print(f"\n📊 Composição do Grafo CD→Loja:")
    print(f"  • CDs únicos: {len(cds_no_grafo):,}")
    print(f"  • Lojas únicas: {len(lojas_no_grafo):,}")
    
    # Verificar se o grafo CD→Loja é válido
    if num_nos_cd_loja == 0:
        print("❌ ERRO: Grafo CD→Loja vazio!")
        usar_grafo_cd_loja = False
    elif num_nos_cd_loja == 1:
        print("⚠️ AVISO: Grafo CD→Loja com apenas 1 nó - análise limitada")
    else:
        print("✅ Grafo CD→Loja criado com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Hierarquia e Complexidade

# COMMAND ----------

print("🔍 Analisando hierarquia e complexidade...")

# Análise do grafo CD→CD (se existir)
if usar_grafo_cd_cd:
    print("\n🏗️ ANÁLISE CD→CD:")
    
    # Calcular SCCs (Componentes Fortemente Conectadas)
    sccs_cd_cd = list(nx.strongly_connected_components(G_cd_cd))
    ciclos_cd_cd = [scc for scc in sccs_cd_cd if len(scc) > 1]
    
    print(f"📊 Análise de Complexidade CD→CD:")
    print(f"  • Total de SCCs: {len(sccs_cd_cd)}")
    print(f"  • SCCs com ciclos (>1 nó): {len(ciclos_cd_cd)}")
    print(f"  • SCCs isolados: {len(sccs_cd_cd) - len(ciclos_cd_cd)}")
    
    # Criar mapeamento de CD para tamanho da SCC
    cd_scc_size = {}
    for i, scc in enumerate(sccs_cd_cd):
        size = len(scc)
        for cd in scc:
            cd_scc_size[cd] = size
    
    # Calcular níveis hierárquicos para CD→CD
    def calcular_niveis_hierarquicos_cd_cd(G):
        """Calcula o nível hierárquico de cada CD baseado na distância máxima a partir de fontes"""
        
        # Encontrar CDs fonte (sem predecessores)
        fontes = [n for n in G.nodes() if G.in_degree(n) == 0]
        
        # Se não há fontes, usar CDs com menor grau de entrada
        if not fontes:
            min_in_degree = min(G.in_degree(n) for n in G.nodes())
            fontes = [n for n in G.nodes() if G.in_degree(n) == min_in_degree]
        
        niveis = {}
        
        # Para cada fonte, calcular distâncias usando BFS
        for fonte in fontes:
            distancias = nx.single_source_shortest_path_length(G, fonte)
            for cd, distancia in distancias.items():
                if cd not in niveis or distancia > niveis[cd]:
                    niveis[cd] = distancia
        
        return niveis, fontes
    
    niveis_hierarquicos_cd_cd, cds_fonte = calcular_niveis_hierarquicos_cd_cd(G_cd_cd)
    
    print(f"\n📊 Análise Hierárquica CD→CD:")
    print(f"  • CDs fonte identificados: {len(cds_fonte)}")
    print(f"  • Profundidade máxima: {max(niveis_hierarquicos_cd_cd.values()) if niveis_hierarquicos_cd_cd else 0}")
    print(f"  • Níveis únicos: {len(set(niveis_hierarquicos_cd_cd.values()))}")
    
    if cds_fonte:
        print(f"  • CDs fonte: {list(cds_fonte)[:10]}")  # Mostrar apenas os primeiros 10

# Análise do grafo CD→Loja (se existir)
if usar_grafo_cd_loja:
    print("\n🏪 ANÁLISE CD→Loja:")
    
    # Calcular SCCs (Componentes Fortemente Conectadas)
    sccs_cd_loja = list(nx.strongly_connected_components(G_cd_loja))
    ciclos_cd_loja = [scc for scc in sccs_cd_loja if len(scc) > 1]
    
    print(f"📊 Análise de Complexidade CD→Loja:")
    print(f"  • Total de SCCs: {len(sccs_cd_loja)}")
    print(f"  • SCCs com ciclos (>1 nó): {len(ciclos_cd_loja)}")
    print(f"  • SCCs isolados: {len(sccs_cd_loja) - len(ciclos_cd_loja)}")
    
    # Criar mapeamento de filial para tamanho da SCC
    filial_scc_size = {}
    for i, scc in enumerate(sccs_cd_loja):
        size = len(scc)
        for filial in scc:
            filial_scc_size[filial] = size
    
    # Calcular níveis hierárquicos para CD→Loja
    def calcular_niveis_hierarquicos_cd_loja(G, cds_no_grafo):
        """Calcula o nível hierárquico baseado na distância dos CDs (fontes)"""
        
        niveis = {}
        
        # CDs são nível 0 (fontes)
        for cd in cds_no_grafo:
            niveis[cd] = 0
        
        # Calcular níveis das lojas baseado na distância dos CDs
        for cd_fonte in cds_no_grafo:
            try:
                distancias = nx.single_source_shortest_path_length(G, cd_fonte)
                for filial, distancia in distancias.items():
                    if filial not in niveis or distancia > niveis[filial]:
                        niveis[filial] = distancia
            except:
                # Se não conseguir calcular distância, manter nível atual
                continue
        
        return niveis
    
    niveis_hierarquicos_cd_loja = calcular_niveis_hierarquicos_cd_loja(G_cd_loja, cds_no_grafo)
    
    print(f"\n📊 Análise Hierárquica CD→Loja:")
    print(f"  • CDs fonte (nível 0): {len(cds_no_grafo):,}")
    print(f"  • Profundidade máxima: {max(niveis_hierarquicos_cd_loja.values()) if niveis_hierarquicos_cd_loja else 0}")
    print(f"  • Níveis únicos: {len(set(niveis_hierarquicos_cd_loja.values()))}")
    
    # Mostrar distribuição por nível
    if niveis_hierarquicos_cd_loja:
        distribuicao_niveis = {}
        for filial, nivel in niveis_hierarquicos_cd_loja.items():
            if nivel not in distribuicao_niveis:
                distribuicao_niveis[nivel] = 0
            distribuicao_niveis[nivel] += 1
        
        print(f"\n📈 Distribuição por Nível Hierárquico:")
        for nivel in sorted(distribuicao_niveis.keys()):
            count = distribuicao_niveis[nivel]
            tipo = "CDs Fonte" if nivel == 0 else f"Nível {nivel}"
            print(f"  • {tipo}: {count:,} filiais")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação dos Dados para Visualização

# COMMAND ----------

print("📊 Preparando dados para visualização...")

# Determinar qual grafo usar para visualização
if usar_grafo_cd_cd and usar_grafo_cd_loja:
    print("🎯 Ambos os grafos disponíveis - escolhendo CD→CD para visualização principal")
    G_principal = G_cd_cd
    conexoes_principal = conexoes_cd_cd_pandas
    niveis_principal = niveis_hierarquicos_cd_cd
    scc_size_principal = cd_scc_size
    tipo_grafo = "CD→CD"
elif usar_grafo_cd_cd:
    print("🎯 Usando grafo CD→CD para visualização")
    G_principal = G_cd_cd
    conexoes_principal = conexoes_cd_cd_pandas
    niveis_principal = niveis_hierarquicos_cd_cd
    scc_size_principal = cd_scc_size
    tipo_grafo = "CD→CD"
elif usar_grafo_cd_loja:
    print("🎯 Usando grafo CD→Loja para visualização")
    G_principal = G_cd_loja
    conexoes_principal = conexoes_cd_loja_pandas
    niveis_principal = niveis_hierarquicos_cd_loja
    scc_size_principal = filial_scc_size
    tipo_grafo = "CD→Loja"
else:
    print("❌ ERRO: Nenhum grafo válido para visualização!")
    raise ValueError("Não é possível criar visualização sem grafos válidos!")

print(f"✅ Grafo selecionado: {tipo_grafo}")
print(f"📊 Nós: {G_principal.number_of_nodes():,}")
print(f"📊 Arestas: {G_principal.number_of_edges():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação dos Dados para Visualização

# COMMAND ----------

print("📊 Preparando dados para visualização...")

# Preparar métricas para cada nó do grafo principal
nos_metricas = []

for no in G_principal.nodes():
    grau_total = G_principal.degree(no)
    grau_entrada = G_principal.in_degree(no)
    grau_saida = G_principal.out_degree(no)
    nivel_hierarquico = niveis_principal.get(no, 0)
    scc_size = scc_size_principal.get(no, 1)
    em_ciclo = scc_size > 1
    
    nos_metricas.append({
        'no': no,
        'grau_total': grau_total,
        'grau_entrada': grau_entrada,
        'grau_saida': grau_saida,
        'nivel_hierarquico': nivel_hierarquico,
        'scc_size': scc_size,
        'em_ciclo': em_ciclo
    })

# Converter para DataFrame
nos_df = pd.DataFrame(nos_metricas)

print(f"📊 Métricas preparadas para {len(nos_df):,} nós ({tipo_grafo})")
print(f"🔍 Primeiras 5 métricas:")
print(nos_df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amostragem para Visualização (se necessário)

# COMMAND ----------

print("🎯 Preparando amostra para visualização...")

# Se há muitos nós, amostrar os de maior grau
MAX_NOS_VISUALIZACAO = 50

if len(nos_df) > MAX_NOS_VISUALIZACAO:
    print(f"⚠️ Muitos nós ({len(nos_df)}). Amostrando os {MAX_NOS_VISUALIZACAO} de maior grau...")
    
    # Ordenar por grau total e pegar os top
    nos_amostra = nos_df.nlargest(MAX_NOS_VISUALIZACAO, 'grau_total')
    
    # Criar subgrafo apenas com os nós amostrados
    nos_amostra_list = nos_amostra['no'].tolist()
    G_amostra = G_principal.subgraph(nos_amostra_list)
    
    # Filtrar arestas apenas entre nós amostrados
    if tipo_grafo == "CD→CD":
        arestas_amostra = conexoes_principal[
            (conexoes_principal['cd_atende'].isin(nos_amostra_list)) & 
            (conexoes_principal['cd_entrega'].isin(nos_amostra_list))
        ]
    else:  # CD→Loja
        arestas_amostra = conexoes_principal[
            (conexoes_principal['cd_atende'].isin(nos_amostra_list)) & 
            (conexoes_principal['loja_atendida'].isin(nos_amostra_list))
        ]
    
    print(f"📊 Amostra selecionada: {len(nos_amostra)} nós ({tipo_grafo})")
    print(f"🔗 Arestas na amostra: {len(arestas_amostra)}")
    
else:
    print(f"✅ Quantidade de nós ({len(nos_df)}) adequada para visualização completa")
    nos_amostra = nos_df
    G_amostra = G_principal
    arestas_amostra = conexoes_principal

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação do Gráfico Interativo Plotly

# COMMAND ----------

print("📊 Criando gráfico interativo...")

# Preparar posições dos nós
# Usar layout hierárquico com nível no eixo Y
pos = {}
for _, row in nos_amostra.iterrows():
    no = row['no']
    nivel = row['nivel_hierarquico']
    
    # Posição X aleatória dentro do nível
    x_pos = np.random.uniform(-1, 1)
    pos[no] = (x_pos, -nivel)  # Negativo para nível 0 no topo

# Criar figura
fig = go.Figure()

# Adicionar arestas (linhas direcionais)
for _, edge in arestas_amostra.iterrows():
    if tipo_grafo == "CD→CD":
        origem = edge['cd_atende']
        destino = edge['cd_entrega']
    else:  # CD→Loja
        origem = edge['cd_atende']
        destino = edge['loja_atendida']
    
    if origem in pos and destino in pos:
        x0, y0 = pos[origem]
        x1, y1 = pos[destino]
        
        # Adicionar seta direcional
        fig.add_trace(go.Scatter(
            x=[x0, x1, None],
            y=[y0, y1, None],
            mode='lines',
            line=dict(color='lightgray', width=1),
            hoverinfo='none',
            showlegend=False
        ))

# Adicionar nós
for _, row in nos_amostra.iterrows():
    no = row['no']
    x, y = pos[no]
    
    # Cor baseada no tamanho da SCC (ciclos)
    scc_size = row['scc_size']
    if scc_size > 1:
        cor = 'red'  # Vermelho para ciclos
    else:
        cor = 'blue'  # Azul para nós isolados
    
    # Tamanho baseado no grau total
    tamanho = max(10, min(30, row['grau_total'] * 2))
    
    # Tooltip com métricas
    hovertext = (
        f"{tipo_grafo}: {no}<br>"
        f"Nível: {row['nivel_hierarquico']}<br>"
        f"Grau Total: {row['grau_total']}<br>"
        f"Grau Entrada: {row['grau_entrada']}<br>"
        f"Grau Saída: {row['grau_saida']}<br>"
        f"SCC Size: {scc_size}<br>"
        f"Em Ciclo: {'Sim' if row['em_ciclo'] else 'Não'}"
    )
    
    fig.add_trace(go.Scatter(
        x=[x],
        y=[y],
        mode='markers',
        marker=dict(
            size=tamanho,
            color=cor,
            line=dict(width=2, color='black')
        ),
        text=hovertext,
        hoverinfo='text',
        name=f"{tipo_grafo} {no}",
        showlegend=False
    ))

# Configurar layout
fig.update_layout(
    title=dict(
        text=f"Complexidade da Malha Logística - {tipo_grafo}<br><sub>Eixo Y = Nível Hierárquico | Cor = SCC (Vermelho=Ciclo) | Tamanho = Grau Total</sub>",
        x=0.5,
        font=dict(size=16)
    ),
    xaxis=dict(
        title="Posição Horizontal",
        showgrid=True,
        zeroline=False
    ),
    yaxis=dict(
        title="Nível Hierárquico (0 = Fonte)",
        showgrid=True,
        zeroline=False
    ),
    width=1200,
    height=800,
    showlegend=False,
    hovermode='closest'
)

# Adicionar anotações para legenda
fig.add_annotation(
    x=0.02, y=0.98,
    xref='paper', yref='paper',
    text="🔴 Vermelho = Ciclos<br>🔵 Azul = Nós Isolados<br>📏 Tamanho = Grau Total",
    showarrow=False,
    bgcolor='rgba(255,255,255,0.8)',
    bordercolor='black',
    borderwidth=1
)

print("✅ Gráfico criado com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento do Gráfico HTML

# COMMAND ----------

print("💾 Salvando gráfico como HTML...")

# Salvar como HTML
html_path = f"/dbfs/tmp/malha_{tipo_grafo.lower().replace('→', '_')}.html"
fig.write_html(html_path)

print(f"✅ Gráfico salvo em: {html_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo das Métricas

# COMMAND ----------

print(f"📊 RESUMO DA ANÁLISE DA MALHA - {tipo_grafo}")
print("=" * 60)

# Estatísticas gerais
print(f"📈 ESTATÍSTICAS GERAIS:")
print(f"  • Total de nós: {G_principal.number_of_nodes():,}")
print(f"  • Total de arestas: {G_principal.number_of_edges():,}")
print(f"  • Densidade do grafo: {nx.density(G_principal):.4f}")
print(f"  • Nós visualizados: {len(nos_amostra):,}")
print(f"  • Arestas visualizadas: {len(arestas_amostra):,}")

# Análise específica por tipo de grafo
if tipo_grafo == "CD→CD":
    print(f"\n🏗️ ANÁLISE CD→CD:")
    print(f"  • CDs que atendem outros CDs: {conexoes_cd_cd_pandas['cd_atende'].nunique() if len(conexoes_cd_cd_pandas) > 0 else 0:,}")
    print(f"  • CDs que são atendidos: {conexoes_cd_cd_pandas['cd_entrega'].nunique() if len(conexoes_cd_cd_pandas) > 0 else 0:,}")
    print(f"  • Total de conexões CD→CD: {len(conexoes_cd_cd_pandas):,}")
    
    if usar_grafo_cd_cd:
        print(f"\n🔄 ANÁLISE DE CICLOS:")
        print(f"  • Componentes fortemente conectadas: {len(sccs_cd_cd) if 'sccs_cd_cd' in locals() else 0}")
        print(f"  • Ciclos detectados: {len(ciclos_cd_cd) if 'ciclos_cd_cd' in locals() else 0}")
        print(f"  • CDs em ciclos: {sum(len(scc) for scc in ciclos_cd_cd) if 'ciclos_cd_cd' in locals() else 0}")
        
        print(f"\n📊 ANÁLISE HIERÁRQUICA:")
        print(f"  • CDs fonte: {len(cds_fonte) if 'cds_fonte' in locals() else 0:,}")
        print(f"  • Profundidade máxima: {max(niveis_hierarquicos_cd_cd.values()) if niveis_hierarquicos_cd_cd else 0}")
        print(f"  • Níveis únicos: {len(set(niveis_hierarquicos_cd_cd.values())) if niveis_hierarquicos_cd_cd else 0}")

else:  # CD→Loja
    print(f"\n🏪 ANÁLISE CD→Loja:")
    print(f"  • CDs que atendem lojas: {conexoes_cd_loja_pandas['cd_atende'].nunique() if len(conexoes_cd_loja_pandas) > 0 else 0:,}")
    print(f"  • Lojas atendidas: {conexoes_cd_loja_pandas['loja_atendida'].nunique() if len(conexoes_cd_loja_pandas) > 0 else 0:,}")
    print(f"  • Total de conexões CD→Loja: {len(conexoes_cd_loja_pandas):,}")
    
    if usar_grafo_cd_loja:
        print(f"\n🔄 ANÁLISE DE CICLOS:")
        print(f"  • Componentes fortemente conectadas: {len(sccs_cd_loja) if 'sccs_cd_loja' in locals() else 0}")
        print(f"  • Ciclos detectados: {len(ciclos_cd_loja) if 'ciclos_cd_loja' in locals() else 0}")
        print(f"  • Filiais em ciclos: {sum(len(scc) for scc in ciclos_cd_loja) if 'ciclos_cd_loja' in locals() else 0}")
        
        print(f"\n📊 ANÁLISE HIERÁRQUICA:")
        print(f"  • CDs fonte: {len(cds_no_grafo) if 'cds_no_grafo' in locals() else 0:,}")
        print(f"  • Profundidade máxima: {max(niveis_hierarquicos_cd_loja.values()) if niveis_hierarquicos_cd_loja else 0}")
        print(f"  • Níveis únicos: {len(set(niveis_hierarquicos_cd_loja.values())) if niveis_hierarquicos_cd_loja else 0}")

# Top nós por grau
print(f"\n🏆 TOP 10 NÓS POR GRAU TOTAL:")
top_nos = nos_amostra.nlargest(10, 'grau_total')
for _, row in top_nos.iterrows():
    ciclo_info = " (CICLO)" if row['em_ciclo'] else ""
    print(f"  • {row['no']}: Grau {row['grau_total']} (Nível {row['nivel_hierarquico']}){ciclo_info}")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exportação do Resumo JSON

# COMMAND ----------

print("📄 Exportando resumo como JSON...")

# Criar resumo estruturado
resumo = {
    "tipo_grafo": tipo_grafo,
    "estatisticas_gerais": {
        "total_nos": int(G_principal.number_of_nodes()),
        "total_arestas": int(G_principal.number_of_edges()),
        "densidade": float(nx.density(G_principal)),
        "nos_visualizados": int(len(nos_amostra)),
        "arestas_visualizadas": int(len(arestas_amostra))
    },
    "configuracao": {
        "ambiente": AMBIENTE_TABELA,
        "modo": MODO_EXECUCAO,
        "usar_samples": USAR_SAMPLES,
        "tabela_origem": TABELA_PLANO_ABASTECIMENTO
    }
}

# Adicionar métricas específicas do tipo de grafo
if tipo_grafo == "CD→CD":
    resumo["estatisticas_cd_cd"] = {
        "cds_unicos_atendem": int(conexoes_cd_cd_pandas['cd_atende'].nunique()) if len(conexoes_cd_cd_pandas) > 0 else 0,
        "cds_unicos_atendidos": int(conexoes_cd_cd_pandas['cd_entrega'].nunique()) if len(conexoes_cd_cd_pandas) > 0 else 0,
        "total_conexoes_cd_cd": int(len(conexoes_cd_cd_pandas))
    }
    
    if usar_grafo_cd_cd:
        resumo["analise_hierarquica"] = {
            "cds_fonte": int(len(cds_fonte)) if 'cds_fonte' in locals() else 0,
            "profundidade_maxima": int(max(niveis_hierarquicos_cd_cd.values())) if niveis_hierarquicos_cd_cd else 0,
            "niveis_unicos": int(len(set(niveis_hierarquicos_cd_cd.values()))) if niveis_hierarquicos_cd_cd else 0
        }
        
        resumo["analise_complexidade"] = {
            "total_sccs": int(len(sccs_cd_cd)) if 'sccs_cd_cd' in locals() else 0,
            "sccs_com_ciclos": int(len(ciclos_cd_cd)) if 'ciclos_cd_cd' in locals() else 0,
            "sccs_isolados": int(len(sccs_cd_cd) - len(ciclos_cd_cd)) if 'sccs_cd_cd' in locals() and 'ciclos_cd_cd' in locals() else 0
        }

else:  # CD→Loja
    resumo["estatisticas_cd_loja"] = {
        "cds_unicos_atendem": int(conexoes_cd_loja_pandas['cd_atende'].nunique()) if len(conexoes_cd_loja_pandas) > 0 else 0,
        "lojas_unicas_atendidas": int(conexoes_cd_loja_pandas['loja_atendida'].nunique()) if len(conexoes_cd_loja_pandas) > 0 else 0,
        "total_conexoes_cd_loja": int(len(conexoes_cd_loja_pandas))
    }
    
    if usar_grafo_cd_loja:
        resumo["analise_hierarquica"] = {
            "cds_fonte": int(len(cds_no_grafo)) if 'cds_no_grafo' in locals() else 0,
            "profundidade_maxima": int(max(niveis_hierarquicos_cd_loja.values())) if niveis_hierarquicos_cd_loja else 0,
            "niveis_unicos": int(len(set(niveis_hierarquicos_cd_loja.values()))) if niveis_hierarquicos_cd_loja else 0
        }
        
        resumo["analise_complexidade"] = {
            "total_sccs": int(len(sccs_cd_loja)) if 'sccs_cd_loja' in locals() else 0,
            "sccs_com_ciclos": int(len(ciclos_cd_loja)) if 'ciclos_cd_loja' in locals() else 0,
            "sccs_isolados": int(len(sccs_cd_loja) - len(ciclos_cd_loja)) if 'sccs_cd_loja' in locals() and 'ciclos_cd_loja' in locals() else 0
        }

# Adicionar top nós
resumo["top_nos"] = nos_amostra.nlargest(10, 'grau_total').to_dict('records')

# Salvar JSON
json_path = f"/dbfs/tmp/malha_{tipo_grafo.lower().replace('→', '_')}_resumo.json"
with open(json_path, 'w') as f:
    json.dump(resumo, f, indent=2, ensure_ascii=False)

print(f"✅ Resumo JSON salvo em: {json_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibição do Gráfico

# COMMAND ----------

# Exibir o gráfico
fig.show()

# COMMAND ----------

print(f"🎉 Análise da complexidade da malha - {tipo_grafo} concluída com sucesso!")
print("=" * 80)
print("📁 ARQUIVOS GERADOS:")
print(f"  • Gráfico HTML: {html_path}")
print(f"  • Resumo JSON: {json_path}")
print("=" * 80)
print("🔍 INTERPRETAÇÃO DOS RESULTADOS:")
print("  • Eixo Y: Nível hierárquico (0 = fonte, números maiores = mais distante)")
print("  • Cor Vermelha: Nós que fazem parte de ciclos")
print("  • Cor Azul: Nós isolados (sem ciclos)")
print("  • Tamanho do nó: Proporcional ao grau total (conexões)")
print(f"  • Linhas: Fluxos direcionais {tipo_grafo}")
print("=" * 80)
print(f"📊 RESUMO EXECUTIVO:")
print(f"  • Tipo de análise: {tipo_grafo}")
print(f"  • Ambiente: {AMBIENTE_TABELA}")
print(f"  • Modo: {MODO_EXECUCAO}")
print(f"  • Nós analisados: {len(nos_amostra):,}")
print(f"  • Arestas analisadas: {len(arestas_amostra):,}")
print("=" * 80)
