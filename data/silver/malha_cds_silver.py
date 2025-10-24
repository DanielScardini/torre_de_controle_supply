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

# Configurações fixas
TABELA_PLANO_ABASTECIMENTO = "data_engineering_prd.context_logistica.planoabastecimento"

print(f"📊 Tabela de origem: {TABELA_PLANO_ABASTECIMENTO}")
print(f"🔧 Modo: Processamento completo (sem limitações)")

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

# Contar total de registros
total_registros = plano_df.count()
print(f"\n📊 Total de registros: {total_registros:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identificação das Colunas de Conexão

# COMMAND ----------

print("🔍 Identificando colunas de conexão...")

# Colunas da tabela planoabastecimento (estrutura real)
COLUNA_CD_ATENDE = "CdFilialAtende"  # CD que atende (origem) - SEMPRE CD
COLUNA_CD_ENTREGA = "CdFilialEntrega"  # CD que entrega (destino) - SEMPRE CD
COLUNA_LOJA = "CdLoja"  # Loja específica atendida - SEMPRE LOJA

print(f"🔧 Configuração das colunas:")
print(f"  • Coluna CD ATENDE: {COLUNA_CD_ATENDE} (SEMPRE CD)")
print(f"  • Coluna CD ENTREGA: {COLUNA_CD_ENTREGA} (SEMPRE CD)")
print(f"  • Coluna LOJA: {COLUNA_LOJA} (SEMPRE LOJA)")

# Verificar se as colunas existem
colunas_necessarias = [COLUNA_CD_ATENDE, COLUNA_CD_ENTREGA, COLUNA_LOJA]
print(f"\n✅ Verificação das colunas:")
for col in colunas_necessarias:
    if col in plano_df.columns:
        print(f"  ✅ {col}: Encontrada")
    else:
        print(f"  ❌ {col}: NÃO encontrada")

# Processar todos os registros
print(f"🚀 Processando todos os registros ({total_registros:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise da Estrutura CD→CD e CD→Loja

# COMMAND ----------

print("🏗️ Analisando estrutura CD→CD e CD→Loja...")
print("📋 IMPORTANTE: CdFilialAtende e CdFilialEntrega são SEMPRE CDs, CdLoja é SEMPRE LOJA")

# Converter para Pandas para análise mais eficiente
# Usar sample para evitar problemas de memória com tabelas grandes
if total_registros > 100000:
    print(f"⚠️ Tabela grande ({total_registros:,} registros). Usando sample de 10%...")
    plano_pandas = plano_df.sample(0.1).toPandas()
else:
    plano_pandas = plano_df.toPandas()

print(f"\n📊 Análise CD→CD (quando CdFilialAtende != CdFilialEntrega):")
conexoes_cd_cd = plano_pandas[
    (plano_pandas[COLUNA_CD_ATENDE] != plano_pandas[COLUNA_CD_ENTREGA]) &
    (plano_pandas[COLUNA_CD_ATENDE].notna()) &
    (plano_pandas[COLUNA_CD_ENTREGA].notna())
]

print(f"  📋 NOTA: Ambos CdFilialAtende e CdFilialEntrega são SEMPRE CDs")
print(f"  • Conexões CD→CD encontradas: {len(conexoes_cd_cd):,}")
print(f"  • CDs únicos que atendem: {conexoes_cd_cd[COLUNA_CD_ATENDE].nunique():,}")
print(f"  • CDs únicos que são atendidos: {conexoes_cd_cd[COLUNA_CD_ENTREGA].nunique():,}")

print(f"\n📊 Análise CD→Loja (quando CdFilialAtende == CdFilialEntrega):")
conexoes_cd_loja = plano_pandas[
    (plano_pandas[COLUNA_CD_ATENDE] == plano_pandas[COLUNA_CD_ENTREGA]) &
    (plano_pandas[COLUNA_CD_ATENDE].notna()) &
    (plano_pandas[COLUNA_LOJA].notna())
]

print(f"  📋 NOTA: CdFilialAtende é SEMPRE CD, CdLoja é SEMPRE LOJA")
print(f"  • Conexões CD→Loja encontradas: {len(conexoes_cd_loja):,}")
print(f"  • CDs únicos que atendem lojas: {conexoes_cd_loja[COLUNA_CD_ATENDE].nunique():,}")
print(f"  • Lojas únicas atendidas: {conexoes_cd_loja[COLUNA_LOJA].nunique():,}")

print(f"\n📊 Análise Combinada:")
print(f"  • Total de CDs únicos: {plano_pandas[COLUNA_CD_ATENDE].nunique():,}")
print(f"  📋 NOTA: Todos os valores são CDs (CdFilialAtende)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construção dos Grafos

# COMMAND ----------

print("🔗 Construindo grafos NetworkX...")

# Preparar dados para CD→CD
if len(conexoes_cd_cd) > 0:
    conexoes_cd_cd_df = conexoes_cd_cd[[COLUNA_CD_ATENDE, COLUNA_CD_ENTREGA]].copy()
    conexoes_cd_cd_df.columns = ['cd_atende', 'cd_entrega']
    
    # Remover duplicatas e auto-loops
    conexoes_cd_cd_df = conexoes_cd_cd_df.drop_duplicates()
    conexoes_cd_cd_df = conexoes_cd_cd_df[conexoes_cd_cd_df['cd_atende'] != conexoes_cd_cd_df['cd_entrega']]
    
    print(f"✅ Conexões CD→CD preparadas: {len(conexoes_cd_cd_df):,}")
    print(f"  • CDs origem únicos: {conexoes_cd_cd_df['cd_atende'].nunique():,}")
    print(f"  • CDs destino únicos: {conexoes_cd_cd_df['cd_entrega'].nunique():,}")
    
    # Top 10 CDs por grau de saída
    top_cds_saida = conexoes_cd_cd_df['cd_atende'].value_counts().head(10)
    print(f"  • Top 10 CDs por grau de saída:")
    for cd, count in top_cds_saida.items():
        print(f"    - {cd}: {count} conexões")
    
    # Top 10 CDs por grau de entrada
    top_cds_entrada = conexoes_cd_cd_df['cd_entrega'].value_counts().head(10)
    print(f"  • Top 10 CDs por grau de entrada:")
    for cd, count in top_cds_entrada.items():
        print(f"    - {cd}: {count} conexões")
else:
    conexoes_cd_cd_df = pd.DataFrame(columns=['cd_atende', 'cd_entrega'])
    print("⚠️ Nenhuma conexão CD→CD encontrada")

# Preparar dados para CD→Loja
if len(conexoes_cd_loja) > 0:
    conexoes_cd_loja_df = conexoes_cd_loja[[COLUNA_CD_ATENDE, COLUNA_LOJA]].copy()
    conexoes_cd_loja_df.columns = ['cd_atende', 'loja_atendida']
    
    # Remover duplicatas
    conexoes_cd_loja_df = conexoes_cd_loja_df.drop_duplicates()
    
    print(f"✅ Conexões CD→Loja preparadas: {len(conexoes_cd_loja_df):,}")
    print(f"  • CDs únicos: {conexoes_cd_loja_df['cd_atende'].nunique():,}")
    print(f"  • Lojas únicas: {conexoes_cd_loja_df['loja_atendida'].nunique():,}")
    
    # Top 10 CDs por número de lojas atendidas
    top_cds_lojas = conexoes_cd_loja_df['cd_atende'].value_counts().head(10)
    print(f"  • Top 10 CDs por lojas atendidas:")
    for cd, count in top_cds_lojas.items():
        print(f"    - {cd}: {count} lojas")
else:
    conexoes_cd_loja_df = pd.DataFrame(columns=['cd_atende', 'loja_atendida'])
    print("⚠️ Nenhuma conexão CD→Loja encontrada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação dos Dados Antes da Análise de Grafos

# COMMAND ----------

print("🔍 Validando dados antes da análise de grafos...")

# Verificar se temos dados suficientes
usar_grafo_cd_cd = len(conexoes_cd_cd_df) > 0
usar_grafo_cd_loja = len(conexoes_cd_loja_df) > 0

print(f"📋 NOTA: CdFilialAtende e CdFilialEntrega são SEMPRE CDs")
print(f"📋 NOTA: CdFilialAtende é SEMPRE CD, CdLoja é SEMPRE LOJA")

if not usar_grafo_cd_cd and not usar_grafo_cd_loja:
    raise ValueError("❌ Nenhum tipo de conexão válida encontrada para análise de grafos")

if usar_grafo_cd_cd:
    print(f"✅ Grafo CD→CD: {len(conexoes_cd_cd_df):,} conexões válidas")
    
if usar_grafo_cd_loja:
    print(f"✅ Grafo CD→Loja: {len(conexoes_cd_loja_df):,} conexões válidas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise com NetworkX

# COMMAND ----------

print("🔬 Iniciando análise com NetworkX...")

# Criar grafos
G_cd_cd = nx.DiGraph()
G_cd_loja = nx.DiGraph()

# Adicionar arestas CD→CD
if usar_grafo_cd_cd:
    print(f"🔗 Adicionando {len(conexoes_cd_cd_df):,} arestas CD→CD...")
    for _, row in conexoes_cd_cd_df.iterrows():
        G_cd_cd.add_edge(row['cd_atende'], row['cd_entrega'])
    
    print(f"✅ Grafo CD→CD criado:")
    print(f"  • Nós: {G_cd_cd.number_of_nodes():,}")
    print(f"  • Arestas: {G_cd_cd.number_of_edges():,}")
    print(f"  • Densidade: {nx.density(G_cd_cd):.4f}")

# Adicionar arestas CD→Loja
if usar_grafo_cd_loja:
    print(f"🔗 Adicionando {len(conexoes_cd_loja_df):,} arestas CD→Loja...")
    for _, row in conexoes_cd_loja_df.iterrows():
        G_cd_loja.add_edge(row['cd_atende'], row['loja_atendida'])
    
    print(f"✅ Grafo CD→Loja criado:")
    print(f"  • Nós: {G_cd_loja.number_of_nodes():,}")
    print(f"  • Arestas: {G_cd_loja.number_of_edges():,}")
    print(f"  • Densidade: {nx.density(G_cd_loja):.4f}")

# Validar grafos não vazios
if usar_grafo_cd_cd and G_cd_cd.number_of_nodes() == 0:
    print("⚠️ Grafo CD→CD está vazio, desabilitando...")
    usar_grafo_cd_cd = False

if usar_grafo_cd_loja and G_cd_loja.number_of_nodes() == 0:
    print("⚠️ Grafo CD→Loja está vazio, desabilitando...")
    usar_grafo_cd_loja = False

if not usar_grafo_cd_cd and not usar_grafo_cd_loja:
    raise ValueError("❌ Nenhum grafo válido para análise")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detecção de Componentes Fortemente Conectadas (SCCs)

# COMMAND ----------

print("🔄 Detectando componentes fortemente conectadas (SCCs)...")

# Detectar SCCs para CD→CD
if usar_grafo_cd_cd:
    print(f"🔍 Analisando SCCs no grafo CD→CD...")
    sccs_cd_cd = list(nx.strongly_connected_components(G_cd_cd))
    ciclos_cd_cd = [scc for scc in sccs_cd_cd if len(scc) > 1]
    
    print(f"✅ SCCs CD→CD detectadas:")
    print(f"  • Total de SCCs: {len(sccs_cd_cd)}")
    print(f"  • SCCs com ciclos (tamanho > 1): {len(ciclos_cd_cd)}")
    print(f"  • SCCs isoladas: {len(sccs_cd_cd) - len(ciclos_cd_cd)}")
    
    if ciclos_cd_cd:
        print(f"  • CDs em ciclos: {sum(len(scc) for scc in ciclos_cd_cd)}")
        print(f"  • Maior ciclo: {max(len(scc) for scc in ciclos_cd_cd)} CDs")

# Detectar SCCs para CD→Loja
if usar_grafo_cd_loja:
    print(f"🔍 Analisando SCCs no grafo CD→Loja...")
    sccs_cd_loja = list(nx.strongly_connected_components(G_cd_loja))
    ciclos_cd_loja = [scc for scc in sccs_cd_loja if len(scc) > 1]
    
    print(f"✅ SCCs CD→Loja detectadas:")
    print(f"  • Total de SCCs: {len(sccs_cd_loja)}")
    print(f"  • SCCs com ciclos (tamanho > 1): {len(ciclos_cd_loja)}")
    print(f"  • SCCs isoladas: {len(sccs_cd_loja) - len(ciclos_cd_loja)}")
    
    if ciclos_cd_loja:
        print(f"  • Filiais em ciclos: {sum(len(scc) for scc in ciclos_cd_loja)}")
        print(f"  • Maior ciclo: {max(len(scc) for scc in ciclos_cd_loja)} filiais")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de Níveis Hierárquicos

# COMMAND ----------

print("📊 Calculando níveis hierárquicos...")

def calcular_niveis_hierarquicos_cd_cd(G):
    """Calcula níveis hierárquicos para grafo CD→CD usando BFS"""
    niveis = {}
    
    # Encontrar nós fonte (sem arestas de entrada)
    nos_fonte = [n for n in G.nodes() if G.in_degree(n) == 0]
    
    if not nos_fonte:
        # Se não há nós fonte explícitos, usar todos os nós como nível 0
        print("⚠️ Nenhum nó fonte encontrado, usando todos os nós como nível 0")
        for n in G.nodes():
            niveis[n] = 0
        return niveis, nos_fonte
    
    # BFS para calcular distâncias máximas
    for fonte in nos_fonte:
        niveis[fonte] = 0
        fila = deque([(fonte, 0)])
        visitados = {fonte}
        
        while fila:
            no_atual, nivel_atual = fila.popleft()
            
            for vizinho in G.successors(no_atual):
                if vizinho not in visitados:
                    visitados.add(vizinho)
                    novo_nivel = nivel_atual + 1
                    niveis[vizinho] = max(niveis.get(vizinho, 0), novo_nivel)
                    fila.append((vizinho, novo_nivel))
    
    return niveis, nos_fonte

def calcular_niveis_hierarquicos_cd_loja(G):
    """Calcula níveis hierárquicos para grafo CD→Loja usando BFS"""
    niveis = {}
    
    # Encontrar CDs (nós com arestas de saída)
    cds_no_grafo = [n for n in G.nodes() if G.out_degree(n) > 0]
    
    if not cds_no_grafo:
        print("⚠️ Nenhum CD encontrado no grafo")
        return niveis, []
    
    # Usar CDs como nível 0
    for cd in cds_no_grafo:
        niveis[cd] = 0
    
    # BFS para calcular distâncias
    fila = deque([(cd, 0) for cd in cds_no_grafo])
    visitados = set(cds_no_grafo)
    
    while fila:
        no_atual, nivel_atual = fila.popleft()
        
        for vizinho in G.successors(no_atual):
            if vizinho not in visitados:
                visitados.add(vizinho)
                novo_nivel = nivel_atual + 1
                niveis[vizinho] = novo_nivel
                fila.append((vizinho, novo_nivel))
    
    return niveis, cds_no_grafo

# Calcular níveis para CD→CD
if usar_grafo_cd_cd:
    print(f"📊 Calculando níveis hierárquicos CD→CD...")
    niveis_hierarquicos_cd_cd, cds_fonte = calcular_niveis_hierarquicos_cd_cd(G_cd_cd)
    
    print(f"✅ Níveis CD→CD calculados:")
    print(f"  • CDs fonte: {len(cds_fonte)}")
    print(f"  • Profundidade máxima: {max(niveis_hierarquicos_cd_cd.values()) if niveis_hierarquicos_cd_cd else 0}")
    print(f"  • Níveis únicos: {len(set(niveis_hierarquicos_cd_cd.values())) if niveis_hierarquicos_cd_cd else 0}")

# Calcular níveis para CD→Loja
if usar_grafo_cd_loja:
    print(f"📊 Calculando níveis hierárquicos CD→Loja...")
    niveis_hierarquicos_cd_loja, cds_no_grafo = calcular_niveis_hierarquicos_cd_loja(G_cd_loja)
    
    print(f"✅ Níveis CD→Loja calculados:")
    print(f"  • CDs no grafo: {len(cds_no_grafo)}")
    print(f"  • Profundidade máxima: {max(niveis_hierarquicos_cd_loja.values()) if niveis_hierarquicos_cd_loja else 0}")
    print(f"  • Níveis únicos: {len(set(niveis_hierarquicos_cd_loja.values())) if niveis_hierarquicos_cd_loja else 0}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação das Métricas para Visualização

# COMMAND ----------

print("📊 Preparando métricas para visualização...")

# Escolher grafo principal para visualização (priorizar CD→CD)
if usar_grafo_cd_cd:
    G_principal = G_cd_cd
    conexoes_principal = conexoes_cd_cd_df
    niveis_principal = niveis_hierarquicos_cd_cd
    scc_size_principal = {node: len(next(scc for scc in sccs_cd_cd if node in scc)) for node in G_cd_cd.nodes()}
    tipo_grafo = "CD→CD"
    print(f"🎯 Usando grafo CD→CD para visualização")
else:
    G_principal = G_cd_loja
    conexoes_principal = conexoes_cd_loja_df
    niveis_principal = niveis_hierarquicos_cd_loja
    scc_size_principal = {node: len(next(scc for scc in sccs_cd_loja if node in scc)) for node in G_cd_loja.nodes()}
    tipo_grafo = "CD→Loja"
    print(f"🎯 Usando grafo CD→Loja para visualização")

# Criar DataFrame com métricas
nos_data = []
for node in G_principal.nodes():
    grau_total = G_principal.degree(node)
    grau_entrada = G_principal.in_degree(node)
    grau_saida = G_principal.out_degree(node)
    nivel_hierarquico = niveis_principal.get(node, 0)
    scc_size = scc_size_principal.get(node, 1)
    em_ciclo = scc_size > 1
    
    nos_data.append({
        'no': node,
        'grau_total': grau_total,
        'grau_entrada': grau_entrada,
        'grau_saida': grau_saida,
        'nivel_hierarquico': nivel_hierarquico,
        'scc_size': scc_size,
        'em_ciclo': em_ciclo
    })

nos_df = pd.DataFrame(nos_data)

print(f"📊 Métricas preparadas para {len(nos_df):,} nós ({tipo_grafo})")
print(f"🔍 Primeiras 5 métricas:")
print(nos_df.head())

# Usar todos os nós para visualização
print(f"✅ Usando todos os {len(nos_df):,} nós para visualização")
nos_amostra = nos_df
G_amostra = G_principal
arestas_amostra = conexoes_principal

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação do Gráfico Interativo Plotly

# COMMAND ----------

print("🎨 Criando gráfico interativo Plotly...")

# Preparar dados para Plotly
pos_x = []
pos_y = []
textos = []
cores = []
tamanhos = []

for _, row in nos_amostra.iterrows():
    # Posição Y baseada no nível hierárquico (invertido para nível 0 no topo)
    nivel_max = nos_amostra['nivel_hierarquico'].max()
    y_pos = nivel_max - row['nivel_hierarquico']
    
    # Posição X baseada no grau para melhor distribuição
    # Usar grau total para espalhar nós com mais conexões
    x_pos = (row['grau_total'] % 10) * 0.2 - 1  # Distribuir entre -1 e 1
    
    pos_x.append(x_pos)
    pos_y.append(y_pos)
    
    # Cor baseada no tamanho da SCC (ciclos em vermelho)
    if row['em_ciclo']:
        cor = 'red'
    else:
        cor = 'blue'
    
    cores.append(cor)
    
    # Tamanho baseado no grau total
    tamanho = max(10, min(50, row['grau_total'] * 2))
    tamanhos.append(tamanho)
    
    # Texto do tooltip
    texto = f"<b>{row['no']}</b><br>"
    texto += f"Grau Total: {row['grau_total']}<br>"
    texto += f"Grau Entrada: {row['grau_entrada']}<br>"
    texto += f"Grau Saída: {row['grau_saida']}<br>"
    texto += f"Nível Hierárquico: {row['nivel_hierarquico']}<br>"
    texto += f"SCC Size: {row['scc_size']}<br>"
    texto += f"Em Ciclo: {'Sim' if row['em_ciclo'] else 'Não'}"
    
    textos.append(texto)

# Criar gráfico
fig = go.Figure()

# Adicionar nós
fig.add_trace(go.Scatter(
    x=pos_x,
    y=pos_y,
    mode='markers',
    marker=dict(
        size=tamanhos,
        color=cores,
        opacity=0.8,
        line=dict(width=2, color='black')
    ),
    text=textos,
    hovertemplate='%{text}<extra></extra>',
    name='Nós',
    showlegend=False
))

# Adicionar arestas
if len(arestas_amostra) > 0:
    arestas_x = []
    arestas_y = []
    
    for _, row in arestas_amostra.iterrows():
        origem = row.iloc[0]  # Primeira coluna
        destino = row.iloc[1]  # Segunda coluna
        
        # Encontrar posições dos nós
        origem_idx = nos_amostra[nos_amostra['no'] == origem].index
        destino_idx = nos_amostra[nos_amostra['no'] == destino].index
        
        if len(origem_idx) > 0 and len(destino_idx) > 0:
            origem_idx = origem_idx[0]
            destino_idx = destino_idx[0]
            
            # Adicionar linha da origem ao destino
            arestas_x.extend([pos_x[origem_idx], pos_x[destino_idx], None])
            arestas_y.extend([pos_y[origem_idx], pos_y[destino_idx], None])
    
    fig.add_trace(go.Scatter(
        x=arestas_x,
        y=arestas_y,
        mode='lines',
        line=dict(color='gray', width=1),
        hoverinfo='none',
        showlegend=False
    ))

# Configurar layout
fig.update_layout(
    title=f"Malha Logística - {tipo_grafo}<br><sub>Nós: {len(nos_amostra):,} | Arestas: {len(arestas_amostra):,}</sub>",
    xaxis_title="Posição X",
    yaxis_title="Nível Hierárquico (0 = topo)",
    yaxis=dict(autorange='reversed'),
    showlegend=False,
    width=1000,
    height=600,
    plot_bgcolor='white',
    annotations=[
        dict(
            text="🔴 Vermelho = Ciclos | 🔵 Azul = Isolados",
            xref="paper", yref="paper",
            x=0.02, y=0.98,
            showarrow=False,
            font=dict(size=12),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="black",
            borderwidth=1
        )
    ]
)

print("✅ Gráfico criado com sucesso!")

# Exibir o gráfico no notebook
print("🎨 Exibindo gráfico interativo no notebook...")
fig.show()

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
    print(f"  • CDs que atendem outros CDs: {conexoes_cd_cd_df['cd_atende'].nunique() if len(conexoes_cd_cd_df) > 0 else 0:,}")
    print(f"  • CDs que são atendidos: {conexoes_cd_cd_df['cd_entrega'].nunique() if len(conexoes_cd_cd_df) > 0 else 0:,}")
    print(f"  • Total de conexões CD→CD: {len(conexoes_cd_cd_df):,}")
    
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
    print(f"  • CDs que atendem lojas: {conexoes_cd_loja_df['cd_atende'].nunique() if len(conexoes_cd_loja_df) > 0 else 0:,}")
    print(f"  • Lojas atendidas: {conexoes_cd_loja_df['loja_atendida'].nunique() if len(conexoes_cd_loja_df) > 0 else 0:,}")
    print(f"  • Total de conexões CD→Loja: {len(conexoes_cd_loja_df):,}")
    
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
# MAGIC ## Salvamento dos Resultados em JSON

# COMMAND ----------

print("💾 Salvando resultados em JSON para consumo pelos dashboards...")

# Criar estrutura JSON otimizada para dashboards
resultado_json = {
    "metadata": {
        "tipo_analise": tipo_grafo,
        "data_processamento": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tabela_origem": TABELA_PLANO_ABASTECIMENTO,
        "total_registros_processados": total_registros
    },
    "estatisticas_gerais": {
        "total_nos": int(G_principal.number_of_nodes()),
        "total_arestas": int(G_principal.number_of_edges()),
        "densidade": float(nx.density(G_principal)),
        "nos_visualizados": int(len(nos_amostra)),
        "arestas_visualizadas": int(len(arestas_amostra))
    }
}

# Adicionar métricas específicas por tipo de grafo
if tipo_grafo == "CD→CD":
    resultado_json["analise_cd_cd"] = {
        "cds_unicos_atendem": int(conexoes_cd_cd_df['cd_atende'].nunique()) if len(conexoes_cd_cd_df) > 0 else 0,
        "cds_unicos_atendidos": int(conexoes_cd_cd_df['cd_entrega'].nunique()) if len(conexoes_cd_cd_df) > 0 else 0,
        "total_conexoes": int(len(conexoes_cd_cd_df)),
        "top_cds_por_grau_saida": conexoes_cd_cd_df['cd_atende'].value_counts().head(10).to_dict() if len(conexoes_cd_cd_df) > 0 else {},
        "top_cds_por_grau_entrada": conexoes_cd_cd_df['cd_entrega'].value_counts().head(10).to_dict() if len(conexoes_cd_cd_df) > 0 else {}
    }
    
    if usar_grafo_cd_cd:
        resultado_json["complexidade_cd_cd"] = {
            "total_sccs": int(len(sccs_cd_cd)),
            "sccs_com_ciclos": int(len(ciclos_cd_cd)),
            "sccs_isoladas": int(len(sccs_cd_cd) - len(ciclos_cd_cd)),
            "cds_em_ciclos": int(sum(len(scc) for scc in ciclos_cd_cd)),
            "maior_ciclo": int(max(len(scc) for scc in ciclos_cd_cd)) if ciclos_cd_cd else 0
        }
        
        resultado_json["hierarquia_cd_cd"] = {
            "cds_fonte": int(len(cds_fonte)),
            "profundidade_maxima": int(max(niveis_hierarquicos_cd_cd.values())) if niveis_hierarquicos_cd_cd else 0,
            "niveis_unicos": int(len(set(niveis_hierarquicos_cd_cd.values()))) if niveis_hierarquicos_cd_cd else 0,
            "distribuicao_niveis": dict(pd.Series(list(niveis_hierarquicos_cd_cd.values())).value_counts().sort_index())
        }

else:  # CD→Loja
    resultado_json["analise_cd_loja"] = {
        "cds_unicos_atendem": int(conexoes_cd_loja_df['cd_atende'].nunique()) if len(conexoes_cd_loja_df) > 0 else 0,
        "lojas_unicas_atendidas": int(conexoes_cd_loja_df['loja_atendida'].nunique()) if len(conexoes_cd_loja_df) > 0 else 0,
        "total_conexoes": int(len(conexoes_cd_loja_df)),
        "top_cds_por_lojas_atendidas": conexoes_cd_loja_df['cd_atende'].value_counts().head(10).to_dict() if len(conexoes_cd_loja_df) > 0 else {}
    }
    
    if usar_grafo_cd_loja:
        resultado_json["complexidade_cd_loja"] = {
            "total_sccs": int(len(sccs_cd_loja)),
            "sccs_com_ciclos": int(len(ciclos_cd_loja)),
            "sccs_isoladas": int(len(sccs_cd_loja) - len(ciclos_cd_loja)),
            "filiais_em_ciclos": int(sum(len(scc) for scc in ciclos_cd_loja)),
            "maior_ciclo": int(max(len(scc) for scc in ciclos_cd_loja)) if ciclos_cd_loja else 0
        }
        
        resultado_json["hierarquia_cd_loja"] = {
            "cds_no_grafo": int(len(cds_no_grafo)),
            "profundidade_maxima": int(max(niveis_hierarquicos_cd_loja.values())) if niveis_hierarquicos_cd_loja else 0,
            "niveis_unicos": int(len(set(niveis_hierarquicos_cd_loja.values()))) if niveis_hierarquicos_cd_loja else 0,
            "distribuicao_niveis": dict(pd.Series(list(niveis_hierarquicos_cd_loja.values())).value_counts().sort_index())
        }

# Adicionar dados das conexões para desenhar o grafo
resultado_json["conexoes"] = []

if tipo_grafo == "CD→CD":
    for _, row in conexoes_cd_cd_df.iterrows():
        resultado_json["conexoes"].append({
            "origem": str(row['cd_atende']),
            "destino": str(row['cd_entrega']),
            "tipo": "CD→CD",
            "origem_tipo": "CD",
            "destino_tipo": "CD"
        })
else:  # CD→Loja
    for _, row in conexoes_cd_loja_df.iterrows():
        resultado_json["conexoes"].append({
            "origem": str(row['cd_atende']),
            "destino": str(row['loja_atendida']),
            "tipo": "CD→Loja",
            "origem_tipo": "CD",
            "destino_tipo": "Loja"
        })

# Adicionar dados dos nós com posições para visualização
resultado_json["nos"] = []
for _, row in nos_amostra.iterrows():
    resultado_json["nos"].append({
        "id": str(row['no']),
        "tipo": "CD" if tipo_grafo == "CD→CD" else ("CD" if row['grau_saida'] > 0 else "Loja"),
        "grau_total": int(row['grau_total']),
        "grau_entrada": int(row['grau_entrada']),
        "grau_saida": int(row['grau_saida']),
        "nivel_hierarquico": int(row['nivel_hierarquico']),
        "em_ciclo": bool(row['em_ciclo']),
        "scc_size": int(row['scc_size'])
    })

# Salvar JSON
json_path = f"/dbfs/tmp/malha_logistica_{tipo_grafo.lower().replace('→', '_')}.json"
with open(json_path, 'w', encoding='utf-8') as f:
    json.dump(resultado_json, f, indent=2, ensure_ascii=False)

print(f"✅ Resultados salvos em: {json_path}")
print(f"📊 Estrutura do JSON para Visualização:")
print(f"  • Metadata: informações gerais")
print(f"  • Estatísticas gerais: métricas do grafo")
print(f"  • Análise específica: métricas por tipo de conexão")
print(f"  • Complexidade: análise de ciclos e SCCs")
print(f"  • Hierarquia: níveis e distribuição")
print(f"  • Conexões: dados das arestas para desenhar setas direcionais")
print(f"  • Nós: dados dos vértices com propriedades para visualização")

# Mostrar resumo do JSON salvo
print(f"\n📋 RESUMO DO JSON SALVO:")
print(f"  • Tamanho estimado: ~{len(json.dumps(resultado_json)):,} caracteres")
print(f"  • Total de conexões: {len(resultado_json['conexoes'])}")
print(f"  • Total de nós: {len(resultado_json['nos'])}")
print(f"  • Nós em ciclos: {sum(1 for no in resultado_json['nos'] if no['em_ciclo'])}")
print(f"  • Nós fonte (nível 0): {sum(1 for no in resultado_json['nos'] if no['nivel_hierarquico'] == 0)}")