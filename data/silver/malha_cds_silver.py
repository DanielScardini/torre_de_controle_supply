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
TABELA_MALHA_CDS = f"data_engineering_{AMBIENTE_TABELA.lower()}.context_logistica.planoabastecimento"

print(f"📊 Tabela de origem: {TABELA_MALHA_CDS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento e Limpeza dos Dados

# COMMAND ----------

print("📥 Carregando dados da malha de CDs...")

# Carregar dados da tabela
malha_df = spark.table(TABELA_MALHA_CDS)

# Verificar colunas disponíveis
print(f"📋 Colunas disponíveis: {malha_df.columns}")

# Aplicar sample se configurado para desenvolvimento
if USAR_SAMPLES:
    malha_df = malha_df.sample(0.1).limit(SAMPLE_SIZE)
    print(f"🔬 Modo TEST: Aplicado sample de {SAMPLE_SIZE} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construção do Grafo CD→CD

# COMMAND ----------

print("🔗 Construindo grafo dirigido CD→CD...")

# Selecionar colunas de origem e destino (ajustar conforme schema real)
# Assumindo que as colunas são CdFilialAtende e CdFilialEntrega
arestas_df = (
    malha_df
    .select(
        F.col("CdFilialAtende").alias("origem"),
        F.col("CdFilialEntrega").alias("destino")
    )
    .filter(
        F.col("origem").isNotNull() & 
        F.col("destino").isNotNull() &
        (F.col("origem") != F.col("destino"))  # Remove auto-loops
    )
    .dropDuplicates()
)

# Coletar dados para análise em Python
arestas_pandas = arestas_df.toPandas()

print(f"📊 Arestas encontradas: {len(arestas_pandas):,}")
print(f"🔍 Primeiras 5 arestas:")
print(arestas_pandas.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Grafos com NetworkX

# COMMAND ----------

print("🕸️ Criando grafo NetworkX...")

# Criar grafo dirigido
G = nx.DiGraph()

# Adicionar arestas
for _, row in arestas_pandas.iterrows():
    G.add_edge(row['origem'], row['destino'])

# Estatísticas básicas
num_nos = G.number_of_nodes()
num_arestas = G.number_of_edges()

print(f"📊 Estatísticas do Grafo:")
print(f"  • Nós (CDs): {num_nos:,}")
print(f"  • Arestas (fluxos): {num_arestas:,}")
print(f"  • Densidade: {nx.density(G):.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detecção de Componentes Fortemente Conectadas (SCCs)

# COMMAND ----------

print("🔄 Detectando componentes fortemente conectadas (SCCs)...")

# Calcular SCCs
sccs = list(nx.strongly_connected_components(G))

# Filtrar SCCs com mais de 1 nó (ciclos reais)
ciclos = [scc for scc in sccs if len(scc) > 1]

print(f"📊 Análise de SCCs:")
print(f"  • Total de SCCs: {len(sccs)}")
print(f"  • SCCs com ciclos (>1 nó): {len(ciclos)}")
print(f"  • SCCs isolados: {len(sccs) - len(ciclos)}")

# Criar mapeamento de CD para tamanho da SCC
cd_scc_size = {}
for i, scc in enumerate(sccs):
    size = len(scc)
    for cd in scc:
        cd_scc_size[cd] = size

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de Níveis Hierárquicos

# COMMAND ----------

print("📊 Calculando níveis hierárquicos...")

def calcular_niveis_hierarquicos(G):
    """Calcula o nível hierárquico de cada nó baseado na distância máxima a partir de fontes"""
    
    # Encontrar nós fonte (sem predecessores)
    fontes = [n for n in G.nodes() if G.in_degree(n) == 0]
    
    # Se não há fontes, usar nós com menor grau de entrada
    if not fontes:
        min_in_degree = min(G.in_degree(n) for n in G.nodes())
        fontes = [n for n in G.nodes() if G.in_degree(n) == min_in_degree]
    
    niveis = {}
    
    # Para cada fonte, calcular distâncias usando BFS
    for fonte in fontes:
        distancias = nx.single_source_shortest_path_length(G, fonte)
        for no, distancia in distancias.items():
            if no not in niveis or distancia > niveis[no]:
                niveis[no] = distancia
    
    return niveis, fontes

# Calcular níveis hierárquicos
niveis_hierarquicos, nos_fonte = calcular_niveis_hierarquicos(G)

print(f"📊 Análise Hierárquica:")
print(f"  • Nós fonte identificados: {len(nos_fonte)}")
print(f"  • Profundidade máxima: {max(niveis_hierarquicos.values()) if niveis_hierarquicos else 0}")
print(f"  • Níveis únicos: {len(set(niveis_hierarquicos.values()))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação dos Dados para Visualização

# COMMAND ----------

print("📊 Preparando dados para visualização...")

# Criar DataFrame com métricas de cada CD
cds_metrics = []

for cd in G.nodes():
    # Grau total (in + out)
    grau_total = G.degree(cd)
    grau_entrada = G.in_degree(cd)
    grau_saida = G.out_degree(cd)
    
    # Nível hierárquico
    nivel = niveis_hierarquicos.get(cd, 0)
    
    # Tamanho da SCC
    scc_size = cd_scc_size.get(cd, 1)
    
    # Se faz parte de ciclo
    em_ciclo = scc_size > 1
    
    cds_metrics.append({
        'cd': cd,
        'grau_total': grau_total,
        'grau_entrada': grau_entrada,
        'grau_saida': grau_saida,
        'nivel_hierarquico': nivel,
        'scc_size': scc_size,
        'em_ciclo': em_ciclo
    })

# Converter para DataFrame
cds_df = pd.DataFrame(cds_metrics)

print(f"📊 Métricas calculadas para {len(cds_df)} CDs")
print(f"🔍 Primeiras 5 métricas:")
print(cds_df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amostragem para Visualização (se necessário)

# COMMAND ----------

print("🎯 Preparando amostra para visualização...")

# Se há muitos CDs, amostrar os de maior grau
MAX_NOS_VISUALIZACAO = 50

if len(cds_df) > MAX_NOS_VISUALIZACAO:
    print(f"⚠️ Muitos CDs ({len(cds_df)}). Amostrando os {MAX_NOS_VISUALIZACAO} de maior grau...")
    
    # Ordenar por grau total e pegar os top
    cds_amostra = cds_df.nlargest(MAX_NOS_VISUALIZACAO, 'grau_total')
    
    # Criar subgrafo apenas com os CDs amostrados
    cds_amostra_list = cds_amostra['cd'].tolist()
    G_amostra = G.subgraph(cds_amostra_list)
    
    # Filtrar arestas apenas entre CDs amostrados
    arestas_amostra = arestas_pandas[
        (arestas_pandas['origem'].isin(cds_amostra_list)) & 
        (arestas_pandas['destino'].isin(cds_amostra_list))
    ]
    
    print(f"📊 Amostra selecionada: {len(cds_amostra)} CDs")
    print(f"🔗 Arestas na amostra: {len(arestas_amostra)}")
    
else:
    print(f"✅ Quantidade de CDs ({len(cds_df)}) adequada para visualização completa")
    cds_amostra = cds_df
    G_amostra = G
    arestas_amostra = arestas_pandas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação do Gráfico Interativo Plotly

# COMMAND ----------

print("📊 Criando gráfico interativo...")

# Preparar posições dos nós
# Usar layout hierárquico com nível no eixo Y
pos = {}
for _, row in cds_amostra.iterrows():
    cd = row['cd']
    nivel = row['nivel_hierarquico']
    
    # Posição X aleatória dentro do nível
    x_pos = np.random.uniform(-1, 1)
    pos[cd] = (x_pos, -nivel)  # Negativo para nível 0 no topo

# Criar figura
fig = go.Figure()

# Adicionar arestas (linhas direcionais)
for _, edge in arestas_amostra.iterrows():
    origem = edge['origem']
    destino = edge['destino']
    
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

# Adicionar nós (CDs)
for _, row in cds_amostra.iterrows():
    cd = row['cd']
    x, y = pos[cd]
    
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
        f"CD: {cd}<br>"
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
        name=f"CD {cd}",
        showlegend=False
    ))

# Configurar layout
fig.update_layout(
    title=dict(
        text="Complexidade da Malha Logística de CDs<br><sub>Eixo Y = Nível Hierárquico | Cor = SCC (Vermelho=Ciclo) | Tamanho = Grau Total</sub>",
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
html_path = "/dbfs/tmp/malha_cds.html"
fig.write_html(html_path)

print(f"✅ Gráfico salvo em: {html_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo das Métricas

# COMMAND ----------

print("📊 RESUMO DA ANÁLISE DA MALHA DE CDs")
print("=" * 60)

# Estatísticas gerais
print(f"📈 ESTATÍSTICAS GERAIS:")
print(f"  • Total de CDs: {num_nos:,}")
print(f"  • Total de fluxos: {num_arestas:,}")
print(f"  • Densidade do grafo: {nx.density(G):.4f}")

# Análise de ciclos
print(f"\n🔄 ANÁLISE DE CICLOS:")
print(f"  • Componentes fortemente conectadas: {len(sccs)}")
print(f"  • Ciclos detectados: {len(ciclos)}")
print(f"  • CDs em ciclos: {sum(len(scc) for scc in ciclos)}")

# Análise hierárquica
print(f"\n📊 ANÁLISE HIERÁRQUICA:")
print(f"  • Nós fonte: {len(nos_fonte)}")
print(f"  • Profundidade máxima: {max(niveis_hierarquicos.values()) if niveis_hierarquicos else 0}")
print(f"  • Níveis únicos: {len(set(niveis_hierarquicos.values()))}")

# Top CDs por grau
print(f"\n🏆 TOP 10 CDs POR GRAU TOTAL:")
top_cds = cds_df.nlargest(10, 'grau_total')
for _, row in top_cds.iterrows():
    ciclo_info = " (CICLO)" if row['em_ciclo'] else ""
    print(f"  • {row['cd']}: Grau {row['grau_total']} (Nível {row['nivel_hierarquico']}){ciclo_info}")

# Ciclos maiores
if ciclos:
    print(f"\n🔄 MAIORES CICLOS:")
    ciclos_ordenados = sorted(ciclos, key=len, reverse=True)
    for i, ciclo in enumerate(ciclos_ordenados[:5]):
        print(f"  • Ciclo {i+1}: {len(ciclo)} CDs - {list(ciclo)}")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exportação do Resumo JSON

# COMMAND ----------

print("📄 Exportando resumo como JSON...")

# Criar resumo estruturado
resumo = {
    "estatisticas_gerais": {
        "total_cds": int(num_nos),
        "total_fluxos": int(num_arestas),
        "densidade": float(nx.density(G))
    },
    "analise_ciclos": {
        "total_sccs": len(sccs),
        "ciclos_detectados": len(ciclos),
        "cds_em_ciclos": sum(len(scc) for scc in ciclos),
        "ciclos": [list(ciclo) for ciclo in ciclos]
    },
    "analise_hierarquica": {
        "nos_fonte": list(nos_fonte),
        "profundidade_maxima": max(niveis_hierarquicos.values()) if niveis_hierarquicos else 0,
        "niveis_unicos": len(set(niveis_hierarquicos.values()))
    },
    "top_cds": cds_df.nlargest(10, 'grau_total').to_dict('records'),
    "configuracao": {
        "ambiente": AMBIENTE_TABELA,
        "modo": MODO_EXECUCAO,
        "usar_samples": USAR_SAMPLES,
        "tabela_origem": TABELA_MALHA_CDS
    }
}

# Salvar JSON
json_path = "/dbfs/tmp/malha_cds_resumo.json"
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

print("🎉 Análise da complexidade da malha de CDs concluída com sucesso!")
print("=" * 80)
print("📁 ARQUIVOS GERADOS:")
print(f"  • Gráfico HTML: {html_path}")
print(f"  • Resumo JSON: {json_path}")
print("=" * 80)
print("🔍 INTERPRETAÇÃO DOS RESULTADOS:")
print("  • Eixo Y: Nível hierárquico (0 = fonte, números maiores = mais distante)")
print("  • Cor Vermelha: CDs que fazem parte de ciclos")
print("  • Cor Azul: CDs isolados (sem ciclos)")
print("  • Tamanho do nó: Proporcional ao grau total (conexões)")
print("  • Linhas: Fluxos direcionais entre CDs")
print("=" * 80)
