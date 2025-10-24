# Catálogo de Tabelas - Torre de Controle Supply Chain

## Visão Geral

Este catálogo documenta todas as tabelas implementadas nas diferentes camadas de dados da Torre de Controle, seguindo o padrão **Medallion Architecture** (Bronze, Silver, Gold).

## 📊 Camada Bronze

### Tabelas Implementadas

#### 1. **Vendas Consolidadas**
- **Tabela**: `databox.bcg_comum.supply_bronze_vendas_90d_on_off`
- **Propósito**: Consolidação de vendas online e offline dos últimos 90 dias
- **Estrutura**: Cada linha representa uma combinação única de SKU x Loja x Dia
- **Canais**: Separação por sufixos `_OFF` (offline) e `_ON` (online)
- **Fonte**: 
  - `data_engineering_prd.app_logistica.gi_boss_vendas_offline`
  - `data_engineering_prd.app_logistica.gi_boss_vendas_online`
- **Processamento**: Outer join com `fillna(0)` para garantir dados completos
- **Metadados**: 
  - `DataHoraProcessamento` (GMT-3 São Paulo)
  - `DataProcessamento`
  - `FonteDados`
  - `VersaoProcessamento`

#### 2. **Estoque de Lojas**
- **Tabela**: `databox.bcg_comum.supply_bronze_estoque_lojas`
- **Propósito**: Estoque das lojas ativas enriquecido com dados estratégicos do GEF
- **Filtros**: `StLoja == "ATIVA"` e `DsEstoqueLojaDeposito == "L"`
- **Fonte**: 
  - `data_engineering_prd.app_logistica.gi_boss_qualidade_estoque`
  - `databox.logistica_comum.gef_visao_estoque_lojas` (enriquecimento)
- **Join**: `CdFilial + CdSku + DtAtual`
- **Métricas GEF Incluídas**:
  - `ESTOQUE_SEGURANCA`, `LEADTIME_MEDIO`, `COBERTURA_ES_DIAS`
  - `ESTOQUE_ALVO`, `COBERTURA_ATUAL`, `COBERTURA_ALVO`
  - `DDV_SEM_OUTLIER`, `DDV_FUTURO`, `GRADE`, `TRANSITO`
  - `ESTOQUE_PROJETADO`, `COBERTURA_ATUAL_C_TRANISTO_DIAS`
  - `MEDIA_3`, `MEDIA_6`, `MEDIA_9`, `MEDIA_12`
  - `DDV_SO`, `DDV_CO`, `CLUSTER_OBG`, `CLUSTER_SUG`

#### 3. **Estoque de Depósitos (CDs)**
- **Tabela**: `databox.bcg_comum.supply_bronze_estoque_cds`
- **Propósito**: Estoque dos centros de distribuição enriquecido com métricas GEF agregadas das lojas atendidas
- **Filtros**: `DsEstoqueLojaDeposito == "D"`
- **Fonte**: 
  - `data_engineering_prd.app_logistica.gi_boss_qualidade_estoque`
  - `databox.logistica_comum.gef_visao_estoque_lojas` (enriquecimento)
  - `data_engineering_prd.context_logistica.planoabastecimento` (agregação)
- **Processamento**: Agregação GEF das lojas por CD via plano de abastecimento
- **Agregação**: 
  - **SUM**: `ESTOQUE_SEGURANCA`, `ESTOQUE_ALVO`, `DDV_*`, `GRADE`, `TRANSITO`, `ESTOQUE_PROJETADO`, `MEDIA_*`, `CLUSTER_*`
  - **AVG**: `LEADTIME_MEDIO`, `COBERTURA_ES_DIAS`, `COBERTURA_ATUAL`, `COBERTURA_ALVO`
- **Lógica**: Soma das demandas das lojas que cada CD atende

### Características da Camada Bronze

- **Formato**: Delta Lake (Parquet otimizado)
- **Particionamento**: Por data (`DtAtual`)
- **Modo de Salvamento**: `overwrite` (atualização completa)
- **Validações**: 
  - Detecção de duplicatas nas chaves de join
  - Monitoramento de multiplicação de registros
  - Validação de formato de datas
- **Cache**: Implementado com limpeza automática
- **Samples**: Suporte para desenvolvimento (`USAR_SAMPLES=True`)

## 📈 Camada Silver

### Tabelas Implementadas

#### 1. **Master Table - Lojas**
- **Tabela**: `databox.bcg_comum.supply_{ambiente}_master_vendas_estoque_lojas`
- **Propósito**: Tabela mestra consolidando estoque atual com vendas históricas das lojas
- **Granularidade**: `CdSku x CdFilial x DtAtual` (sempre dados de hoje)
- **Fonte**: 
  - `supply_{ambiente}_bronze_vendas_90d_on_off` (vendas)
  - `supply_{ambiente}_bronze_estoque_lojas` (estoque atual)
- **Janelas Temporais**: MTD, YTD, M-1, M-2, M-3, Last 7d/30d/90d/4w
- **Médias Móveis**: 7d, 14d, 30d, 60d, 90d
- **Otimização**: Window functions + dropDuplicates para performance

#### 2. **Master Table - CDs**
- **Tabela**: `databox.bcg_comum.supply_{ambiente}_master_vendas_estoque_cds`
- **Propósito**: Tabela mestra consolidando estoque atual com vendas históricas dos CDs
- **Granularidade**: `CdSku x CdFilial x DtAtual` (sempre dados de hoje)
- **Fonte**: 
  - `supply_{ambiente}_bronze_vendas_90d_on_off` (vendas)
  - `supply_{ambiente}_bronze_estoque_cds` (estoque atual)
- **Janelas Temporais**: Mesmas janelas das lojas
- **Médias Móveis**: Mesmas médias móveis das lojas
- **Otimização**: Window functions + dropDuplicates para performance

#### 3. **Análise de Malha Logística**
- **Arquivo**: `data/silver/malha_cds_silver.py`
- **Propósito**: Análise de complexidade da rede logística CD→CD e CD→Loja
- **Saída**: JSON com dados para visualização de grafos
- **Métricas**: 
  - Componentes fortemente conectadas (SCCs)
  - Níveis hierárquicos
  - Graus de entrada/saída
  - Detecção de ciclos
- **Visualização**: Plotly interativo com NetworkX

## 🏆 Camada Gold

### Status: Em Desenvolvimento
- **Propósito**: Dados agregados e master tables
- **Agregações**: A serem implementadas
- **Métricas**: A serem calculadas

## 📋 Views

### Status: Em Desenvolvimento
- **Propósito**: Views SQL otimizadas para dashboards
- **Dashboards**: A serem criados

## 🔄 Fluxo de Processamento Atual

### 1. Vendas Bronze
```python
# Processamento de vendas online e offline
vendas_offline_df = spark.table("data_engineering_prd.app_logistica.gi_boss_vendas_offline")
vendas_online_df = spark.table("data_engineering_prd.app_logistica.gi_boss_vendas_online")

# Consolidação com outer join
vendas_consolidadas_df = vendas_offline_df.join(
    vendas_online_df, 
    on=["DtAtual", "year_month", "CdFilial", "CdSku"], 
    how="outer"
).fillna(0)
```

### 2. Estoque Bronze
```python
# Processamento de estoque com enriquecimento GEF
estoque_df = spark.table("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
gef_df = spark.table("databox.logistica_comum.gef_visao_estoque_lojas")

# Join com validações
estoque_enriquecido_df = estoque_df.join(
    gef_df,
    on=["CdFilial", "CdSku", "DtAtual"],
    how="left"
)
```

## 📊 Métricas e KPIs Disponíveis

### Vendas
- **Receita Total**: `Receita_OFF + Receita_ON`
- **Quantidade Total**: `QtMercadoria_OFF + QtMercadoria_ON`
- **Presença de Venda**: `TeveVenda_OFF + TeveVenda_ON`

### Estoque
- **DDE (Dias de Demanda Estoque)**: `VrTotalVv / VrVndCmv`
- **Cobertura Atual**: `COBERTURA_ATUAL` (do GEF)
- **Estoque de Segurança**: `ESTOQUE_SEGURANCA` (do GEF)
- **Lead Time Médio**: `LEADTIME_MEDIO` (do GEF)

## 🔍 Validações Implementadas

### Vendas
- Validação de filiais ativas
- Consolidação com outer join
- Preenchimento de valores nulos com zero

### Estoque
- Validação de duplicatas nas chaves de join
- Monitoramento de multiplicação de registros
- Validação de formato de datas (`yyyy-MM-dd`)
- Remoção automática de duplicatas do GEF

## 📅 Frequência de Atualização

- **Vendas**: Diária (processamento dos últimos 90 dias)
- **Estoque**: Diária (dados do dia atual)
- **GEF**: Diária (sincronização com fonte)

## 🛠️ Configurações de Desenvolvimento

### Samples para Desenvolvimento
```python
USAR_SAMPLES = True  # Alterar para False em produção
SAMPLE_SIZE = 100000  # Para vendas
SAMPLE_SIZE = 10000   # Para estoque
```

### Cache Inteligente
- Cache aplicado em DataFrames intermediários
- Limpeza automática após processamento
- Otimização de memória

## 📝 Metadados Padrão

Todas as tabelas Bronze incluem:
- `DataHoraProcessamento`: Timestamp de processamento (GMT-3)
- `DataProcessamento`: Data de processamento
- `FonteDados`: Origem dos dados
- `VersaoProcessamento`: Versão do processamento

## 🔗 Dependências

### Tabelas de Origem
- `data_engineering_prd.app_logistica.gi_boss_vendas_offline`
- `data_engineering_prd.app_logistica.gi_boss_vendas_online`
- `data_engineering_prd.app_logistica.gi_boss_qualidade_estoque`
- `databox.logistica_comum.gef_visao_estoque_lojas`

### Tecnologias
- **Apache Spark**: Processamento distribuído
- **Delta Lake**: Formato de armazenamento
- **Databricks**: Plataforma de execução
- **Python**: Linguagem de programação

---

**Última Atualização**: 2024  
**Versão**: 1.0  
**Responsável**: Torre de Controle Supply Chain
