# Índice de Documentação - Torre de Controle Supply Chain

## 📚 Documentação Principal

### Arquitetura e Estrutura
- [README Principal](../README.md) - Visão geral do projeto
- [Arquitetura](ARCHITECTURE.md) - Arquitetura da solução
- [Guia de Desenvolvimento](DEVELOPMENT.md) - Como contribuir

### Camadas de Dados
- [Visão Geral das Camadas](../data/README.md) - Medallion Architecture
- [Catálogo de Tabelas](CATALOGO_TABELAS.md) - Tabelas implementadas
- [Camada Bronze](../data/bronze/README.md) - Dados brutos
- [Camada Silver](../data/silver/README.md) - Dados limpos
- [Camada Gold](../data/gold/README.md) - Dados agregados
- [Views](../data/views/README.md) - Views para dashboards

## 📊 Catálogo de Tabelas por Camada

### 🥉 Camada Bronze (Implementada)

#### Vendas
- **Tabela**: `databox.bcg_comum.supply_bronze_vendas_90d_on_off`
- **Arquivo**: `data/bronze/vendas_bronze.py`
- **Descrição**: Vendas online e offline consolidadas (90 dias)
- **Canais**: Separação por sufixos `_OFF` e `_ON`

#### Estoque
- **Lojas**: `databox.bcg_comum.supply_bronze_estoque_lojas`
- **CDs**: `databox.bcg_comum.supply_bronze_estoque_cds`
- **Arquivo**: `data/bronze/estoque_bronze.py`
- **Descrição**: Estoque enriquecido com dados do GEF
- **Join**: `CdFilial + CdSku + DtAtual`

### 🥈 Camada Silver (Planejada)
- **Status**: Em desenvolvimento
- **Propósito**: Dados limpos e validados
- **Transformações**: A serem implementadas

### 🥇 Camada Gold (Planejada)
- **Status**: Em desenvolvimento
- **Propósito**: Dados agregados e master tables
- **Métricas**: A serem calculadas

### 📋 Views (Planejadas)
- **Status**: Em desenvolvimento
- **Propósito**: Views SQL para dashboards
- **Dashboards**: A serem criados

## 🔧 Características Técnicas

### Formato e Armazenamento
- **Formato**: Delta Lake (Parquet otimizado)
- **Modo**: Overwrite (atualização completa)
- **Particionamento**: Por data (`DtAtual`)
- **Retenção**: Configurável por tabela

### Validações Implementadas
- **Duplicatas**: Detecção nas chaves de join
- **Multiplicação**: Monitoramento de registros
- **Formato**: Validação de datas (`yyyy-MM-dd`)
- **Integridade**: Checks de referência

### Metadados Padrão
- `DataHoraProcessamento`: Timestamp GMT-3 São Paulo
- `DataProcessamento`: Data de processamento
- `FonteDados`: Origem dos dados
- `VersaoProcessamento`: Versão do processamento

## 🚀 Execução e Configuração

### Desenvolvimento
```python
USAR_SAMPLES = True
SAMPLE_SIZE = 100000  # Para vendas
SAMPLE_SIZE = 10000   # Para estoque
```

### Produção
```python
USAR_SAMPLES = False
# Processamento completo
```

### Cache e Performance
- Cache inteligente em DataFrames intermediários
- Limpeza automática após processamento
- Otimização de memória

## 📈 Métricas Disponíveis

### Vendas
- Receita Total (`Receita_OFF + Receita_ON`)
- Quantidade Total (`QtMercadoria_OFF + QtMercadoria_ON`)
- Presença de Venda (`TeveVenda_OFF + TeveVenda_ON`)

### Estoque
- DDE (Dias de Demanda Estoque)
- Cobertura Atual (do GEF)
- Estoque de Segurança (do GEF)
- Lead Time Médio (do GEF)

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

## 📅 Frequência de Atualização

- **Vendas**: Diária (últimos 90 dias)
- **Estoque**: Diária (dados atuais)
- **GEF**: Diária (sincronização)

## 🛠️ Ferramentas de Desenvolvimento

### Notebooks Databricks
- Formato padrão com células `# COMMAND ----------`
- Documentação em Markdown (`# MAGIC %md`)
- Logs com emojis para melhor visualização
- Validações automáticas

### Versionamento
- Git com branches feature
- Commits semânticos
- Documentação atualizada

---

**Última Atualização**: 2024  
**Versão**: 1.0  
**Responsável**: Torre de Controle Supply Chain
