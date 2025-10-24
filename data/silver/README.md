# Camada Silver - Dados Limpos e Conformados

A camada Silver representa dados limpos, conformados e prontos para análise de negócio. Esta camada aplica transformações de qualidade, padronização e consolidação dos dados brutos da camada Bronze.

## 📊 Master Tables - Vendas + Estoque

### **Arquivo Principal**: `vendas_estoque_silver.py`

Este notebook cria duas master tables separadas, consolidando dados de vendas históricas com posição atual de estoque para LOJAS e DEPÓSITOS (CDs), criando visões unificadas para análise de demanda vs disponibilidade.

#### **Estrutura das Master Tables:**
- **Granularidade**: `CdSku` x `CdFilial` x `DtAtual` (apenas hoje)
- **Estoque**: Posição atual de cada SKU/Filial
- **Vendas**: Múltiplas janelas temporais agregadas
- **Separação**: LOJAS e CDs mantidos em tabelas distintas

#### **Master Tables Geradas:**
- **LOJAS**: `supply_{ambiente}_master_vendas_estoque_lojas`
- **CDs**: `supply_{ambiente}_master_vendas_estoque_cds`

#### **Janelas Temporais de Vendas:**
- **MTD**: Month-to-Date
- **YTD**: Year-to-Date
- **Last 7d, 30d, 90d, 4w**: Períodos móveis
- **M-1, M-2, M-3**: Meses anteriores
- **Médias Móveis**: 7d, 14d, 30d, 60d, 90d

#### **Estratégias de Otimização:**
- ✅ **Agregação Inteligente**: Reduz volume de vendas antes do join
- ✅ **Cache Estratégico**: Mantém apenas dados essenciais em memória
- ✅ **Joins Otimizados**: Usa broadcast joins para tabelas pequenas
- ✅ **Particionamento**: Aproveita particionamento por data
- ✅ **Limpeza Automática**: Libera memória automaticamente
- ✅ **Separação de LOJAS e CDs**: Mantém entidades distintas

## ⚙️ Configurações de Ambiente

### Widgets Interativos
Os notebooks utilizam widgets do Databricks para configuração interativa:

- **Modo de Execução**: `TEST` (com samples + 1 dia) ou `RUN` (completo + 90 dias)
- **Ambiente da Tabela**: `DEV` (desenvolvimento) ou `PROD` (produção)
- **Tamanho do Sample**: Configurável via widget (apenas para TEST)

### Interface de Configuração
- **Dropdowns**: Para seleção de modo e ambiente
- **Campo de Texto**: Para tamanho do sample
- **Sem Edição de Código**: Configuração direta na interface

### Parametrização de Tabelas
- **DEV**: Lê tabelas `supply_dev_*` da camada Bronze
- **PROD**: Lê tabelas `supply_prd_*` da camada Bronze
- **Salvamento**: Master tables salvas com prefixo correspondente

## 📋 Tabelas Geradas

### Ambiente DEV
- `databox.bcg_comum.supply_dev_master_vendas_estoque_lojas`
- `databox.bcg_comum.supply_dev_master_vendas_estoque_cds`

### Ambiente PROD
- `databox.bcg_comum.supply_prd_master_vendas_estoque_lojas`
- `databox.bcg_comum.supply_prd_master_vendas_estoque_cds`

## 🎯 Casos de Uso

### Análise de Demanda
- Comparar vendas históricas com estoque atual
- Identificar padrões de sazonalidade
- Analisar tendências por canal (ON/OFF)

### Previsão de Ruptura
- Identificar SKUs com alta demanda e baixo estoque
- Alertas baseados em médias móveis
- Análise de cobertura de estoque

### Otimização de Estoque
- Balancear disponibilidade vs custo
- Análise de DDE (Dias de Estoque)
- Otimização por cluster ABC

### Dashboards e Relatórios
- Dados prontos para visualização
- Múltiplas janelas temporais em uma tabela
- Performance otimizada para consultas

## 📚 Documentação Adicional

- [Configurações de Ambiente](../bronze/CONFIGURACOES_AMBIENTE.md)
- [Catálogo de Tabelas](../CATALOGO_TABELAS.md)
- [Índice de Documentação](../INDICE_DOCUMENTACAO.md)