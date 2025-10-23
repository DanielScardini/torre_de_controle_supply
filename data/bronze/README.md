# Bronze Layer - Raw Data Processing

## Visão Geral

A camada Bronze armazena dados brutos sem transformações, preservando a estrutura original dos dados de origem.

## Estrutura de Tabelas

### Sales Data
- `bronze.raw_online_sales`: Vendas online brutas
- `bronze.raw_offline_sales`: Vendas offline brutas

### Inventory Data
- `bronze.raw_stock_positions`: Posições de estoque brutas
- `bronze.raw_movements`: Movimentações de estoque brutas
- `bronze.raw_allocations`: Alocações brutas

### Quality Data
- `bronze.raw_quality`: Dados de qualidade brutos

### Planning Data
- `bronze.raw_s_and_op`: Dados S&OP brutos
- `bronze.raw_supply_plan`: Plano de abastecimento bruto

### Reference Data
- `bronze.raw_products`: Produtos brutos
- `bronze.raw_stores`: Lojas brutas
- `bronze.raw_distribution_centers`: CDs brutos

## Schema Padrão

Todas as tabelas Bronze incluem:
- `ingestion_timestamp`: Timestamp da ingestão
- `source_system`: Sistema de origem
- `file_name`: Nome do arquivo original
- `year`, `month`, `day`: Partições temporais

## Processamento

### Ingestão Diária
- Execução automática via Databricks Jobs
- Validação básica de schema
- Particionamento automático por data

### Retenção
- Dados mantidos por 2 anos
- Compressão automática após 30 dias
- Arquivo para cold storage após 1 ano
