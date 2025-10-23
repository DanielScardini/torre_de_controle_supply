# Gold Layer - Business Ready Data

## Visão Geral

A camada Gold contém dados agregados e master tables prontos para consumo em dashboards e relatórios.

## Estrutura de Tabelas

### Master Tables (Dimensões)
- `gold.dim_products`: Dimensão produtos
- `gold.dim_stores`: Dimensão lojas
- `gold.dim_distribution_centers`: Dimensão CDs
- `gold.dim_categories`: Dimensão categorias
- `gold.dim_regions`: Dimensão regiões
- `gold.dim_time`: Dimensão tempo

### Fact Tables
- `gold.fact_sales`: Fato vendas
- `gold.fact_inventory`: Fato estoque
- `gold.fact_movements`: Fato movimentações
- `gold.fact_allocations`: Fato alocações

### Aggregated Metrics
- `gold.inventory_health`: Saúde do estoque por localização
- `gold.aging_analysis`: Análise de aging por categoria/região
- `gold.coverage_metrics`: Métricas de cobertura
- `gold.rupture_analysis`: Análise de ruptura

### Forecasting
- `gold.demand_forecasts`: Previsões de demanda
- `gold.supply_forecasts`: Previsões de suprimento
- `gold.allocation_projection`: Projeções de alocação

## KPIs Calculados

### Company Level KPIs
- **Total Inventory (R$)**: Valor total do estoque
- **Coverage**: Dias de cobertura médios
- **Aging (R$)**: Valor de estoque envelhecido
- **Rupture (% R$)**: Percentual de ruptura

### Location Based Metrics
- **In Transit**: Estoque em trânsito
- **In Stores**: Estoque nas lojas
- **In Distribution Centers**: Estoque nos CDs
- **Scheduled**: Estoque agendado

### Type Based Metrics
- **Showroom**: Estoque para mostruário
- **Balance**: Estoque disponível
- **Safety Stock**: Estoque de segurança
- **Cycle Stock**: Estoque de ciclo

## Agregações Disponíveis

### Por Categoria
- Telefonia Celular
- Eletrodomésticos
- Informática
- Móveis

### Por Região
- Norte
- Nordeste
- Centro-Oeste
- Sudeste
- Sul

### Por Período
- Diário
- Semanal
- Mensal
- Trimestral

## Otimizações

### Performance
- Índices otimizados
- Particionamento inteligente
- Cache de resultados frequentes
- Compressão de dados históricos

### Manutenção
- VACUUM automático
- OPTIMIZE regular
- Z-ORDER por colunas de filtro
- Compaction automática
