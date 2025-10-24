# Camadas de Dados - Torre de Controle

## Visão Geral

Este documento descreve a estrutura e organização das camadas de dados na Torre de Controle, seguindo o padrão **Medallion Architecture** (Bronze, Silver, Gold).

## 📊 Catálogo de Tabelas

Para uma visão detalhada das tabelas implementadas, consulte o [Catálogo de Tabelas](CATALOGO_TABELAS.md).

### Tabelas Implementadas (Bronze)
- **Vendas**: `databox.bcg_comum.supply_bronze_vendas_90d_on_off`
- **Estoque Lojas**: `databox.bcg_comum.supply_bronze_estoque_lojas`
- **Estoque CDs**: `databox.bcg_comum.supply_bronze_estoque_cds`

## Estrutura das Camadas

### 1. Camada Bronze (`data/bronze/`)

**Propósito**: Armazenamento de dados brutos sem transformações

**Características**:
- Dados preservados exatamente como recebidos
- Schema evolution suportado
- Histórico completo mantido
- Particionamento por data de ingestão

**Estrutura**:
```
data/bronze/
├── raw_sales/                    # Dados brutos de vendas
│   ├── online_sales/            # Vendas online
│   └── offline_sales/           # Vendas offline
├── raw_inventory/               # Dados brutos de estoque
│   ├── stock_positions/         # Posições de estoque
│   ├── movements/               # Movimentações de estoque
│   └── allocations/             # Alocações
├── raw_quality/                 # Dados de qualidade
├── raw_planning/                # Dados de planejamento
│   ├── s_and_op/               # S&OP
│   └── supply_plan/            # Plano de abastecimento
└── raw_reference/               # Dados de referência
    ├── products/               # Produtos
    ├── stores/                 # Lojas
    └── distribution_centers/   # Centros de distribuição
```

**Formato**: Delta Lake (Parquet otimizado)
**Particionamento**: `year=YYYY/month=MM/day=DD`

### 2. Camada Silver (`data/silver/`)

**Propósito**: Dados limpos e validados

**Transformações Aplicadas**:
- Limpeza de dados duplicados
- Validação de tipos e formatos
- Padronização de nomenclaturas
- Enriquecimento com dados de referência
- Validação de regras de negócio

**Estrutura**:
```
data/silver/
├── cleaned_sales/              # Vendas limpas e validadas
├── cleaned_inventory/          # Estoque limpo e validado
├── cleaned_quality/           # Qualidade limpa e validada
├── cleaned_planning/          # Planejamento limpo e validado
├── enriched_data/             # Dados enriquecidos
│   ├── sales_with_product_info/
│   ├── inventory_with_location/
│   └── planning_with_forecasts/
└── validated_data/            # Dados após validação
    ├── quality_checks/
    └── business_rules/
```

**Validações Implementadas**:
- Checks de integridade referencial
- Validação de ranges de valores
- Detecção de outliers
- Validação de regras de negócio

### 3. Camada Gold (`data/gold/`)

**Propósito**: Dados agregados e master tables prontos para consumo

**Conteúdo**:
- Master tables de produtos, lojas, CDs
- Agregações por categoria, região, período
- Métricas calculadas
- Views otimizadas para dashboards

**Estrutura**:
```
data/gold/
├── master_tables/             # Tabelas mestras
│   ├── dim_products/          # Dimensão produtos
│   ├── dim_stores/            # Dimensão lojas
│   ├── dim_distribution_centers/ # Dimensão CDs
│   ├── dim_categories/        # Dimensão categorias
│   ├── dim_regions/           # Dimensão regiões
│   └── dim_time/              # Dimensão tempo
├── fact_tables/               # Tabelas de fatos
│   ├── fact_sales/            # Fato vendas
│   ├── fact_inventory/        # Fato estoque
│   ├── fact_movements/        # Fato movimentações
│   └── fact_allocations/      # Fato alocações
├── aggregated_metrics/        # Métricas agregadas
│   ├── inventory_health/      # Saúde do estoque
│   ├── aging_analysis/        # Análise de aging
│   ├── coverage_metrics/      # Métricas de cobertura
│   └── rupture_analysis/      # Análise de ruptura
└── forecasting/               # Dados de previsão
    ├── demand_forecasts/      # Previsões de demanda
    ├── supply_forecasts/      # Previsões de suprimento
    └── allocation_projection/  # Projeções de alocação
```

### 4. Views (`data/views/`)

**Propósito**: Views SQL otimizadas para dashboards

**Estrutura**:
```
data/views/
├── dashboard_views/           # Views para dashboards
│   ├── allocation_dashboard/   # Views para dashboard de alocação
│   ├── stores_dashboard/       # Views para dashboard de lojas
│   ├── cds_dashboard/          # Views para dashboard de CDs
│   └── products_dashboard/     # Views para dashboard de produtos
├── kpi_views/                  # Views para KPIs
│   ├── company_kpis/          # KPIs de companhia
│   ├── regional_kpis/         # KPIs regionais
│   └── operational_kpis/       # KPIs operacionais
└── reporting_views/            # Views para relatórios
    ├── executive_reports/      # Relatórios executivos
    ├── operational_reports/    # Relatórios operacionais
    └── analytical_reports/     # Relatórios analíticos
```

## Fluxo de Processamento

### 1. Ingestão Bronze
```python
# Exemplo de ingestão para camada Bronze
def ingest_bronze_data(source_path, target_table):
    """
    Ingesta dados brutos para camada Bronze
    """
    df = spark.read.format("parquet").load(source_path)
    
    # Adiciona metadados de ingestão
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("source_system", lit("datalake"))
    
    # Salva na camada Bronze
    df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .saveAsTable(f"bronze.{target_table}")
```

### 2. Processamento Silver
```python
# Exemplo de processamento para camada Silver
def process_silver_data(bronze_table, silver_table):
    """
    Processa dados da camada Bronze para Silver
    """
    df = spark.table(f"bronze.{bronze_table}")
    
    # Aplicar transformações
    df = clean_data(df)
    df = validate_data(df)
    df = enrich_data(df)
    
    # Salva na camada Silver
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"silver.{silver_table}")
```

### 3. Agregação Gold
```python
# Exemplo de agregação para camada Gold
def create_gold_metrics(silver_tables):
    """
    Cria métricas agregadas na camada Gold
    """
    # Agregações por categoria
    category_metrics = calculate_category_metrics(silver_tables)
    
    # Agregações por região
    regional_metrics = calculate_regional_metrics(silver_tables)
    
    # Métricas de companhia
    company_metrics = calculate_company_metrics(silver_tables)
    
    # Salva métricas
    save_metrics(category_metrics, regional_metrics, company_metrics)
```

## Métricas Calculadas

### KPIs de Companhia
- **Estoque Total (R$)**: Soma do valor de estoque por localização
- **Cobertura**: Média ponderada de dias de cobertura
- **Aging (R$)**: Valor de estoque com mais de X dias
- **Ruptura (% R$)**: Percentual de ruptura por valor

### Métricas de Localização
- **Em Trânsito**: Estoque em movimento entre CDs e lojas
- **Em Lojas**: Estoque disponível nas lojas
- **Em CDs**: Estoque nos centros de distribuição
- **Agendado**: Estoque com entrega agendada

### Métricas por Tipo
- **Mostruário**: Estoque para exposição em lojas
- **Saldo**: Estoque disponível para venda
- **Estoque de Segurança**: Buffer de segurança calculado
- **Estoque Ciclo**: Estoque para reposição automática

## Qualidade de Dados

### Validações Automáticas
- Integridade referencial
- Ranges de valores válidos
- Detecção de outliers estatísticos
- Consistência temporal

### Portal de Validação
- Interface para validação manual
- Alertas de qualidade
- Aprovação de dados críticos
- Histórico de validações

## Monitoramento

### Métricas de Processamento
- Tempo de processamento por camada
- Volume de dados processados
- Taxa de sucesso das validações
- Alertas de falhas

### Métricas de Qualidade
- Percentual de dados válidos
- Número de alertas de qualidade
- Tempo de resolução de problemas
- Satisfação dos usuários
