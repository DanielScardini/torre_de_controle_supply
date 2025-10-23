# Views - Dashboard Optimized Queries

## Visão Geral

As views são consultas SQL otimizadas para alimentar os dashboards da Torre de Controle, fornecendo dados agregados e métricas calculadas.

## Estrutura de Views

### Dashboard Views
- `views.allocation_dashboard`: Dados para dashboard de alocação
- `views.stores_dashboard`: Dados para dashboard de lojas
- `views.cds_dashboard`: Dados para dashboard de CDs
- `views.products_dashboard`: Dados para dashboard de produtos

### KPI Views
- `views.company_kpis`: KPIs de companhia
- `views.regional_kpis`: KPIs regionais
- `views.operational_kpis`: KPIs operacionais

### Reporting Views
- `views.executive_reports`: Relatórios executivos
- `views.operational_reports`: Relatórios operacionais
- `views.analytical_reports`: Relatórios analíticos

## Views Principais

### Allocation Dashboard View
```sql
CREATE OR REPLACE VIEW views.allocation_dashboard AS
SELECT 
    region,
    state,
    distribution_center,
    store,
    category,
    sku,
    current_stock_value,
    coverage_days,
    aging_value,
    rupture_percentage,
    health_level,
    forecast_stock_value,
    forecast_coverage_days
FROM gold.inventory_health ih
JOIN gold.dim_regions r ON ih.region_id = r.region_id
JOIN gold.dim_stores s ON ih.store_id = s.store_id
JOIN gold.dim_distribution_centers dc ON ih.dc_id = dc.dc_id
JOIN gold.dim_products p ON ih.product_id = p.product_id
WHERE ih.date = CURRENT_DATE()
```

### Company KPIs View
```sql
CREATE OR REPLACE VIEW views.company_kpis AS
SELECT 
    'Total Inventory' as metric_name,
    SUM(current_stock_value) as current_value,
    SUM(previous_stock_value) as previous_value,
    ((SUM(current_stock_value) - SUM(previous_stock_value)) / SUM(previous_stock_value)) * 100 as change_percentage
FROM gold.inventory_health
WHERE date = CURRENT_DATE()

UNION ALL

SELECT 
    'Coverage Days' as metric_name,
    AVG(coverage_days) as current_value,
    AVG(previous_coverage_days) as previous_value,
    ((AVG(coverage_days) - AVG(previous_coverage_days)) / AVG(previous_coverage_days)) * 100 as change_percentage
FROM gold.inventory_health
WHERE date = CURRENT_DATE()
```

## Otimizações

### Performance
- Views materializadas para consultas frequentes
- Índices otimizados para filtros comuns
- Cache de resultados para KPIs principais
- Particionamento por data para consultas temporais

### Manutenção
- Refresh automático das views materializadas
- Monitoramento de performance das consultas
- Alertas para views com baixa performance
- Documentação das dependências entre views
