# Silver Layer - Cleaned and Validated Data

## Visão Geral

A camada Silver contém dados limpos e validados, prontos para análise e processamento posterior.

## Estrutura de Tabelas

### Cleaned Data
- `silver.cleaned_sales`: Vendas limpas e validadas
- `silver.cleaned_inventory`: Estoque limpo e validado
- `silver.cleaned_quality`: Qualidade limpa e validada
- `silver.cleaned_planning`: Planejamento limpo e validado

### Enriched Data
- `silver.sales_with_product_info`: Vendas enriquecidas com dados de produto
- `silver.inventory_with_location`: Estoque enriquecido com localização
- `silver.planning_with_forecasts`: Planejamento enriquecido com previsões

### Validated Data
- `silver.quality_checks`: Resultados de checks de qualidade
- `silver.business_rules`: Validações de regras de negócio

## Transformações Aplicadas

### Limpeza de Dados
- Remoção de duplicatas
- Tratamento de valores nulos
- Padronização de formatos
- Correção de inconsistências

### Validações
- Integridade referencial
- Ranges de valores válidos
- Regras de negócio específicas
- Consistência temporal

### Enriquecimento
- Join com tabelas de referência
- Cálculo de campos derivados
- Classificações e categorizações
- Geocodificação de endereços

## Qualidade de Dados

### Métricas de Qualidade
- Percentual de dados válidos
- Número de alertas por tabela
- Tempo de resolução de problemas
- Satisfação dos usuários

### Alertas Automáticos
- Dados fora do range esperado
- Inconsistências detectadas
- Falhas de validação
- Problemas de performance
