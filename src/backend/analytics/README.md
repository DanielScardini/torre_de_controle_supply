# Analytics - Métricas e Análises

## Visão Geral

O módulo de Analytics contém toda a lógica para cálculo de métricas, análises e previsões que alimentam os dashboards da Torre de Controle.

## Estrutura do Módulo

### 1. Inventory Metrics (`inventory_metrics.py`)
Cálculo de métricas relacionadas ao estoque:
- Saúde do estoque por localização
- Análise de aging
- Métricas de cobertura
- Análise de ruptura

### 2. Allocation Metrics (`allocation_metrics.py`)
Métricas de alocação e distribuição:
- Eficiência de alocação
- Gargalos de distribuição
- Otimização de rotas
- Métricas de performance por região

### 3. Forecasting (`forecasting.py`)
Previsões e projeções:
- Previsão de demanda
- Projeção de estoque
- Análise de sazonalidade
- Alertas preditivos

## Métricas Implementadas

### KPIs de Companhia
- **Estoque Total (R$)**: Valor total do estoque
- **Cobertura**: Dias de cobertura médios
- **Aging (R$)**: Valor de estoque envelhecido
- **Ruptura (% R$)**: Percentual de ruptura

### Métricas de Localização
- **Em Trânsito**: Estoque em movimento
- **Em Lojas**: Estoque nas lojas
- **Em CDs**: Estoque nos centros de distribuição
- **Agendado**: Estoque com entrega agendada

### Métricas por Tipo
- **Mostruário**: Estoque para exposição
- **Saldo**: Estoque disponível
- **Estoque de Segurança**: Buffer de segurança
- **Estoque Ciclo**: Estoque de reposição

## Análises Disponíveis

### Análise de Saúde do Estoque
- Cálculo de score de saúde por localização
- Identificação de pontos críticos
- Recomendações de ação
- Alertas automáticos

### Análise de Aging
- Cálculo de aging por categoria
- Identificação de produtos obsoletos
- Estratégias de liquidação
- Impacto financeiro

### Análise de Ruptura
- Detecção de rupturas atuais
- Previsão de rupturas futuras
- Impacto nas vendas
- Ações preventivas

### Análise de Cobertura
- Cálculo de dias de cobertura
- Identificação de sub/sobre-estoque
- Otimização de níveis
- Planejamento de compras

## Previsões e Projeções

### Previsão de Demanda
- Modelos estatísticos
- Análise de sazonalidade
- Fatores externos
- Colaboração com S&OP

### Projeção de Estoque
- Simulação de cenários
- Impacto de mudanças
- Otimização de níveis
- Planejamento estratégico

### Alertas Preditivos
- Detecção de anomalias
- Alertas de risco
- Recomendações automáticas
- Escalação de problemas

## Integração com Dashboards

### APIs de Dados
- Funções para cálculo de métricas
- Cache de resultados
- Atualização em tempo real
- Histórico de métricas

### Visualizações
- Preparação de dados para gráficos
- Agregações por período
- Filtros dinâmicos
- Drill-down capabilities
