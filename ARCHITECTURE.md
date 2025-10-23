# Arquitetura da Torre de Controle - Supply Chain

## Visão Geral da Arquitetura

A arquitetura da Torre de Controle segue o padrão de **Medallion Architecture** (Bronze, Silver, Gold) integrada com uma aplicação Streamlit para visualização e análise de dados de supply chain.

## Diagrama de Arquitetura

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   INGESTÃO      │    │  PROCESSAMENTO  │    │   INTERFACE     │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │Inputs       │ │    │ │Cluster      │ │    │ │Streamlit    │ │
│ │Manuais      │ │───▶│ │Databricks   │ │───▶│ │Dashboard    │ │
│ │(Excel)      │ │    │ │R$ 2.0k/mês  │ │    │ │             │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │Datalake     │ │───▶│ │SQL Warehouse│ │    │ │Plotly       │ │
│ │(Qualidade,  │ │    │ │R$ 1.5k/mês  │ │    │ │Visualizações│ │
│ │Vendas ON/   │ │    │ └─────────────┘ │    │ └─────────────┘ │
│ │OFF, Plano   │ │    │                 │    │                 │
│ │Abast.)      │ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ └─────────────┘ │    │ │Master Tables│ │    │ │Databricks   │ │
│                 │    │ │(Gold Layer) │ │    │ │Apps         │ │
│                 │    │ └─────────────┘ │    │ │R$ 8.9k/mês  │ │
└─────────────────┘    └─────────────────┘    │ └─────────────┘ │
                                               └─────────────────┘
```

## Camadas de Dados

### 1. Camada Bronze (Raw Data)
- **Propósito**: Armazenamento de dados brutos sem transformações
- **Fontes**:
  - Datalake com dados de qualidade, vendas online/offline, plano de abastecimento
  - Inputs manuais via Excel (Agenda, S&OP)
- **Características**:
  - Dados preservados exatamente como recebidos
  - Schema evolution suportado
  - Histórico completo mantido

### 2. Camada Silver (Cleaned Data)
- **Propósito**: Dados limpos e validados
- **Transformações**:
  - Limpeza de dados duplicados
  - Validação de tipos e formatos
  - Padronização de nomenclaturas
  - Enriquecimento com dados de referência
- **Validações**:
  - Portal de validação para garantir confiabilidade
  - Checks de qualidade de dados
  - Alertas para dados inconsistentes

### 3. Camada Gold (Business Ready)
- **Propósito**: Dados agregados e master tables prontos para consumo
- **Conteúdo**:
  - Master tables de produtos, lojas, CDs
  - Agregações por categoria, região, período
  - Métricas calculadas (Aging, DDE, Ruptura, Cobertura)
  - Views otimizadas para dashboards

## Componentes da Arquitetura

### 1. Ingestão de Dados
- **Azure Blob Storage**: Armazenamento temporário de dados Excel
- **Datalake**: Fonte principal de dados estruturados
- **Portal de Validação**: Interface para validação manual de dados críticos

### 2. Processamento
- **Databricks Cluster**: 
  - Processamento distribuído
  - Custo: R$ 2.0k/mês (operação) + R$ 6.2k/mês (desenvolvimento)
- **SQL Warehouse**: 
  - Consultas analíticas otimizadas
  - Custo: R$ 1.5k/mês (operação)

### 3. Interface
- **Streamlit Application**:
  - Dashboards interativos
  - Filtros dinâmicos
  - Visualizações em tempo real
- **Plotly Integration**:
  - Gráficos interativos
  - Mapas geográficos
  - KPIs visuais
- **Databricks Apps**:
  - Deploy nativo no Databricks
  - Custo: R$ 8.9k/mês (operação)

## Fluxo de Dados

### 1. Ingestão Diária
```
Datalake → Bronze Layer → Validação → Silver Layer → Gold Layer
```

### 2. Processamento em Tempo Real
```
Gold Layer → Views → Streamlit Cache → Dashboard Updates
```

### 3. Projeções e Forecasting
```
Historical Data → ML Models → Forecast Tables → Dashboard Projections
```

## Métricas e KPIs

### KPIs de Companhia
- **Estoque Total (R$)**: Valor total do estoque
- **Cobertura**: Dias de cobertura de estoque
- **Aging (R$)**: Valor de estoque envelhecido
- **Ruptura (% R$)**: Percentual de ruptura por valor

### Métricas de Localização
- **Em Trânsito**: Estoque em movimento
- **Em Lojas**: Estoque nas lojas
- **Em CDs**: Estoque nos centros de distribuição
- **Agendado**: Estoque com entrega agendada

### Métricas por Tipo
- **Mostruário**: Estoque para exposição
- **Saldo**: Estoque disponível para venda
- **Estoque de Segurança**: Buffer de segurança
- **Estoque Ciclo**: Estoque para reposição

## Segurança e Governança

### Controle de Acesso
- Autenticação via Databricks
- Controle de acesso baseado em roles
- Auditoria de acesso aos dados

### Qualidade de Dados
- Validações automáticas na camada Silver
- Portal de validação manual
- Alertas de qualidade em tempo real

### Monitoramento
- Logs de processamento
- Métricas de performance
- Alertas de falhas

## Custos Estimados

| Componente | Operação | Desenvolvimento |
|------------|----------|-----------------|
| Cluster Databricks | R$ 2.0k/mês | R$ 6.2k/mês |
| SQL Warehouse | R$ 1.5k/mês | - |
| Databricks Apps | R$ 8.9k/mês | - |
| **Total** | **R$ 12.4k/mês** | **R$ 6.2k/mês** |

## Escalabilidade

### Horizontal
- Cluster Databricks auto-scaling
- SQL Warehouse elastic
- Streamlit workers distribuídos

### Vertical
- Aumento de recursos conforme demanda
- Otimização de queries
- Cache inteligente

## Considerações de Performance

### Otimizações Implementadas
- Particionamento por data nas tabelas Bronze/Silver
- Índices otimizados na camada Gold
- Cache de resultados frequentes
- Compressão de dados históricos

### Monitoramento de Performance
- Métricas de tempo de resposta
- Uso de recursos do cluster
- Performance de queries SQL
- Latência dos dashboards
