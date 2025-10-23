# Backend - Lógica de Negócio e Processamento

## Visão Geral

O backend da Torre de Controle contém toda a lógica de negócio, processamento de dados e análises que alimentam os dashboards Streamlit.

## Estrutura do Backend

### 1. Data Processing (`src/backend/data_processing/`)
Scripts para processamento das camadas Bronze, Silver e Gold.

### 2. Analytics (`src/backend/analytics/`)
Módulos para cálculo de métricas, análises e previsões.

### 3. Utils (`src/backend/utils/`)
Utilitários para conexões, validações e funções auxiliares.

## Módulos Principais

### Data Processing
- `bronze_layer.py`: Processamento da camada Bronze
- `silver_layer.py`: Processamento da camada Silver  
- `gold_layer.py`: Processamento da camada Gold

### Analytics
- `inventory_metrics.py`: Métricas de estoque
- `allocation_metrics.py`: Métricas de alocação
- `forecasting.py`: Previsões e projeções

### Utils
- `database.py`: Conexões com banco de dados
- `validators.py`: Validações de dados
- `helpers.py`: Funções auxiliares

## Padrões de Desenvolvimento

### Estrutura de Classes
- Classes para encapsular lógica de negócio
- Métodos estáticos para funções utilitárias
- Interfaces claras entre módulos

### Tratamento de Erros
- Logging estruturado
- Exceções customizadas
- Fallbacks para falhas de dados

### Performance
- Cache de resultados frequentes
- Processamento assíncrono quando possível
- Otimização de consultas SQL
