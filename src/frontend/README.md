# Frontend - Interface Streamlit

## Visão Geral

O frontend da Torre de Controle é construído com Streamlit e Plotly, fornecendo dashboards interativos para visualização e análise de dados de supply chain.

## Estrutura do Frontend

### 1. Main Application (`src/frontend/main.py`)
Aplicação principal Streamlit com navegação entre páginas.

### 2. Pages (`src/frontend/pages/`)
Páginas individuais para cada módulo do dashboard:
- `allocation.py`: Dashboard de alocação de estoque
- `stores.py`: Dashboard de lojas
- `distribution_centers.py`: Dashboard de CDs
- `products.py`: Dashboard de produtos

### 3. Components (`src/frontend/components/`)
Componentes reutilizáveis para construção dos dashboards:
- `charts.py`: Componentes de gráficos Plotly
- `filters.py`: Componentes de filtros
- `kpi_cards.py`: Cards de KPIs

## Dashboards Implementados

### 1. Dashboard de Alocação
- **Visão Geral**: Mapa geográfico do Brasil com status de alocação
- **Filtros**: Categoria, SKU, Métrica
- **Módulos**: Alocação, Lojas, CDs, Produtos
- **KPIs**: Estoque Total, Cobertura, Aging, Ruptura
- **Visualizações**: 
  - Mapa colorido por saúde do estoque
  - Tooltips com métricas detalhadas
  - Timeline de projeções (D+7)

### 2. Dashboard de Lojas
- **Foco**: Análise por loja individual
- **Métricas**: Performance por loja, cobertura, ruptura
- **Filtros**: Região, categoria, período
- **Visualizações**: Gráficos de barras, linhas, mapas

### 3. Dashboard de CDs
- **Foco**: Centros de distribuição
- **Métricas**: Capacidade, utilização, gargalos
- **Filtros**: Região, tipo de CD, período
- **Visualizações**: Gráficos de capacidade, fluxo de distribuição

### 4. Dashboard de Produtos
- **Foco**: Análise por produto/categoria
- **Métricas**: Rotatividade, aging, demanda
- **Filtros**: Categoria, SKU, período
- **Visualizações**: Gráficos de rotatividade, análise ABC

## Componentes Reutilizáveis

### Charts (`components/charts.py`)
- `create_map_chart()`: Mapa geográfico do Brasil
- `create_kpi_chart()`: Gráficos de KPIs
- `create_trend_chart()`: Gráficos de tendência
- `create_comparison_chart()`: Gráficos de comparação

### Filters (`components/filters.py`)
- `create_category_filter()`: Filtro de categoria
- `create_sku_filter()`: Filtro de SKU
- `create_metric_filter()`: Filtro de métrica
- `create_date_filter()`: Filtro de período

### KPI Cards (`components/kpi_cards.py`)
- `create_kpi_card()`: Card individual de KPI
- `create_kpi_grid()`: Grid de KPIs
- `create_comparison_card()`: Card com comparação

## Padrões de Desenvolvimento

### Estrutura de Páginas
```python
# Template padrão para páginas
def render_page():
    st.set_page_config(page_title="Título da Página")
    
    # Sidebar com filtros
    filters = create_filters()
    
    # Conteúdo principal
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Visualizações principais
        render_main_charts(filters)
    
    with col2:
        # KPIs e métricas
        render_kpi_cards(filters)
```

### Gerenciamento de Estado
- Uso de `st.session_state` para persistência
- Cache de dados com `@st.cache_data`
- Filtros compartilhados entre páginas

### Responsividade
- Layout adaptativo com `st.columns`
- Componentes responsivos
- Otimização para diferentes tamanhos de tela

## Integração com Backend

### Conexão com Dados
- Uso de views SQL da camada Gold
- Cache inteligente de consultas
- Atualização automática de dados

### Performance
- Lazy loading de componentes
- Paginação de dados grandes
- Otimização de consultas SQL
