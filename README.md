# Torre de Controle - Supply Chain

## Visão Geral

A Torre de Controle de Supply Chain é uma ferramenta de visibilidade de estoque que destaca os maiores riscos e impactos na cadeia de suprimentos. Esta aplicação permite monitoramento em tempo real, projeções futuras e tomada de decisões baseadas em dados.

## Objetivos

- **Transparência Ponta-a-ponta**: Visão completa da cadeia com clareza e priorização de informações por impacto
- **Agilidade de Reação**: Atualização dinâmica para monitoramento das posições de estoque
- **Clareza dos Passos Importantes**: Alertas priorizados e recomendações de ações preventivas
- **Respostas Mais Precisas**: Recomendações adequadas no momento oportuno para solução de problemas

## Funcionalidades Principais

1. **Integração de Dados**: Combinação entre múltiplas bases, habilitando fonte única para decisão
2. **Processamento Automático**: Ganho em agilidade com atualização diária via datalake
3. **Visualização em Dashboard**: Monitoramento da performance e riscos atuais na cadeia
4. **Gestão à Vista**: Governança sobre alertas críticos para fóruns periódicos
5. **Suporte à Decisão**: Geração de alertas e recomendações com inteligência de negócios

## Tecnologias

- **Databricks**: Plataforma principal para processamento e execução
- **Streamlit**: Framework para criação de dashboards interativos
- **Plotly**: Biblioteca para visualizações avançadas
- **Python**: Linguagem principal de desenvolvimento
- **SQL**: Para processamento de dados e criação de views

## Estrutura do Projeto

```
torre_de_controle_supply/
├── README.md                           # Este arquivo
├── ARCHITECTURE.md                     # Documentação da arquitetura
├── DEVELOPMENT.md                      # Guia de desenvolvimento
├── requirements.txt                    # Dependências Python
├── databricks-requirements.txt         # Dependências específicas do Databricks
├── .gitignore                          # Arquivos ignorados pelo Git
├── config/                             # Configurações
│   ├── __init__.py
│   ├── settings.py                     # Configurações principais
│   └── databricks_config.py           # Configurações do Databricks
├── data/                               # Camadas de dados
│   ├── bronze/                         # Dados brutos
│   ├── silver/                         # Dados limpos e validados
│   ├── gold/                           # Dados agregados e master tables
│   └── views/                          # Views SQL para dashboards
├── src/                                # Código fonte
│   ├── __init__.py
│   ├── backend/                        # Lógica de negócio e processamento
│   │   ├── __init__.py
│   │   ├── data_processing/            # Scripts de processamento
│   │   │   ├── __init__.py
│   │   │   ├── bronze_layer.py         # Processamento bronze
│   │   │   ├── silver_layer.py         # Processamento silver
│   │   │   └── gold_layer.py           # Processamento gold
│   │   ├── analytics/                  # Análises e métricas
│   │   │   ├── __init__.py
│   │   │   ├── inventory_metrics.py    # Métricas de estoque
│   │   │   ├── allocation_metrics.py   # Métricas de alocação
│   │   │   └── forecasting.py          # Previsões e projeções
│   │   └── utils/                      # Utilitários
│   │       ├── __init__.py
│   │       ├── database.py             # Conexões com banco
│   │       ├── validators.py           # Validações de dados
│   │       └── helpers.py              # Funções auxiliares
│   └── frontend/                       # Interface Streamlit
│       ├── __init__.py
│       ├── pages/                      # Páginas do dashboard
│       │   ├── __init__.py
│       │   ├── allocation.py           # Dashboard de alocação
│       │   ├── stores.py               # Dashboard de lojas
│       │   ├── distribution_centers.py # Dashboard de CDs
│       │   └── products.py              # Dashboard de produtos
│       ├── components/                 # Componentes reutilizáveis
│       │   ├── __init__.py
│       │   ├── charts.py               # Componentes de gráficos
│       │   ├── filters.py              # Componentes de filtros
│       │   └── kpi_cards.py            # Cards de KPIs
│       └── main.py                     # Aplicação principal
├── notebooks/                          # Jupyter notebooks para desenvolvimento
│   ├── data_exploration/               # Exploração de dados
│   └── prototyping/                   # Protótipos de visualizações
├── tests/                              # Testes automatizados
│   ├── __init__.py
│   ├── test_backend/                   # Testes do backend
│   └── test_frontend/                  # Testes do frontend
└── docs/                               # Documentação adicional
    ├── api/                            # Documentação de APIs
    └── user_guide/                     # Guia do usuário
```

## Como Executar

### Pré-requisitos

- Python 3.8+
- Databricks workspace configurado
- Acesso aos dados de entrada (datalake, Excel, etc.)

### Instalação

1. Clone o repositório
2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure as variáveis de ambiente no arquivo `config/settings.py`

### Execução

Para executar a aplicação Streamlit:

```bash
streamlit run src/frontend/main.py
```

## Contribuição

Consulte o arquivo `DEVELOPMENT.md` para informações sobre como contribuir com o projeto.

## Licença

Este projeto é propriedade do GRUPO CASASBAHIA.