# Guia de Desenvolvimento - Torre de Controle

## Visão Geral

Este guia fornece informações essenciais para desenvolvedores que trabalham na Torre de Controle de Supply Chain.

## Pré-requisitos

### Software Necessário
- Python 3.8+
- Git
- Databricks CLI
- IDE (VS Code, PyCharm, etc.)

### Contas e Acessos
- Acesso ao workspace Databricks
- Permissões para criar clusters e warehouses
- Acesso aos dados de origem (datalake, Excel)

## Configuração do Ambiente

### 1. Clone do Repositório
```bash
git clone <repository-url>
cd torre_de_controle_supply
```

### 2. Configuração do Ambiente Virtual
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows
```

### 3. Instalação de Dependências
```bash
pip install -r requirements.txt
```

### 4. Configuração do Databricks
```bash
databricks configure --token
```

### 5. Configuração de Variáveis de Ambiente
```bash
cp .env.example .env
# Editar .env com suas configurações
```

## Estrutura do Projeto

### Organização de Diretórios
```
torre_de_controle_supply/
├── config/                 # Configurações
├── data/                   # Camadas de dados
├── src/                    # Código fonte
│   ├── backend/           # Lógica de negócio
│   └── frontend/          # Interface Streamlit
├── notebooks/             # Jupyter notebooks
├── tests/                 # Testes automatizados
└── docs/                  # Documentação
```

### Padrões de Código

#### Python
- PEP 8 compliance
- Type hints obrigatórios
- Docstrings para todas as funções
- Logging estruturado

#### Estrutura de Arquivos
- Um arquivo por classe/funcionalidade
- Imports organizados (stdlib, third-party, local)
- Configurações centralizadas

## Desenvolvimento

### 1. Processamento de Dados

#### Camada Bronze
- Ingestão de dados brutos
- Preservação do schema original
- Metadados de ingestão

#### Camada Silver
- Limpeza e validação
- Enriquecimento com dados de referência
- Aplicação de regras de negócio

#### Camada Gold
- Agregações e métricas
- Master tables
- Views otimizadas

### 2. Desenvolvimento de Dashboards

#### Estrutura de Páginas
```python
# Template padrão para páginas Streamlit
def render_page():
    st.set_page_config(page_title="Título")
    
    # Filtros na sidebar
    filters = create_filters()
    
    # Conteúdo principal
    render_content(filters)
```

#### Componentes Reutilizáveis
- Charts com Plotly
- Filtros dinâmicos
- Cards de KPIs
- Tabelas interativas

### 3. Testes

#### Estrutura de Testes
```
tests/
├── test_backend/          # Testes do backend
├── test_frontend/         # Testes do frontend
└── test_integration/     # Testes de integração
```

#### Execução de Testes
```bash
pytest tests/
pytest tests/test_backend/
pytest --cov=src tests/
```

## Deploy

### 1. Deploy no Databricks

#### Preparação
```bash
# Validar código
black src/
flake8 src/
mypy src/

# Executar testes
pytest tests/
```

#### Deploy da Aplicação
```bash
# Deploy via Databricks CLI
databricks apps deploy
```

### 2. Configuração de Jobs

#### Jobs de Processamento
- Bronze layer: Diário às 06:00
- Silver layer: Diário às 07:00
- Gold layer: Diário às 08:00

#### Jobs de Monitoramento
- Validação de qualidade: A cada 4 horas
- Alertas de performance: Contínuo

## Monitoramento

### 1. Logs
- Logs estruturados em JSON
- Níveis: DEBUG, INFO, WARNING, ERROR
- Rotação automática de logs

### 2. Métricas
- Performance de queries
- Uso de recursos
- Qualidade de dados
- Erros e exceções

### 3. Alertas
- Falhas de processamento
- Degradação de performance
- Problemas de qualidade
- Falhas de conexão

## Troubleshooting

### Problemas Comuns

#### Conexão com Databricks
- Verificar token de acesso
- Validar configurações de rede
- Verificar permissões

#### Performance de Queries
- Analisar planos de execução
- Verificar índices e particionamento
- Otimizar agregações

#### Problemas de Dados
- Validar schemas de entrada
- Verificar qualidade dos dados
- Analisar logs de processamento

### Debugging

#### Logs de Debug
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

#### Profiling de Performance
```python
import cProfile
cProfile.run('your_function()')
```

## Contribuição

### 1. Fluxo de Trabalho
1. Criar branch a partir de `main`
2. Desenvolver feature
3. Executar testes
4. Criar pull request
5. Code review
6. Merge para `main`

### 2. Padrões de Commit
```
feat: adicionar nova funcionalidade
fix: corrigir bug
docs: atualizar documentação
test: adicionar testes
refactor: refatorar código
```

### 3. Code Review
- Revisar lógica de negócio
- Verificar performance
- Validar testes
- Confirmar documentação

## Recursos Adicionais

### Documentação
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Plotly Documentation](https://plotly.com/python/)
- [Databricks Documentation](https://docs.databricks.com/)

### Ferramentas
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- [VS Code Databricks Extension](https://marketplace.visualstudio.com/items?itemName=databricks.databricks)

### Suporte
- Canal #torre-controle no Slack
- Issues no repositório Git
- Documentação interna da empresa
