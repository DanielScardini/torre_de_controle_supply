# Utils - Utilitários e Funções Auxiliares

## Visão Geral

O módulo Utils contém funções auxiliares, utilitários e ferramentas compartilhadas entre os diferentes módulos da Torre de Controle.

## Estrutura do Módulo

### 1. Database (`database.py`)
Conexões e operações com banco de dados:
- Conexão com Databricks
- Execução de queries SQL
- Gerenciamento de conexões
- Pool de conexões

### 2. Validators (`validators.py`)
Validações de dados e regras de negócio:
- Validação de schemas
- Validação de regras de negócio
- Validação de qualidade de dados
- Alertas de validação

### 3. Helpers (`helpers.py`)
Funções auxiliares gerais:
- Formatação de dados
- Conversões de tipos
- Funções de data/hora
- Utilitários matemáticos

## Funcionalidades Implementadas

### Database Utils
- **Conexão Segura**: Conexões com autenticação
- **Query Builder**: Construtor de queries dinâmicas
- **Result Cache**: Cache de resultados de queries
- **Error Handling**: Tratamento de erros de conexão

### Validation Utils
- **Schema Validation**: Validação de schemas de dados
- **Business Rules**: Validação de regras de negócio
- **Data Quality**: Checks de qualidade de dados
- **Alert System**: Sistema de alertas

### Helper Functions
- **Data Formatting**: Formatação de números, datas, moedas
- **Type Conversion**: Conversões seguras de tipos
- **Date Operations**: Operações com datas e períodos
- **Math Utils**: Funções matemáticas auxiliares

## Padrões de Uso

### Database Operations
```python
# Exemplo de uso das funções de database
from src.backend.utils.database import execute_query, get_connection

# Executar query
result = execute_query("SELECT * FROM gold.inventory_health")

# Obter conexão
conn = get_connection()
```

### Data Validation
```python
# Exemplo de uso das funções de validação
from src.backend.utils.validators import validate_schema, validate_business_rules

# Validar schema
is_valid = validate_schema(df, expected_schema)

# Validar regras de negócio
validation_results = validate_business_rules(df)
```

### Helper Functions
```python
# Exemplo de uso das funções auxiliares
from src.backend.utils.helpers import format_currency, parse_date

# Formatar moeda
formatted_value = format_currency(1234.56)

# Parsear data
parsed_date = parse_date("2024-01-15")
```

## Configurações

### Database Configuration
- Configurações de conexão
- Timeouts e retries
- Pool de conexões
- Logging de queries

### Validation Configuration
- Regras de validação
- Thresholds de qualidade
- Configurações de alertas
- Templates de mensagens

### Helper Configuration
- Locales e formatos
- Configurações de timezone
- Precisão numérica
- Configurações de cache

## Monitoramento

### Database Metrics
- Tempo de execução de queries
- Número de conexões ativas
- Taxa de erro de conexão
- Uso de cache

### Validation Metrics
- Número de validações executadas
- Taxa de sucesso das validações
- Tempo de execução das validações
- Alertas gerados

### Helper Metrics
- Uso de funções auxiliares
- Performance de formatação
- Cache hit rate
- Erros de conversão
