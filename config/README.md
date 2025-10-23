# Configurações da Torre de Controle

## Visão Geral

Este módulo contém todas as configurações necessárias para o funcionamento da Torre de Controle, incluindo configurações de banco de dados, Databricks e parâmetros da aplicação.

## Estrutura de Configurações

### 1. Settings (`settings.py`)
Configurações principais da aplicação:
- Configurações gerais
- Parâmetros de performance
- Configurações de cache
- Configurações de logging

### 2. Databricks Config (`databricks_config.py`)
Configurações específicas do Databricks:
- Configurações de cluster
- Configurações de warehouse
- Configurações de storage
- Configurações de segurança

## Configurações Implementadas

### Application Settings
- **Environment**: Desenvolvimento, Homologação, Produção
- **Debug Mode**: Modo de debug ativado/desativado
- **Cache Settings**: Configurações de cache
- **Logging Level**: Nível de logging

### Database Settings
- **Connection String**: String de conexão com Databricks
- **Timeout**: Timeout de conexão
- **Retry Attempts**: Número de tentativas de reconexão
- **Pool Size**: Tamanho do pool de conexões

### Databricks Settings
- **Cluster Config**: Configurações do cluster
- **Warehouse Config**: Configurações do warehouse
- **Storage Config**: Configurações de storage
- **Security Config**: Configurações de segurança

### Performance Settings
- **Query Timeout**: Timeout de queries
- **Cache TTL**: Tempo de vida do cache
- **Batch Size**: Tamanho dos lotes de processamento
- **Parallel Jobs**: Número de jobs paralelos

## Variáveis de Ambiente

### Obrigatórias
- `DATABRICKS_HOST`: Host do Databricks
- `DATABRICKS_TOKEN`: Token de acesso
- `DATABRICKS_CATALOG`: Catálogo de dados
- `DATABRICKS_SCHEMA`: Schema padrão

### Opcionais
- `DEBUG_MODE`: Modo de debug
- `LOG_LEVEL`: Nível de logging
- `CACHE_TTL`: TTL do cache
- `QUERY_TIMEOUT`: Timeout de queries

## Configurações por Ambiente

### Desenvolvimento
- Debug mode ativado
- Logging detalhado
- Cache desabilitado
- Timeouts reduzidos

### Homologação
- Debug mode desativado
- Logging intermediário
- Cache habilitado
- Timeouts padrão

### Produção
- Debug mode desativado
- Logging mínimo
- Cache otimizado
- Timeouts aumentados

## Segurança

### Autenticação
- Token-based authentication
- Rotação automática de tokens
- Controle de acesso baseado em roles
- Auditoria de acesso

### Criptografia
- Criptografia de dados em trânsito
- Criptografia de dados em repouso
- Chaves de criptografia rotativas
- Certificados SSL/TLS

## Monitoramento

### Métricas de Configuração
- Uso de configurações
- Tempo de carregamento
- Erros de configuração
- Validação de configurações

### Alertas
- Configurações inválidas
- Falhas de conexão
- Timeouts de queries
- Problemas de autenticação
