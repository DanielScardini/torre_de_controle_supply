# Bronze Layer - Raw Data Processing

## Visão Geral

A camada Bronze é responsável pelo armazenamento de dados brutos sem transformações significativas, preservando a integridade dos dados originais e adicionando apenas metadados essenciais de processamento.

## 📊 Catálogo de Tabelas

Para uma visão completa das tabelas implementadas, consulte o [Catálogo de Tabelas](../CATALOGO_TABELAS.md).

### Tabelas Implementadas

#### 1. **Vendas Consolidadas**
- **Tabela**: `databox.bcg_comum.supply_bronze_vendas_90d_on_off`
- **Arquivo**: `vendas_bronze.py`
- **Descrição**: Consolidação de vendas online e offline com outer join

#### 2. **Estoque de Lojas**
- **Tabela**: `databox.bcg_comum.supply_bronze_estoque_lojas`
- **Arquivo**: `estoque_bronze.py`
- **Descrição**: Estoque das lojas enriquecido com dados do GEF

#### 3. **Estoque de Depósitos**
- **Tabela**: `databox.bcg_comum.supply_bronze_estoque_cds`
- **Arquivo**: `estoque_bronze.py`
- **Descrição**: Estoque dos CDs enriquecido com dados do GEF

## 🔧 Características Técnicas

- **Formato**: Delta Lake (Parquet otimizado)
- **Modo**: Overwrite (atualização completa)
- **Particionamento**: Por data (`DtAtual`)
- **Validações**: Duplicatas, multiplicação de registros, formato de datas
- **Cache**: Implementado com limpeza automática
- **Samples**: Suporte para desenvolvimento

## ⚙️ Configurações de Ambiente

### Widgets Interativos
Os notebooks utilizam widgets do Databricks para configuração interativa:

- **Modo de Execução**: `TEST` (com samples + 1 dia) ou `RUN` (completo + 90 dias)
- **Ambiente da Tabela**: `DEV` (desenvolvimento) ou `PROD` (produção)
- **Tamanho do Sample**: Configurável via widget (apenas para TEST)

### Interface de Configuração
- **Dropdowns**: Para seleção de modo e ambiente
- **Campo de Texto**: Para tamanho do sample
- **Sem Edição de Código**: Configuração direta na interface

## 📋 Metadados Padrão

Todas as tabelas incluem:
- `DataHoraProcessamento`: Timestamp GMT-3 São Paulo
- `DataProcessamento`: Data de processamento
- `FonteDados`: Origem dos dados
- `VersaoProcessamento`: Versão do processamento

## 🚀 Execução

### Via Widgets (Recomendado)
1. Abrir o notebook no Databricks
2. Configurar os widgets na parte superior do notebook
3. Executar o notebook

### Configurações Disponíveis
- **Desenvolvimento**: `TEST` + `DEV` + Sample configurável
- **Teste Produção**: `TEST` + `PROD` + Sample configurável  
- **Produção Real**: `RUN` + `PROD` (sem samples)

### Exemplo de Uso
```python
# Widgets configurados automaticamente
# Modo de Execução: TEST
# Ambiente da Tabela: DEV
# Tamanho do Sample: 100000
# Resultado: supply_dev_* com samples
```

## 📚 Documentação Adicional

- [Catálogo Completo de Tabelas](../CATALOGO_TABELAS.md)
- [Arquitetura de Dados](../README.md)
- [Guia de Desenvolvimento](../../DEVELOPMENT.md)
- [Configurações de Ambiente](CONFIGURACOES_AMBIENTE.md)