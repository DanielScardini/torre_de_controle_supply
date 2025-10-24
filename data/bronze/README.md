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

## 📋 Metadados Padrão

Todas as tabelas incluem:
- `DataHoraProcessamento`: Timestamp GMT-3 São Paulo
- `DataProcessamento`: Data de processamento
- `FonteDados`: Origem dos dados
- `VersaoProcessamento`: Versão do processamento

## 🚀 Execução

Os notebooks podem ser executados no Databricks com as seguintes configurações:
- **Desenvolvimento**: `USAR_SAMPLES=True`
- **Produção**: `USAR_SAMPLES=False`

## 📚 Documentação Adicional

- [Catálogo Completo de Tabelas](../CATALOGO_TABELAS.md)
- [Arquitetura de Dados](../README.md)
- [Guia de Desenvolvimento](../../DEVELOPMENT.md)