# Configurações Centralizadas - Torre de Controle Supply Chain

## Visão Geral

Este arquivo centraliza as configurações de ambiente para os notebooks de processamento Bronze, facilitando a gestão de parâmetros entre diferentes ambientes.

## 🎛️ Widgets do Databricks

### Configuração Interativa
Os notebooks utilizam widgets do Databricks para configuração interativa:

```python
# Widgets disponíveis
dbutils.widgets.dropdown("modo_execucao", "TEST", ["TEST", "RUN"], "Modo de Execução")
dbutils.widgets.dropdown("ambiente_tabela", "DEV", ["DEV", "PROD"], "Ambiente da Tabela")
dbutils.widgets.text("sample_size", "100000", "Tamanho do Sample (apenas para TEST)")

# Obter valores
MODO_EXECUCAO = dbutils.widgets.get("modo_execucao")
AMBIENTE_TABELA = dbutils.widgets.get("ambiente_tabela")
SAMPLE_SIZE = int(dbutils.widgets.get("sample_size"))
```

### Vantagens dos Widgets
- **Interface Amigável**: Configuração via dropdowns e campos de texto
- **Sem Edição de Código**: Alterações diretas na interface do Databricks
- **Validação**: Valores pré-definidos evitam erros
- **Reutilização**: Configurações persistem entre execuções

## 📊 Configurações por Notebook

### vendas_bronze.py
```python
# Configurações de ambiente
MODO_EXECUCAO: str = "TEST"  # TEST ou RUN
AMBIENTE_TABELA: str = "DEV"  # DEV ou PROD

# Tabela de destino (parametrizada)
TABELA_BRONZE_VENDAS: str = f"databox.bcg_comum.supply_{AMBIENTE_TABELA.lower()}_vendas_90d_on_off"

# Samples baseados no modo
USAR_SAMPLES: bool = (MODO_EXECUCAO == "TEST")
DIAS_PROCESSAMENTO: int = 1 if MODO_EXECUCAO == "TEST" else 90
SAMPLE_SIZE: int = 100000
```

### estoque_bronze.py
```python
# Configurações de ambiente
MODO_EXECUCAO: str = "TEST"  # TEST ou RUN
AMBIENTE_TABELA: str = "DEV"  # DEV ou PROD

# Tabelas de destino (parametrizadas)
TABELA_BRONZE_ESTOQUE_LOJA: str = f"databox.bcg_comum.supply_{AMBIENTE_TABELA.lower()}_estoque_lojas"
TABELA_BRONZE_ESTOQUE_CD: str = f"databox.bcg_comum.supply_{AMBIENTE_TABELA.lower()}_estoque_cds"

# Samples baseados no modo
USAR_SAMPLES: bool = (MODO_EXECUCAO == "TEST")
DIAS_PROCESSAMENTO: int = 1 if MODO_EXECUCAO == "TEST" else 90
SAMPLE_SIZE: int = 10000
```

## 📅 Controle de Período de Dados

### **DIAS_PROCESSAMENTO** 📊
- **TEST**: Processa apenas **1 dia** de dados históricos
  - ✅ Execução ultra-rápida para desenvolvimento
  - ✅ Ideal para testes de lógica e validações
  - ✅ Reduz custos de processamento durante desenvolvimento

- **RUN**: Processa **90 dias** de dados históricos
  - ✅ Análise completa para produção
  - ✅ Dados suficientes para análises estatísticas
  - ✅ Período padrão para relatórios de negócio

### Configuração Automática
```python
# Definido automaticamente baseado no MODO_EXECUCAO
DIAS_PROCESSAMENTO: int = 1 if MODO_EXECUCAO == "TEST" else 90

# Usado nos filtros de data
.filter(F.col("DtAprovacao").between(data_inicio_int, hoje_int))
```

## 🎯 Cenários de Uso

### Desenvolvimento Local (TEST + DEV)
```python
MODO_EXECUCAO = "TEST"
AMBIENTE_TABELA = "DEV"
# Resultado: supply_dev_* com samples + 1 dia de dados
```

### Teste em Produção (TEST + PROD)
```python
MODO_EXECUCAO = "TEST"
AMBIENTE_TABELA = "PROD"
# Resultado: supply_prd_* com samples + 1 dia de dados
```

### Produção Real (RUN + PROD)
```python
MODO_EXECUCAO = "RUN"
AMBIENTE_TABELA = "PROD"
# Resultado: supply_prd_* dados completos + 90 dias de dados
```

## 📋 Tabelas Geradas

### Ambiente DEV
- `databox.bcg_comum.supply_dev_vendas_90d_on_off`
- `databox.bcg_comum.supply_dev_estoque_lojas`
- `databox.bcg_comum.supply_dev_estoque_cds`

### Ambiente PROD
- `databox.bcg_comum.supply_prd_vendas_90d_on_off`
- `databox.bcg_comum.supply_prd_estoque_lojas`
- `databox.bcg_comum.supply_prd_estoque_cds`

## 🚀 Execução

### Via Widgets (Recomendado)
1. Abrir o notebook no Databricks
2. Configurar os widgets na parte superior:
   - **Modo de Execução**: TEST ou RUN
   - **Ambiente da Tabela**: DEV ou PROD
   - **Tamanho do Sample**: Número (apenas para TEST)
3. Executar o notebook

### Para Desenvolvimento
- **Modo de Execução**: TEST
- **Ambiente da Tabela**: DEV
- **Tamanho do Sample**: 100000 (vendas) ou 10000 (estoque)

### Para Produção
- **Modo de Execução**: RUN
- **Ambiente da Tabela**: PROD
- **Tamanho do Sample**: (ignorado em RUN)

## ⚠️ Considerações Importantes

### Segurança
- **Nunca** executar `RUN + PROD` em ambiente de desenvolvimento
- **Sempre** validar configurações antes da execução
- **Verificar** se as tabelas de destino estão corretas

### Performance
- **TEST**: Rápido para desenvolvimento e testes
- **RUN**: Completo mas pode ser lento em datasets grandes
- **Samples**: Configuráveis por tipo de dados

### Manutenção
- **Centralizar** mudanças de configuração neste arquivo
- **Documentar** novos parâmetros
- **Versionar** configurações por ambiente

---

**Última Atualização**: 2024  
**Versão**: 1.0  
**Responsável**: Torre de Controle Supply Chain
