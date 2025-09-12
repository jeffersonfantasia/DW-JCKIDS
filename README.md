# DW-JCKIDS — Data Warehouse para BI da JC Kids

Bem-vindo ao repositório de Data Warehouse (DW) criado para suportar o BI da JC Kids. Aqui você encontra toda a lógica, estrutura, procedimentos, funções e documentação necessárias para implementar, manter e expandir o DW em Oracle 19c, cobrindo toda a cadeia contábil, fiscal, comercial e operacional da empresa.

***

## Sumário

- [Visão Geral do Projeto](#visão-geral-do-projeto)
- [Arquitetura e Modelagem](#arquitetura-e-modelagem)
- [Organização dos Diretórios](#organização-dos-diretórios)
- [Fluxo Completo de Construção](#fluxo-completo-de-construção)
- [Estrutura das Tabelas Principais](#estrutura-das-tabelas-principais)
- [Packages e Procedures](#packages-e-procedures)
- [Fluxos de ETL](#fluxos-de-etl)
- [Padrões e Regras de Negócio](#padrões-e-regras-de-negócio)
- [Como Executar/Sincronizar](#como-executarsincronizar)
- [Dicionário de Dados](#dicionário-de-dados)
- [Auditoria, Logging e Controle de Erros](#auditoria-logging-e-controle-de-erros)
- [Boas Práticas e Observações](#boas-práticas-e-observações)
- [Colaboradores e Contato](#colaboradores-e-contato)

***

## Visão Geral do Projeto

Este Data Warehouse foi modelado para centralizar, padronizar e disponibilizar dados críticos dos principais processos operacionais e contábeis do negócio, permitindo análises, auditorias e integração de informações gerenciais, fiscais e financeiras com alta performance.

Arquitetura pensada para rastreabilidade, flexibilidade e reuso por múltiplas áreas do BI.

***

## Arquitetura e Modelagem

- **Modelagem em estrela** (Star Schema): tabelas fato core (movimentação contábil, produtos, clientes, etc.) ao centro, ligadas a múltiplas dimensões (calendário, região, categoria, filial, etc.).
- **Fluxo modular de tratamento:** cada área/tema da empresa é tratada via procedures e packages independentes, seguindo o conceito de Data Mart setorial.
- **PL/SQL Orquestrado por Packages:** toda lógica de negócio, integração e tratamento está concentrada em packages robustas, principalmente para contabilidade (`PKG_BI_CONTABILIDADE`).
- **Pipeline Functions**: funções que retornam tabelas para uso em fluxos de transformação/cálculo.

***

## Organização dos Diretórios

- Cada diretório na raiz corresponde a uma área/processo.
  - Exemplo: `Contabilidade`, `Cliente`, `Estoque`, `Mov_Produto`, `Despesas`, `Apuracao_Fiscal`, etc.
- Dentro de cada área:
  - Procedures de integração/sincronização
  - Views para análise e consumo via BI
  - Funções utilitárias e tipos (`Types_Functions`)
  - Scripts de criação de tabelas e índices

***

## Fluxo Completo de Construção

1. **Extração dos dados brutos** dos sistemas fontes (ERP, legado, arquivos, etc.)
2. **Tratamentos e normalização via Packages**: Executando as procedures (`PRC_SINC_*`) que criam/populam tabelas intermediárias
3. **Aplicação das regras de negócio** via functions/ETL para:
   - Conciliar lançamentos contábeis, fiscais e operacionais
   - Sanear, ajustar e classificar movimentos
4. **Construção das tabelas e fatos finais** para o BI (Consulte diretório `TabelasDW`)
5. **Geração de views analíticas e agregadas** para rápida consulta e dashboards

***

## Estrutura das Tabelas Principais

- Todas as tabelas físico-lógicas estão detalhadas em `TabelasDW/01_PRC_SINC_TABELAS_DW.sql`
- Padrão: 
  - Colunas de negócios (ex: CODFILIAL, CODPROD, DATA, VALOR, etc.)
  - Colunas técnicas (DT_UPDATE)
  - Constraints e índices para performance e integridade

### Exemplos de Fatos e Dimensões

- `BI_SINC_CONTABILIDADE`: Lançamentos contábeis detalhados, com chaves para empresa, filial, centro de custo, plano de contas, valores, históricos.
- `BI_SINC_MOV_PRODUTO`, `BI_SINC_CLIENTE`, etc.: consolidação de produtos, clientes, fornecedores, eventos fiscais, apurações mensais.

***

## Packages e Procedures

- **Procedures padrão `PRC_SINC_*`**: automatizam criação, atualização e manutenção de tabelas, garantindo idempotência e integridade dos dados.
- **Package principal**: `PKG_BI_CONTABILIDADE` — centraliza toda lógica de movimentação, rateio, ajustes, auditorias e validações contábeis.
- **Uso de funções type-table** (em `Types_Functions/`), permitindo retornos dinâmicos de tabelas para tratar etapas intermediárias e facilitar manutenção.

***

## Fluxos de ETL

- Executar as procedures na ordem definida por dependência nos fluxos (veja diagramas na documentação visual/Miro).
- Cada fluxo é auto-suficiente, com ETL modular, permitindo paralelismo e facilidade na recuperação de falhas.

***

## Padrões e Regras de Negócio

- Todos os tratamentos e regras de conciliação estão documentados nos scripts das funções e procedures.
- Seguir sempre as transações atômicas para integração de dados: rollback automático em caso de erro.
- Tabelas de parâmetros (`BI_SINC_PARAMETROS_GLOBAL`) controlam as regras variáveis do negócio.

***

## Como Executar/Sincronizar

1. Execute a procedure de sincronização principal do módulo necessário (ex: `EXEC PRC_SINC_TABELAS_DW`)
2. Siga para as procedures de cada área/tema de acordo com o necessário.
3. Sugerido uso de jobs agendados Oracle para garantir atualização periódica e monitoramento via logs.

***

## Dicionário de Dados

- Para cada tabela, descreva:
  - Nome
  - Colunas (nome, tipo, descrição)
  - Relacionamentos principais
- Sugestão: gerar documentação automática a partir dos scripts, utilizando ferramentas como Oracle Data Modeler.

***

## Auditoria, Logging e Controle de Erros

- Todas as procedures utilizam tratamento de exceções padrão Oracle, com rollback e registro de erro conforme necessário.
- Recomendado centralizar logs técnicos e funcionais para diagnóstico.

***

## Boas Práticas e Observações

- Realize manutenção periódica dos índices para manter performance
- Evite hardcode de regras de negócio — use tabelas de parâmetros
- Versionar scripts de alterações de schema no repositório para rastreabilidade total

***

## Colaboradores e Contato

- Jefferson Fantasia — jeffersonfantasia@gmail.com (atual responsável pelo projeto)
- Para colaboração, submeta Pull Requests ou issues detalhadas

***
