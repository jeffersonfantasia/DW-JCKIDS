# DW-JCKIDS — Data Warehouse para BI da JC Kids

**Data Warehouse desenvolvido para centralizar, padronizar e consolidar informações contábeis, fiscais, financeiras e comerciais da JC Kids, utilizando Oracle 19c e best practices em BI.**

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

O DW-JCKIDS consolida dados de múltiplos módulos do ERP (contabilidade, fiscal, vendas, estoque, financeiro, etc.) em um modelo unificado, de fácil consumo por ferramentas de BI como Power BI.

**Principais objetivos:**
- Unificação dos dados em um único repositório corporativo
- Performance superior em consultas analíticas e relatórios gerenciais
- Rastreabilidade, controle de qualidade, auditoria e histórico de dados
- Flexibilidade para evolução dos requisitos de negócio

***

## Arquitetura e Modelagem

- **Modelo em estrela** (Star Schema): tabelas fato (ex: `BI_SINC_CONTABILIDADE`) centralizam informações detalhadas; dimensões (`BI_SINC_PRODUTO`, `BI_SINC_FILIAL`, etc.) fornecem atributos descritivos.
- **Pacotes PL/SQL**: Toda lógica de integração, tratamento, consistência e agregação está encapsulada em packages (ex: `PKG_BI_CONTABILIDADE`).
- **Funções pipeline (pipelined functions)**: São utilizadas para tabelas temporárias e ETLs flexíveis.

**Exemplo visual:**  
```
[BI_SINC_CONTABILIDADE] <-- [BI_SINC_PRODUTO]
                         <-- [BI_SINC_FILIAL]
                         <-- [BI_SINC_CLIENTE]
                         <-- [BI_SINC_PLANO_CONTAS_JC]
```

***

## Organização dos Diretórios

Cada área de negócio tem seu próprio diretório, facilitando manutenção e evolução:

```
Apuracao_Fiscal/      -- Apuração de impostos
Banco/                -- Movimento bancário
Calendario/           -- Datas e períodos
Centro_Custo/         -- Centros de custo
Cliente/              -- Cadastro de clientes
Contabilidade/        -- Regras e lógicas contábeis principais
Estoque/              -- Controle de estoques
Fornecedor/           -- Cadastro de fornecedores
Mov_Produto/          -- Movimentações de produto
TabelasDW/            -- Scripts gerais para toda a estrutura DW
Types_Functions/      -- Tipos e funções auxiliares Oracle
```

*Exemplo real de script de sincronização*:  
`Contabilidade/02_PRC_SINC_CONTABILIDADE.sql`

***

## Fluxo Completo de Construção

1. **Extração de dados dos sistemas origem**  
2. **Pré-tratamento e staging em tabelas temporárias**
3. **Aplicação de regras de negócio nas procedures e functions**
4. **Carga nas tabelas finais do Data Warehouse (ex: `BI_SINC_CONTABILIDADE`)**
5. **Geração de views analíticas agregadas para BI**

> **Exemplo:**  
> Para atualizar dados de estoque:  
> 1. Executar `Estoque/01_PRC_SINC_HIST_ESTOQUE.sql`  
> 2. Validar em `BI_SINC_ESTOQUE`

***

## Estrutura das Tabelas Principais

As tabelas principais ficam em `TabelasDW/01_PRC_SINC_TABELAS_DW.sql`.

### Exemplo de tabela fato (resumido):

```sql
CREATE TABLE BI_SINC_CONTABILIDADE (
  CODLANC        VARCHAR2(40),
  CODEMPRESA     VARCHAR2(2),
  CODFILIAL      VARCHAR2(2),
  DATA           DATE,
  VALOR          NUMBER(15,2),
  HISTORICO      VARCHAR2(450),
  ORIGEM         VARCHAR2(80),
  -- outros campos omitidos
  CONSTRAINT PK_CONTABILIDADE PRIMARY KEY (CODLANC, OPERACAO)
);
```

### Exemplo de tabela dimensão:

```sql
CREATE TABLE BI_SINC_PRODUTO (
  CODPROD        NUMBER(6),
  PRODUTO        VARCHAR2(40),
  DEPARTAMENTO   VARCHAR2(25),
  MARCA          VARCHAR2(40),
  -- outros campos
  CONSTRAINT PK_CODPROD PRIMARY KEY (CODPROD)
);
```

***

## Packages e Procedures

Toda lógica de tratamento está em packages, garantindo centralização e reaproveitamento.

**Exemplo:**  
- `Contabilidade/PKG_BI_CONTABILIDADE.bdy`  
  - Functions de geração de lançamentos, rateios, conciliações.
- `Mov_Produto/01_PRC_SINC_MOV_PRODUTO.sql`  
  - Procedure para inserir movimentos de produto tratados.

***

## Fluxos de ETL

1. Execute as procedures na ordem lógica (por exemplo, sincronize primeiro clientes, depois produtos, depois vendas).
2. Verifique logs por possíveis erros.
3. As views agregadas (`VIEW_BI_SINC_*`) permitem consumir dados tratados diretamente no BI.

> **Exemplo prático:**  
> ```sql
> EXEC Contabilidade.PRC_SINC_CONTABILIDADE;
> EXEC Mov_Produto.PRC_SINC_MOV_PRODUTO;
> SELECT * FROM BI_SINC_CONTABILIDADE;
> ```

***

## Padrões e Regras de Negócio

- Todo tratamento/exceção está registrado nos próprios scripts.
- As principais regras ficam em functions e são reaproveitadas.
- Parâmetros de negócio variáveis estão em `BI_SINC_PARAMETROS_GLOBAL`.

> **Exemplo:**  
> O campo "TIPOLANCAMENTO" em `BI_SINC_CONTABILIDADE` diferencia lançamentos fiscais de gerenciais, conforme lógica na package.

***

## Como Executar/Sincronizar

- Execute cada procedure conforme a necessidade de carga ou atualização.
- O ideal é criar jobs Oracle (agendamentos) para garantir atualização automática e periódica.

> **Exemplo de execução manual:**  
> ```sql
> EXEC TabelasDW.PRC_SINC_TABELAS_DW;
> EXEC Cliente.PRC_SINC_CLIENTE;
> EXEC Contabilidade.PRC_SINC_CONTABILIDADE;
> ```

***

## Dicionário de Dados

**BI_SINC_CONTABILIDADE**  
| Coluna     | Tipo         | Descrição                  |
| ---------- | ------------ | ------------------------- |
| CODLANC    | VARCHAR2(40) | Código do lançamento      |
| CODEMPRESA | VARCHAR2(2)  | Código da empresa         |
| CODFILIAL  | VARCHAR2(2)  | Filial                    |
| DATA       | DATE         | Data do lançamento        |
| VALOR      | NUMBER(15,2) | Valor do movimento        |
| ...        | ...          | ...                       |

*Para o dicionário completo consulte o script TabelasDW/01_PRC_SINC_TABELAS_DW.sql.*

***

## Auditoria, Logging e Controle de Erros

- Procedures utilizam tratamento padrão de exceções do Oracle.
- Quando ocorre erro, há rollback e o erro pode ser logado via DBMS_OUTPUT ou em futuras implementações de tabela de logs.

> **Exemplo de tratamento:**
> ```sql
> BEGIN
>   -- .. rotina
> EXCEPTION
>   WHEN OTHERS THEN
>     ROLLBACK;
>     -- DBMS_OUTPUT.PUT_LINE(SQLERRM);
> END;
> ```

***

## Boas Práticas e Observações

- Mantenha scripts de modificação versionados no repositório
- Não insira regras de negócio fixas (hardcoded): use parâmetros!
- Atualize sempre documentação quando alterar objetos
- Realize manutenção dos índices periodicamente

***

## Colaboradores e Contato

**Responsável:**  
Jefferson Fantasia  
