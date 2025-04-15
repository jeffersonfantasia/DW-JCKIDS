----USADO PARA FAZER O TRATAMENTO DA MOVIMENTAÇÃO NO PADRAO CONTABILIDADE


---------MODELO PARA RECEBER OS LANCAMENTOS PARA CONTABILIDADE
DROP TYPE T_CONTABIL_TABLE;
DROP TYPE T_CONTABIL_RECORD;

CREATE OR REPLACE TYPE T_CONTABIL_TABLE IS TABLE OF T_CONTABIL_RECORD;
CREATE OR REPLACE TYPE T_CONTABIL_RECORD IS OBJECT
(
  CODLANC         VARCHAR2(12),
  CODEMPRESA      VARCHAR2(2),
  DATA            DATE,
  TIPOLANCAMENTO  NUMBER,
  IDENTIFICADOR   NUMBER,
  DOCUMENTO       NUMBER,
  CONTADEBITO     NUMBER,
  CONTACREDITO    NUMBER,
  CODCC_DEBITO    VARCHAR2(4),
  CODCC_CREDITO   VARCHAR2(4),
  ATIVIDADE       VARCHAR2(350),
  HISTORICO       VARCHAR2(450),
  VALOR           NUMBER(12, 2),
  ORIGEM          VARCHAR2(80),
  ENVIAR_CONTABIL VARCHAR2(1),
  DTCANCEL        DATE
)
;
--------FORNECEDORES 
DROP TYPE T_FORNEC_TABLE;
DROP TYPE T_FORNEC_RECORD;

CREATE OR REPLACE TYPE T_FORNEC_TABLE IS TABLE OF T_FORNEC_RECORD;
CREATE OR REPLACE TYPE T_FORNEC_RECORD IS OBJECT
(
  CODFORNEC NUMBER
)
;

--------CONTAS GERENCIAIS 
DROP TYPE T_CONTA_TABLE;
DROP TYPE T_CONTA_RECORD;

CREATE OR REPLACE TYPE T_CONTA_TABLE IS TABLE OF T_CONTA_RECORD;
CREATE OR REPLACE TYPE T_CONTA_RECORD IS OBJECT
(
  CODCONTA NUMBER
)
;

--------GRUPOS GERENCIAIS 
DROP TYPE T_GRUPO_TABLE;
DROP TYPE T_GRUPO_RECORD;

CREATE OR REPLACE TYPE T_GRUPO_TABLE IS TABLE OF T_GRUPO_RECORD;
CREATE OR REPLACE TYPE T_GRUPO_RECORD IS OBJECT
(
  CODGRUPO NUMBER
)
;

--------BANCOS
DROP TYPE T_BANCO_TABLE;
DROP TYPE T_BANCO_RECORD;

CREATE OR REPLACE TYPE T_BANCO_TABLE IS TABLE OF T_BANCO_RECORD;
CREATE OR REPLACE TYPE T_BANCO_RECORD IS OBJECT
(
  CODBANCO NUMBER
)
;

--------CENTRO DE CUSTO POR FILIAL
DROP TYPE T_CC_FILIAL_TABLE;
DROP TYPE T_CC_FILIAL_RECORD;

CREATE OR REPLACE TYPE T_CC_FILIAL_TABLE IS TABLE OF T_CC_FILIAL_RECORD;
CREATE OR REPLACE TYPE T_CC_FILIAL_RECORD IS OBJECT
(
  CODFILIAL VARCHAR(2),
  CODCC     VARCHAR(3)
)
;

--------CENTRO DE CUSTO POR SUPERVISOR 
DROP TYPE T_CC_VENDEDOR_TABLE;
DROP TYPE T_CC_VENDEDOR_RECORD;

CREATE OR REPLACE TYPE T_CC_VENDEDOR_TABLE IS TABLE OF T_CC_VENDEDOR_RECORD;
CREATE OR REPLACE TYPE T_CC_VENDEDOR_RECORD IS OBJECT
(
  CODSUPERVISOR NUMBER,
  CODCC         VARCHAR(3)
)
;

--------CENTRO DE CUSTO POR SUPERVISOR 
DROP TYPE T_CC_GERENTE_TABLE;
DROP TYPE T_CC_GERENTE_RECORD;

CREATE OR REPLACE TYPE T_CC_GERENTE_TABLE IS TABLE OF T_CC_GERENTE_RECORD;
CREATE OR REPLACE TYPE T_CC_GERENTE_RECORD IS OBJECT
(
  CODGERENTE    NUMBER,
  CODCC         VARCHAR(3)
)
;
