CREATE OR REPLACE PROCEDURE PRC_SINC_TABELAS_DW AS
  v_table_exists NUMBER;
BEGIN
  ----BI_SINC_FILIAL
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_FILIAL';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_FILIAL
  (
     CODFILIAL VARCHAR2(2),
     EMPRESA   VARCHAR2(150),
     FILIAL    VARCHAR2(25),
     ORDEM     NUMBER(2),
     DT_UPDATE DATE,
     CONSTRAINT PK_CODFILIAL PRIMARY KEY (CODFILIAL)
  )';
  END IF;

  ----TEMP_FILIAL
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_FILIAL';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_FILIAL
  (
     CODFILIAL VARCHAR2(2),
     EMPRESA   VARCHAR2(150),
     FILIAL    VARCHAR2(25),
     ORDEM     NUMBER(2)
  )
  ON COMMIT PRESERVE ROWS';
  END IF;

  ----BI_SINC_PRODUTO
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_PRODUTO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PRODUTO
  (
     CODPROD         NUMBER(6),
     PRODUTO         VARCHAR2(40),
     CODPRODMASTER   NUMBER(6),
     PRODUTOMASTER   VARCHAR2(40),
     CODDEPTO        NUMBER(6),
     DEPARTAMENTO    VARCHAR2(25),
     CODSECAO        NUMBER(6),
     SECAO           VARCHAR2(40),
     CODCATEGORIA    NUMBER(6),
     CATEGORIA       VARCHAR2(40),
     CODLINHA        NUMBER(6),
     LINHA           VARCHAR2(40),
     PRODUTOFILHO    VARCHAR2(1),
     CODFORNEC       NUMBER(6),
     CODMARCA        NUMBER(8),
     MARCA           VARCHAR2(40),
     CODFAB          VARCHAR2(30),
     CODBARRAS       NUMBER(20),
     CODBARRASMASTER NUMBER(14),
     PESO            NUMBER(12, 6),
     LARGURA         NUMBER(20, 6),
     ALTURA          NUMBER(20, 6),
     COMPRIMENTO     NUMBER(20, 6),
     VOLUME          NUMBER(20, 8),
     QTCXMASTER      NUMBER(8, 2),
     IMPORTADO       VARCHAR2(1),
     REVENDA         VARCHAR2(1),
     NCM             VARCHAR2(15),
     NCMEX           VARCHAR2(20),
     TIPOMERCADORIA  VARCHAR2(20),
     FORALINHA       VARCHAR2(1),
     CERTIFICACAO    VARCHAR2(200),
     DT_UPDATE       DATE,
     CONSTRAINT PK_CODPROD PRIMARY KEY (CODPROD)
  )';
  END IF;

  ----TEMP_PCPRODUT
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PRODUTO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PRODUTO
  (
     CODPROD         NUMBER(6),
     PRODUTO         VARCHAR2(40),
     CODPRODMASTER   NUMBER(6),
     PRODUTOMASTER   VARCHAR2(40),
     CODDEPTO        NUMBER(6),
     DEPARTAMENTO    VARCHAR2(25),
     CODSECAO        NUMBER(6),
     SECAO           VARCHAR2(40),
     CODCATEGORIA    NUMBER(6),
     CATEGORIA       VARCHAR2(40),
     CODLINHA        NUMBER(6),
     LINHA           VARCHAR2(40),
     PRODUTOFILHO    VARCHAR2(1),
     CODFORNEC       NUMBER(6),
     CODMARCA        NUMBER(8),
     MARCA           VARCHAR2(40),
     CODFAB          VARCHAR2(30),
     CODBARRAS       NUMBER(20),
     CODBARRASMASTER NUMBER(14),
     PESO            NUMBER(12, 6),
     LARGURA         NUMBER(20, 6),
     ALTURA          NUMBER(20, 6),
     COMPRIMENTO     NUMBER(20, 6),
     VOLUME          NUMBER(20, 8),
     QTCXMASTER      NUMBER(8, 2),
     IMPORTADO       VARCHAR2(1),
     REVENDA         VARCHAR2(1),
     NCM             VARCHAR2(15),
     NCMEX           VARCHAR2(20),
     TIPOMERCADORIA  VARCHAR2(20),
     FORALINHA       VARCHAR2(1),
     CERTIFICACAO    VARCHAR2(200)
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;

  ----BI_SINC_FORNECEDOR
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_FORNECEDOR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_FORNECEDOR
  (
     CODFORNEC      NUMBER(6),
     FORNECEDOR     VARCHAR2(60),
     CODFORNECPRINC NUMBER(6),
     FORNECPRINC    VARCHAR2(70),
     CNPJ           VARCHAR2(18),
     TIPO           VARCHAR2(35),
     DT_UPDATE      DATE,
     CONSTRAINT PK_CODFORNEC PRIMARY KEY (CODFORNEC)
  )';
  END IF;

  ----TEMP_FORNECEDOR
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_FORNECEDOR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_FORNECEDOR
  (
     CODFORNEC      NUMBER(6),
     FORNECEDOR     VARCHAR2(60),
     CODFORNECPRINC NUMBER(6),
     FORNECPRINC    VARCHAR2(70),
     CNPJ           VARCHAR2(18),
     TIPO           VARCHAR2(35)
  )
  ON COMMIT PRESERVE ROWS';
  END IF;

  ----JFAREACOMERCIAL
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'JFAREACOMERCIAL';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE JFAREACOMERCIAL
  (
     CODAREA       NUMBER(2),
     AREACOMERCIAL VARCHAR2(40),
     CONSTRAINT PK_CODAREA PRIMARY KEY (CODAREA)
  )';
  END IF;

  ----TEMP_JFAREACOMERCIAL
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_JFAREACOMERCIAL';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_JFAREACOMERCIAL
  (
     CODAREA       NUMBER(2),
     AREACOMERCIAL VARCHAR2(40)
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;

  ----BI_SINC_VENDEDOR
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_VENDEDOR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_VENDEDOR
  (
     CODUSUR       NUMBER(6),
     NOMEORIGINAL  VARCHAR2(40),
     VENDEDOR      VARCHAR2(40),
     BLOQUEIO      VARCHAR2(1),
     CODSUPERVISOR NUMBER(4),
     SUPERVISOR    VARCHAR2(40),
     CODGERENTE    NUMBER(4),
     GERENTE       VARCHAR2(40),
     CODAREA       NUMBER(2),
     AREACOMERCIAL VARCHAR2(40),
     DT_UPDATE     DATE,
     CONSTRAINT PK_CODUSUR PRIMARY KEY (CODUSUR)
  )';
  END IF;

  ----TEMP_PCUSUARI
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PCUSUARI';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PCUSUARI
  (
     CODUSUR       NUMBER(6),
     NOMEORIGINAL  VARCHAR2(40),
     VENDEDOR      VARCHAR2(40),
     BLOQUEIO      VARCHAR2(1),
     CODSUPERVISOR NUMBER(4),
     SUPERVISOR    VARCHAR2(40),
     CODGERENTE    NUMBER(4),
     GERENTE       VARCHAR2(40),
     CODAREA       NUMBER(2),
     AREACOMERCIAL VARCHAR2(40)
  )
  ON COMMIT PRESERVE ROWS';
  END IF;

  ----BI_SINC_COMPRADOR
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_COMPRADOR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_COMPRADOR
  (
     MATRICULA NUMBER(6),
     COMPRADOR VARCHAR2(15),
     DT_UPDATE DATE,
     CONSTRAINT PK_MATRICULA PRIMARY KEY (MATRICULA)
  )';
  END IF;

  ----TEMP_PCEMPR
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PCEMPR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PCEMPR
  (
     MATRICULA NUMBER(6),
     COMPRADOR VARCHAR2(15)
  )
  ON COMMIT PRESERVE ROWS';
  END IF;

  ----BI_SINC_CLIENTE
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_CLIENTE';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_CLIENTE
  (
     CODCLI             NUMBER(6),
     CLIENTE            VARCHAR2(60),
     CODCLIREDE         VARCHAR2(8),
     CLIENTEREDE        VARCHAR2(80),
     CNPJ               VARCHAR2(18),
     CEP                VARCHAR2(9),
     UF                 VARCHAR2(2),
     CODUSUR            NUMBER(4),
     CODPRACA           NUMBER(4),
     PRACA              VARCHAR2(25),
     CODATIVIDADE       NUMBER(6),
     RAMOATIVIDADE      VARCHAR2(40),
     BLOQUEIODEFINITIVO VARCHAR2(1),
     BLOQUEIOATUAL      VARCHAR2(1),
     LIMITECREDITO      NUMBER(12, 2),
     DT_UPDATE          DATE,
     CONSTRAINT PK_CODCLI PRIMARY KEY (CODCLI)
  )';
  END IF;

  ----TEMP_PCCLIENT
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PCCLIENT';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PCCLIENT
  (
     CODCLI             NUMBER(6),
     CLIENTE            VARCHAR2(60),
     CODCLIREDE         VARCHAR2(8),
     CLIENTEREDE        VARCHAR2(80),
     CNPJ               VARCHAR2(18),
     CEP                VARCHAR2(9),
     UF                 VARCHAR2(2),
     CODUSUR            NUMBER(4),
     CODPRACA           NUMBER(4),
     PRACA              VARCHAR2(25),
     CODATIVIDADE       NUMBER(6),
     RAMOATIVIDADE      VARCHAR2(40),
     BLOQUEIODEFINITIVO VARCHAR2(1),
     BLOQUEIOATUAL      VARCHAR2(1),
     LIMITECREDITO      NUMBER(12, 2)
  )
  ON COMMIT PRESERVE ROWS';
  END IF;

  ----BI_SINC_ESTOQUE
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_ESTOQUE';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_ESTOQUE
  (
     CODFILIAL           VARCHAR2(2),
     CODPROD             NUMBER(6),
     QTCONTABIL          NUMBER(22, 8),
     QTGERENCIAL         NUMBER(22, 8),
     QTBLOQUEADA         NUMBER(20, 6),
     QTPENDENTE          NUMBER(16, 3),
     QTRESERVADA         NUMBER(22, 6),
     QTAVARIADA          NUMBER(20, 6),
     QTDISPONIVEL        NUMBER(20, 6),
     QTFRENTELOJA        NUMBER(22, 6),
     QTDEPOSITO          NUMBER(22, 6),
     VALORULTENT         NUMBER(18, 6),
     CUSTOREPOSICAO      NUMBER(18, 6),
     CUSTOFINANCEIRO     NUMBER(18, 6),
     CUSTOCONTABIL       NUMBER(18, 6),
     VLESTOQUECONTABIL   NUMBER(18, 6),
     VLESTOQUEFINANCEIRO NUMBER(18, 6),
     VLESTOQUEGERENCIAL  NUMBER(18, 6),
     VLESTOQUELOJA       NUMBER(18, 6),
     VLESTOQUEDEPOSITO   NUMBER(18, 6),
     VLESTOQUEDISPONIVEL NUMBER(18, 6),
     VLESTOQUEAVARIADO   NUMBER(18, 6),
     CODBLOQUEIO         NUMBER(4),
     MOTIVOBLOQUEIO      VARCHAR2(30),
     DT_UPDATE           DATE,
     CONSTRAINT PK_CODFILIAL_CODPROD PRIMARY KEY (CODFILIAL, CODPROD)
  )';
  END IF;

  ----TEMP_ESTOQUE
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_ESTOQUE';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_ESTOQUE
  (
     CODFILIAL           VARCHAR2(2),
     CODPROD             NUMBER(6),
     QTCONTABIL          NUMBER(22, 8),
     QTGERENCIAL         NUMBER(22, 8),
     QTBLOQUEADA         NUMBER(20, 6),
     QTPENDENTE          NUMBER(16, 3),
     QTRESERVADA         NUMBER(22, 6),
     QTAVARIADA          NUMBER(20, 6),
     QTDISPONIVEL        NUMBER(20, 6),
     QTFRENTELOJA        NUMBER(22, 6),
     QTDEPOSITO          NUMBER(22, 6),
     VALORULTENT         NUMBER(18, 6),
     CUSTOREPOSICAO      NUMBER(18, 6),
     CUSTOFINANCEIRO     NUMBER(18, 6),
     CUSTOCONTABIL       NUMBER(18, 6),
     VLESTOQUECONTABIL   NUMBER(18, 6),
     VLESTOQUEFINANCEIRO NUMBER(18, 6),
     VLESTOQUEGERENCIAL  NUMBER(18, 6),
     VLESTOQUELOJA       NUMBER(18, 6),
     VLESTOQUEDEPOSITO   NUMBER(18, 6),
     VLESTOQUEDISPONIVEL NUMBER(18, 6),
     VLESTOQUEAVARIADO   NUMBER(18, 6),
     CODBLOQUEIO         NUMBER(4),
     MOTIVOBLOQUEIO      VARCHAR2(30)
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;

  ----BI_SINC_PRECO_COMPRA
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_PRECO_COMPRA';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PRECO_COMPRA
  (
     CODFILIAL       VARCHAR2(2),
     CODPROD         NUMBER(6),
     PTABELA         NUMBER(18, 6),
     PCOMPRA         NUMBER(18, 6),
     VLIPI           NUMBER(18, 6),
     VLST            NUMBER(18, 6),
     PBRUTO          NUMBER(18, 6),
     VLCREDICMS      NUMBER(18, 6),
     VLPIS           NUMBER(18, 6),
     VLCOFINS        NUMBER(18, 6),
     CUSTOLIQ        NUMBER(18, 6),
     BASEICMS        NUMBER(18, 6),
     BASEST          NUMBER(18, 6),
     BASEPISCOFINS   NUMBER(18, 6),
     PERCPIS         NUMBER(18, 6),
     PERCCOFINS      NUMBER(18, 6),
     PERCIPI         NUMBER(18, 6),
     PERCICMS        NUMBER(18, 6),
     PERCICMSRED     NUMBER(18, 6),
     PERCCREDICMS    NUMBER(18, 6),
     PERCIVA         NUMBER(18, 6),
     PERCALIQEXT     NUMBER(18, 6),
     PERCALIQINT     NUMBER(18, 6),
     PERCALIQEXTGUIA NUMBER(18, 6),
     REDBASEALIQEXT  NUMBER(18, 6),
     PERCALIQSTRED   NUMBER(18, 6),
     DT_UPDATE       DATE,
     CONSTRAINT PK_PRECO_COMPRA PRIMARY KEY (CODFILIAL, CODPROD)
  )';
  END IF;

  ----TEMP_PCPRODFILIAL
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PRECO_COMPRA';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PRECO_COMPRA
  (
     CODFILIAL       VARCHAR2(2),
     CODPROD         NUMBER(6),
     PTABELA         NUMBER(18, 6),
     PCOMPRA         NUMBER(18, 6),
     VLIPI           NUMBER(18, 6),
     VLST            NUMBER(18, 6),
     PBRUTO          NUMBER(18, 6),
     VLCREDICMS      NUMBER(18, 6),
     VLPIS           NUMBER(18, 6),
     VLCOFINS        NUMBER(18, 6),
     CUSTOLIQ        NUMBER(18, 6),
     BASEICMS        NUMBER(18, 6),
     BASEST          NUMBER(18, 6),
     BASEPISCOFINS   NUMBER(18, 6),
     PERCPIS         NUMBER(18, 6),
     PERCCOFINS      NUMBER(18, 6),
     PERCIPI         NUMBER(18, 6),
     PERCICMS        NUMBER(18, 6),
     PERCICMSRED     NUMBER(18, 6),
     PERCCREDICMS    NUMBER(18, 6),
     PERCIVA         NUMBER(18, 6),
     PERCALIQEXT     NUMBER(18, 6),
     PERCALIQINT     NUMBER(18, 6),
     PERCALIQEXTGUIA NUMBER(18, 6),
     REDBASEALIQEXT  NUMBER(18, 6),
     PERCALIQSTRED   NUMBER(18, 6)
  )
  ON COMMIT PRESERVE ROWS';
  END IF;

  ----BI_SINC_MOV_PRODUTO
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_MOV_PRODUTO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_MOV_PRODUTO
  (
     NUMTRANSITEM    NUMBER(18),
     MOVIMENTO       VARCHAR2(1),
     TIPOMOV         VARCHAR2(60),
     CODFILIAL       VARCHAR2(2),
     NUMTRANSACAO    NUMBER(10),
     CODCOB          VARCHAR2(4),
     PARCELAS        NUMBER(4),
     PRAZO           NUMBER(6),
     CODUSUR         NUMBER(4),
     CODFORNEC       NUMBER(8),
     CODCLI          NUMBER(8),
     DATA            DATE,
     CODPROD         NUMBER(6),
     QT              NUMBER(20, 6),
     CUSTOFINANCEIRO NUMBER(18, 6),
     CUSTOREPOSICAO  NUMBER(18, 6),
     CUSTOCONTABIL   NUMBER(18, 6),
     VLCONTABIL      NUMBER(18, 6),
     PUNIT           NUMBER(18, 6),
     PTABELA         NUMBER(18, 6),
     VLPRODUTO       NUMBER(18, 6),
     VLDESCONTO      NUMBER(18, 6),
     CST_ICMS        VARCHAR2(3),
     CFOP            NUMBER(8),
     VLBASEICMS      NUMBER(18, 6),
     PERCICMS        NUMBER(12, 4),
     VLICMS          NUMBER(18, 6),
     VLICMSBENEFICIO NUMBER(18, 6),
     VLST            NUMBER(18, 6),
     VLSTGUIA        NUMBER(18, 6),
     PERCIPI         NUMBER(12, 4),
     VLIPI           NUMBER(18, 6),
     CST_PISCOFINS   VARCHAR2(3),
     VLBASEPISCOFINS NUMBER(18, 6),
     PERCPIS         NUMBER(12, 4),
     PERCCOFINS      NUMBER(12, 4),
     VLPIS           NUMBER(18, 6),
     VLCOFINS        NUMBER(18, 6),
     VLFRETE         NUMBER(18, 6),
     VLOUTRASDESP    NUMBER(18, 6),
     VLICMSDIFAL     NUMBER(18, 6),
     VLCMVGERENCIAL  NUMBER(18, 6),
     VLCMVCONTABIL   NUMBER(18, 6),
     DTCANCEL        DATE,
     DT_UPDATE       DATE,
     CONSTRAINT PK_NUMTRANSITEM PRIMARY KEY (NUMTRANSITEM)
  ) ';
  END IF;

  ----TEMP_MOV_PRODUTO
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_MOV_PRODUTO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_MOV_PRODUTO
  (
     NUMTRANSITEM    NUMBER(18),
     MOVIMENTO       VARCHAR2(1),
     TIPOMOV         VARCHAR2(60),
     CODFILIAL       VARCHAR2(2),
     NUMTRANSACAO    NUMBER(10),
     CODCOB          VARCHAR2(4),
     PARCELAS        NUMBER(4),
     PRAZO           NUMBER(6),
     CODUSUR         NUMBER(4),
     CODFORNEC       NUMBER(8),
     CODCLI          NUMBER(8),
     DATA            DATE,
     CODPROD         NUMBER(6),
     QT              NUMBER(20, 6),
     CUSTOFINANCEIRO NUMBER(18, 6),
     CUSTOREPOSICAO  NUMBER(18, 6),
     CUSTOCONTABIL   NUMBER(18, 6),
     VLCONTABIL      NUMBER(18, 6),
     PUNIT           NUMBER(18, 6),
     PTABELA         NUMBER(18, 6),
     VLPRODUTO       NUMBER(18, 6),
     VLDESCONTO      NUMBER(18, 6),
     CST_ICMS        VARCHAR2(3),
     CFOP            NUMBER(8),
     VLBASEICMS      NUMBER(18, 6),
     PERCICMS        NUMBER(12, 4),
     VLICMS          NUMBER(18, 6),
     VLICMSBENEFICIO NUMBER(18, 6),
     VLST            NUMBER(18, 6),
     VLSTGUIA        NUMBER(18, 6),
     PERCIPI         NUMBER(12, 4),
     VLIPI           NUMBER(18, 6),
     CST_PISCOFINS   VARCHAR2(3),
     VLBASEPISCOFINS NUMBER(18, 6),
     PERCPIS         NUMBER(12, 4),
     PERCCOFINS      NUMBER(12, 4),
     VLPIS           NUMBER(18, 6),
     VLCOFINS        NUMBER(18, 6),
     VLFRETE         NUMBER(18, 6),
     VLOUTRASDESP    NUMBER(18, 6),
     VLICMSDIFAL     NUMBER(18, 6),
     VLCMVGERENCIAL  NUMBER(18, 6),
     VLCMVCONTABIL   NUMBER(18, 6),
     DTCANCEL        DATE
  )
  ON COMMIT PRESERVE ROWS';
  END IF;

  ----BI_SINC_PRECO_VENDA
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_PRECO_VENDA';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PRECO_VENDA
  (
     CODPROD     NUMBER(6),
     NUMREGIAO   NUMBER(4),
     PRECOVENDA  NUMBER(18, 6),
     MARGEMIDEAL NUMBER(6, 2),
     DT_UPDATE   DATE,
     CONSTRAINT PK_BI_SINC_PRECO_VENDA PRIMARY KEY (CODPROD, NUMREGIAO)
  ) ';
  END IF;

  ----TEMP_PRECO_VENDA
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PRECO_VENDA';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PRECO_VENDA
  (
     CODPROD     NUMBER(6),
     NUMREGIAO   NUMBER(4),
     PRECOVENDA  NUMBER(18, 6),
     MARGEMIDEAL NUMBER(6, 2)
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;

  ----BI_SINC_PRECO_VENDA_PROMOCIONAL
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_PRECO_VENDA_PROMOCIONAL';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PRECO_VENDA_PROMOCIONAL
  (
     CODPRECOPROM     NUMBER(10),
     CODFILIAL        VARCHAR2(2),
     CODPROD          NUMBER(6),
     NUMREGIAO        NUMBER(4),
     PRECOPROMOCIONAL NUMBER(18, 6),
     DTINICIOPROMOCAO DATE,
     DTFIMPROMOCAO    DATE,
     DT_UPDATE        DATE,
     CONSTRAINT PK_BI_SINC_PRECO_VENDA_PROMO PRIMARY KEY (CODPRECOPROM)
  ) ';
  END IF;

  ----TEMP_PRECO_VENDA_PROMOCIONAL
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PRECO_VENDA_PROMOCIONAL';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PRECO_VENDA_PROMOCIONAL
  (
     CODPRECOPROM     NUMBER(10),
     CODFILIAL        VARCHAR2(2),
     CODPROD          NUMBER(6),
     NUMREGIAO        NUMBER(4),
     PRECOPROMOCIONAL NUMBER(18, 6),
     DTINICIOPROMOCAO DATE,
     DTFIMPROMOCAO    DATE
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;

  ----BI_SINC_PEDIDO_COMPRA
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_PEDIDO_COMPRA';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PEDIDO_COMPRA
  (
     CODFILIAL    VARCHAR2(2),
     DATA         DATE,
     CODFORNEC    NUMBER(6),
     CODCOMPRADOR NUMBER(8),
     TIPO         VARCHAR2(20),
     NUMPED       NUMBER(10),
     NUMSEQ       NUMBER(6),
     CODPROD      NUMBER(6),
     PRECOCOMPRA  NUMBER(18, 6),
     QTPEDIDA     NUMBER(20, 6),
     QTENTREGUE   NUMBER(20, 6),
     QTSALDO      NUMBER(20, 6),
     VLPEDIDO     NUMBER(18, 6),
     VLENTREGUE   NUMBER(18, 6),
     VLSALDO      NUMBER(18, 6),
     DT_UPDATE    DATE,
     CONSTRAINT PK_BI_SINC_PEDIDO_COMPRA PRIMARY KEY (NUMPED, NUMSEQ, CODPROD )
  )';
  END IF;

  ----TEMP_PCITEM
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PCITEM';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PCITEM
  (
     CODFILIAL    VARCHAR2(2),
     DATA         DATE,
     CODFORNEC    NUMBER(6),
     CODCOMPRADOR NUMBER(8),
     TIPO         VARCHAR2(20),
     NUMPED       NUMBER(10),
     NUMSEQ       NUMBER(6),
     CODPROD      NUMBER(6),
     PRECOCOMPRA  NUMBER(18, 6),
     QTPEDIDA     NUMBER(20, 6),
     QTENTREGUE   NUMBER(20, 6),
     QTSALDO      NUMBER(20, 6),
     VLPEDIDO     NUMBER(18, 6),
     VLENTREGUE   NUMBER(18, 6),
     VLSALDO      NUMBER(18, 6)
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;

  ----BI_SINC_LANC_PAGAR
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_LANC_PAGAR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_LANC_PAGAR
  (
     RECNUM         NUMBER(8),
     CODFILIAL      VARCHAR2(2),
     DTCOMPETENCIA  DATE,
     DTVENCIMENTO   DATE,
     DTPAGAMENTO    DATE,
     DTCONTABIL     DATE,
     TIPO           VARCHAR2(25),
     VALOR          NUMBER(14, 2),
     VLJUROS        NUMBER(14, 2),
     VLDESCONTO     NUMBER(14, 2),
     VALORAPAGAR    NUMBER(14, 2),
     CODBANCO       NUMBER(4),
     CODCONTA       NUMBER(10),
     CODFORNEC      NUMBER(8),
     TIPOPARCEIRO   VARCHAR2(1),
     NUMTRANS       NUMBER(10),
     NUMNOTA        NUMBER(10),
     DUPLICATA      VARCHAR2(1),
     HISTORICO      VARCHAR2(200),
     OBSERVACAO     VARCHAR2(200),
     RECNUMPRINC    NUMBER(8),
     CODROTINABAIXA NUMBER(4),
     DT_UPDATE      DATE,
     CONSTRAINT PK_BI_SINC_LANC_PAGAR PRIMARY KEY (RECNUM)
  ) ';
  END IF;

  ----TEMP_LANC_PAGAR
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_LANC_PAGAR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_LANC_PAGAR
  (
     RECNUM         NUMBER(8),
     CODFILIAL      VARCHAR2(2),
     DTCOMPETENCIA  DATE,
     DTVENCIMENTO   DATE,
     DTPAGAMENTO    DATE,
     DTCONTABIL     DATE,
     TIPO           VARCHAR2(25),
     VALOR          NUMBER(14, 2),
     VLJUROS        NUMBER(14, 2),
     VLDESCONTO     NUMBER(14, 2),
     VALORAPAGAR    NUMBER(14, 2),
     CODBANCO       NUMBER(4),
     CODCONTA       NUMBER(10),
     CODFORNEC      NUMBER(8),
     TIPOPARCEIRO   VARCHAR2(1),
     NUMTRANS       NUMBER(10),
     NUMNOTA        NUMBER(10),
     DUPLICATA      VARCHAR2(1),
     HISTORICO      VARCHAR2(200),
     OBSERVACAO     VARCHAR2(200),
     RECNUMPRINC    NUMBER(8),
     CODROTINABAIXA NUMBER(4)
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;

  ----BI_SINC_REGIAO
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_REGIAO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_REGIAO
  (
     NUMREGIAO NUMBER(4),
     REGIAO    VARCHAR2(40),
     DT_UPDATE DATE,
     CONSTRAINT PK_BI_SINC_REGIAO PRIMARY KEY (NUMREGIAO)
  ) ';
  END IF;

  ----TEMP_REGIAO
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_REGIAO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_REGIAO
  (
     NUMREGIAO NUMBER(4),
     REGIAO    VARCHAR2(40)
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;

  ----BI_SINC_CALENDARIO
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_CALENDARIO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_CALENDARIO
  (
    DATA                    DATE,
    DIA                     NUMBER(2),
    ANO                     NUMBER(4),
    NUM_MES                 NUMBER(2),
    NOME_MES                VARCHAR2(12),
    NOME_MES_ABREV          VARCHAR2(3),
    NUM_MES_ANO             NUMBER(6),
    NUM_MES_ANO_SEQ         NUMBER(6),
    MES_ANO                 VARCHAR2(8),
    NUM_DIA_ANO             NUMBER(3),
    NUM_TRIMESTRE           NUMBER(1),
    NOME_TRIMESTRE          VARCHAR2(12),
    NOME_TRIMESTRE_ABREV    VARCHAR2(6),
    NUM_TRIMESTRE_ANO       NUMBER(6),
    NOME_TRIMESTRE_ANO      VARCHAR2(12),
    NUM_TRIMESTRE_JC        NUMBER(1),
    NOME_TRIMESTRE_JC       VARCHAR2(12),
    NOME_TRIMESTRE_ABREV_JC VARCHAR2(6),
    NUM_TRIMESTRE_ANO_JC    NUMBER(6),
    NOME_TRIMESTRE_ANO_JC   VARCHAR2(12),
    NUM_SEMANA_MES          NUMBER(1),
    NOME_SEMANA_MES         VARCHAR2(10),
    NUM_SEMANA_ANO          NUMBER(2),
    NOME_SEMANA_ANO         VARCHAR2(12),
    NUM_DIA_SEMANA          NUMBER(1),
    NOME_DIA_SEMANA         VARCHAR2(14),
    NOME_DIA_SEMANA_ABREV   VARCHAR2(3),
    CALCULO_MES_ATUAL       NUMBER(4),
    CALCULO_TRIMESTRE_ATUAL NUMBER(4),
    CALCULO_ANO_ATUAL       NUMBER(4),
    DIA_UTIL_FINANCEIRO     DATE,
    DT_UPDATE               DATE,
    CONSTRAINT PK_BI_SINC_CALENDARIO PRIMARY KEY (DATA)
  )';
  END IF;

  ----TEMP_CALENDARIO
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_CALENDARIO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_CALENDARIO
  (
    DATA                    DATE,
    DIA                     NUMBER(2),
    ANO                     NUMBER(4),
    NUM_MES                 NUMBER(2),
    NOME_MES                VARCHAR2(12),
    NOME_MES_ABREV          VARCHAR2(3),
    NUM_MES_ANO             NUMBER(6),
    NUM_MES_ANO_SEQ         NUMBER(6),
    MES_ANO                 VARCHAR2(8),
    NUM_DIA_ANO             NUMBER(3),
    NUM_TRIMESTRE           NUMBER(1),
    NOME_TRIMESTRE          VARCHAR2(12),
    NOME_TRIMESTRE_ABREV    VARCHAR2(6),
    NUM_TRIMESTRE_ANO       NUMBER(6),
    NOME_TRIMESTRE_ANO      VARCHAR2(12),
    NUM_TRIMESTRE_JC        NUMBER(1),
    NOME_TRIMESTRE_JC       VARCHAR2(12),
    NOME_TRIMESTRE_ABREV_JC VARCHAR2(6),
    NUM_TRIMESTRE_ANO_JC    NUMBER(6),
    NOME_TRIMESTRE_ANO_JC   VARCHAR2(12),
    NUM_SEMANA_MES          NUMBER(1),
    NOME_SEMANA_MES         VARCHAR2(10),
    NUM_SEMANA_ANO          NUMBER(2),
    NOME_SEMANA_ANO         VARCHAR2(12),
    NUM_DIA_SEMANA          NUMBER(1),
    NOME_DIA_SEMANA         VARCHAR2(14),
    NOME_DIA_SEMANA_ABREV   VARCHAR2(3),
    CALCULO_MES_ATUAL       NUMBER(4),
    CALCULO_TRIMESTRE_ATUAL NUMBER(4),
    CALCULO_ANO_ATUAL       NUMBER(4),
    DIA_UTIL_FINANCEIRO     DATE
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;

  ----BI_SINC_TABELAS
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_TABELAS';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_TABELAS
  (
     TABELA         VARCHAR2(128),
     QTREGISTROS    NUMBER,
     MAIOR_DTUPDATE DATE,
     LAST_REFRESH   DATE,
     DT_UPDATE      DATE,
     CONSTRAINT PK_BI_SINC_TABELAS PRIMARY KEY (TABELA)
  ) ';
  END IF;

  ----TEMP_TABELAS
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_TABELAS';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_TABELAS
  (
     TABELA         VARCHAR2(128),
     QTREGISTROS    NUMBER,
     MAIOR_DTUPDATE DATE,
     LAST_REFRESH   DATE
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;

  ----BI_SINC_META_VENDEDOR
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_META_VENDEDOR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_META_VENDEDOR
  (
     DATA      DATA,
     CODUSUR   NUMBER(4),
     VLMETA    NUMBER(18, 6),
     DT_UPDATE DATE,
     CONSTRAINT PK_META_VENDEDOR PRIMARY KEY (DATA, CODUSUR)
  ) ';
  END IF;

  ----TEMP_META_VENDEDOR
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_META_VENDEDOR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_META_VENDEDOR
  (
     DATA      DATA,
     CODUSUR   NUMBER(4),
     VLMETA    NUMBER(18, 6)
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;

  ----BI_SINC_PEDIDO_VENDA
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_PEDIDO_VENDA';
  IF v_table_exists = 0 THEN
	EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PEDIDO_VENDA
  (
     CODFILIAL         VARCHAR2(2),
     CODFILIALRETIRA   VARCHAR2(2),
     DATA              DATE,
     DATALIMITE        DATE,
     NUMPED            NUMBER(10),
     TIPOVENDA         NUMBER(5),
     CODCLI            NUMBER(6),
     CODPROD           NUMBER(6),
     QT                NUMBER(20, 6),
     PVENDA            NUMBER(18, 6),
     VLPRODUTO         NUMBER(18, 6),
     VLPEDIDO          NUMBER(18, 6),
     CODUSUR           NUMBER(4),
     POSICAO           VARCHAR2(2),
     TIPOBLOQUEIO      VARCHAR2(1),
     MOTIVOBLOQUEIO    VARCHAR2(200),
     OBSPEDIDO         VARCHAR2(25),
     CODMOTIVOPENDENTE NUMBER(2),
     DT_UPDATE         DATE,
     CONSTRAINT PK_PEDIDO_VENDA PRIMARY KEY (NUMPED, CODPROD)
  )  ';
  END IF;

  ----TEMP_PEDIDO_VENDA
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PEDIDO_VENDA';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PEDIDO_VENDA
  (
     CODFILIAL         VARCHAR2(2),
     CODFILIALRETIRA   VARCHAR2(2),
     DATA              DATE,
     DATALIMITE        DATE,
     NUMPED            NUMBER(10),
     TIPOVENDA         NUMBER(5),
     CODCLI            NUMBER(6),
     CODPROD           NUMBER(6),
     QT                NUMBER(20, 6),
     PVENDA            NUMBER(18, 6),
     VLPRODUTO         NUMBER(18, 6),
     VLPEDIDO          NUMBER(18, 6),
     CODUSUR           NUMBER(4),
     POSICAO           VARCHAR2(2),
     TIPOBLOQUEIO      VARCHAR2(1),
     MOTIVOBLOQUEIO    VARCHAR2(200),
     OBSPEDIDO         VARCHAR2(25),
     CODMOTIVOPENDENTE NUMBER(2)
  )
  ON COMMIT PRESERVE ROWS ';
  END IF;


END;
