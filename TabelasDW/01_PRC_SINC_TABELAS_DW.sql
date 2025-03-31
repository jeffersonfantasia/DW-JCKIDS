CREATE OR REPLACE PROCEDURE PRC_SINC_TABELAS_DW AS

  v_table_exists NUMBER;
  v_index_exists NUMBER;

BEGIN
  ----BI_SINC_FILIAL
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_FILIAL';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_FILIAL
  (
     CODFILIAL  VARCHAR2(2),
     EMPRESA    VARCHAR2(150),
     FILIAL     VARCHAR2(25),
     ORDEM      NUMBER(2),
     CODEMPRESA VARCHAR2(2), 
     CODFORNEC  NUMBER(6),
     CODCLI     NUMBER(6),
     TIPOFILIAL VARCHAR2(25),
     DT_UPDATE  DATE,
     CONSTRAINT PK_CODFILIAL PRIMARY KEY (CODFILIAL)
  )';
  END IF;

  ----BI_SINC_PRODUTO
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_PRODUTO';
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
     TIPOCOMISSAO    VARCHAR2(40),
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
     ENVIAFV         VARCHAR2(1),
     NCM             VARCHAR2(15),
     NCMEX           VARCHAR2(20),
     TIPOMERCADORIA  VARCHAR2(20),
     FORALINHA       VARCHAR2(1),
     CERTIFICACAO    VARCHAR2(200),
     DT_UPDATE       DATE,
     CONSTRAINT PK_CODPROD PRIMARY KEY (CODPROD)
  )';
  END IF;

  ----BI_SINC_FORNECEDOR
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_FORNECEDOR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_FORNECEDOR
  (
     CODFORNEC      NUMBER(6),
     FORNECEDOR     VARCHAR2(60),
     CODFORNECPRINC NUMBER(6),
     FORNECPRINC    VARCHAR2(70),
     CNPJ           VARCHAR2(18),
     TIPO           VARCHAR2(1),
     DT_UPDATE      DATE,
     CONSTRAINT PK_CODFORNEC PRIMARY KEY (CODFORNEC)
  )';
  END IF;

  ----JFAREACOMERCIAL
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'JFAREACOMERCIAL';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE JFAREACOMERCIAL
  (
     CODAREA       NUMBER(2),
     AREACOMERCIAL VARCHAR2(40),
     CONSTRAINT PK_CODAREA PRIMARY KEY (CODAREA)
  )';
  END IF;

  ----BI_SINC_VENDEDOR
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_VENDEDOR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_VENDEDOR
  (
     CODUSUR       NUMBER(6),
     NOMEORIGINAL  VARCHAR2(40),
     VENDEDOR      VARCHAR2(40),
     CARGO         VARCHAR2(20),
     CODFILIAL     VARCHAR2(2),
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

  ----BI_SINC_COMPRADOR
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_COMPRADOR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_COMPRADOR
  (
     MATRICULA NUMBER(6),
     COMPRADOR VARCHAR2(15),
     DT_UPDATE DATE,
     CONSTRAINT PK_MATRICULA PRIMARY KEY (MATRICULA)
  )';
  END IF;

  ----BI_SINC_CLIENTE
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_CLIENTE';
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
     CODFILIALJCCLUB    VARCHAR2(2),
     CODUSUR            NUMBER(4),
     CODPRACA           NUMBER(4),
     PRACA              VARCHAR2(25),
     CODATIVIDADE       NUMBER(6),
     RAMOATIVIDADE      VARCHAR2(40),
     BLOQUEIODEFINITIVO VARCHAR2(1),
     BLOQUEIOATUAL      VARCHAR2(1),
     LIMITECREDITO      NUMBER(12, 2),
     DTCADASTRO         DATE,
     DT_UPDATE          DATE,
     CONSTRAINT PK_CODCLI PRIMARY KEY (CODCLI)
  )';
  END IF;

  ----BI_SINC_ESTOQUE
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_ESTOQUE';
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
     VLESTOQUEBLOQUEADO  NUMBER(18, 6),
     VLESTOQUEVENDA      NUMBER(18, 6),
     CODBLOQUEIO         NUMBER(4),
     MOTIVOBLOQUEIO      VARCHAR2(30),
     DT_UPDATE           DATE,
     CONSTRAINT PK_CODFILIAL_CODPROD PRIMARY KEY (CODFILIAL, CODPROD)
  )';
  END IF;

  ----BI_SINC_PRECO_COMPRA
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_PRECO_COMPRA';
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

  ----BI_SINC_MOV_PRODUTO
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_MOV_PRODUTO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_MOV_PRODUTO
  (
     NUMTRANSITEM      NUMBER(18),
     MOVIMENTO         VARCHAR2(1),
     TIPOMOV           VARCHAR2(60),
     TIPOMOVGER        VARCHAR2(60),
     CODFILIAL         VARCHAR2(2),
     NUMTRANSACAO      NUMBER(10),
     TEMVENDAORIG      VARCHAR2(1),
     CODCOB            VARCHAR2(4),
     PARCELAS          NUMBER(4),
     PRAZO             NUMBER(6),
     CODUSUR           NUMBER(4),
     CODFORNEC         NUMBER(8),
     CODCLI            NUMBER(8),
     NUMNOTA           NUMBER(10),
     DATA              DATE,
     CODPROD           NUMBER(6),
     QT                NUMBER(20, 6),
     CUSTOFINANCEIRO   NUMBER(18, 6),
     CUSTOREPOSICAO    NUMBER(18, 6),
     CUSTOCONTABIL     NUMBER(18, 6),
     VLCONTABIL        NUMBER(18, 6),
     PUNIT             NUMBER(18, 6),
     PTABELA           NUMBER(18, 6),
     VLPRODUTO         NUMBER(18, 6),
     VLDESCONTO        NUMBER(18, 6),
     CST_ICMS          VARCHAR2(3),
     CFOP              NUMBER(8),
     VLBASEICMS        NUMBER(18, 6),
     PERCICMS          NUMBER(12, 4),
     VLICMS            NUMBER(18, 6),
     VLICMSBENEFICIO   NUMBER(18, 6),
     VLST              NUMBER(18, 6),
     VLSTGUIA          NUMBER(18, 6),
     PERCIPI           NUMBER(12, 4),
     VLIPI             NUMBER(18, 6),
     CST_PISCOFINS     VARCHAR2(3),
     VLBASEPISCOFINS   NUMBER(18, 6),
     PERCPIS           NUMBER(12, 4),
     PERCCOFINS        NUMBER(12, 4),
     VLPIS             NUMBER(18, 6),
     VLCOFINS          NUMBER(18, 6),
     VLFRETE           NUMBER(18, 6),
     VLOUTRASDESP      NUMBER(18, 6),
     VLICMSDIFAL       NUMBER(18, 6),
     VLCMVGERENCIAL    NUMBER(18, 6),
     VLCMVCONTABIL     NUMBER(18, 6),
     DTCANCEL          DATE,
     DT_UPDATE         DATE,
     CONSTRAINT PK_NUMTRANSITEM PRIMARY KEY (NUMTRANSITEM)
  ) ';
  END IF;

  ----INDICES BI_SINC_MOV_PRODUTO
  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_MOV_PRODUTO'
     AND index_name = 'IDX_TIPOMOV';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TIPOMOV ON BI_SINC_MOV_PRODUTO (TIPOMOV)';
  END IF;

  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_MOV_PRODUTO'
     AND index_name = 'IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_01
  ON BI_SINC_MOV_PRODUTO (TIPOMOVGER, CODFILIAL, CODUSUR, NUMTRANSACAO, TEMVENDAORIG,
NUMNOTA, DATA, CFOP, CODPROD, CODCLI, CODFORNEC, CODCOB, MOVIMENTO)';
  END IF;

  ----BI_SINC_PRECO_VENDA
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_PRECO_VENDA';
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

  ----BI_SINC_PRECO_VENDA_PROMOCIONAL
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_PRECO_VENDA_PROMOCIONAL';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PRECO_VENDA_PROMOCIONAL
  (
     CODPRECOPROM     NUMBER(10),
     CODFILIAL        VARCHAR2(2),
     CODPROD          NUMBER(6),
     NUMREGIAO        NUMBER(4),
     CODATIVIDADE     NUMBER(6),
     PRECOPROMOCIONAL NUMBER(18, 6),
     DTINICIOPROMOCAO DATE,
     DTFIMPROMOCAO    DATE,
     ATIVO            VARCHAR(2),
     DT_UPDATE        DATE,
     CONSTRAINT PK_BI_SINC_PRECO_VENDA_PROMO PRIMARY KEY (CODPRECOPROM)
  ) ';
  END IF;

  ----BI_SINC_PEDIDO_COMPRA
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_PEDIDO_COMPRA';
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

  ----BI_SINC_LANC_PAGAR
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_LANC_PAGAR';
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

  ----BI_SINC_REGIAO
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_REGIAO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_REGIAO
  (
     NUMREGIAO NUMBER(4),
     REGIAO    VARCHAR2(40),
     DT_UPDATE DATE,
     CONSTRAINT PK_BI_SINC_REGIAO PRIMARY KEY (NUMREGIAO)
  ) ';
  END IF;

  ----BI_SINC_CALENDARIO
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_CALENDARIO';
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

  ----INDICES BI_SINC_CALENDARIO
  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_CALENDARIO'
     AND index_name = 'CALENDARIO_IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX CALENDARIO_IDX_01
  ON BI_SINC_CALENDARIO (ANO, NUM_TRIMESTRE_JC, NUM_MES_ANO)';
  END IF;

  ----BI_SINC_TABELAS
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_TABELAS';
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

  ----BI_SINC_META_VENDEDOR
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_META_VENDEDOR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_META_VENDEDOR
  (
     DATA      DATE,
     CODUSUR   NUMBER(4),
     VLMETA    NUMBER(18, 6),
     DT_UPDATE DATE,
     CONSTRAINT PK_META_VENDEDOR PRIMARY KEY (DATA, CODUSUR)
  ) ';
  END IF;

  ----BI_SINC_META_CLIENTE
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_META_CLIENTE';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_META_CLIENTE
  (
     DATA       DATE,
     CODCLI     NUMBER(6),
     CODCLIREDE VARCHAR2(10),
     CODUSUR    NUMBER(4),
     VLMETA     NUMBER(18, 6),
     DT_UPDATE  DATE,
     CONSTRAINT PK_META_CLIENTE PRIMARY KEY (DATA, CODCLI)
  ) ';
  END IF;

  ----BI_SINC_PEDIDO_VENDA
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_PEDIDO_VENDA';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PEDIDO_VENDA
  (
     CODFILIAL         VARCHAR2(2),
     CODFILIALRETIRA   VARCHAR2(2),
     DATA              DATE,
     DATALIMITE        DATE,
     NUMPED            NUMBER(10),
     TIPOVENDA         NUMBER(5),
     NUMSEQ            NUMBER(6),
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
     CONSTRAINT PK_PEDIDO_VENDA PRIMARY KEY (NUMPED, CODPROD, NUMSEQ)
  )  ';
  END IF;

  ----BI_SINC_PLANO_CONTAS_JC
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_PLANO_CONTAS_JC';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PLANO_CONTAS_JC
  (
     CODCLASSIFICA VARCHAR2(12),
     CONTA         VARCHAR2(60),
     NIVEL         NUMBER(1),
     TIPOCONTA     NUMBER(1),
     CODGERENCIAL  NUMBER(4),
     CODCONTABIL   NUMBER(5),
     CODBALANCO    NUMBER(1),
     CODDRE        NUMBER(2),
     CODEBTIDA     NUMBER(1),
     CONTAN1       VARCHAR2(30),
     CONTAN2       VARCHAR2(40),
     CONTAN3       VARCHAR2(60),
     CONTAN4       VARCHAR2(80),
     CONTAN5       VARCHAR2(80),
     DT_UPDATE     DATE,
     CONSTRAINT PK_PLANO_CONTAS_JC PRIMARY KEY (CODCLASSIFICA)
  ) ';
  END IF;

  ----INDICES BI_SINC_PLANO_CONTAS_JC
  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_PLANO_CONTAS_JC'
     AND index_name = 'PLANO_CONTAS_JC_IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX PLANO_CONTAS_JC_IDX_01
  ON BI_SINC_PLANO_CONTAS_JC (CODGERENCIAL, CODCONTABIL, CODDRE)';
  END IF;

  ----BI_SINC_DRE_JC
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_DRE_JC';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_DRE_JC
  (
     CODDRE      NUMBER(2),
     SUBCONTADRE VARCHAR2(60),
     SUBTOTAL    NUMBER(1),
     CONTADRE    VARCHAR2(60),
     GRUPODRE    VARCHAR2(60),
     DT_UPDATE   DATE,
     CONSTRAINT PK_DRE_JC PRIMARY KEY (CODDRE)
  ) ';
  END IF;

  ----BI_SINC_PLANO_CONTAS_ESTILO
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_PLANO_CONTAS_ESTILO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PLANO_CONTAS_ESTILO
  (
     IDCONTABIL    VARCHAR2(10),
     CONTA         VARCHAR2(200),
     CODCONTA      VARCHAR2(6),
     CONTAN1       VARCHAR2(150),
     CONTAN2       VARCHAR2(120),
     CONTAN3       VARCHAR2(80),
     CONTAN4       VARCHAR2(60),
     DT_UPDATE     DATE,
     CONSTRAINT PK_PLANO_CONTAS_ESTILO PRIMARY KEY (IDCONTABIL)
  ) ';
  END IF;

  ----BI_SINC_PARAMETROS_GLOBAL
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_PARAMETROS_GLOBAL';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PARAMETROS_GLOBAL
  (
     PARAMETRO VARCHAR2(60),
     VALOR     VARCHAR2(24),
     DESCRICAO VARCHAR2(128),
     DT_UPDATE DATE,
     CONSTRAINT PK_PARAMETROS_GLOBAL PRIMARY KEY (PARAMETRO)
  ) ';
  END IF;

  ----BI_SINC_CENTRO_CUSTO
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_CENTRO_CUSTO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_CENTRO_CUSTO
  (
     CODCENTROCUSTO VARCHAR2(4),
     CENTROCUSTO    VARCHAR2(32),
     DT_UPDATE      DATE,
     CONSTRAINT PK_CENTRO_CUSTO PRIMARY KEY (CODCENTROCUSTO)
  ) ';
  END IF;

  ----BI_SINC_FORNECEDOR_CONTABIL
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_FORNECEDOR_CONTA';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_FORNECEDOR_CONTA
  (
     CODEMPRESA    VARCHAR2(2),
     CODFORNEC     NUMBER(6),
     CODCONTABIL   NUMBER(5),
     CODCLASSIFICA VARCHAR2(12),
     DT_UPDATE      DATE,
     CONSTRAINT PK_FORNECEDOR_CONTA PRIMARY KEY (CODEMPRESA, CODFORNEC, CODCONTABIL)
  )';
  END IF;

  ----BI_SINC_CONTABILIDADE
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_CONTABILIDADE';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_CONTABILIDADE
  (
     CODLANC         VARCHAR2(40),
     CODEMPRESA      VARCHAR2(2),
     DATA            DATE,
     TIPOLANCAMENTO  NUMBER(1),
     IDENTIFICADOR   NUMBER(10),
     DOCUMENTO       NUMBER(10),
     OPERACAO        VARCHAR2(1),
     CODGERENCIAL    NUMBER(6),
     CODCC           VARCHAR2(4),
     CODDRE          NUMBER(2),
     CODCONTABIL     NUMBER(6),
     IDGERENCIAL     VARCHAR2(10),
     IDCONTABIL      VARCHAR2(10),
     ATIVIDADE       VARCHAR2(350),
     HISTORICO       VARCHAR2(450),
     VALOR           NUMBER(15, 2),
     ORIGEM          VARCHAR2(80),
     ENVIAR_CONTABIL VARCHAR2(1),
     DT_UPDATE       DATE,
     CONSTRAINT PK_CONTABILIDADE PRIMARY KEY (CODLANC, OPERACAO)
  )';
  END IF;

  ----INDICES BI_SINC_CONTABILIDADE
  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_CONTABILIDADE'
     AND index_name = 'CONTABILIDADE_IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX CONTABILIDADE_IDX_01
  ON BI_SINC_CONTABILIDADE (DATA, CODCC, TIPOLANCAMENTO, IDENTIFICADOR, DOCUMENTO, CODGERENCIAL, CODDRE,
  CODCONTABIL, IDGERENCIAL, IDCONTABIL, ATIVIDADE, ORIGEM, ENVIAR_CONTABIL)';
  END IF;

  ----BI_SINC_DESPESA_FISCAL
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_DESPESA_FISCAL';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_DESPESA_FISCAL
  (
     CODEMPRESA      VARCHAR2(2),
     CODFILIAL       VARCHAR2(2),
     DATA            DATE,
     ANO             NUMBER(4),
     NUMTRANSENT     NUMBER(10),
     NUMNOTA         NUMBER(10),
     CODCONTA        NUMBER(10),
     CODFORNEC       NUMBER(6),
     FORNECEDOR      VARCHAR2(80),
     ESPECIE         VARCHAR2(2),
     CFOP            NUMBER(8),
     VALOR           NUMBER(12, 2),
     CST_ICMS        VARCHAR2(3),
     VLBASEICMS      NUMBER(18, 6),
     PERCICMS        NUMBER(12, 4),
     VLICMS          NUMBER(18, 6),
     CST_PISCOFINS   VARCHAR2(3),
     VLBASEPISCOFINS NUMBER(18, 6),
     PERCPIS         NUMBER(12, 4),
     PERCCOFINS      NUMBER(12, 4),
     VLPIS           NUMBER(18, 6),
     VLCOFINS        NUMBER(18, 6),
     VLDIFAL         NUMBER(12, 2),
     DT_UPDATE       DATE,
     CONSTRAINT PK_DESPESA_FISCAL PRIMARY KEY (NUMTRANSENT)
  )';
  END IF;

  ----INDICES BI_SINC_DESPESA_FISCAL
  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_DESPESA_FISCAL'
     AND index_name = 'DESPESA_FISCAL_IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX DESPESA_FISCAL_IDX_01
  ON BI_SINC_DESPESA_FISCAL (CODEMPRESA, CODFILIAL, DATA, ANO, NUMNOTA, CODCONTA,
CODFORNEC, ESPECIE, CFOP, CST_ICMS, CST_PISCOFINS )';
  END IF;

  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_DESPESA_FISCAL'
     AND index_name = 'DESPESA_FISCAL_IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX DESPESA_FISCAL_IDX_01
  ON BI_SINC_DESPESA_FISCAL (ANO, NUMNOTA, CODFORNEC)';
  END IF;

  ----BI_SINC_LANC_PAGAR_BASE
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_LANC_PAGAR_BASE';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_LANC_PAGAR_BASE
  (
     CODEMPRESA         VARCHAR2(2),
     CODFILIAL          VARCHAR2(2),
     DTLANC             DATE,
     RECNUM             NUMBER(8),
     RECNUMPRINC        NUMBER(8),
     ADIANTAMENTO       VARCHAR2(1),
     ANO_COMPETENCIA    NUMBER(4),
     DTCOMPETENCIA      DATE,
     DTVENCIMENTO       DATE,
     DTVENCUTIL         DATE,
     CODFLUXO           NUMBER(2),
     CODCC              VARCHAR2(3),
     PERCRATEIO         NUMBER(10,6),
     VLRATEIO           NUMBER(12,2),
     VALOR              NUMBER(12,2),
     VLJUROS            NUMBER(12,2),
     VLDESCONTO         NUMBER(12,2),
     VLDEVOLUCAO        NUMBER(12,2),
     VLIMPOSTO          NUMBER(12,2),
     CODCONTA           NUMBER(6),
     GRUPOCONTA         NUMBER(4),
     CODFORNEC          NUMBER(8),
     TIPOPARCEIRO       VARCHAR2(1),
     NUMNOTADEV         NUMBER(10),
     NUMNOTA            NUMBER(10),
     HISTORICO          VARCHAR2(300),
     NUMTRANS           NUMBER(10),
     DTPAGAMENTO        DATE,
     DTCOMPENSACAO      DATE,
     CODBANCO           NUMBER(4),
     CONTABANCO         NUMBER(6),
     CODROTINABAIXA     NUMBER(6),
     DTESTORNOBAIXA     DATE,
     DT_UPDATE          DATE,
     CONSTRAINT PK_LANC_PAGAR_BASE PRIMARY KEY (RECNUM, CODCC)
  )';
  END IF;

  ----INDICES BI_SINC_LANC_PAGAR_BASE
  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_LANC_PAGAR_BASE'
     AND index_name = 'LANC_PAGAR_BASE_IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX LANC_PAGAR_BASE_IDX_01
  ON BI_SINC_LANC_PAGAR_BASE (CODEMPRESA, CODFILIAL, ANO_COMPETENCIA, DTLANC, RECNUMPRINC,
  DTCOMPETENCIA, CODCONTA, GRUPOCONTA, CODFORNEC, NUMNOTA, NUMTRANS, DTCOMPENSACAO,  CONTABANCO)';
  END IF;

  ----BI_SINC_LANC_RECEBER_BASE
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_LANC_RECEBER_BASE';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_LANC_RECEBER_BASE
  (
     CODEMPRESA       VARCHAR2(2),
     CODFILIAL        VARCHAR2(2),
     DTEMISSAO        DATE,
     DTDESD           DATE,
     DTESTORNO        DATE,
     DTVENCIMENTO     DATE,
     DTVENCUTIL       DATE,
     CODFLUXO         NUMBER(2),
     DIASVENCIDOS     NUMBER(6),
     CODINADIMPLENCIA NUMBER(2),
     HISTORICO        VARCHAR(180),
     CODCLI           NUMBER(9),
     CONTACLIENTE     NUMBER(6),
     CODCOBORIG       VARCHAR2(4),
     CODCOB           VARCHAR2(4),
     CODUSUR          NUMBER(4),
     NUMTRANSVENDA    NUMBER(10),
     NUMNOTA          NUMBER(10),
     PREST            VARCHAR2(2),
     VALOR            NUMBER(10, 2),
     VLDESCONTO       NUMBER(10, 2),
     VLJUROS          NUMBER(10, 2),
     VALORLIQ         NUMBER(10, 2),
     CARTORIO         VARCHAR2(3),
     PROTESTO         VARCHAR2(3),
     DTINCLUSAOMANUAL DATE,
     VLESTORNO        NUMBER(10, 2),
     VLRECEBIDO       NUMBER(10, 2),
     DTPAGAMENTO      DATE,
     NUMTRANS         NUMBER(8),
     DTCOMPENSACAO    DATE,
     CODBANCO         NUMBER(4),
     CONTABANCO       NUMBER(6),
     DT_UPDATE        DATE,
     CONSTRAINT PK_LANC_RECEBER_BASE PRIMARY KEY (NUMTRANSVENDA, PREST)
  )';
  END IF;

  ----INDICES BI_SINC_LANC_RECEBER_BASE
  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_LANC_RECEBER_BASE'
     AND index_name = 'LANC_RECEBER_BASE_IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX LANC_RECEBER_BASE_IDX_01
  ON BI_SINC_LANC_RECEBER_BASE (CODEMPRESA, CODFILIAL, DTVENCIMENTO, DTESTORNO, 
  DTVENCUTIL, CONTACLIENTE, CODUSUR, CODCOB, NUMNOTA, NUMTRANS, DTCOMPENSACAO, CONTABANCO)';
  END IF;

  ----BI_SINC_BANCO
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_BANCO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_BANCO
  (
     CODBANCO    NUMBER(4),
     BANCO       VARCHAR2(30),
     CODFILIAL   VARCHAR2(2),
     TIPO        VARCHAR2(1),
     FLUXOCX     VARCHAR2(1),
     OBSERVACAO  VARCHAR2(20),
     DT_UPDATE   DATE,
     CONSTRAINT PK_BANCO PRIMARY KEY (CODBANCO)
  ) ';
  END IF;

  ----BI_SINC_SALDO_BANCO
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_SALDO_BANCO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_SALDO_BANCO
  (
     CODFILIAL      VARCHAR2(2),
     CODBANCO       NUMBER(4),
     DATA           DATE,
     DATACOMPLETA   DATE,
     VLSALDO        NUMBER(16, 2),
     DTCONCIL       DATE,
     VLSALDOCONCIL  NUMBER(16, 2),
     DT_UPDATE      DATE,
     CONSTRAINT PK_SALDO_BANCO PRIMARY KEY (CODBANCO, DATA)
  ) ';
  END IF;

  ----BI_SINC_CREDITO_CLIENTE
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_CREDITO_CLIENTE';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_CREDITO_CLIENTE
  (
     CODEMPRESA         VARCHAR2(2),
     CODFILIAL          VARCHAR2(2),
     CODCLI             NUMBER(6),
     CODCLIDEV          NUMBER(6),
     CODIGO             NUMBER(10),
     NUMCRED            NUMBER(10),
     DTDESCONTO         DATE,
     DTESTORNO          DATE,
     DTCOMPENSACAO      DATE,
     CONTACLIENTE       NUMBER(6),
     CONTABANCO         NUMBER(6),
     VALOR              NUMBER(14, 2),
     VLMOVCR            NUMBER(14, 2),
     DUPLIC             NUMBER(10),
     NUMNOTADEV         NUMBER(10),
     NUMERARIO          VARCHAR2(1),
     CODROTINA          VARCHAR2(6),
     NUMTRANS           NUMBER(10),
     NUMTRANS_MN        NUMBER(10),
     NUMTRANSBAIXA      NUMBER(10),
     NUMTRANSVENDADESC  NUMBER(12),
     NUMTRANSENTDEVCLI  NUMBER(10),
     NUMLANC            NUMBER(8),
     NUMLANCBAIXA       NUMBER(8),
     DT_UPDATE          DATE,
     CONSTRAINT PK_CREDITO_CLIENTE PRIMARY KEY (CODIGO)
  )';
  END IF;

  ----INDICES BI_SINC_CREDITO_CLIENTE
  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_CREDITO_CLIENTE'
     AND index_name = 'CREDITO_CLIENTE_IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX CREDITO_CLIENTE_IDX_01
  ON BI_SINC_CREDITO_CLIENTE (CODEMPRESA, CODFILIAL, CODCLI, NUMCRED, 
  NUMERARIO, NUMTRANS, NUMTRANS_MN, NUMTRANSBAIXA, NUMTRANSVENDADESC, NUMTRANSENTDEVCLI, NUMLANC, NUMLANCBAIXA,
  DTDESCONTO, DTESTORNO, DTCOMPENSACAO)';
  END IF;

  ----BI_SINC_VERBA_FORNECEDOR
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_VERBA_FORNECEDOR';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_VERBA_FORNECEDOR
  (
     CODEMPRESA         VARCHAR2(2),
     CODFILIAL          VARCHAR2(2),
     CODFORNEC          NUMBER(6),
     NUMTRANSCRFOR      NUMBER(6),
     NUMVERBA           NUMBER(10),
     DTPAGVERBA         DATE,
     DTPAGLANC          DATE,
     DTCOMPENSACAO      DATE,
     CONTABANCO         NUMBER(6),
     VALOR              NUMBER(18, 2),
     CODROTINALANC      NUMBER(6),
     NUMTRANSENT        NUMBER(10),
     NUMTRANSVENDADEV   NUMBER(10),
     NUMNOTADEV         NUMBER(10),
     NUMNOTADESC        VARCHAR2(12),
     NUMLANC            NUMBER(8),
     NUMTRANS           NUMBER(8),
     DT_UPDATE          DATE,
     CONSTRAINT PK_VERBA_FORNECEDOR PRIMARY KEY (NUMTRANSCRFOR)
  )';
  END IF;

  ----INDICES BI_SINC_VERBA_FORNECEDOR
  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_VERBA_FORNECEDOR'
     AND index_name = 'VERBA_FORNECEDOR_IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX VERBA_FORNECEDOR_IDX_01
  ON BI_SINC_VERBA_FORNECEDOR (CODEMPRESA, CODFILIAL, CODFORNEC, CODROTINALANC, 
  NUMTRANSENT, NUMTRANSVENDADEV, NUMNOTADEV, NUMNOTADESC, NUMLANC, NUMTRANS,DTPAGVERBA, DTPAGLANC, DTCOMPENSACAO)';
  END IF;

  ----BI_SINC_MOV_BANCO
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_MOV_BANCO';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_MOV_BANCO
  (
     CODEMPRESA     VARCHAR2(2),
     CODFILIAL      VARCHAR2(2),
     NUMSEQ         NUMBER(6),
     NUMTRANS       NUMBER(10),
     DATA           DATE,
     DTCOMPENSACAO  DATE,
     CODCOB         VARCHAR2(4),
     CODBANCO       NUMBER(4),
     CONTABANCO     NUMBER(6),
     TIPO           VARCHAR2(1),
     VALOR          NUMBER(14, 2),
     HISTORICO      VARCHAR2(300),
     CODROTINALANC  NUMBER(6),
     DT_UPDATE      DATE,
     CONSTRAINT PK_MOV_BANCO PRIMARY KEY (NUMSEQ)
  )';
  END IF;

  ----INDICES BI_SINC_MOV_BANCO
  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_MOV_BANCO'
     AND index_name = 'MOV_BANCO_IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX MOV_BANCO_IDX_01
  ON BI_SINC_MOV_BANCO (CODEMPRESA, CODFILIAL, NUMTRANS, DATA, DTCOMPENSACAO, CONTABANCO, TIPO, CODROTINALANC)';
  END IF;

  ----BI_SINC_MOV_FOLHA
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'BI_SINC_MOV_FOLHA';
  IF v_table_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_MOV_FOLHA
  (
     CODLANC         VARCHAR2(40),
     CODEMPRESA      VARCHAR2(2),
     DATA            DATE,
     TIPOLANCAMENTO  NUMBER(1),
     IDENTIFICADOR   NUMBER(10),
     DOCUMENTO       NUMBER(10),
     CONTADEBITO     NUMBER(6),
     CONTACREDITO    NUMBER(6),
     CODCC_DEBITO    VARCHAR2(4),
     CODCC_CREDITO   VARCHAR2(4),
     ATIVIDADE       VARCHAR2(350),
     HISTORICO       VARCHAR2(450),
     VALOR           NUMBER(15, 2),
     ORIGEM          VARCHAR2(80),
     ENVIAR_CONTABIL VARCHAR2(1),
     DTCANCEL        DATE,
     DT_UPDATE       DATE,
     CONSTRAINT PK_MOV_FOLHA PRIMARY KEY (CODLANC, IDENTIFICADOR)
  )';
  END IF;

  ----INDICES BI_SINC_MOV_FOLHA
  SELECT COUNT(*)
    INTO v_index_exists
    FROM user_indexes
   WHERE table_name = 'BI_SINC_MOV_FOLHA'
     AND index_name = 'MOV_FOLHA_IDX_01';
  IF v_index_exists = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX MOV_FOLHA_IDX_01
  ON BI_SINC_MOV_FOLHA (DATA, TIPOLANCAMENTO, DOCUMENTO, CONTADEBITO, CONTACREDITO, ATIVIDADE, ORIGEM, ENVIAR_CONTABIL)';
  END IF;


END;
