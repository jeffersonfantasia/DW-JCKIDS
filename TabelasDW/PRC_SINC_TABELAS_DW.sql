CREATE OR REPLACE PROCEDURE PRC_SINC_TABELAS_DW AS
  v_table_exists NUMBER;
BEGIN
  ----BI_SINC_MARCA
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_MARCA';
  IF v_table_exists = 0
  THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_MARCA (
            CODMARCA NUMBER(8),
            MARCA VARCHAR2(40),
            ATIVO VARCHAR2(1),
            DT_UPDATE DATE,
            DT_SINC DATE,
            CONSTRAINT PK_CODMARCA PRIMARY KEY (CODMARCA)
        )';
  END IF;

  ----TEMP_PCMARCA
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PCMARCA';
  IF v_table_exists = 0
  THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PCMARCA (
		        CODMARCA NUMBER(8),
            MARCA VARCHAR2(40),
            ATIVO VARCHAR2(1)
		) ON COMMIT PRESERVE ROWS';
  END IF;

  ----BI_SINC_FILIAL
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_FILIAL';
  IF v_table_exists = 0
  THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_FILIAL (
            CODFILIAL VARCHAR2(2),
            EMPRESA VARCHAR2(150),
            FILIAL VARCHAR2(25),
            DT_UPDATE DATE,
            DT_SINC DATE,
            CONSTRAINT PK_CODFILIAL PRIMARY KEY (CODFILIAL)
        )';
  END IF;

  ----TEMP_PCFILIAL
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PCFILIAL';
  IF v_table_exists = 0
  THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PCFILIAL (
				CODFILIAL VARCHAR2(2),
				EMPRESA VARCHAR2(150),
				FILIAL VARCHAR2(25)
		) ON COMMIT PRESERVE ROWS';
  END IF;

  ----BI_SINC_PRODUTO
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINC_PRODUTO';
  IF v_table_exists = 0
  THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINC_PRODUTO (
            CODPROD NUMBER(6),
            PRODUTO VARCHAR2(40),
            CODDEPTO NUMBER(6),
            DEPARTAMENTO VARCHAR2(25),
						CODSECAO NUMBER(6),
						SECAO VARCHAR2(40),
						CODCATEGORIA NUMBER(6),
						CATEGORIA VARCHAR2(40),
						CODLINHA NUMBER(6),
						LINHA VARCHAR2(40),
						CODFORNEC NUMBER(6),
						CODMARCA NUMBER(8),
						CODFAB VARCHAR2(30),
						CODBARRAS NUMBER(20),
						CODBARRASMASTER NUMBER(14),
						PESO NUMBER(12,6),
						LARGURA NUMBER(10,6),
						ALTURA NUMBER(10,6),
						COMPRIMENTO NUMBER(10,6),
						VOLUME NUMBER(20,8),
						QTCXMASTER NUMBER(8,2),
						IMPORTADO VARCHAR2(1),
						REVENDA VARCHAR2(1),
						NCM VARCHAR2(15),
						NCMEX VARCHAR2(20),
						TIPOMERCADORIA VARCHAR2(2),
						FORALINHA VARCHAR2(2),
						CERTIFICACAO VARCHAR2(200),
						DTEXCLUSAO DATE,
						DT_UPDATE DATE,
            DT_SINC DATE,
            CONSTRAINT PK_CODPROD PRIMARY KEY (CODPROD)
        )';
  END IF;

  ----TEMP_PCPRODUT
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PCPRODUT';
  IF v_table_exists = 0
  THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PCPRODUT (
        CODPROD NUMBER(6),
            PRODUTO VARCHAR2(40),
            CODDEPTO NUMBER(6),
            DEPARTAMENTO VARCHAR2(25),
            CODSECAO NUMBER(6),
            SECAO VARCHAR2(40),
            CODCATEGORIA NUMBER(6),
            CATEGORIA VARCHAR2(40),
            CODLINHA NUMBER(6),
            LINHA VARCHAR2(40),
            CODFORNEC NUMBER(6),
            CODMARCA NUMBER(8),
            CODFAB VARCHAR2(30),
            CODBARRAS NUMBER(20),
            CODBARRASMASTER NUMBER(14),
            PESO NUMBER(12,6),
            LARGURA NUMBER(10,6),
            ALTURA NUMBER(10,6),
            COMPRIMENTO NUMBER(10,6),
            VOLUME NUMBER(20,8),
            QTCXMASTER NUMBER(8,2),
            IMPORTADO VARCHAR2(1),
            REVENDA VARCHAR2(1),
            NCM VARCHAR2(15),
            NCMEX VARCHAR2(20),
            TIPOMERCADORIA VARCHAR2(2),
            FORALINHA VARCHAR2(2),
            CERTIFICACAO VARCHAR2(200),
            DTEXCLUSAO DATE
    ) ON COMMIT PRESERVE ROWS';
  END IF;
END;
