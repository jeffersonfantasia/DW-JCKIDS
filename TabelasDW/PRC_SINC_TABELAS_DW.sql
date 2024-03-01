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

END;
