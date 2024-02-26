CREATE OR REPLACE PROCEDURE PCR_SINC_TABELAS_DW AS
  v_table_exists NUMBER;
BEGIN
  -- Verifica se a tabela BI_SINCMARCA existe e a cria se não existir
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'BI_SINCMARCA';
  IF v_table_exists = 0
  THEN
    EXECUTE IMMEDIATE 'CREATE TABLE BI_SINCMARCA (
            CODMARCA NUMBER(8),
            MARCA VARCHAR2(40),
            ATIVO VARCHAR2(1),
            DT_UPDATE DATE,
            DT_SINC DATE,
            DTSINC_ERRO DATE,
            MSG_ERRO VARCHAR2(200),
            CONSTRAINT PK_CODMARCA PRIMARY KEY (CODMARCA)
        )';
  END IF;

  -- Verifica se a tabela TEMP_PCMARCA existe e a cria se não existir
  SELECT COUNT(*)
    INTO v_table_exists
    FROM user_tables
   WHERE table_name = 'TEMP_PCMARCA';
  IF v_table_exists = 0
  THEN
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PCMARCA ON COMMIT PRESERVE ROWS AS
     SELECT M.CODMARCA, M.MARCA, M.ATIVO
       FROM PCMARCA M
       LEFT JOIN BI_SINCMARCA S ON S.CODMARCA = M.CODMARCA
      WHERE S.DT_UPDATE IS NULL
         OR M.MARCA <> S.MARCA
         OR M.ATIVO <> S.ATIVO';
  END IF;
END;

