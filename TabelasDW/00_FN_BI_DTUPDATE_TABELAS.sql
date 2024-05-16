CREATE OR REPLACE FUNCTION FN_BI_DTUPDATE_TABELAS(pTabela VARCHAR2)
  RETURN DATE IS
  v_maior_data_update DATE;
  v_sql               VARCHAR2(1000);
BEGIN
  v_sql := 'SELECT MAX(DT_UPDATE) FROM ' || pTabela;

  EXECUTE IMMEDIATE v_sql
    INTO v_maior_data_update;

  RETURN v_maior_data_update;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN NULL;
  WHEN OTHERS THEN
    -- Tratamento de erro geral
    RETURN NULL;
END;
