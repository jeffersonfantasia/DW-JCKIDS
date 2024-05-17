CREATE OR REPLACE FUNCTION FN_BI_QTREGISTROS_TABELAS(pTabela VARCHAR2)
  RETURN NUMBER IS
  v_qt_registros NUMBER;
  v_sql          VARCHAR2(1000);
BEGIN
  v_sql := 'SELECT COUNT(*) FROM ' || pTabela;

  EXECUTE IMMEDIATE v_sql
    INTO v_qt_registros;

  RETURN v_qt_registros;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN NULL;
  WHEN OTHERS THEN
    -- Tratamento de erro geral
    RETURN NULL;
END;
