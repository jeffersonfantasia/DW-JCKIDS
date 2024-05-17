CREATE OR REPLACE PROCEDURE PRC_CRIA_AREACOMERCIAL AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_JFAREACOMERCIAL
    (CODAREA, AREACOMERCIAL)
        SELECT 1,
           'DISTRIBUICAO'
      FROM DUAL
    UNION ALL
    SELECT 2,
           'CORPORATIVO'
      FROM DUAL
    UNION ALL
    SELECT 3,
           'VAREJO'
      FROM DUAL
    UNION ALL
    SELECT 4,
           'OUTROS'
      FROM DUAL;

  FOR temp_rec IN (SELECT * FROM TEMP_JFAREACOMERCIAL)
  
  LOOP
    BEGIN
      UPDATE JFAREACOMERCIAL
         SET AREACOMERCIAL = temp_rec.AREACOMERCIAL
       WHERE CODAREA = temp_rec.CODAREA;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO JFAREACOMERCIAL
          (CODAREA, AREACOMERCIAL)
        VALUES
          (temp_rec.CODAREA, temp_rec.AREACOMERCIAL);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000,
                                'Erro durante a criação da tabela: ' ||
                                SQLERRM);
    END;
  END LOOP;

  COMMIT;
  -- Exclui os registros da tabela temporária TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_JFAREACOMERCIAL';
END;
