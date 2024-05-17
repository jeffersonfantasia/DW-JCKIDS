CREATE OR REPLACE PROCEDURE PRC_SINC_TABELAS_UPDATE AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_TABELAS
    (TABELA,
     QTREGISTROS,
     MAIOR_DTUPDATE,
     LAST_REFRESH)
    WITH TABELAS AS
     (SELECT TABLE_NAME TABELA,
             FN_BI_QTREGISTROS_TABELAS(TABLE_NAME) QTREGISTROS,
             FN_BI_DTUPDATE_TABELAS(TABLE_NAME) MAIOR_DTUPDATE,
             SYSDATE LAST_REFRESH
        FROM USER_TABLES T
       WHERE TABLE_NAME LIKE 'BI_SINC%'
       ORDER BY MAIOR_DTUPDATE DESC)
    SELECT T.*
      FROM TABELAS T
      LEFT JOIN BI_SINC_TABELAS S ON S.TABELA = T.TABELA
     WHERE S.DT_UPDATE IS NULL
        OR S.QTREGISTROS <> T.QTREGISTROS
        OR S.MAIOR_DTUPDATE <> T.MAIOR_DTUPDATE;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_TABELAS)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_TABELAS
         SET QTREGISTROS  = temp_rec.QTREGISTROS,
             MAIOR_DTUPDATE = temp_rec.MAIOR_DTUPDATE,
             LAST_REFRESH = temp_rec.LAST_REFRESH
       WHERE TABELA = temp_rec.TABELA;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_TABELAS
          (TABELA,
           QTREGISTROS,
           MAIOR_DTUPDATE,
           LAST_REFRESH,
           DT_UPDATE)
        VALUES
          (temp_rec.TABELA,
           temp_rec.QTREGISTROS,
           temp_rec.MAIOR_DTUPDATE,
           temp_rec.LAST_REFRESH,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000,
                                'Erro durante a insercao na tabela: ' ||
                                SQLERRM);
    END;
  END LOOP;

  COMMIT;

  -- Exclui os registros da tabela temporária TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_TABELAS';
END;
