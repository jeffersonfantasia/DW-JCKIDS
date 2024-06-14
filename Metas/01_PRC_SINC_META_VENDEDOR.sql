CREATE OR REPLACE PROCEDURE PRC_SINC_META_VENDEDOR AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_META_VENDEDOR
    (DATA,
     CODUSUR,
     VLMETA)
    WITH META AS
     (SELECT M.DATA,
             M.CODUSUR,
             SUM(M.VLVENDAPREV) VLMETA
        FROM PCMETARCA M
       WHERE M.DATA >= TO_DATE('01/01/2024', 'DD/MM/YYYY')
       GROUP BY M.DATA,
                M.CODUSUR)
    
    SELECT M.*
      FROM META M
      LEFT JOIN BI_SINC_META_VENDEDOR S ON S.DATA = M.DATA
                                       AND S.CODUSUR = M.CODUSUR
     WHERE S.DT_UPDATE IS NULL
        OR S.VLMETA <> M.VLMETA;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_META_VENDEDOR)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_META_VENDEDOR
         SET VLMETA    = temp_rec.VLMETA,
             DT_UPDATE = SYSDATE
       WHERE DATA = temp_rec.DATA
         AND CODUSUR = temp_rec.CODUSUR;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_META_VENDEDOR
          (DATA,
           CODUSUR,
           VLMETA,
           DT_UPDATE)
        VALUES
          (temp_rec.DATA,
           temp_rec.CODUSUR,
           temp_rec.VLMETA,
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
  EXECUTE IMMEDIATE 'DELETE TEMP_META_VENDEDOR';
END;
