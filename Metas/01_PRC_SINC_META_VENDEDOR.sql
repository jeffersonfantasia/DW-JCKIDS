CREATE OR REPLACE PROCEDURE PRC_SINC_META_VENDEDOR AS
BEGIN
  FOR r IN (
            
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
                  OR S.VLMETA <> M.VLMETA)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_META_VENDEDOR
         SET VLMETA    = r.VLMETA,
             DT_UPDATE = SYSDATE
       WHERE DATA = r.DATA
         AND CODUSUR = r.CODUSUR;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_META_VENDEDOR
          (DATA,
           CODUSUR,
           VLMETA,
           DT_UPDATE)
        VALUES
          (r.DATA,
           r.CODUSUR,
           r.VLMETA,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

  --EXCLUIR REGISTROS QUE NAO PERTENCEM MAIS A PCPEDI
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_META_VENDEDOR
 WHERE (DATA, CODUSUR) IN (SELECT S.DATA,
                                  S.CODUSUR
                             FROM BI_SINC_META_VENDEDOR S
                             LEFT JOIN PCMETARCA I ON S.DATA = I.DATA
                                                  AND S.CODUSUR = I.CODUSUR
                            WHERE I.DATA IS NULL)';
    COMMIT;
  END;
END;
