CREATE OR REPLACE PROCEDURE PRC_SINC_CENTRO_CUSTO AS
BEGIN

  FOR r IN (WITH CENTROCUSTO AS
               (SELECT CODIGOCENTROCUSTO CODCENTROCUSTO,
                      (CODIGOCENTROCUSTO || ' - ' || DESCRICAO) CENTROCUSTO
                 FROM PCCENTROCUSTO
               UNION
               SELECT '0' CODCENTROCUSTO,
                      '0 - SEM CC' CENTROCUSTO
                 FROM DUAL)
              
              SELECT C.*
                FROM CENTROCUSTO C
                LEFT JOIN BI_SINC_CENTRO_CUSTO S ON S.CODCENTROCUSTO = C.CODCENTROCUSTO
               WHERE S.DT_UPDATE IS NULL
                  OR S.CENTROCUSTO <> C.CENTROCUSTO)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_CENTRO_CUSTO
         SET CENTROCUSTO = r.CENTROCUSTO,
             DT_UPDATE   = SYSDATE
       WHERE CODCENTROCUSTO = r.CODCENTROCUSTO;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_CENTRO_CUSTO
          (CODCENTROCUSTO,
           CENTROCUSTO,
           DT_UPDATE)
        VALUES
          (r.CODCENTROCUSTO,
           r.CENTROCUSTO,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

END;
