CREATE OR REPLACE PROCEDURE PRC_SINC_CENTRO_CUSTO AS
BEGIN

  FOR temp_rec IN (WITH CENTROCUSTO AS
                      (SELECT CODIGOCENTROCUSTO CODCENTROCUSTO,
                             (CODIGOCENTROCUSTO || ' - ' || DESCRICAO) CENTROCUSTO
                        FROM PCCENTROCUSTO
                       WHERE LENGTH(CODIGOCENTROCUSTO) <= 3
                         AND RECEBE_LANCTO = 'S')
                     
                     SELECT C.*
                       FROM CENTROCUSTO C
                       LEFT JOIN BI_SINC_CENTRO_CUSTO S ON S.CODCENTROCUSTO =
                                                           C.CODCENTROCUSTO
                      WHERE S.DT_UPDATE IS NULL
                         OR S.CENTROCUSTO <> C.CENTROCUSTO)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_CENTRO_CUSTO
         SET CENTROCUSTO = temp_rec.CENTROCUSTO,
             DT_UPDATE   = SYSDATE
       WHERE CODCENTROCUSTO = temp_rec.CODCENTROCUSTO;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_CENTRO_CUSTO
          (CODCENTROCUSTO,
           CENTROCUSTO,
           DT_UPDATE)
        VALUES
          (temp_rec.CODCENTROCUSTO,
           temp_rec.CENTROCUSTO,
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

END;
