CREATE OR REPLACE PROCEDURE PRC_SINC_REGIAO AS
BEGIN

FOR temp_rec IN (
    SELECT R.NUMREGIAO,
           R.REGIAO
      FROM PCREGIAO R
      LEFT JOIN BI_SINC_REGIAO S ON S.NUMREGIAO = R.NUMREGIAO
     WHERE S.DT_UPDATE IS NULL
        OR S.REGIAO <> R.REGIAO
)

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_REGIAO
         SET REGIAO = temp_rec.REGIAO
       WHERE NUMREGIAO = temp_rec.NUMREGIAO;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_REGIAO
          (NUMREGIAO,
           REGIAO,
           DT_UPDATE)
        VALUES
          (temp_rec.NUMREGIAO,
           temp_rec.REGIAO,
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
