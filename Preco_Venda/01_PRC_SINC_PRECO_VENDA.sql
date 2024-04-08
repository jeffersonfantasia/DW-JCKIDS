CREATE OR REPLACE PROCEDURE PRC_SINC_PRECO_VENDA AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PRECO_VENDA
    (CODPROD,
     NUMREGIAO,
     PRECOVENDA,
     MARGEMIDEAL)
    WITH PRECO_VENDA AS
     (SELECT P.CODPROD,
             P.NUMREGIAO,
             P.PVENDA PRECOVENDA,
             P.MARGEM MARGEMIDEAL
        FROM PCTABPR P
        JOIN PCREGIAO R ON R.NUMREGIAO = P.NUMREGIAO
       WHERE NVL(P.PVENDA, 0) > 0)
    SELECT P.*
      FROM PRECO_VENDA P
      LEFT JOIN BI_SINC_PRECO_VENDA S ON S.CODPROD = P.CODPROD
                                     AND S.NUMREGIAO = P.NUMREGIAO
     WHERE S.DT_UPDATE IS NULL
        OR S.PRECOVENDA <> P.PRECOVENDA
        OR S.MARGEMIDEAL <> P.MARGEMIDEAL;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PRECO_VENDA)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_PRECO_VENDA
         SET PRECOVENDA  = temp_rec.PRECOVENDA,
             MARGEMIDEAL = temp_rec.MARGEMIDEAL
       WHERE CODPROD = temp_rec.CODPROD
         AND NUMREGIAO = temp_rec.NUMREGIAO;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_PRECO_VENDA
          (CODPROD,
           NUMREGIAO,
           PRECOVENDA,
           MARGEMIDEAL,
           DT_UPDATE)
        VALUES
          (temp_rec.CODPROD,
           temp_rec.NUMREGIAO,
           temp_rec.PRECOVENDA,
           temp_rec.MARGEMIDEAL,
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
  EXECUTE IMMEDIATE 'DELETE TEMP_PRECO_VENDA';
END;
