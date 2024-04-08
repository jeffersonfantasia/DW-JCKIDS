CREATE OR REPLACE PROCEDURE PRC_SINC_PRECO_VENDA_PROMOCIONAL AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PRECO_VENDA_PROMOCIONAL
    (CODPRECOPROM,
     CODFILIAL,
     CODPROD,
     NUMREGIAO,
     PRECOPROMOCIONAL,
     DTINICIOPROMOCAO,
     DTFIMPROMOCAO)
    WITH PRECO AS
     (SELECT CODPRECOPROM,
             CODFILIAL,
             CODPROD,
             NUMREGIAO,
             PRECOFIXO PRECOPROMOCIONAL,
             DTINICIOVIGENCIA DTINICIOPROMOCAO,
             DTFIMVIGENCIA DTFIMPROMOCAO
        FROM PCPRECOPROM M
      WHERE DTINICIOVIGENCIA <= TRUNC(SYSDATE)
      AND DTFIMVIGENCIA >= TRUNC(SYSDATE)
      )
    SELECT P.*
      FROM PRECO P
      LEFT JOIN BI_SINC_PRECO_VENDA_PROMOCIONAL S ON S.CODPRECOPROM =
                                                     P.CODPRECOPROM
     WHERE S.DT_UPDATE IS NULL
        OR S.CODFILIAL <> P.CODFILIAL
        OR S.CODPROD <> P.CODPROD
        OR S.NUMREGIAO <> P.NUMREGIAO
        OR S.PRECOPROMOCIONAL <> P.PRECOPROMOCIONAL
        OR S.DTINICIOPROMOCAO <> P.DTINICIOPROMOCAO
        OR S.DTFIMPROMOCAO <> P.DTFIMPROMOCAO;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PRECO_VENDA_PROMOCIONAL)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_PRECO_VENDA_PROMOCIONAL
         SET CODFILIAL        = temp_rec.CODFILIAL,
             CODPROD          = temp_rec.CODPROD,
             NUMREGIAO        = temp_rec.NUMREGIAO,
             PRECOPROMOCIONAL = temp_rec.PRECOPROMOCIONAL,
             DTINICIOPROMOCAO = temp_rec.DTINICIOPROMOCAO,
             DTFIMPROMOCAO    = temp_rec.DTFIMPROMOCAO
       WHERE CODPRECOPROM = temp_rec.CODPRECOPROM;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_PRECO_VENDA_PROMOCIONAL
          (CODPRECOPROM,
           CODFILIAL,
           CODPROD,
           NUMREGIAO,
           PRECOPROMOCIONAL,
           DTINICIOPROMOCAO,
           DTFIMPROMOCAO,
           DT_UPDATE)
        VALUES
          (temp_rec.CODPRECOPROM,
           temp_rec.CODFILIAL,
           temp_rec.CODPROD,
           temp_rec.NUMREGIAO,
           temp_rec.PRECOPROMOCIONAL,
           temp_rec.DTINICIOPROMOCAO,
           temp_rec.DTFIMPROMOCAO,
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

  -- Exclui os registros vencidos que naão fazem mais parte da promocao ativa
  EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_PRECO_VENDA_PROMOCIONAL WHERE DTFIMPROMOCAO < TRUNC(SYSDATE)';

  -- Exclui os registros da tabela temporária TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_PRECO_VENDA_PROMOCIONAL';
END;
