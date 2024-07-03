CREATE OR REPLACE PROCEDURE PRC_SINC_PRECO_VENDA_PROMOCIONAL AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PRECO_VENDA_PROMOCIONAL
    (CODPRECOPROM,
     CODFILIAL,
     CODPROD,
     NUMREGIAO,
     CODATIVIDADE,
     PRECOPROMOCIONAL,
     DTINICIOPROMOCAO,
     DTFIMPROMOCAO,
     ATIVO)
  
    WITH PRECO AS
     (SELECT M.CODPRECOPROM,
             M.CODFILIAL,
             M.CODPROD,
             M.NUMREGIAO,
             NVL(M.CODATIV, 0) CODATIVIDADE,
             M.PRECOFIXO PRECOPROMOCIONAL,
             M.DTINICIOVIGENCIA DTINICIOPROMOCAO,
             M.DTFIMVIGENCIA DTFIMPROMOCAO,
             (CASE
               WHEN M.DTINICIOVIGENCIA <= TRUNC(SYSDATE) AND
                    M.DTFIMVIGENCIA >= TRUNC(SYSDATE) THEN
                'S'
               ELSE
                'N'
             END) ATIVO
        FROM PCPRECOPROM M
        JOIN BI_SINC_PRODUTO P ON P.CODPROD = M.CODPROD)
    SELECT P.*
      FROM PRECO P
      LEFT JOIN BI_SINC_PRECO_VENDA_PROMOCIONAL S ON S.CODPRECOPROM = P.CODPRECOPROM
     WHERE S.DT_UPDATE IS NULL
        OR S.CODFILIAL <> P.CODFILIAL
        OR S.CODPROD <> P.CODPROD
        OR S.NUMREGIAO <> P.NUMREGIAO
        OR S.CODATIVIDADE <> P.CODATIVIDADE
        OR S.PRECOPROMOCIONAL <> P.PRECOPROMOCIONAL
        OR S.DTINICIOPROMOCAO <> P.DTINICIOPROMOCAO
        OR S.DTFIMPROMOCAO <> P.DTFIMPROMOCAO
        OR S.ATIVO <> P.ATIVO;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condi��es mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PRECO_VENDA_PROMOCIONAL)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_PRECO_VENDA_PROMOCIONAL
         SET CODFILIAL        = temp_rec.CODFILIAL,
             CODPROD          = temp_rec.CODPROD,
             NUMREGIAO        = temp_rec.NUMREGIAO,
             CODATIVIDADE     = temp_rec.CODATIVIDADE,
             PRECOPROMOCIONAL = temp_rec.PRECOPROMOCIONAL,
             DTINICIOPROMOCAO = temp_rec.DTINICIOPROMOCAO,
             DTFIMPROMOCAO    = temp_rec.DTFIMPROMOCAO,
             ATIVO            = temp_rec.ATIVO
       WHERE CODPRECOPROM = temp_rec.CODPRECOPROM;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_PRECO_VENDA_PROMOCIONAL
          (CODPRECOPROM,
           CODFILIAL,
           CODPROD,
           NUMREGIAO,
           CODATIVIDADE,
           PRECOPROMOCIONAL,
           DTINICIOPROMOCAO,
           DTFIMPROMOCAO,
           ATIVO,
           DT_UPDATE)
        VALUES
          (temp_rec.CODPRECOPROM,
           temp_rec.CODFILIAL,
           temp_rec.CODPROD,
           temp_rec.NUMREGIAO,
           temp_rec.CODATIVIDADE,
           temp_rec.PRECOPROMOCIONAL,
           temp_rec.DTINICIOPROMOCAO,
           temp_rec.DTFIMPROMOCAO,
           temp_rec.ATIVO,
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

  -- Exclui os registros da tabela tempor�ria TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_PRECO_VENDA_PROMOCIONAL';
END;
