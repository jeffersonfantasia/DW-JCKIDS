CREATE OR REPLACE PROCEDURE PRC_SINC_PRECO_VENDA_PROMOCIONAL AS
BEGIN
FOR temp_rec IN (
  
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
        OR S.ATIVO <> P.ATIVO
)

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
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

  --EXCLUIR REGISTROS QUE NAO PERTENCEM MAIS A PCPRECOPROM
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_PRECO_VENDA_PROMOCIONAL
       WHERE (CODPRECOPROM) IN
             (SELECT S.CODPRECOPROM
                FROM BI_SINC_PRECO_VENDA_PROMOCIONAL S
                JOIN PCPRECOPROMLOG P ON S.CODPRECOPROM = P.CODPRECOPROM)';
  END;

	COMMIT;
END;
