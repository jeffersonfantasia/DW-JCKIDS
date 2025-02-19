CREATE OR REPLACE PROCEDURE PRC_SINC_BANCO AS
BEGIN

  FOR r IN (WITH BANCOS AS
               (SELECT B.CODBANCO,
                      B.NOME BANCO,
                      B.CODFILIAL,
                      B.TIPOCXBCO TIPO,
                      B.FLUXOCX,
                      B.CODBACEN OBSERVACAO
                 FROM PCBANCO B)
              
              SELECT B.*
                FROM BANCOS B
                LEFT JOIN BI_SINC_BANCO S ON S.CODBANCO = B.CODBANCO
               WHERE S.DT_UPDATE IS NULL
                  OR NVL(S.BANCO, '0') <> B.BANCO
                  OR NVL(S.CODFILIAL, '0') <> B.CODFILIAL
                  OR NVL(S.TIPO, '0') <> B.TIPO
                  OR NVL(S.FLUXOCX, '0') <> B.FLUXOCX
                  OR NVL(S.OBSERVACAO, '0') <> B.OBSERVACAO)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionada
  
  LOOP
    BEGIN
      UPDATE BI_SINC_BANCO
         SET BANCO      = r.BANCO,
             CODFILIAL  = r.CODFILIAL,
             TIPO       = r.TIPO,
             FLUXOCX    = r.FLUXOCX,
             OBSERVACAO = r.OBSERVACAO,
             DT_UPDATE  = SYSDATE
       WHERE CODBANCO = r.CODBANCO;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_BANCO
          (CODBANCO,
           BANCO,
           CODFILIAL,
           TIPO,
           FLUXOCX,
           OBSERVACAO,
           DT_UPDATE)
        VALUES
          (r.CODBANCO,
           r.BANCO,
           r.CODFILIAL,
           r.TIPO,
           r.FLUXOCX,
           r.OBSERVACAO,
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
