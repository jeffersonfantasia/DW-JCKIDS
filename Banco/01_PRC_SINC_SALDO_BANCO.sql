CREATE OR REPLACE PROCEDURE PRC_SINC_SALDO_BANCO AS

   -----------------------DATAS DE ATUALIZACAO
   vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 365;
   --vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY'); 

BEGIN

  FOR r IN (WITH CALENDARIO AS
               (SELECT C.DATA,
                      B.CODBANCO,
                      B.CODFILIAL
                 FROM BI_SINC_CALENDARIO C
                CROSS JOIN (SELECT CODBANCO,
                                  CODFILIAL
                             FROM PCBANCO) B
                WHERE C.DATA >= vDATA_MOV_INCREMENTAL
                  AND C.DATA <= TRUNC(SYSDATE)),
              
              ULTIMO_SALDO AS
               (SELECT *
                 FROM (SELECT M.CODBANCO,
                              M.DATA,
                              M.DATACOMPLETA,
                              M.VLSALDO,
                              ROW_NUMBER() OVER (PARTITION BY M.CODBANCO, M.DATA ORDER BY M.DATACOMPLETA DESC, M.NUMTRANS DESC) AS RN
                         FROM PCMOVCR M
                        WHERE M.CODCOB = 'D'
                          AND M.DTESTORNO IS NULL
                        ORDER BY M.CODBANCO,
                                 M.DATACOMPLETA DESC,
                                 M.NUMTRANS DESC)
                WHERE RN = 1),
              
              ULTIMO_SALDO_CONCILIADO AS
               (SELECT *
                 FROM (SELECT M.CODBANCO,
                              M.DTCONCIL,
                              M.VLSALDOCONCIL,
                              ROW_NUMBER() OVER (PARTITION BY M.CODBANCO, TO_DATE(M.DTCONCIL, 'DD/MM/YYYY') ORDER BY M.DTCONCIL DESC) AS RN
                         FROM PCMOVCR M
                        WHERE M.CODCOB = 'D'
                          AND M.DTESTORNO IS NULL
                          AND M.DTCONCIL IS NOT NULL
                        ORDER BY M.CODBANCO,
                                 M.DATACOMPLETA DESC)
                WHERE RN = 1),
              
              BANCOS AS
               (SELECT C.CODFILIAL,
                      C.CODBANCO,
                      C.DATA,
                      COALESCE(M.DATACOMPLETA,
                               LAG(M.DATACOMPLETA IGNORE NULLS) OVER(PARTITION BY C.CODBANCO ORDER BY C.DATA)) DATACOMPLETA,
                      COALESCE(M.VLSALDO, LAG(M.VLSALDO IGNORE NULLS) OVER(PARTITION BY C.CODBANCO ORDER BY C.DATA)) VLSALDO,
                      COALESCE(D.DTCONCIL, LAG(D.DTCONCIL IGNORE NULLS) OVER(PARTITION BY C.CODBANCO ORDER BY C.DATA)) AS DTCONCIL,
                      COALESCE(D.VLSALDOCONCIL,
                               LAG(D.VLSALDOCONCIL IGNORE NULLS) OVER(PARTITION BY C.CODBANCO ORDER BY C.DATA)) VLSALDOCONCIL
                 FROM CALENDARIO C
                 LEFT JOIN ULTIMO_SALDO M ON M.DATA = C.DATA
                                         AND M.CODBANCO = C.CODBANCO
                 LEFT JOIN ULTIMO_SALDO_CONCILIADO D ON D.CODBANCO = C.CODBANCO
                                                    AND C.DATA = TO_DATE(D.DTCONCIL, 'DD/MM/YYYY')
                 LEFT JOIN BI_SINC_BANCO B ON B.CODBANCO = M.CODBANCO
                 WHERE C.DATA >= vDATA_MOV_INCREMENTAL)
              
              SELECT B.*
                FROM BANCOS B
                LEFT JOIN BI_SINC_SALDO_BANCO S ON S.CODBANCO = B.CODBANCO
                                               AND S.DATA = B.DATA
               WHERE S.DT_UPDATE IS NULL
                  OR NVL(S.CODFILIAL, '0') <> B.CODFILIAL
                  OR NVL(S.DATACOMPLETA, TO_DATE('01/01/1889', 'DD/MM/YYYY')) <>
                     NVL(B.DATACOMPLETA, TO_DATE('01/01/1889', 'DD/MM/YYYY'))
                  OR NVL(S.VLSALDO, 0) <> NVL(B.VLSALDO, 0)
                  OR NVL(S.DTCONCIL, TO_DATE('01/01/1889', 'DD/MM/YYYY')) <>
                     NVL(B.DTCONCIL, TO_DATE('01/01/1889', 'DD/MM/YYYY'))
                  OR NVL(S.VLSALDOCONCIL, 0) <> NVL(B.VLSALDOCONCIL, 0))
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionada
  
  LOOP
    BEGIN
      UPDATE BI_SINC_SALDO_BANCO
         SET CODFILIAL     = r.CODFILIAL,
             DATACOMPLETA  = r.DATACOMPLETA,
             VLSALDO       = r.VLSALDO,
             DTCONCIL      = r.DTCONCIL,
             VLSALDOCONCIL = r.VLSALDOCONCIL,
             DT_UPDATE     = SYSDATE
       WHERE CODBANCO = r.CODBANCO
         AND DATA = r.DATA;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_SALDO_BANCO
          (CODFILIAL,
           CODBANCO,
           DATA,
           DATACOMPLETA,
           VLSALDO,
           DTCONCIL,
           VLSALDOCONCIL,
           DT_UPDATE)
        VALUES
          (r.CODFILIAL,
           r.CODBANCO,
           r.DATA,
           r.DATACOMPLETA,
           r.VLSALDO,
           r.DTCONCIL,
           r.VLSALDOCONCIL,
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
