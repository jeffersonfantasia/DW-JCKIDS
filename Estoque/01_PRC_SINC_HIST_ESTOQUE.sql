CREATE OR REPLACE PROCEDURE PRC_SINC_HIST_ESTOQUE AS


BEGIN
  FOR r IN (WITH ESTOQUE AS
               (SELECT E.DATA,
                      E.CODFILIAL,
                      SUM(ROUND(ROUND(E.QTEST, 6) * ROUND(E.CUSTOCONT, 6),2)) VLESTOQUECONTABIL,
                      SUM(ROUND(ROUND(E.QTESTGER, 6) * ROUND(E.CUSTOFIN, 6),2)) VLESTOQUEFINANCEIRO,
                      SUM(ROUND(ROUND(E.QTESTGER, 6) * ROUND(E.CUSTOREP, 6),2)) VLESTOQUEGERENCIAL
                 FROM PCHISTEST E
                WHERE 1 = 1
                  AND E.DATA >= TRUNC(SYSDATE) - 1
                  --AND E.DATA >= '01/01/2020'
                  AND NVL(E.TIPOMERCDEPTO, 'X') <> 'IM'
                  AND NVL(E.TIPOMERCDEPTO, 'X') <> 'CI'
                  AND NVL(E.TIPOMERC, 'X') NOT IN ('MC', 'ME', 'PB')
                  AND ROUND(E.QTEST, 6) > 0
                  AND ROUND(E.CUSTOCONT, 6) > 0
                GROUP BY E.DATA,
                         E.CODFILIAL
               UNION ALL
               SELECT TRUNC(SYSDATE) DATA,
                      E.CODFILIAL,
                      ROUND(SUM(VLESTOQUECONTABIL), 2) VLESTOQUECONTABIL,
                      ROUND(SUM(VLESTOQUEFINANCEIRO), 2) VLESTOQUEFINANCEIRO,
                      ROUND(SUM(VLESTOQUEGERENCIAL), 2) VLESTOQUEGERENCIAL
                 FROM BI_SINC_ESTOQUE E
                GROUP BY E.CODFILIAL)
              
              SELECT E.*
                FROM ESTOQUE E
                LEFT JOIN BI_SINC_HIST_ESTOQUE S ON S.DATA = E.DATA
                                                AND S.CODFILIAL = E.CODFILIAL
               WHERE S.DT_UPDATE IS NULL
                  OR S.VLESTOQUECONTABIL <> E.VLESTOQUECONTABIL
                  OR S.VLESTOQUEFINANCEIRO <> E.VLESTOQUEFINANCEIRO
                  OR S.VLESTOQUEGERENCIAL <> E.VLESTOQUEGERENCIAL)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_HIST_ESTOQUE
         SET VLESTOQUECONTABIL   = r.VLESTOQUECONTABIL,
             VLESTOQUEFINANCEIRO = r.VLESTOQUEFINANCEIRO,
             VLESTOQUEGERENCIAL  = r.VLESTOQUEGERENCIAL,
             DT_UPDATE           = SYSDATE
       WHERE DATA = r.DATA
         AND CODFILIAL = r.CODFILIAL;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_HIST_ESTOQUE
          (DATA,
           CODFILIAL,
           VLESTOQUECONTABIL,
           VLESTOQUEFINANCEIRO,
           VLESTOQUEGERENCIAL,
           DT_UPDATE)
        VALUES
          (r.DATA,
           r.CODFILIAL,
           r.VLESTOQUECONTABIL,
           r.VLESTOQUEFINANCEIRO,
           r.VLESTOQUEGERENCIAL,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

END;
