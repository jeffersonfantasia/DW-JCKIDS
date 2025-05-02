CREATE OR REPLACE PROCEDURE PRC_SINC_HIST_ESTOQUE AS

  -----------------------DATAS DE ATUALIZACAO
  vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 7;
  --vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

BEGIN
  FOR r IN (WITH ESTOQUE AS
               (SELECT E.DATA,
                      E.CODFILIAL,
                      E.CODPROD,
                      NVL(E.QTEST, 0) QTCONTABIL,
                      NVL(E.QTESTGER, 0) QTGERENCIAL,
                      ROUND(ROUND(E.QTEST, 3) * ROUND(E.CUSTOCONT, 3), 2) VLESTOQUECONTABIL,
                      ROUND(ROUND(E.QTESTGER, 3) * ROUND(E.CUSTOFIN, 3), 2) VLESTOQUEFINANCEIRO,
                      ROUND(ROUND(E.QTESTGER, 3) * ROUND(E.CUSTOREP, 3), 2) VLESTOQUEGERENCIAL
                 FROM PCHISTEST E
                WHERE E.DATA >= vDATA_MOV_INCREMENTAL
                  AND NVL(E.TIPOMERCDEPTO, 'X') <> 'IM'
                  AND NVL(E.TIPOMERCDEPTO, 'X') <> 'CI'
                  AND NVL(E.TIPOMERC, 'X') NOT IN ('MC', 'ME', 'PB')
                  AND ((ROUND(E.QTEST, 3) > 0 OR ROUND(E.QTESTGER, 3) > 0)))
              
              SELECT E.*
                FROM ESTOQUE E
                LEFT JOIN BI_SINC_HIST_ESTOQUE S ON S.DATA = E.DATA
                                                AND S.CODFILIAL = E.CODFILIAL
                                                AND S.CODPROD = E.CODPROD
               WHERE S.DT_UPDATE IS NULL
                  OR S.QTCONTABIL <> E.QTCONTABIL
                  OR S.QTGERENCIAL <> E.QTGERENCIAL
                  OR S.VLESTOQUECONTABIL <> E.VLESTOQUECONTABIL
                  OR S.VLESTOQUEFINANCEIRO <> E.VLESTOQUEFINANCEIRO
                  OR S.VLESTOQUEGERENCIAL <> E.VLESTOQUEGERENCIAL)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_HIST_ESTOQUE
         SET QTCONTABIL          = r.QTCONTABIL,
             QTGERENCIAL         = r.QTGERENCIAL,
             VLESTOQUECONTABIL   = r.VLESTOQUECONTABIL,
             VLESTOQUEFINANCEIRO = r.VLESTOQUEFINANCEIRO,
             VLESTOQUEGERENCIAL  = r.VLESTOQUEGERENCIAL,
             DT_UPDATE           = SYSDATE
       WHERE DATA = r.DATA
         AND CODFILIAL = r.CODFILIAL
         AND CODPROD = r.CODPROD;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_HIST_ESTOQUE
          (DATA,
           CODFILIAL,
           CODPROD,
           QTCONTABIL,
           QTGERENCIAL,
           VLESTOQUECONTABIL,
           VLESTOQUEFINANCEIRO,
           VLESTOQUEGERENCIAL,
           DT_UPDATE)
        VALUES
          (r.DATA,
           r.CODFILIAL,
           r.CODPROD,
           r.QTCONTABIL,
           r.QTGERENCIAL,
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
