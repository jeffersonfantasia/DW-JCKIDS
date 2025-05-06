CREATE OR REPLACE PROCEDURE PRC_SINC_APURACAO_CMV AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 90;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

  -----------------------ESTOQUE_31/12/2019
  vESTOQUE_DEZ19_F1 NUMBER := 449187.86;
  vESTOQUE_DEZ19_F2 NUMBER := 3557003.73;
  vESTOQUE_DEZ19_F7 NUMBER := 795570.26;
  --vDATA_INICIAL     DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

  -----------------------VARIAVEIS PARA CALCULO
  vVALOR_ESTOQUE_ANTERIOR NUMBER := 0;
  vVALOR_ESTOQUE_ATUAL    NUMBER;
  vVALOR_DEBITO           NUMBER;
  vVALOR_CREDITO          NUMBER;
  vVALOR_CMV              NUMBER;

BEGIN
  FOR r IN (WITH CONTABIL_AGG AS
               (SELECT S.CODFILIAL,
                      TO_DATE(TRUNC(TO_DATE(S.DATA, 'DD/MM/YYYY'), 'MM'), 'DD/MM/YYYY') DATA,
                      S.CODGERENCIAL,
                      S.OPERACAO,
                      S.VALOR
                 FROM BI_SINC_CONTABILIDADE S
                WHERE S.ORIGEM NOT IN ('APURA_CMV')
                  AND S.CODGERENCIAL IN (1174, 1187)),
              
              ESTOQUE_ACC AS
               (SELECT E.CODFILIAL,
                      E.DATA,
                      E.VLESTOQUECONTABIL
                 FROM BI_SINC_HIST_ESTOQUE E
                WHERE E.CODFILIAL NOT IN (3, 4)
                ORDER BY E.CODFILIAL,
                         E.DATA),
              
              CLASSIFICA_DATA_ESTOQUE AS
               (SELECT E.CODFILIAL,
                      TO_DATE(TRUNC(TO_DATE(E.DATA, 'DD/MM/YYYY'), 'MM'), 'DD/MM/YYYY') DATA,
                      E.VLESTOQUECONTABIL VALOR,
                      ROW_NUMBER() OVER (PARTITION BY E.CODFILIAL, TO_DATE(TRUNC(TO_DATE(E.DATA, 'DD/MM/YYYY'), 'MM'), 'DD/MM/YYYY') ORDER BY E.CODFILIAL, E.DATA DESC) AS RN
                 FROM ESTOQUE_ACC E),
              
              TIPOS_AGG AS
               (SELECT CODFILIAL,
                      DATA,
                      'ESTOQUE FIM' TIPO,
                      VALOR
                 FROM CLASSIFICA_DATA_ESTOQUE
                WHERE RN = 1
               UNION ALL
               SELECT S.CODFILIAL,
                      S.DATA,
                      'VLDEBITO' TIPO,
                      SUM(S.VALOR) VALOR
                 FROM CONTABIL_AGG S
                 JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = S.CODGERENCIAL
                WHERE S.OPERACAO = 'D'
                GROUP BY S.CODFILIAL,
                         S.DATA
               UNION ALL
               SELECT S.CODFILIAL,
                      S.DATA,
                      'VLCREDITO' TIPO,
                      SUM(S.VALOR * -1) VALOR
                 FROM CONTABIL_AGG S
                 JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = S.CODGERENCIAL
                WHERE S.OPERACAO = 'C'
                GROUP BY S.CODFILIAL,
                         S.DATA),
              
              TIPOS_AGG_PIVOT AS
               (SELECT *
                 FROM (SELECT CODFILIAL,
                              DATA,
                              TIPO,
                              SUM(VALOR) VALOR
                         FROM TIPOS_AGG
                        WHERE VALOR IS NOT NULL
                        GROUP BY CODFILIAL,
                                 DATA,
                                 TIPO)
               PIVOT(SUM(VALOR)
                  FOR TIPO IN('VLDEBITO' AS VLDEBITO, 'VLCREDITO' AS VLCREDITO, 'ESTOQUE FIM' AS ESTOQUEFIM))),
              
              APURA_CMV AS
               (SELECT ROW_NUMBER() OVER (PARTITION BY CODFILIAL ORDER BY CODFILIAL, DATA) RN,
                      CODFILIAL,
                      DATA,
                      NVL(VLDEBITO, 0) VLDEBITO,
                      NVL(VLCREDITO, 0) VLCREDITO,
                      NVL(ESTOQUEFIM, 0) ESTOQUEFIM
                 FROM TIPOS_AGG_PIVOT
                ORDER BY CODFILIAL,
                         DATA)
              
              SELECT M.*
                FROM APURA_CMV M
               WHERE M.DATA >= vDATA_MOV_INCREMENTAL
               ORDER BY CODFILIAL,
                        M.RN)
  
  LOOP
    BEGIN
      IF r.RN = 1 THEN
        CASE r.CODFILIAL
          WHEN '1' THEN
            vVALOR_ESTOQUE_ANTERIOR := vESTOQUE_DEZ19_F1;
          WHEN '2' THEN
            vVALOR_ESTOQUE_ANTERIOR := vESTOQUE_DEZ19_F2;
          WHEN '7' THEN
            vVALOR_ESTOQUE_ANTERIOR := vESTOQUE_DEZ19_F7;
          ELSE
            vVALOR_ESTOQUE_ANTERIOR := 0;
        END CASE;
      ELSE
        vVALOR_ESTOQUE_ANTERIOR := vVALOR_ESTOQUE_ATUAL;
      END IF;
    
      -- ATRIBUI OS RESULTADOS DA CONSULTA AS VARIAVEIS
      vVALOR_DEBITO        := r.VLDEBITO;
      vVALOR_CREDITO       := r.VLCREDITO;
      vVALOR_ESTOQUE_ATUAL := r.ESTOQUEFIM;
    
      -- CALCULO DO CMV 
      vVALOR_CMV := NVL(vVALOR_ESTOQUE_ANTERIOR, 0) + NVL(vVALOR_DEBITO, 0) - NVL(vVALOR_CREDITO, 0) -
                    NVL(vVALOR_ESTOQUE_ATUAL, 0);
    
      -- FAZ O UPSERT
      UPDATE BI_SINC_APURACAO_CMV
         SET VLESTOQUEANT = vVALOR_ESTOQUE_ANTERIOR,
             VLDEBITO     = vVALOR_DEBITO,
             VLCREDITO    = vVALOR_CREDITO,
             VLESTOQUEFIM = vVALOR_ESTOQUE_ATUAL,
             VLCMV        = vVALOR_CMV,
             DT_UPDATE    = SYSDATE
       WHERE DATA = r.DATA
         AND CODFILIAL = r.CODFILIAL;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_APURACAO_CMV
          (CODFILIAL,
           DATA,
           VLESTOQUEANT,
           VLDEBITO,
           VLCREDITO,
           VLESTOQUEFIM,
           VLCMV,
           DT_UPDATE)
        VALUES
          (r.CODFILIAL,
           r.DATA,
           vVALOR_ESTOQUE_ANTERIOR,
           vVALOR_DEBITO,
           vVALOR_CREDITO,
           vVALOR_ESTOQUE_ATUAL,
           vVALOR_CMV,
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
