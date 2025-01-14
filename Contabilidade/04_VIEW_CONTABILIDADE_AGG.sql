CREATE OR REPLACE VIEW VIEW_CONTABILIDADE_AGG AS 
  WITH CONTABIL_AGG AS
   (SELECT CODEMPRESA,
           TO_DATE(TRUNC(TO_DATE(C.DATA, 'DD/MM/YYYY'), 'MM')) DATA,
           CODGERENCIAL,
           CODCONTABIL,
           CODDRE,
           CODCC,
           VALOR
      FROM BI_SINC_CONTABILIDADE C)
  
  SELECT CODEMPRESA,
         DATA,
         CODGERENCIAL,
         CODCONTABIL,
         CODDRE,
         CODCC,
         SUM(VALOR) VALOR
    FROM CONTABIL_AGG
   GROUP BY CODEMPRESA,
            DATA,
            CODGERENCIAL,
            CODCONTABIL,
            CODDRE,
            CODCC;