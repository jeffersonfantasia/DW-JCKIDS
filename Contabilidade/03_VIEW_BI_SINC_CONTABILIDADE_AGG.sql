CREATE OR REPLACE VIEW VIEW_BI_SINC_CONTABILIDADE_AGG AS  

  WITH CONTABIL_AGG AS
   (SELECT IDGERENCIAL,
           IDCONTABIL,
           CODEMPRESA,
           CODFILIAL,
           TO_DATE(TRUNC(TO_DATE(C.DATA, 'DD/MM/YYYY'), 'MM')) DATA,
           CODGERENCIAL,
           CODCONTABIL,
           CODDRE,
           CODDFC,
           CODCC,
           VALOR
      FROM BI_SINC_CONTABILIDADE C)
  
  SELECT IDGERENCIAL,
         IDCONTABIL,
         CODEMPRESA,
         CODFILIAL,
         DATA,
         CODGERENCIAL,
         CODCONTABIL,
         CODDRE,
         CODDFC,
         CODCC,
         SUM(VALOR) VALOR
    FROM CONTABIL_AGG
   GROUP BY IDGERENCIAL,
            IDCONTABIL,
            CODEMPRESA,
            CODFILIAL,
            DATA,
            CODGERENCIAL,
            CODCONTABIL,
            CODDRE,
            CODDFC,
            CODCC;
