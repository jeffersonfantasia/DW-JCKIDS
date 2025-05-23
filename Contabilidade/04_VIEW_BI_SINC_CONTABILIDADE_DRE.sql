CREATE OR REPLACE VIEW VIEW_BI_SINC_CONTABILIDADE_DRE AS

  WITH DRE_AGG AS
   (SELECT C.CODDRE,
           C.CODEMPRESA,
           C.CODFILIAL,
           TO_DATE(TRUNC(TO_DATE(C.DATA, 'DD/MM/YYYY'), 'MM')) DATA,
           C.CODCC,
           C.VALOR
      FROM BI_SINC_CONTABILIDADE C
     WHERE C.CODDRE IS NOT NULL)
  
  SELECT CODDRE,
         CODEMPRESA,
         CODFILIAL,
         DATA,
         CODCC,
         (SUM(VALOR) * -1) VALOR
    FROM DRE_AGG
   GROUP BY CODDRE,
            CODEMPRESA,
            CODFILIAL,
            DATA,
            CODCC;
