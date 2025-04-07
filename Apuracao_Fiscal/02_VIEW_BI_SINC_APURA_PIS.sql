CREATE OR REPLACE VIEW VIEW_BI_SINC_APURA_PIS AS

    WITH FISCAL_DATE AS
     (SELECT TO_DATE(TRUNC(TO_DATE(F.DATA, 'DD/MM/YYYY'), 'MM')) DATA,
             (CASE
               WHEN F.MOVIMENTO = 'E' THEN
                'VLCREDITO'
               ELSE
                'VLDEBITO'
             END) TIPO,
             F.VALORPIS VALOR
        FROM BI_SINC_FISCAL F),
    
    FISCAL_AGG AS
     (SELECT F.DATA,
             F.TIPO,
             ROUND(SUM(F.VALOR), 2) VALOR
        FROM FISCAL_DATE F
       GROUP BY F.DATA,
                F.TIPO),
    
    FISCAL_PIVOT AS
     (SELECT * FROM FISCAL_AGG PIVOT(SUM(VALOR) FOR TIPO IN('VLCREDITO' AS VLCREDITO, 'VLDEBITO' AS VLDEBITO)))
    
    SELECT ROW_NUMBER() OVER (ORDER BY DATA) RN,
           DATA,
           VLCREDITO,
           VLDEBITO,
           ROUND((VLCREDITO - VLDEBITO), 2) VLSALDO
      FROM FISCAL_PIVOT
     ORDER BY DATA
