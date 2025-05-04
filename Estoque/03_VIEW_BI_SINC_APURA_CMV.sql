CREATE OR REPLACE VIEW VIEW_BI_SINC_APURA_CMV AS

    WITH CENTROS_CUSTOS_FILIAL AS
     (SELECT CODFILIAL,
             (CASE
               WHEN CODFILIAL IN ('1') THEN
                '1.1'
               WHEN CODFILIAL IN ('2', '7', '9', '10') THEN
                '3.1'
               WHEN CODFILIAL IN ('3', '5', '6') THEN
                '4.1'
               WHEN CODFILIAL IN ('8') THEN
                '1.2'
               WHEN CODFILIAL IN ('11') THEN
                '2.2'
               WHEN CODFILIAL IN ('12') THEN
                '1.3'
               WHEN CODFILIAL IN ('13') THEN
                '1.4'
               WHEN CODFILIAL IN ('14') THEN
                '1.5'
             END) CODCC
        FROM BI_SINC_FILIAL F),
    
    FAT_LIQ_CC AS
     (SELECT S.CODFILIAL,
             S.DATA,
             S.CODCC,
             SUM(S.VALOR * -1) VALOR
        FROM VIEW_BI_SINC_CONTABILIDADE_AGG S
        JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = S.CODGERENCIAL
       WHERE C.CODGERENCIAL IN (3101, 3102)
       GROUP BY S.CODFILIAL,
                S.DATA,
                S.CODCC
       ORDER BY DATA),
    
    FAT_PERC_CC AS
     (SELECT CODFILIAL,
             DATA,
             CODCC,
             VALOR,
             SUM(VALOR) OVER (PARTITION BY CODFILIAL, DATA ORDER BY DATA) VLTOTAL
        FROM FAT_LIQ_CC),
    
    FAT_PERC_CC_TRATADO AS
     (SELECT S.CODFILIAL,
             S.DATA,
             S.CODCC,
             S.VALOR,
             S.VLTOTAL,
             (CASE
               WHEN S.VLTOTAL = 0 THEN
                1
               ELSE
                ROUND(S.VALOR / S.VLTOTAL, 6)
             END) PERC
        FROM FAT_PERC_CC S)
    
    SELECT L.CODEMPRESA,
           S.CODFILIAL,
           S.DATA,
           S.VLCMV,
           NVL(F.CODCC, C.CODCC) CODCC,
           F.PERC,
           ROUND((S.VLCMV * NVL(F.PERC, 1)), 2) VLCMVRATEIO
      FROM BI_SINC_APURACAO_CMV S
      LEFT JOIN BI_SINC_FILIAL L ON L.CODFILIAL = S.CODFILIAL
      LEFT JOIN CENTROS_CUSTOS_FILIAL C ON C.CODFILIAL = S.CODFILIAL
      LEFT JOIN FAT_PERC_CC_TRATADO F ON F.CODFILIAL = S.CODFILIAL
                                     AND F.DATA = S.DATA
