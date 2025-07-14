CREATE OR REPLACE VIEW VIEW_BI_SINC_APURA_COMPETE AS 

 WITH TIPOS AS
  (SELECT TO_DATE(TRUNC(TO_DATE(F.DATA, 'DD/MM/YYYY'), 'MM')) DATA,
          MOVIMENTO,
          (CASE
            WHEN MOVIMENTO = 'E' THEN
             (CASE
               WHEN F.PERCICMS = 4 THEN
                1
               WHEN (F.PERCICMS NOT IN (0, 17) AND F.CFOP NOT IN (1949, 2949)) THEN
                2
               ELSE
                3
             END)
            ELSE
             (CASE
               WHEN NOT (F.CFOP IN (5949, 6949) AND F.PERCICMS <> 0) THEN
                (CASE
                  WHEN SUBSTR(TO_CHAR(F.CFOP), 0, 1) = '5'
                       AND F.PERCICMS <> 0 THEN
                   4
                  WHEN ((F.CFOP = 6152 AND F.PERCICMS = 4) OR (F.CFOP = 6108 AND F.PERCICMS <> 0)) THEN
                   5
                  WHEN (F.CFOP = 6202 OR F.PERCICMS = 0) THEN
                   6
                  ELSE
                   7
                END)
               ELSE
                99
             END)
          END) CODTIPO,
          (CASE
            WHEN MOVIMENTO = 'E' THEN
             (CASE
               WHEN F.PERCICMS = 4 THEN
                'ENT COMPETE IMP'
               WHEN (F.PERCICMS NOT IN (0, 17) AND F.CFOP NOT IN (1949, 2949)) THEN
                'ENT COMPETE'
               ELSE
                'ENT FORA COMPETE'
             END)
            ELSE
             (CASE
               WHEN NOT (F.CFOP IN (5949, 6949) AND F.PERCICMS <> 0) THEN
                (CASE
                  WHEN SUBSTR(TO_CHAR(F.CFOP), 0, 1) = '5'
                       AND F.PERCICMS <> 0 THEN
                   'SAID BASERED'
                  WHEN ((F.CFOP = 6152 AND F.PERCICMS = 4) OR (F.CFOP = 6108 AND F.PERCICMS <> 0)) THEN
                   'SAID FORA TRANSF COMPETE'
                  WHEN (F.CFOP = 6202 OR F.PERCICMS = 0) THEN
                   'SAID FORA COMPETE'
                  ELSE
                   'SAID COMPETE'
                END)
               ELSE
                'FORA'
             END)
          END) TIPO,
          F.CFOP,
          PERCICMS,
          VALORCONTABIL,
          VLBASEICMS,
          VALORICMS
     FROM BI_SINC_FISCAL F
    WHERE F.CODFILIAL = '11'
      AND F.ESPECIE <> 'NS'),
 
 TIPOS_AGG AS
  (SELECT DATA,
          MOVIMENTO,
          CODTIPO,
          TIPO,
          SUM(VALORCONTABIL) VLCONTABIL,
          SUM(VLBASEICMS) VLBASEICMS,
          SUM(VALORICMS) VLICMS
     FROM TIPOS
    GROUP BY DATA,
             MOVIMENTO,
             CODTIPO,
             TIPO
    ORDER BY CODTIPO),
 
 TIPOS_AGG_TRATADO AS
  (SELECT DATA,
          (CASE CODTIPO
            WHEN 1 THEN
             'VLICMS_ENT_IMP'
            WHEN 2 THEN
             'VLICMS_ENT_NAC'
            WHEN 4 THEN
             'VLCONT_SAID_RED'
            WHEN 5 THEN
             'VLCONT_SAID_FORA_TRANSF'
            WHEN 7 THEN
             'VLCONT_SAID_COMPETE'
            ELSE
             NULL
          END) TIPO_VALOR,
          (CASE
            WHEN CODTIPO IN (1, 2) THEN
             VLICMS
            WHEN CODTIPO IN (4, 5, 7) THEN
             VLCONTABIL
            ELSE
             NULL
          END) VALOR
     FROM TIPOS_AGG
   UNION ALL
   SELECT DATA,
          'VLBASEICMS_ENT_NAC' TIPO_VALOR,
          (CASE
            WHEN CODTIPO IN (2) THEN
             VLBASEICMS
            ELSE
             NULL
          END) VALOR
     FROM TIPOS_AGG
   UNION ALL
   SELECT DATA,
          'VLICMS_SAID_COMPETE' TIPO_VALOR,
          (CASE
            WHEN CODTIPO IN (7) THEN
             VLICMS
            ELSE
             NULL
          END) VALOR
     FROM TIPOS_AGG
   UNION ALL
   SELECT DATA,
          'VLCONT_SAID_RED' TIPO_VALOR,
          0 VALOR
     FROM TIPOS_AGG
    GROUP BY DATA
   UNION ALL
   SELECT DATA,
          'VLCREDITO' TIPO_VALOR,
          SUM(VLICMS) VALOR
     FROM TIPOS_AGG
    WHERE MOVIMENTO = 'E'
    GROUP BY DATA
   UNION ALL
   SELECT DATA,
          'VLDEBITO' TIPO_VALOR,
          SUM(VLICMS) VALOR
     FROM TIPOS_AGG
    WHERE MOVIMENTO = 'S'
    GROUP BY DATA),
 
 TIPOS_AGG_TRATADO_PIVOT AS
  (SELECT *
     FROM (SELECT DATA,
                  TIPO_VALOR,
                  SUM(VALOR) VALOR
             FROM TIPOS_AGG_TRATADO
            WHERE VALOR IS NOT NULL
            GROUP BY DATA,
                     TIPO_VALOR)
   PIVOT(SUM(VALOR)
      FOR TIPO_VALOR IN('VLCREDITO' AS VLCREDITO,
                       'VLDEBITO' AS VLDEBITO,
                       'VLBASEICMS_ENT_NAC' AS VLBASEICMS_ENT_NAC,
                       'VLCONT_SAID_COMPETE' AS VLCONT_SAID_COMPETE,
                       'VLCONT_SAID_FORA_TRANSF' AS VLCONT_SAID_FORA_TRANSF,
                       'VLCONT_SAID_RED' AS VLCONT_SAID_RED,
                       'VLICMS_SAID_COMPETE' AS VLICMS_SAID_COMPETE,
                       'VLICMS_ENT_IMP' AS VLICMS_ENT_IMP,
                       'VLICMS_ENT_NAC' AS VLICMS_ENT_NAC)))
 
 SELECT ROW_NUMBER() OVER (ORDER BY DATA) RN,
        DATA,
        NVL(VLCREDITO, 0) VLCREDITO,
        NVL(VLDEBITO, 0) VLDEBITO,
        NVL(VLBASEICMS_ENT_NAC, 0) VLBASEICMS_ENT_NAC,
        NVL(VLCONT_SAID_COMPETE, 0) VLCONT_SAID_COMPETE,
        NVL(VLCONT_SAID_FORA_TRANSF, 0) VLCONT_SAID_FORA_TRANSF,
        NVL(VLCONT_SAID_RED, 0) VLCONT_SAID_RED,
        NVL(VLICMS_SAID_COMPETE, 0) VLICMS_SAID_COMPETE,
        NVL(VLICMS_ENT_IMP, 0) VLICMS_ENT_IMP,
        NVL(VLICMS_ENT_NAC, 0) VLICMS_ENT_NAC
   FROM TIPOS_AGG_TRATADO_PIVOT
  ORDER BY DATA
