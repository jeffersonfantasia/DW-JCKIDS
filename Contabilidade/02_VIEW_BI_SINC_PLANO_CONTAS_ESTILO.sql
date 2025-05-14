CREATE OR REPLACE VIEW VIEW_BI_SINC_PLANO_CONTAS_ESTILO AS

      WITH EMPRESA AS
       (SELECT F.CODEMPRESA FROM BI_SINC_FILIAL F GROUP BY F.CODEMPRESA),
      
      PLANO_JC_EMPRESAS AS
       (SELECT E.CODEMPRESA,
               C.CONTA,
               C.CODCONTA CODCONTABIL,
               C.CODN1,
               C.CONTAN1,
               C.CODN2,
               C.CONTAN2,
               C.CODN3,
               C.CONTAN3,
               C.CODN4,
               C.CONTAN4
          FROM BI_SINC_PLANO_CONTAS_ESTILO C
         CROSS JOIN EMPRESA E
         ORDER BY C.ORDEM,
                  E.CODEMPRESA),
      
      PLANO_UNIFICADO AS
       (SELECT * FROM VIEW_BI_SINC_FORNEC_PLANO_CONTAS_ESTILO UNION ALL SELECT * FROM PLANO_JC_EMPRESAS)
      
      
      SELECT (C.CODCONTABIL || '-' || C.CODEMPRESA) IDCONTABIL,
             C.CODEMPRESA,
             C.CONTA,
             C.CODCONTABIL,
             C.CODN1,
             C.CONTAN1,
             C.CODN2,
             C.CONTAN2,
             C.CODN3,
             C.CONTAN3,
             C.CODN4,
             C.CONTAN4
        FROM PLANO_UNIFICADO C
       ORDER BY C.CODN1, TO_NUMBER(C.CODCONTABIL), C.CODEMPRESA;
