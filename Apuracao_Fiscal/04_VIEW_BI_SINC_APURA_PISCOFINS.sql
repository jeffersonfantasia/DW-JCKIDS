CREATE OR REPLACE VIEW VIEW_BI_SINC_APURA_PISCOFINS AS

      SELECT 'PIS' TIPO,
             A.DATA,
             A.VLCREDITO,
             A.VLDEBITO,
             A.VLSALDO,
             A.VLPAGAR,
             A.VLRECUPERAR,
             A.DT_UPDATE
        FROM BI_SINC_APURACAO_PIS A
      UNION ALL
      SELECT 'COFINS' TIPO,
             A.DATA,
             A.VLCREDITO,
             A.VLDEBITO,
             A.VLSALDO,
             A.VLPAGAR,
             A.VLRECUPERAR,
             A.DT_UPDATE
        FROM BI_SINC_APURACAO_COFINS A
