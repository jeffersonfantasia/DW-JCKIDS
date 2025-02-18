CREATE OR REPLACE VIEW VIEW_BI_SINC_LANCAMENTOS AS

  SELECT DISTINCT 'P' MOVIMENTO,
                  L.CODFILIAL,
                  L.DTVENCIMENTO,
                  L.DTVENCUTIL,
                  NVL(L.DTCOMPENSACAO, L.DTPAGAMENTO) DTCOMPENSACAO,
                  L.TIPO,
                  (L.VALOR - L.VLDESCONTO - L.VLDEVOLUCAO) VALOR,
                  DECODE(L.TIPOPARCEIRO, 'F', L.CODFORNEC, 0) CODFORNEC,
                  DECODE(L.TIPOPARCEIRO, 'C', L.CODFORNEC, 0) CODCLI,
                  L.CODCONTA,
                  L.NUMNOTA,
                  'D' CODCOB,
                  0 CODUSUR,
                  0 CODINADIMPLENCIA,
                  (CASE
                    WHEN L.HISTORICO IS NOT NULL THEN
                     'N� Lanc: ' || L.RECNUM || ' - ' || L.HISTORICO
                    ELSE
                     'N� Lanc: ' || L.RECNUM
                  END) HISTORICO
    FROM BI_SINC_LANC_PAGAR_BASE L
  UNION ALL
  SELECT 'R' MOVIMENTO,
         R.CODFILIAL,
         R.DTVENCIMENTO,
         R.DTVENCUTIL,
         NVL(R.DTCOMPENSACAO, R.DTPAGAMENTO) DTCOMPENSACAO,
         R.TIPO,
         0 CODCONTA,
         NVL(R.VLRECEBIDO, R.VALORLIQ) VALOR,
         0 CODFORNEC,
         R.CODCLI,
         R.NUMNOTA,
         R.CODCOB,
         R.CODUSUR,
         R.CODINADIMPLENCIA,
         (CASE
           WHEN R.HISTORICO IS NOT NULL THEN
            'N� Transvenda: ' || R.NUMTRANSVENDA || '-' || LPAD(R.PREST, 2, '0') || ' - ' || R.HISTORICO
           ELSE
            'N� Transvenda: ' || R.NUMTRANSVENDA || '-' || LPAD(R.PREST, 2, '0')
         END) HISTORICO
    FROM BI_SINC_LANC_RECEBER_BASE R
   WHERE R.DTDESD IS NULL
     AND R.DTESTORNO IS NULL;
