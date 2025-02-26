CREATE OR REPLACE VIEW VIEW_BI_SINC_LANCAMENTOS AS

  SELECT DISTINCT 'P' MOVIMENTO,
                  L.CODFILIAL,
                  L.DTLANC,
                  L.DTVENCIMENTO,
                  L.DTVENCUTIL,
                  NVL(L.DTCOMPENSACAO, L.DTPAGAMENTO) DTCOMPENSACAO,
                  L.CODBANCO,
                  L.CODFLUXO,
                  L.CODCONTA,
                  ((L.VALOR - L.VLDESCONTO - L.VLDEVOLUCAO) * -1) VALOR,
                  DECODE(L.TIPOPARCEIRO, 'F', L.CODFORNEC, 0) CODFORNEC,
                  DECODE(L.TIPOPARCEIRO, 'C', L.CODFORNEC, 0) CODCLI,
                  TO_CHAR(L.NUMNOTA) NUMNOTA,
                  'D' CODCOB,
                  0 CODUSUR,
                  0 CODINADIMPLENCIA,
                  0 DIASVENCIDOS,
                  (CASE
                    WHEN L.HISTORICO IS NOT NULL THEN
                     'Nº Lanc: ' || L.RECNUM || ' - ' || L.HISTORICO
                    ELSE
                     'Nº Lanc: ' || L.RECNUM
                  END) HISTORICO,
                  L.DT_UPDATE
    FROM BI_SINC_LANC_PAGAR_BASE L
   WHERE L.GRUPOCONTA NOT IN (110, 200, 900)
     AND L.CODCONTA NOT IN (37)
     AND NOT(L.CODCONTA IN (2100) AND L.VALOR < 0)
     AND NOT (NVL(L.CODROTINABAIXA, 0) = 746 AND L.CODBANCO IS NULL)
     AND NOT (L.DTPAGAMENTO IS NOT NULL AND L.NUMTRANS IS NULL)
     AND L.DTESTORNOBAIXA IS NULL
  UNION ALL
  SELECT 'R' MOVIMENTO,
         R.CODFILIAL,
         R.DTEMISSAO DTLANC,
         R.DTVENCIMENTO,
         R.DTVENCUTIL,
         NVL(R.DTCOMPENSACAO, R.DTPAGAMENTO) DTCOMPENSACAO,
         R.CODBANCO,
         R.CODFLUXO,
         0 CODCONTA,
         (CASE
           WHEN R.VLRECEBIDO > 0 THEN
            R.VLRECEBIDO
           ELSE
            R.VALORLIQ
         END) VALOR,
         0 CODFORNEC,
         R.CODCLI,
         (R.NUMNOTA || '-' || LPAD(R.PREST, 2, '0')) NUMNOTA,
         R.CODCOB,
         R.CODUSUR,
         R.CODINADIMPLENCIA,
         R.DIASVENCIDOS,
         (CASE
           WHEN R.HISTORICO IS NOT NULL THEN
            'Nº Transvenda: ' || R.NUMTRANSVENDA || ' - ' || R.HISTORICO
           ELSE
            'Nº Transvenda: ' || R.NUMTRANSVENDA
         END) HISTORICO,
         R.DT_UPDATE
    FROM BI_SINC_LANC_RECEBER_BASE R
   WHERE R.CODCOB NOT IN ('DESD', 'PERD')
     AND NVL(R.VLRECEBIDO,1) <> 0;
