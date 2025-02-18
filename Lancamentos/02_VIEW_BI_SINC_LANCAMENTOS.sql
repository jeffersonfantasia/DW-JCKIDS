CREATE OR REPLACE VIEW VIEW_BI_SINC_LANCAMENTOS AS

  SELECT DISTINCT 'P' MOVIMENTO,
                  L.CODFILIAL,
                  L.DTVENCIMENTO,
                  L.DTVENCUTIL,
                  NVL(L.DTCOMPENSACAO, L.DTPAGAMENTO) DTCOMPENSACAO,
                  L.CODFLUXO,
                  L.CODCONTA,
                  ((L.VALOR - L.VLDESCONTO - L.VLDEVOLUCAO) * -1) VALOR,
                  DECODE(L.TIPOPARCEIRO, 'F', L.CODFORNEC, 0) CODFORNEC,
                  DECODE(L.TIPOPARCEIRO, 'C', L.CODFORNEC, 0) CODCLI,
                  L.NUMNOTA,
                  'D' CODCOB,
                  0 CODUSUR,
                  0 CODINADIMPLENCIA,
                  (CASE
                    WHEN L.HISTORICO IS NOT NULL THEN
                     'Nº Lanc: ' || L.RECNUM || ' - ' || L.HISTORICO
                    ELSE
                     'Nº Lanc: ' || L.RECNUM
                  END) HISTORICO
    FROM BI_SINC_LANC_PAGAR_BASE L
		WHERE L.GRUPOCONTA NOT IN (110,200,900)
		AND L.CODCONTA NOT IN (37, 2100)
  UNION ALL
  SELECT 'R' MOVIMENTO,
         R.CODFILIAL,
         R.DTVENCIMENTO,
         R.DTVENCUTIL,
         NVL(R.DTCOMPENSACAO, R.DTPAGAMENTO) DTCOMPENSACAO,
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
         R.NUMNOTA,
         R.CODCOB,
         R.CODUSUR,
         R.CODINADIMPLENCIA,
         (CASE
           WHEN R.HISTORICO IS NOT NULL THEN
            'Nº Transvenda: ' || R.NUMTRANSVENDA || '-' || LPAD(R.PREST, 2, '0') || ' - ' || R.HISTORICO
           ELSE
            'Nº Transvenda: ' || R.NUMTRANSVENDA || '-' || LPAD(R.PREST, 2, '0')
         END) HISTORICO
    FROM BI_SINC_LANC_RECEBER_BASE R
   WHERE R.DTDESD IS NULL
     AND R.DTESTORNO IS NULL
		 AND R.CODCOB NOT IN ('CRED');
