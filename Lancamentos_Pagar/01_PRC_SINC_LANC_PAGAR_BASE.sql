CREATE OR REPLACE PROCEDURE PRC_SINC_LANC_PAGAR_BASE AS
BEGIN
  
  FOR r IN (WITH BANCO AS
               (SELECT F.CODEMPRESA,
                      B.CODBANCO,
                      C.CODCONTA
                 FROM PCBANCO B
                 LEFT JOIN BI_SINC_FILIAL F ON F.CODFILIAL = B.CODFILIAL
                 LEFT JOIN PCCONTA C ON B.CODBANCO = C.CODCONTAMASTER),
              
              BASE AS
               (SELECT F.CODEMPRESA,
                      L.CODFILIAL,
                      L.RECNUM,
                      NVL(L.ADIANTAMENTO, 'N') ADIANTAMENTO,
                      EXTRACT(YEAR FROM L.DTCOMPETENCIA) ANO_COMPETENCIA,
                      L.DTCOMPETENCIA,
                      L.DTVENC DTVENCIMENTO,
                      (CASE
                        WHEN INSTR(L.HISTORICO2, 'RISCO') > 0 THEN
                         'RISCO SACADO'
                        WHEN L.CODCONTA = 100001 THEN
                         'MERCADORIA'
                        WHEN L.TIPOLANC = 'C' THEN
                         'CONFIRMADO'
                        ELSE
                         'PROVISIONADO'
                      END) TIPO,
                      NVL(R.CODIGOCENTROCUSTO, 0) CODCC,
                      NVL(ROUND(R.PERCRATEIO, 2), 0) PERCRATEIO,
                      R.VALOR VLRATEIO,
                      (CASE
                        WHEN L.DTPAGTO IS NULL THEN
                         L.VALOR
                        ELSE
                         (CASE
                           WHEN (NVL(L.VPAGOBORDERO, 0) > 0 AND L.VPAGO > NVL(L.VPAGOBORDERO, 0)) THEN
                            (L.VPAGOBORDERO - NVL(L.TXPERM, 0))
                           ELSE
                            L.VPAGO
                         END)
                      END) VALOR,
                      NVL(L.TXPERM, 0) VLJUROS,
                      NVL(L.DESCONTOFIN, 0) VLDESCONTO,
                      NVL(L.VALORDEV, 0) VLDEVOLUCAO,
                      (NVL(L.VLIRRF, 0) + NVL(L.VLISS, 0) + NVL(L.VLINSS, 0)) VLIMPOSTO,
                      L.CODCONTA,
                      L.CODFORNEC,
                      L.TIPOPARCEIRO,
                      L.NUMNOTA,
                      NVL(L.DUPLIC, '1') DUPLICATA,
                      (CASE
                        WHEN L.HISTORICO2 IS NULL THEN
                         L.HISTORICO
                        ELSE
                         L.HISTORICO || ' - ' || L.HISTORICO2
                      END) HISTORICO,
                      L.NUMTRANS,
                      L.DTPAGTO DTPAGAMENTO,
                      M.DTCOMPENSACAO,
                      M.CODBANCO,
                      B.CODEMPRESA CODEMPRESABANCO,
                      B.CODCONTA CONTABANCO,
                      L.CODROTINABAIXA,
                      L.DTESTORNOBAIXA,
                      L.DTCANCEL
                 FROM PCLANC L
                 LEFT JOIN PCMOVCR M ON L.NUMTRANS = M.NUMTRANS
                 LEFT JOIN BANCO B ON B.CODBANCO = M.CODBANCO
                 LEFT JOIN BI_SINC_FILIAL F ON F.CODFILIAL = L.CODFILIAL
                 LEFT JOIN PCRATEIOCENTROCUSTO R ON R.RECNUM = L.RECNUM
                 LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                WHERE 1 = 1
                  AND NVL(L.INDICE, '0') NOT IN ('B')
                  AND NVL(L.CODROTINABAIXA, 0) NOT IN (1207, 1502, 1503, 9806, 9876)
                  AND M.DTESTORNO IS NULL
                  AND L.DTCANCEL IS NULL
                  AND NVL(L.CODCONTA, 0) NOT IN (37, 100020, 100023, 101)
                  AND C.GRUPOCONTA NOT IN (680)),
              
              LANCAMENTOS AS
               (SELECT L.CODEMPRESA,
                      L.CODFILIAL,
                      L.RECNUM,
                      L.ADIANTAMENTO,
                      L.ANO_COMPETENCIA,
                      L.DTCOMPETENCIA,
                      L.DTVENCIMENTO,
                      L.TIPO,
                      L.CODCC,
                      L.PERCRATEIO,
                      NVL(L.VLRATEIO, L.VALOR) VLRATEIO,
                      L.VALOR,
                      L.VLJUROS,
                      L.VLDESCONTO,
                      L.VLDEVOLUCAO,
                      L.VLIMPOSTO,
                      L.CODCONTA,
                      L.CODFORNEC,
                      L.TIPOPARCEIRO,
                      L.NUMNOTA,
                      L.DUPLICATA,
                      L.HISTORICO,
                      L.NUMTRANS,
                      L.DTPAGAMENTO,
                      L.DTCOMPENSACAO,
                      L.CODBANCO,
                      L.CODEMPRESABANCO,
                      L.CONTABANCO,
                      L.CODROTINABAIXA,
                      L.DTESTORNOBAIXA
                 FROM BASE L)
              
              SELECT L.*
                FROM LANCAMENTOS L
                LEFT JOIN BI_SINC_LANC_PAGAR_BASE S ON S.RECNUM = L.RECNUM
                                                   AND S.CODCC = L.CODCC
               WHERE S.DT_UPDATE IS NULL
                  OR S.CODEMPRESA <> L.CODEMPRESA
                  OR S.CODFILIAL <> L.CODFILIAL
                  OR S.ADIANTAMENTO <> L.ADIANTAMENTO
                  OR S.ANO_COMPETENCIA <> L.ANO_COMPETENCIA
                  OR NVL(S.DTCOMPETENCIA, '01/01/1899') <> L.DTCOMPETENCIA
                  OR NVL(S.DTVENCIMENTO, '01/01/1899') <> L.DTVENCIMENTO
                  OR S.TIPO <> L.TIPO
                  OR S.PERCRATEIO <> L.PERCRATEIO
                  OR S.VLRATEIO <> L.VLRATEIO
                  OR S.VALOR <> L.VALOR
                  OR S.VLJUROS <> L.VLJUROS
                  OR S.VLDESCONTO <> L.VLDESCONTO
                  OR S.VLDEVOLUCAO <> L.VLDEVOLUCAO
                  OR S.VLIMPOSTO <> L.VLIMPOSTO
                  OR S.CODCONTA <> L.CODCONTA
                  OR S.CODFORNEC <> L.CODFORNEC
                  OR S.TIPOPARCEIRO <> L.TIPOPARCEIRO
                  OR S.NUMNOTA <> L.NUMNOTA
                  OR S.HISTORICO <> L.HISTORICO
                  OR S.NUMTRANS <> L.NUMTRANS
                  OR NVL(S.DTPAGAMENTO, '01/01/1899') <> L.DTPAGAMENTO
                  OR NVL(S.DTCOMPENSACAO, '01/01/1899') <> L.DTCOMPENSACAO
                  OR S.CODBANCO <> L.CODBANCO
                  OR S.CODEMPRESABANCO <> L.CODEMPRESABANCO
                  OR S.CONTABANCO <> L.CONTABANCO
                  OR S.CODROTINABAIXA <> L.CODROTINABAIXA
                  OR NVL(S.DTESTORNOBAIXA, '01/01/1899') <> L.DTESTORNOBAIXA)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_LANC_PAGAR_BASE
         SET CODEMPRESA      = r.CODEMPRESA,
             CODFILIAL       = r.CODFILIAL,
             ADIANTAMENTO    = r.ADIANTAMENTO,
             ANO_COMPETENCIA = r.ANO_COMPETENCIA,
             DTCOMPETENCIA   = r.DTCOMPETENCIA,
             DTVENCIMENTO    = r.DTVENCIMENTO,
             TIPO            = r.TIPO,
             PERCRATEIO      = r.PERCRATEIO,
             VLRATEIO        = r.VLRATEIO,
             VALOR           = r.VALOR,
             VLJUROS         = r.VLJUROS,
             VLDESCONTO      = r.VLDESCONTO,
             VLDEVOLUCAO     = r.VLDEVOLUCAO,
             VLIMPOSTO       = r.VLIMPOSTO,
             CODCONTA        = r.CODCONTA,
             CODFORNEC       = r.CODFORNEC,
             TIPOPARCEIRO    = r.TIPOPARCEIRO,
             NUMNOTA         = r.NUMNOTA,
             HISTORICO       = r.HISTORICO,
             NUMTRANS        = r.NUMTRANS,
             DTPAGAMENTO     = r.DTPAGAMENTO,
             DTCOMPENSACAO   = r.DTCOMPENSACAO,
             CODBANCO        = r.CODBANCO,
             CODEMPRESABANCO = r.CODEMPRESABANCO,
             CONTABANCO      = r.CONTABANCO,
             CODROTINABAIXA  = r.CODROTINABAIXA,
             DTESTORNOBAIXA  = r.DTESTORNOBAIXA,
             DT_UPDATE       = SYSDATE
       WHERE RECNUM = r.RECNUM
         AND CODCC = r.CODCC;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_LANC_PAGAR_BASE
          (CODEMPRESA,
           CODFILIAL,
           RECNUM,
           ADIANTAMENTO,
           ANO_COMPETENCIA,
           DTCOMPETENCIA,
           DTVENCIMENTO,
           TIPO,
           CODCC,
           PERCRATEIO,
           VLRATEIO,
           VALOR,
           VLJUROS,
           VLDESCONTO,
           VLDEVOLUCAO,
           VLIMPOSTO,
           CODCONTA,
           CODFORNEC,
           TIPOPARCEIRO,
           NUMNOTA,
           HISTORICO,
           NUMTRANS,
           DTPAGAMENTO,
           DTCOMPENSACAO,
           CODBANCO,
           CODEMPRESABANCO,
           CONTABANCO,
           CODROTINABAIXA,
           DTESTORNOBAIXA,
           DT_UPDATE)
        VALUES
          (r.CODEMPRESA,
           r.CODFILIAL,
           r.RECNUM,
           r.ADIANTAMENTO,
           r.ANO_COMPETENCIA,
           r.DTCOMPETENCIA,
           r.DTVENCIMENTO,
           r.TIPO,
           r.CODCC,
           r.PERCRATEIO,
           r.VLRATEIO,
           r.VALOR,
           r.VLJUROS,
           r.VLDESCONTO,
           r.VLDEVOLUCAO,
           r.VLIMPOSTO,
           r.CODCONTA,
           r.CODFORNEC,
           r.TIPOPARCEIRO,
           r.NUMNOTA,
           r.HISTORICO,
           r.NUMTRANS,
           r.DTPAGAMENTO,
           r.DTCOMPENSACAO,
           r.CODBANCO,
           r.CODEMPRESABANCO,
           r.CONTABANCO,
           r.CODROTINABAIXA,
           r.DTESTORNOBAIXA,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

/*  --EXCLUIR REGISTROS QUE NAO PERTENCEM MAIS A PCLANC
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_LANC_PAGAR_BASE
 WHERE (RECNUM ) IN
       (SELECT S.RECNUM
         FROM BI_SINC_LANC_PAGAR_BASE S
         LEFT JOIN PCLANC L ON S.RECNUM = L.RECNUM
        WHERE L.RECNUM IS NULL)';
  END;

  COMMIT;*/

END;
