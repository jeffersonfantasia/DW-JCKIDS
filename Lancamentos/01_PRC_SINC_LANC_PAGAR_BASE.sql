CREATE OR REPLACE PROCEDURE PRC_SINC_LANC_PAGAR_BASE AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 120;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

  ------------------------CONTAS
  vCONTA_COMPRA_MERCADORIA NUMBER := 100001;

BEGIN
  FOR r IN (WITH BANCO AS
               (SELECT B.CODBANCO,
                      C.CODCONTA
                 FROM PCBANCO B
                 LEFT JOIN PCCONTA C ON B.CODBANCO = C.CODCONTAMASTER),
              
              PRIMERO_CC_RATEIO AS
               (SELECT *
                 FROM (SELECT R.RECNUM,
                              R.CODIGOCENTROCUSTO,
                              100 PERCRATEIODESC,
                              ROW_NUMBER() OVER (PARTITION BY R.RECNUM ORDER BY R.CODIGOCENTROCUSTO) AS RN
                         FROM PCRATEIOCENTROCUSTO R)
                WHERE RN = 1),
              
              BASE AS
               (SELECT F.CODEMPRESA,
                      L.CODFILIAL,
                      L.DTLANC,
                      L.RECNUM,
                      L.RECNUMPRINC,
                      NVL(L.ADIANTAMENTO, 'N') ADIANTAMENTO,
                      EXTRACT(YEAR FROM L.DTCOMPETENCIA) ANO_COMPETENCIA,
                      L.DTCOMPETENCIA,
                      L.DTVENC DTVENCIMENTO,
                      C.DIA_UTIL_FINANCEIRO DTVENCUTIL,
                      (CASE
                        WHEN INSTR(L.HISTORICO2, 'RISCO') > 0 THEN
                         1
                        WHEN L.CODCONTA = vCONTA_COMPRA_MERCADORIA THEN
                         2
                        WHEN L.TIPOLANC = 'C' THEN
                         3
                        ELSE
                         4
                      END) CODFLUXO,
                      NVL(R.CODIGOCENTROCUSTO, '0') CODCC,
                      PR.CODIGOCENTROCUSTO CODCCMAX,
                      NVL(PR.PERCRATEIODESC, 0) PERCRATEIODESC,
                      NVL(R.PERCRATEIO, 0) PERCRATEIO,
                      R.VALOR VLRATEIO,
                      (CASE
                        WHEN L.DTPAGTO IS NULL THEN
                         L.VALOR
                        ELSE
                         (NVL(L.VPAGOBORDERO, L.VPAGO) - NVL(L.TXPERM, 0) + NVL(L.DESCONTOFIN, 0) + NVL(L.VALORDEV, 0))
                      END) VALOR,
                      NVL(L.TXPERM, 0) VLJUROS,
                      (CASE
                        WHEN NVL(L.DESCONTOFIN, 0) < 1
                             AND R.CODIGOCENTROCUSTO <> NVL(PR.CODIGOCENTROCUSTO, '0') THEN
                         0
                        ELSE
                         NVL(L.DESCONTOFIN, 0)
                      END) VLDESCONTO,
                      NVL(L.VALORDEV, 0) VLDEVOLUCAO,
                      (NVL(L.VLIRRF, 0) + NVL(L.VLISS, 0) + NVL(L.VLINSS, 0)) VLIMPOSTO,
                      L.CODCONTA,
                      C.GRUPOCONTA,
                      L.CODFORNEC,
                      L.TIPOPARCEIRO,
                      L.NUMNOTADEV,
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
                      B.CODCONTA CONTABANCO,
                      L.CODROTINABAIXA,
                      L.DTESTORNOBAIXA
                 FROM PCLANC L
                 LEFT JOIN PCMOVCR M ON L.NUMTRANS = M.NUMTRANS
                 LEFT JOIN BANCO B ON B.CODBANCO = M.CODBANCO
                 LEFT JOIN BI_SINC_FILIAL F ON F.CODFILIAL = L.CODFILIAL
                 LEFT JOIN PCRATEIOCENTROCUSTO R ON R.RECNUM = L.RECNUM
                 LEFT JOIN PRIMERO_CC_RATEIO PR ON PR.RECNUM = L.RECNUM
                                               AND PR.CODIGOCENTROCUSTO = R.CODIGOCENTROCUSTO
                 LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                 LEFT JOIN BI_SINC_CALENDARIO C ON C.DATA = L.DTVENC
                WHERE 1 = 1
                  AND L.DTCOMPETENCIA >= vDATA_MOV_INCREMENTAL
                  AND NVL(L.INDICE, '0') NOT IN ('B')
                  AND NVL(L.CODROTINABAIXA, 0) NOT IN (1207, 1502, 1503, 9806, 9876)
                  AND (M.DTESTORNO IS NULL OR (M.DTESTORNO IS NOT NULL AND M.ESTORNO = 'N'))
                  AND L.DTCANCEL IS NULL
                  AND NVL(L.CODCONTA, 0) NOT IN (37, 100020, 100023, 101)
                  AND C.GRUPOCONTA NOT IN (680)),
              
              LANCAMENTOS AS
               (SELECT L.CODEMPRESA,
                      L.CODFILIAL,
                      L.DTLANC,
                      L.RECNUM,
                      L.RECNUMPRINC,
                      L.ADIANTAMENTO,
                      L.ANO_COMPETENCIA,
                      L.DTCOMPETENCIA,
                      L.DTVENCIMENTO,
                      L.DTVENCUTIL,
                      L.CODFLUXO,
                      L.CODCC,
                      L.PERCRATEIO,
                      L.PERCRATEIODESC,
                      NVL(L.VLRATEIO, L.VALOR) VLRATEIO,
                      L.VALOR,
                      L.VLJUROS,
                      L.VLDESCONTO,
                      L.VLDEVOLUCAO,
                      L.VLIMPOSTO,
                      L.CODCONTA,
                      L.GRUPOCONTA,
                      L.CODFORNEC,
                      L.TIPOPARCEIRO,
                      L.NUMNOTADEV,
                      L.NUMNOTA,
                      L.DUPLICATA,
                      L.HISTORICO,
                      L.NUMTRANS,
                      L.DTPAGAMENTO,
                      L.DTCOMPENSACAO,
                      L.CODBANCO,
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
                  OR NVL(S.DTLANC, '01/01/1899') <> L.DTLANC
                  OR NVL(S.RECNUMPRINC, 0) <> NVL(L.RECNUMPRINC, 0)
                  OR S.ADIANTAMENTO <> L.ADIANTAMENTO
                  OR S.ANO_COMPETENCIA <> L.ANO_COMPETENCIA
                  OR NVL(S.DTCOMPETENCIA, '01/01/1899') <> L.DTCOMPETENCIA
                  OR NVL(S.DTVENCIMENTO, '01/01/1899') <> L.DTVENCIMENTO
                  OR NVL(S.DTVENCUTIL, '01/01/1899') <> L.DTVENCUTIL
                  OR S.CODFLUXO <> L.CODFLUXO
                  OR NVL(S.PERCRATEIO, 0) <> NVL(L.PERCRATEIO, 0)
                  OR NVL(S.PERCRATEIODESC, 0) <> NVL(L.PERCRATEIODESC, 0)
                  OR S.VLRATEIO <> L.VLRATEIO
                  OR S.VALOR <> L.VALOR
                  OR NVL(S.VLJUROS, 0) <> NVL(L.VLJUROS, 0)
                  OR NVL(S.VLDESCONTO, 0) <> NVL(L.VLDESCONTO, 0)
                  OR NVL(S.VLDEVOLUCAO, 0) <> NVL(L.VLDEVOLUCAO, 0)
                  OR NVL(S.VLIMPOSTO, 0) <> NVL(L.VLIMPOSTO, 0)
                  OR NVL(S.CODCONTA, 0) <> NVL(L.CODCONTA, 0)
                  OR S.GRUPOCONTA <> L.GRUPOCONTA
                  OR NVL(S.CODFORNEC, 0) <> NVL(L.CODFORNEC, 0)
                  OR S.TIPOPARCEIRO <> L.TIPOPARCEIRO
                  OR NVL(S.NUMNOTADEV, 0) <> NVL(L.NUMNOTADEV, 0)
                  OR NVL(S.NUMNOTA, 0) <> NVL(L.NUMNOTA, 0)
                  OR S.HISTORICO <> L.HISTORICO
                  OR S.NUMTRANS <> L.NUMTRANS
                  OR NVL(S.DTPAGAMENTO, '01/01/1899') <> L.DTPAGAMENTO
                  OR NVL(S.DTCOMPENSACAO, '01/01/1899') <> L.DTCOMPENSACAO
                  OR NVL(S.CODBANCO, 0) <> NVL(L.CODBANCO, 0)
                  OR NVL(S.CONTABANCO, 0) <> NVL(L.CONTABANCO, 0)
                  OR NVL(S.CODROTINABAIXA, 0) <> NVL(L.CODROTINABAIXA, 0)
                  OR NVL(S.DTESTORNOBAIXA, '01/01/1899') <> L.DTESTORNOBAIXA)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condi��es mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_LANC_PAGAR_BASE
         SET CODEMPRESA      = r.CODEMPRESA,
             CODFILIAL       = r.CODFILIAL,
             DTLANC          = r.DTLANC,
             RECNUMPRINC     = r.RECNUMPRINC,
             ADIANTAMENTO    = r.ADIANTAMENTO,
             ANO_COMPETENCIA = r.ANO_COMPETENCIA,
             DTCOMPETENCIA   = r.DTCOMPETENCIA,
             DTVENCIMENTO    = r.DTVENCIMENTO,
             DTVENCUTIL      = r.DTVENCUTIL,
             CODFLUXO        = r.CODFLUXO,
             PERCRATEIO      = r.PERCRATEIO,
             PERCRATEIODESC  = r.PERCRATEIODESC,
             VLRATEIO        = r.VLRATEIO,
             VALOR           = r.VALOR,
             VLJUROS         = r.VLJUROS,
             VLDESCONTO      = r.VLDESCONTO,
             VLDEVOLUCAO     = r.VLDEVOLUCAO,
             VLIMPOSTO       = r.VLIMPOSTO,
             CODCONTA        = r.CODCONTA,
             GRUPOCONTA      = r.GRUPOCONTA,
             CODFORNEC       = r.CODFORNEC,
             TIPOPARCEIRO    = r.TIPOPARCEIRO,
             NUMNOTADEV      = r.NUMNOTADEV,
             NUMNOTA         = r.NUMNOTA,
             HISTORICO       = r.HISTORICO,
             NUMTRANS        = r.NUMTRANS,
             DTPAGAMENTO     = r.DTPAGAMENTO,
             DTCOMPENSACAO   = r.DTCOMPENSACAO,
             CODBANCO        = r.CODBANCO,
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
           DTLANC,
           RECNUM,
           RECNUMPRINC,
           ADIANTAMENTO,
           ANO_COMPETENCIA,
           DTCOMPETENCIA,
           DTVENCIMENTO,
           DTVENCUTIL,
           CODFLUXO,
           CODCC,
           PERCRATEIO,
           PERCRATEIODESC,
           VLRATEIO,
           VALOR,
           VLJUROS,
           VLDESCONTO,
           VLDEVOLUCAO,
           VLIMPOSTO,
           CODCONTA,
           GRUPOCONTA,
           CODFORNEC,
           TIPOPARCEIRO,
           NUMNOTADEV,
           NUMNOTA,
           HISTORICO,
           NUMTRANS,
           DTPAGAMENTO,
           DTCOMPENSACAO,
           CODBANCO,
           CONTABANCO,
           CODROTINABAIXA,
           DTESTORNOBAIXA,
           DT_UPDATE)
        VALUES
          (r.CODEMPRESA,
           r.CODFILIAL,
           r.DTLANC,
           r.RECNUM,
           r.RECNUMPRINC,
           r.ADIANTAMENTO,
           r.ANO_COMPETENCIA,
           r.DTCOMPETENCIA,
           r.DTVENCIMENTO,
           r.DTVENCUTIL,
           r.CODFLUXO,
           r.CODCC,
           r.PERCRATEIO,
           r.PERCRATEIODESC,
           r.VLRATEIO,
           r.VALOR,
           r.VLJUROS,
           r.VLDESCONTO,
           r.VLDEVOLUCAO,
           r.VLIMPOSTO,
           r.CODCONTA,
           r.GRUPOCONTA,
           r.CODFORNEC,
           r.TIPOPARCEIRO,
           r.NUMNOTADEV,
           r.NUMNOTA,
           r.HISTORICO,
           r.NUMTRANS,
           r.DTPAGAMENTO,
           r.DTCOMPENSACAO,
           r.CODBANCO,
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

  --EXCLUIR REGISTROS QUE NAO PERTENCEM MAIS A PCLANC
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_LANC_PAGAR_BASE
 WHERE RECNUM IN
       (SELECT S.RECNUM
         FROM BI_SINC_LANC_PAGAR_BASE S
         LEFT JOIN PCLANC L ON S.RECNUM = L.RECNUM
        WHERE L.RECNUM IS NULL)';
  END;

  COMMIT;

  --EXCLUIR REGISTROS QUE ESTAO DIFERENTES DA PCRATEIOCENTROCUSTO
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_LANC_PAGAR_BASE
 WHERE (RECNUM, CODCC) IN
       (SELECT S.RECNUM, S.CODCC
       FROM BI_SINC_LANC_PAGAR_BASE S
        LEFT JOIN PCRATEIOCENTROCUSTO R ON R.RECNUM = S.RECNUM
         AND R.CODIGOCENTROCUSTO = S.CODCC
        WHERE S.CODCC <> ''0''
         AND R.RECNUM IS NULL)';
  END;

  COMMIT;

  --EXCLUIR REGISTROS QUE TEM CC ZERO E TB CC PREENCHIDO
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_LANC_PAGAR_BASE
 WHERE (RECNUM, CODCC) IN (WITH RATEIO_CC_ZERO AS (SELECT S.RECNUM,
                                   COUNT(S.CODCC) CONTAGEM
                              FROM BI_SINC_LANC_PAGAR_BASE S
                             GROUP BY S.RECNUM)
                           
                            SELECT S.RECNUM,S.CODCC 
                            FROM BI_SINC_LANC_PAGAR_BASE S LEFT JOIN RATEIO_CC_ZERO R ON R.RECNUM = S.RECNUM
                           WHERE S.CODCC = ''0'' AND R.CONTAGEM > 1)';
  END;

  COMMIT;

END;
