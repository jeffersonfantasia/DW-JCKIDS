CREATE OR REPLACE PROCEDURE PRC_SINC_LANC_RECEBER_BASE AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 120;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

  ------------------------CONTAS
  vCONTA_CLIENTE_NACIONAL NUMBER := 1152;

BEGIN

  FOR r IN (WITH RATREIO_OCORRENCIAS AS
               (SELECT L.CODOCORRENCIA,
                      B.OCORRENCIA,
                      L.NUMTRANSVENDA,
                      L.PREST,
                      L.DATA,
                      ROW_NUMBER() OVER (PARTITION BY L.NUMTRANSVENDA, L.PREST ORDER BY DATA DESC) AS RN
                 FROM PCLOGCOBMAG L
                 JOIN PCOCORBC B ON L.CODOCORRENCIA = B.CODOCORRENCIA
                WHERE B.NUMBANCO = 341),

              OCORRENCIAS AS
               (SELECT L.DATA,
                      L.CODOCORRENCIA,
                      L.OCORRENCIA,
                      L.NUMTRANSVENDA,
                      L.PREST
                 FROM RATREIO_OCORRENCIAS L
                WHERE L.RN = 1),

              RASTREIO_COBRANCA AS
               (SELECT B.DATA,
                      I.NUMTRANSVENDA,
                      I.PREST,
                      C.CODSTATUSCOB,
                      COALESCE(B.OBS3, C.STATUSCOB, B.OBS2, B.OBS1) OBSERVACAO,
                      ROW_NUMBER() OVER (PARTITION BY I.NUMTRANSVENDA, I.PREST ORDER BY B.DATA DESC) AS RN
                 FROM PCHISTCOB B
                 JOIN PCHISTCOBI I ON B.NUMREGCOB = I.NUMREGCOB
                 LEFT JOIN PCSTATUSCOBCLI C ON I.CODSTATUSCOB = C.CODSTATUSCOB
                WHERE C.CODSTATUSCOB IS NOT NULL),

              COBRANCA AS
               (SELECT L.DATA,
                      L.CODSTATUSCOB,
                      L.OBSERVACAO,
                      L.NUMTRANSVENDA,
                      L.PREST
                 FROM RASTREIO_COBRANCA L
                WHERE L.RN = 1),

              BANCO AS
               (SELECT B.CODBANCO,
                      C.CODCONTA
                 FROM PCBANCO B
                 LEFT JOIN PCCONTA C ON B.CODBANCO = C.CODCONTAMASTER),

              CLIENTE AS
               (SELECT C.CODCLI,
                      T.CODCONTA
                 FROM PCCLIENT C
                 JOIN PCCONTA T ON C.CODCLI = T.CODCONTAMASTER
                WHERE T.GRUPOCONTA = 110
                  AND T.CODCONTA > vCONTA_CLIENTE_NACIONAL),

              BASE_RECEBER AS
               (SELECT F.CODEMPRESA,
                      P.CODFILIAL,
                      P.DTEMISSAO,
                      P.DTDESD,
                      P.DTESTORNO,
                      P.DTVENC DTVENCIMENTO,
                      C.DIA_UTIL_FINANCEIRO DTVENCUTIL,
                      (CASE
                        WHEN V.CODAREA = 1 THEN
                         'DISTRIBUICAO'
                        WHEN V.CODAREA = 2 THEN
                         'CORPORATIVO'
                        WHEN CODGERENTE = 3 THEN
                         'LOJAS'
                        WHEN CODGERENTE = 4 THEN
                         'ECOMMERCE'
                        ELSE
                         'OUTROS'
                      END) TIPO,
                      (CASE
                        WHEN DTPAG IS NULL THEN
                         (TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO)
                        ELSE
                         0
                      END) DIASVENCIDOS,
                      (CASE
                        WHEN P.DTPAG IS NOT NULL THEN
                         0
                        WHEN COB.CODSTATUSCOB = 42 THEN
                         11
                        WHEN COB.CODSTATUSCOB = 41 THEN
                         10
                        WHEN COB.CODSTATUSCOB = 43 THEN
                         9
                        WHEN (TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 30
                             AND P.CODCOB = 'JUR' THEN
                         13
                        WHEN (TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 720
                             AND P.CODCOB <> 'JUR' THEN
                         12
                        WHEN O.CODOCORRENCIA = '21' THEN
                         3
                        WHEN O.CODOCORRENCIA = '32'
                             AND P.CODCOB <> 'JUR' THEN
                         6
                        WHEN (TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 75
                             AND P.CODCOB <> 'JUR' THEN
                         8
                        WHEN (TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 45
                             AND P.CODCOB <> 'JUR' THEN
                         7
                        WHEN NVL(P.PROTESTO, 'N') = 'S' THEN
                         5
                        WHEN NVL(P.CARTORIO, 'N') = 'S' THEN
                         4
                        WHEN (TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 5
                             AND P.CODCOB = 'BK' THEN
                         2
                        WHEN ((TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 0 OR P.CODCOB = 'C') THEN
                         1
                        ELSE
                         99
                      END) CODCOBRANCA,
                      (CASE
                        WHEN P.DTPAG IS NOT NULL THEN
                         'TITULO PAGO'
                        WHEN COB.CODSTATUSCOB = 42 THEN
                         '11-TITULO COM O JURÍDICO'
                        WHEN COB.CODSTATUSCOB = 41 THEN
                         '10-TITULO ENVIADO PARA COBRANÇA EXTERNA'
                        WHEN COB.CODSTATUSCOB = 43 THEN
                         '09-TITULO NEGOCIADO COM DATA PARA RECEBIMENTO'
                        WHEN (TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 30
                             AND P.CODCOB = 'JUR' THEN
                         '13- JUROS PENDENTE'
                        WHEN (TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 720
                             AND P.CODCOB <> 'JUR' THEN
                         '12-VERIFICAR BAIXA COMO PERDA E BLOQUEAR DEFINITIVO CLIENTE'
                        WHEN O.CODOCORRENCIA = '21' THEN
                         '03-NEGOCIAR COM CLIENTE - TIT. NAO PROTESTADO'
                        WHEN O.CODOCORRENCIA = '32'
                             AND P.CODCOB <> 'JUR' THEN
                         '06-NEGOCIAR COM CLIENTE APOS PROTESTO'
                        WHEN (TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 75
                             AND P.CODCOB <> 'JUR' THEN
                         '08-VERIFICAR ENVIO AREA JURIDICA'
                        WHEN (TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 45
                             AND P.CODCOB <> 'JUR' THEN
                         '07-VERIFICAR ENVIO PARA COBRANÇA EXTERNA'
                        WHEN NVL(P.PROTESTO, 'N') = 'S' THEN
                         '05-TITULO PROTESTADO'
                        WHEN NVL(P.CARTORIO, 'N') = 'S' THEN
                         '04-TITULO EM CARTÓRIO'
                        WHEN (TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 5
                             AND P.CODCOB = 'BK' THEN
                         '02-PRESTES A ENTRAR EM CARTORIO'
                        WHEN ((TRUNC(SYSDATE) - C.DIA_UTIL_FINANCEIRO) > 0 OR P.CODCOB = 'C') THEN
                         '01-ENTRAR EM CONTATO COM CLIENTE'
                        ELSE
                         '99-FALTA PARAMETRIZAR'
                      END) COBRANCA,
                      P.CODCLI,
                      NVL(T.CODCONTA, vCONTA_CLIENTE_NACIONAL) CONTACLIENTE,
                      NVL(P.CODCOBORIG, P.CODCOB) CODCOBORIG,
                      P.CODCOB,
                      P.CODUSUR,
                      P.NUMTRANSVENDA,
                      P.DUPLIC NUMNOTA,
                      P.PREST,
                      P.VALOR,
                      NVL(P.VALORDESC, 0) VLDESCONTO,
                      (NVL(P.TXPERM, 0) + NVL(P.VLROUTROSACRESC, 0)) VLJUROS,
                      (P.VALOR - NVL(P.VALORDESC, 0)) VALORLIQ,
                      DECODE(NVL(P.CARTORIO, 'N'), 'S', 'SIM', 'NÃO') CARTORIO,
                      DECODE(NVL(P.PROTESTO, 'N'), 'S', 'SIM', 'NÃO') PROTESTO,
                      P.DTINCLUSAOMANUAL,
                      P.VPAGO VLRECEBIDO,
                      P.DTPAG DTPAGAMENTO,
                      P.NUMTRANS,
                      M.DTCOMPENSACAO,
                      M.CODBANCO,
                      B.CODCONTA CONTABANCO
                 FROM PCPREST P
                 LEFT JOIN PCMOVCR M ON P.NUMTRANS = M.NUMTRANS
                 LEFT JOIN BANCO B ON M.CODBANCO = B.CODBANCO
                 LEFT JOIN CLIENTE T ON T.CODCLI = P.CODCLI
                 LEFT JOIN BI_SINC_FILIAL F ON F.CODFILIAL = P.CODFILIAL
                 LEFT JOIN BI_SINC_CALENDARIO C ON C.DATA = P.DTVENC
                 LEFT JOIN BI_SINC_VENDEDOR V ON V.CODUSUR = P.CODUSUR
                 LEFT JOIN OCORRENCIAS O ON O.NUMTRANSVENDA = P.NUMTRANSVENDA
                                        AND O.PREST = P.PREST
                 LEFT JOIN COBRANCA COB ON COB.NUMTRANSVENDA = P.NUMTRANSVENDA
                                       AND COB.PREST = P.PREST
                WHERE (P.DTEMISSAO >= vDATA_MOV_INCREMENTAL OR P.DTPAG IS NULL)
                  AND (M.DTESTORNO IS NULL OR (M.DTESTORNO IS NOT NULL AND M.ESTORNO = 'N'))
                  AND P.DTCANCEL IS NULL)

              SELECT P.*
                FROM BASE_RECEBER P
                LEFT JOIN BI_SINC_LANC_RECEBER_BASE S ON S.NUMTRANSVENDA = P.NUMTRANSVENDA
                                                     AND S.PREST = P.PREST
               WHERE S.DT_UPDATE IS NULL
                  OR S.CODEMPRESA <> P.CODEMPRESA
                  OR S.CODFILIAL <> P.CODFILIAL
                  OR NVL(S.DTEMISSAO, '01/01/1899') <> P.DTEMISSAO
                  OR NVL(S.DTDESD, '01/01/1899') <> P.DTDESD
                  OR NVL(S.DTESTORNO, '01/01/1899') <> P.DTESTORNO
                  OR NVL(S.DTVENCIMENTO, '01/01/1899') <> P.DTVENCIMENTO
                  OR NVL(S.DTVENCUTIL, '01/01/1899') <> P.DTVENCUTIL
                  OR S.TIPO <> P.TIPO
                  OR NVL(S.DIASVENCIDOS, 0) <> NVL(P.DIASVENCIDOS, 0)
                  OR NVL(S.CODCOBRANCA, 0) <> NVL(P.CODCOBRANCA, 0)
                  OR NVL(S.COBRANCA, '0') <> NVL(P.COBRANCA, '0')
                  OR NVL(S.CODCLI, 0) <> NVL(P.CODCLI, 0)
                  OR NVL(S.CONTACLIENTE, 0) <> NVL(P.CONTACLIENTE, 0)
                  OR NVL(S.CODCOBORIG, '0') <> NVL(P.CODCOBORIG, '0')
                  OR NVL(S.CODCOB, '0') <> NVL(P.CODCOB, '0')
                  OR NVL(S.CODUSUR, 0) <> NVL(P.CODUSUR, 0)
                  OR NVL(S.NUMNOTA, 0) <> NVL(P.NUMNOTA, 0)
                  OR NVL(S.VALOR, 0) <> NVL(P.VALOR, 0)
                  OR NVL(S.VLDESCONTO, 0) <> NVL(P.VLDESCONTO, 0)
                  OR NVL(S.VLJUROS, 0) <> NVL(P.VLJUROS, 0)
                  OR NVL(S.VALORLIQ, 0) <> NVL(P.VALORLIQ, 0)
                  OR NVL(S.CARTORIO, '0') <> NVL(P.CARTORIO, '0')
                  OR NVL(S.PROTESTO, '0') <> NVL(P.PROTESTO, '0')
                  OR NVL(S.DTINCLUSAOMANUAL, '01/01/1899') <> NVL(P.DTINCLUSAOMANUAL, '01/01/1899')
                  OR NVL(S.VLRECEBIDO, 0) <> NVL(P.VLRECEBIDO, 0)
                  OR NVL(S.DTPAGAMENTO, '01/01/1899') <> P.DTPAGAMENTO
                  OR NVL(S.NUMTRANS, 0) <> P.NUMTRANS
                  OR NVL(S.DTCOMPENSACAO, '01/01/1899') <> P.DTCOMPENSACAO
                  OR NVL(S.CODBANCO, 0) <> NVL(P.CODBANCO, 0)
                  OR NVL(S.CONTABANCO, 0) <> NVL(P.CONTABANCO, 0))

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas

  LOOP
    BEGIN
      UPDATE BI_SINC_LANC_RECEBER_BASE
         SET CODEMPRESA       = r.CODEMPRESA,
             CODFILIAL        = r.CODFILIAL,
             DTEMISSAO        = r.DTEMISSAO,
             DTDESD           = r.DTDESD,
             DTESTORNO        = r.DTESTORNO,
             DTVENCIMENTO     = r.DTVENCIMENTO,
             DTVENCUTIL       = r.DTVENCUTIL,
             TIPO             = r.TIPO,
             DIASVENCIDOS     = r.DIASVENCIDOS,
             CODCOBRANCA      = r.CODCOBRANCA,
             COBRANCA         = r.COBRANCA,
             CODCLI           = r.CODCLI,
             CONTACLIENTE     = r.CONTACLIENTE,
             CODCOBORIG       = r.CODCOBORIG,
             CODCOB           = r.CODCOB,
             CODUSUR          = r.CODUSUR,
             NUMNOTA          = r.NUMNOTA,
             VALOR            = r.VALOR,
             VLDESCONTO       = r.VLDESCONTO,
             VLJUROS          = r.VLJUROS,
             VALORLIQ         = r.VALORLIQ,
             CARTORIO         = r.CARTORIO,
             PROTESTO         = r.PROTESTO,
             DTINCLUSAOMANUAL = r.DTINCLUSAOMANUAL,
             VLRECEBIDO       = r.VLRECEBIDO,
             DTPAGAMENTO      = r.DTPAGAMENTO,
             NUMTRANS         = r.NUMTRANS,
             DTCOMPENSACAO    = r.DTCOMPENSACAO,
             CODBANCO         = r.CODBANCO,
             CONTABANCO       = r.CONTABANCO,
             DT_UPDATE        = SYSDATE
       WHERE NUMTRANSVENDA = r.NUMTRANSVENDA
         AND PREST = r.PREST;

      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_LANC_RECEBER_BASE
          (CODEMPRESA,
           CODFILIAL,
           DTEMISSAO,
           DTDESD,
           DTESTORNO,
           DTVENCIMENTO,
           DTVENCUTIL,
           TIPO,
           DIASVENCIDOS,
           CODCOBRANCA,
           COBRANCA,
           CODCLI,
           CONTACLIENTE,
           CODCOBORIG,
           CODCOB,
           CODUSUR,
           NUMTRANSVENDA,
           NUMNOTA,
           PREST,
           VALOR,
           VLDESCONTO,
           VLJUROS,
           VALORLIQ,
           CARTORIO,
           PROTESTO,
           DTINCLUSAOMANUAL,
           VLRECEBIDO,
           DTPAGAMENTO,
           NUMTRANS,
           DTCOMPENSACAO,
           CODBANCO,
           CONTABANCO,
           DT_UPDATE)
        VALUES
          (r.CODEMPRESA,
           r.CODFILIAL,
           r.DTEMISSAO,
           r.DTDESD,
           r.DTESTORNO,
           r.DTVENCIMENTO,
           r.DTVENCUTIL,
           r.TIPO,
           r.DIASVENCIDOS,
           r.CODCOBRANCA,
           r.COBRANCA,
           r.CODCLI,
           r.CONTACLIENTE,
           r.CODCOBORIG,
           r.CODCOB,
           r.CODUSUR,
           r.NUMTRANSVENDA,
           r.NUMNOTA,
           r.PREST,
           r.VALOR,
           r.VLDESCONTO,
           r.VLJUROS,
           r.VALORLIQ,
           r.CARTORIO,
           r.PROTESTO,
           r.DTINCLUSAOMANUAL,
           r.VLRECEBIDO,
           r.DTPAGAMENTO,
           r.NUMTRANS,
           r.DTCOMPENSACAO,
           r.CODBANCO,
           r.CONTABANCO,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

  --EXCLUIR REGISTROS QUE NAO PERTENCEM MAIS A PCPREST
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_LANC_RECEBER_BASE
 WHERE (NUMTRANSVENDA, PREST) IN
       (SELECT S.NUMTRANSVENDA, S.PREST
         FROM BI_SINC_LANC_RECEBER_BASE S
         LEFT JOIN PCPREST P ON S.NUMTRANSVENDA = P.NUMTRANSVENDA AND S.PREST = P.PREST
        WHERE P.NUMTRANSVENDA IS NULL)';
  END;

  COMMIT;

END;
