CREATE OR REPLACE PROCEDURE PRC_SINC_MOV_BANCO AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 120;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

BEGIN

  FOR r IN (WITH BANCO AS
               (SELECT B.CODFILIAL,
                      B.CODBANCO,
                      C.CODCONTA
                 FROM PCBANCO B
                 LEFT JOIN PCCONTA C ON B.CODBANCO = C.CODCONTAMASTER),
              
              MOV_EXCLUIR_COB AS
               (SELECT M.NUMTRANS NUMTRANS_EXCLUIR_COB
                 FROM PCMOVCR M
                WHERE M.CODCOB NOT IN ('D')
                  AND M.CODROTINALANC IN (632, 639, 643)
                GROUP BY M.NUMTRANS),
              
              MOV_EXCLUIR_LANC_UNICO AS
               (SELECT M.NUMTRANS NUMTRANS_EXCLUIR_LANC_UNICO,
                      COUNT(M.NUMTRANS) QT
                 FROM PCMOVCR M
                WHERE M.CODROTINALANC IN (632, 639, 643)
                  AND (M.DTESTORNO IS NULL OR (M.DTESTORNO IS NOT NULL AND M.ESTORNO = 'N'))
                GROUP BY M.NUMTRANS
               HAVING COUNT(M.NUMTRANS) < 2),
              
              MOVIMENTO AS
               (SELECT F.CODEMPRESA,
                      B.CODFILIAL,
                      M.NUMSEQ,
                      M.NUMTRANS,
                      M.DATA,
                      M.DTCOMPENSACAO,
                      M.CODCOB,
                      M.CODBANCO,
                      B.CODCONTA CONTABANCO,
                      M.TIPO,
                      M.VALOR,
                      (CASE
                        WHEN LENGTH(NVL(TRIM(HISTORICO2), 1)) = 1 THEN
                         TRIM(M.HISTORICO)
                        ELSE
                         (TRIM(M.HISTORICO) || ' - ' || TRIM(M.HISTORICO2))
                      END) HISTORICO,
                      M.CODROTINALANC
                 FROM PCMOVCR M
                 LEFT JOIN MOV_EXCLUIR_COB R ON R.NUMTRANS_EXCLUIR_COB = M.NUMTRANS
                 LEFT JOIN MOV_EXCLUIR_LANC_UNICO L ON L.NUMTRANS_EXCLUIR_LANC_UNICO = M.NUMTRANS
                 LEFT JOIN BANCO B ON B.CODBANCO = M.CODBANCO
                 LEFT JOIN BI_SINC_FILIAL F ON F.CODFILIAL = B.CODFILIAL
                WHERE M.DATA >= vDATA_MOV_INCREMENTAL
                  AND M.CODROTINALANC IN (632, 639, 643)
                  AND (M.DTESTORNO IS NULL OR (M.DTESTORNO IS NOT NULL AND M.ESTORNO = 'N'))
                  AND R.NUMTRANS_EXCLUIR_COB IS NULL
                  AND L.NUMTRANS_EXCLUIR_LANC_UNICO IS NULL)
              
              SELECT M.*
                FROM MOVIMENTO M
                LEFT JOIN BI_SINC_MOV_BANCO S ON S.NUMSEQ = M.NUMSEQ
               WHERE S.DT_UPDATE IS NULL
                  OR NVL(S.CODEMPRESA, '0') <> NVL(M.CODEMPRESA, '0')
                  OR NVL(S.CODFILIAL, '0') <> NVL(M.CODFILIAL, '0')
                  OR NVL(S.NUMTRANS, 0) <> NVL(M.NUMTRANS, 0)
                  OR NVL(S.DATA, '01/01/1899') <> NVL(M.DATA, '01/01/1899')
                  OR NVL(S.DTCOMPENSACAO, '01/01/1899') <> NVL(M.DTCOMPENSACAO, '01/01/1899')
                  OR NVL(S.CODCOB, '0') <> NVL(M.CODCOB, '0')
                  OR NVL(S.CODBANCO, 0) <> NVL(M.CODBANCO, 0)
                  OR NVL(S.CONTABANCO, 0) <> NVL(M.CONTABANCO, 0)
                  OR NVL(S.TIPO, '0') <> NVL(M.TIPO, '0')
                  OR NVL(S.VALOR, 0) <> NVL(M.VALOR, 0)
                  OR NVL(S.HISTORICO, '0') <> NVL(M.HISTORICO, '0')
                  OR NVL(S.CODROTINALANC, 0) <> NVL(M.CODROTINALANC, 0))
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_MOV_BANCO
         SET CODEMPRESA    = r.CODEMPRESA,
             CODFILIAL     = r.CODFILIAL,
             NUMTRANS      = r.NUMTRANS,
             DATA          = r.DATA,
             DTCOMPENSACAO = r.DTCOMPENSACAO,
             CODCOB        = r.CODCOB,
             CODBANCO      = r.CODBANCO,
             CONTABANCO    = r.CONTABANCO,
             TIPO          = r.TIPO,
             VALOR         = r.VALOR,
             HISTORICO     = r.HISTORICO,
             CODROTINALANC = r.CODROTINALANC,
             DT_UPDATE     = SYSDATE
       WHERE NUMSEQ = r.NUMSEQ;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_MOV_BANCO
          (CODEMPRESA,
           CODFILIAL,
           NUMSEQ,
           NUMTRANS,
           DATA,
           DTCOMPENSACAO,
           CODCOB,
           CODBANCO,
           CONTABANCO,
           TIPO,
           VALOR,
           HISTORICO,
           CODROTINALANC,
           DT_UPDATE)
        VALUES
          (r.CODEMPRESA,
           r.CODFILIAL,
           r.NUMSEQ,
           r.NUMTRANS,
           r.DATA,
           r.DTCOMPENSACAO,
           r.CODCOB,
           r.CODBANCO,
           r.CONTABANCO,
           r.TIPO,
           r.VALOR,
           r.HISTORICO,
           r.CODROTINALANC,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a criação da tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

  --EXCLUIR REGISTROS QUE NAO FORAM ESTORNADOS
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_MOV_BANCO
  WHERE (NUMSEQ) IN (SELECT S.NUMSEQ
                       FROM BI_SINC_MOV_BANCO S
                       LEFT JOIN PCMOVCR M ON S.NUMSEQ = M.NUMSEQ
                      WHERE M.CODROTINALANC IN (632, 639, 643)
                        AND (M.DTESTORNO IS NULL OR (M.DTESTORNO IS NOT NULL AND M.ESTORNO = ''N''))
                        AND M.NUMSEQ IS NULL)';
  END;

  COMMIT;

END;
