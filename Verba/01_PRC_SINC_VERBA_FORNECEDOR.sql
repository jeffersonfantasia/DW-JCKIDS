CREATE OR REPLACE PROCEDURE PRC_SINC_VERBA_FORNECEDOR AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 120;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

BEGIN

  FOR r IN (WITH BANCO AS
               (SELECT B.CODBANCO,
                      C.CODCONTA
                 FROM PCBANCO B
                 LEFT JOIN PCCONTA C ON B.CODBANCO = C.CODCONTAMASTER),
              
              VERBAS_DEVOLUCAO AS
               (SELECT NUMVERBA,
                      NUMTRANSENTDEVFORNEC NUMTRANSVENDADEV
                 FROM PCMOVCRFOR
                WHERE NUMTRANSENTDEVFORNEC IS NOT NULL
                GROUP BY NUMVERBA,
                         NUMTRANSENTDEVFORNEC),
              
              LANCAMENTOS AS
               (SELECT L.RECNUM,
                      L.CODFORNEC,
                      M.DTCOMPENSACAO,
                      L.DTPAGTO,
                      B.CODCONTA CONTABANCO,
                      L.NUMNOTA,
                      L.DUPLIC
                 FROM PCLANC L
                 LEFT JOIN PCMOVCR M ON M.NUMTRANS = L.NUMTRANS
                 LEFT JOIN BANCO B ON B.CODBANCO = M.CODBANCO
                WHERE (M.DTESTORNO IS NULL OR (M.DTESTORNO IS NOT NULL AND M.ESTORNO = 'N'))),
              
              VERBAS AS
               (SELECT F.CODEMPRESA,
                      V.CODFILIAL,
                      NVL(L.CODFORNEC, V.CODFORNEC) CODFORNEC,
                      V.NUMTRANSCRFOR,
                      V.NUMVERBA,
                      V.DTPAGO DTPAGVERBA,
                      L.DTPAGTO DTPAGLANC,
                      L.DTCOMPENSACAO,
                      L.CONTABANCO,
                      ROUND(V.VALOR, 2) VALOR,
                      V.ROTINALANC CODROTINALANC,
                      V.NUMTRANSENT,
                      D.NUMTRANSVENDADEV,
                      S.NUMNOTA NUMNOTADEV,
                      (L.NUMNOTA || '-' || L.DUPLIC) NUMNOTADESC,
                      V.NUMLANC,
                      V.NUMTRANSEST NUMTRANS
                 FROM PCMOVCRFOR V
                 LEFT JOIN VERBAS_DEVOLUCAO D ON V.NUMVERBA = D.NUMVERBA
                 LEFT JOIN LANCAMENTOS L ON L.RECNUM = V.NUMLANC
                 LEFT JOIN BI_SINC_FILIAL F ON F.CODFILIAL = V.CODFILIAL
                 LEFT JOIN PCNFSAID S ON S.NUMTRANSVENDA = D.NUMTRANSVENDADEV
                WHERE V.DTPAGO IS NOT NULL
                  AND V.NUMTRANSCRFORORIGEM IS NULL
                  AND V.ROTINALANC NOT IN (1327)
                  AND (NVL(V.NUMLANC, 0) = 0 AND V.NUMTRANSENT > 0 OR V.NUMLANC > 0 AND NVL(V.NUMTRANSENT, 0) = 0)
                  AND V.DTPAGO >= vDATA_MOV_INCREMENTAL)
              
              SELECT V.*
                FROM VERBAS V
                LEFT JOIN BI_SINC_VERBA_FORNECEDOR S ON S.NUMTRANSCRFOR = V.NUMTRANSCRFOR
               WHERE S.DT_UPDATE IS NULL
                  OR NVL(S.CODEMPRESA, '0') <> NVL(V.CODEMPRESA, '0')
                  OR NVL(S.CODFILIAL, '0') <> NVL(V.CODFILIAL, '0')
                  OR NVL(S.CODFORNEC, 0) <> NVL(V.CODFORNEC, 0)
                  OR NVL(S.NUMVERBA, 0) <> NVL(V.NUMVERBA, 0)
                  OR NVL(S.DTPAGVERBA, '01/01/1899') <> NVL(V.DTPAGVERBA, '01/01/1899')
                  OR NVL(S.DTPAGLANC, '01/01/1899') <> NVL(V.DTPAGLANC, '01/01/1899')
                  OR NVL(S.DTCOMPENSACAO, '01/01/1899') <> NVL(V.DTCOMPENSACAO, '01/01/1899')
                  OR NVL(S.CONTABANCO, 0) <> NVL(V.CONTABANCO, 0)
                  OR NVL(S.VALOR, 0) <> NVL(V.VALOR, 0)
                  OR NVL(S.CODROTINALANC, 0) <> NVL(V.CODROTINALANC, 0)
                  OR NVL(S.NUMTRANSENT, 0) <> NVL(V.NUMTRANSENT, 0)
                  OR NVL(S.NUMTRANSVENDADEV, 0) <> NVL(V.NUMTRANSVENDADEV, 0)
                  OR NVL(S.NUMNOTADEV, 0) <> NVL(V.NUMNOTADEV, 0)
                  OR NVL(S.NUMNOTADESC, '0') <> NVL(V.NUMNOTADESC, '0')
                  OR NVL(S.NUMLANC, 0) <> NVL(V.NUMLANC, 0)
                  OR NVL(S.NUMTRANS, 0) <> NVL(V.NUMTRANS, 0))
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_VERBA_FORNECEDOR
         SET CODEMPRESA       = r.CODEMPRESA,
             CODFILIAL        = r.CODFILIAL,
             CODFORNEC        = r.CODFORNEC,
             NUMVERBA         = r.NUMVERBA,
             DTPAGVERBA       = r.DTPAGVERBA,
             DTPAGLANC        = r.DTPAGLANC,
             DTCOMPENSACAO    = r.DTCOMPENSACAO,
             CONTABANCO       = r.CONTABANCO,
             VALOR            = r.VALOR,
             CODROTINALANC    = r.CODROTINALANC,
             NUMTRANSENT      = r.NUMTRANSENT,
             NUMTRANSVENDADEV = r.NUMTRANSVENDADEV,
             NUMNOTADEV       = r.NUMNOTADEV,
             NUMNOTADESC      = r.NUMNOTADESC,
             NUMLANC          = r.NUMLANC,
             NUMTRANS         = r.NUMTRANS,
             DT_UPDATE        = SYSDATE
       WHERE NUMTRANSCRFOR = r.NUMTRANSCRFOR;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_VERBA_FORNECEDOR
          (CODEMPRESA,
           CODFILIAL,
           CODFORNEC,
           NUMVERBA,
           NUMTRANSCRFOR,
           DTPAGVERBA,
           DTPAGLANC,
           DTCOMPENSACAO,
           CONTABANCO,
           VALOR,
           CODROTINALANC,
           NUMTRANSENT,
           NUMTRANSVENDADEV,
           NUMNOTADEV,
           NUMNOTADESC,
           NUMLANC,
           NUMTRANS,
           DT_UPDATE)
        VALUES
          (r.CODEMPRESA,
           r.CODFILIAL,
           r.CODFORNEC,
           r.NUMVERBA,
           r.NUMTRANSCRFOR,
           r.DTPAGVERBA,
           r.DTPAGLANC,
           r.DTCOMPENSACAO,
           r.CONTABANCO,
           r.VALOR,
           r.CODROTINALANC,
           r.NUMTRANSENT,
           r.NUMTRANSVENDADEV,
           r.NUMNOTADEV,
           r.NUMNOTADESC,
           r.NUMLANC,
           r.NUMTRANS,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a criação da tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

END;
