CREATE OR REPLACE PROCEDURE PRC_SINC_CREDITO_CLIENTE AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 120;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

  ------------------------CONTAS
  vCONTA_CLIENTE_NACIONAL NUMBER := 1152;

BEGIN

  FOR r IN (WITH ORIGEM_NUMERARIO AS
               (SELECT N.NUMCRED,
                      N.NUMTRANS
                 FROM PCCRECLI N
                WHERE NVL(N.NUMCRED, 0) > 0
                  AND NVL(N.NUMTRANS, 0) > 0
                GROUP BY N.NUMCRED,
                         N.NUMTRANS),
              
              DUPLICATA AS
               (SELECT P.NUMTRANSVENDA,
                      P.DUPLIC,
                      P.CODCLI
                 FROM PCPREST P
                GROUP BY P.NUMTRANSVENDA,
                         P.DUPLIC,
                         P.CODCLI),
              
              CLIENTDUP AS
               (SELECT P.NUMTRANSVENDA,
                      P.PREST,
                      P.CODCLI
                 FROM PCPREST P
                GROUP BY P.NUMTRANSVENDA,
                         P.PREST,
                         P.CODCLI),
              
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
              
              CREDITOS AS
               (SELECT F.CODEMPRESA,
                      C.CODFILIAL,
                      C.CODCLI,
                      E.CODFORNEC CODCLIDEV,
                      C.CODIGO,
                      C.NUMCRED,
                      C.DTDESCONTO,
                      C.DTESTORNO,
                      NVL(MN.DTCOMPENSACAO, ME.DTCOMPENSACAO) DTCOMPENSACAO,
                      NVL(T.CODCONTA, vCONTA_CLIENTE_NACIONAL) CONTACLIENTE,
                      (CASE
                        WHEN NVL(C.NUMTRANS, 0) > 0 THEN
                         (SELECT B.CODCONTA FROM BANCO B WHERE B.CODBANCO = MN.CODBANCO)
                        WHEN NVL(C.NUMTRANSBAIXA, 0) > 0 THEN
                         (SELECT B.CODCONTA FROM BANCO B WHERE B.CODBANCO = ME.CODBANCO)
                        ELSE
                         NULL
                      END) CONTABANCO,
                      C.VALOR,
                      (ME.VALOR * -1) VLMOVCR,
                      P.DUPLIC,
                      E.NUMNOTA NUMNOTADEV,
                      C.NUMERARIO,
                      C.CODROTINA,
                      C.NUMTRANS,
                      N.NUMTRANS NUMTRANS_MN,
                      C.NUMTRANSBAIXA,
                      C.NUMTRANSVENDADESC,
                      C.NUMTRANSENTDEVCLI,
                      C.NUMLANC,
                      C.NUMLANCBAIXA
                 FROM PCCRECLI C
                 LEFT JOIN PCMOVCR MN ON MN.NUMTRANS = C.NUMTRANS
                 LEFT JOIN PCMOVCR ME ON ME.NUMTRANS = C.NUMTRANSBAIXA
                 LEFT JOIN ORIGEM_NUMERARIO N ON N.NUMCRED = C.NUMCRED
                 LEFT JOIN DUPLICATA P ON P.NUMTRANSVENDA = C.NUMTRANSVENDADESC
                 LEFT JOIN PCNFENT E ON E.NUMTRANSENT = C.NUMTRANSENTDEVCLI
                 LEFT JOIN BI_SINC_FILIAL F ON F.CODFILIAL = C.CODFILIAL
                 LEFT JOIN CLIENTDUP CD ON CD.NUMTRANSVENDA = C.NUMTRANSVENDADESC
                                       AND CD.PREST = C.PRESTRESTCLI
                 LEFT JOIN CLIENTE T ON T.CODCLI = CD.CODCLI
                WHERE NVL(C.CODROTINA, 0) <> 9801
                  AND C.DTLANC >= vDATA_MOV_INCREMENTAL)
              
              SELECT C.*
                FROM CREDITOS C
                LEFT JOIN BI_SINC_CREDITO_CLIENTE S ON S.CODIGO = C.CODIGO
               WHERE S.DT_UPDATE IS NULL
                  OR NVL(S.CODEMPRESA, '0') <> NVL(C.CODEMPRESA, '0')
                  OR NVL(S.CODFILIAL, '0') <> NVL(C.CODFILIAL, '0')
                  OR NVL(S.CODCLI, 0) <> NVL(C.CODCLI, 0)
                  OR NVL(S.CODCLIDEV, 0) <> NVL(C.CODCLIDEV, 0)
                  OR NVL(S.NUMCRED, 0) <> NVL(C.NUMCRED, 0)
                  OR NVL(S.DTDESCONTO, '01/01/1899') <> NVL(C.DTDESCONTO, '01/01/1899')
                  OR NVL(S.DTESTORNO, '01/01/1899') <> NVL(C.DTESTORNO, '01/01/1899')
                  OR NVL(S.DTCOMPENSACAO, '01/01/1899') <> NVL(C.DTCOMPENSACAO, '01/01/1899')
                  OR NVL(S.CONTACLIENTE, 0) <> NVL(C.CONTACLIENTE, 0)
                  OR NVL(S.CONTABANCO, 0) <> NVL(C.CONTABANCO, 0)
                  OR NVL(S.VALOR, 0) <> NVL(C.VALOR, 0)
                  OR NVL(S.VLMOVCR, 0) <> NVL(C.VLMOVCR, 0)
                  OR NVL(S.DUPLIC, 0) <> NVL(C.DUPLIC, 0)
                  OR NVL(S.NUMNOTADEV, 0) <> NVL(C.NUMNOTADEV, 0)
                  OR NVL(S.NUMERARIO, '0') <> NVL(C.NUMERARIO, '0')
                  OR NVL(S.CODROTINA, 0) <> NVL(C.CODROTINA, 0)
                  OR NVL(S.NUMTRANS, 0) <> NVL(C.NUMTRANS, 0)
                  OR NVL(S.NUMTRANS_MN, 0) <> NVL(C.NUMTRANS_MN, 0)
                  OR NVL(S.NUMTRANSBAIXA, 0) <> NVL(C.NUMTRANSBAIXA, 0)
                  OR NVL(S.NUMTRANSVENDADESC, 0) <> NVL(C.NUMTRANSVENDADESC, 0)
                  OR NVL(S.NUMTRANSENTDEVCLI, 0) <> NVL(C.NUMTRANSENTDEVCLI, 0)
                  OR NVL(S.NUMLANC, 0) <> NVL(C.NUMLANC, 0)
                  OR NVL(S.NUMLANCBAIXA, 0) <> NVL(C.NUMLANCBAIXA, 0))
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_CREDITO_CLIENTE
         SET CODEMPRESA        = r.CODEMPRESA,
             CODFILIAL         = r.CODFILIAL,
             CODCLI            = r.CODCLI,
             CODCLIDEV         = r.CODCLIDEV,
             NUMCRED           = r.NUMCRED,
             DTDESCONTO        = r.DTDESCONTO,
             DTESTORNO         = r.DTESTORNO,
             DTCOMPENSACAO     = r.DTCOMPENSACAO,
             CONTACLIENTE      = r.CONTACLIENTE,
             CONTABANCO        = r.CONTABANCO,
             VALOR             = r.VALOR,
             VLMOVCR           = r.VLMOVCR,
             DUPLIC            = r.DUPLIC,
             NUMNOTADEV        = r.NUMNOTADEV,
             NUMERARIO         = r.NUMERARIO,
             CODROTINA         = r.CODROTINA,
             NUMTRANS          = r.NUMTRANS,
             NUMTRANS_MN       = r.NUMTRANS_MN,
             NUMTRANSBAIXA     = r.NUMTRANSBAIXA,
             NUMTRANSVENDADESC = r.NUMTRANSVENDADESC,
             NUMTRANSENTDEVCLI = r.NUMTRANSENTDEVCLI,
             NUMLANC           = r.NUMLANC,
             NUMLANCBAIXA      = r.NUMLANCBAIXA,
             DT_UPDATE         = SYSDATE
       WHERE CODIGO = r.CODIGO;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_CREDITO_CLIENTE
          (CODEMPRESA,
           CODFILIAL,
           CODCLI,
           CODCLIDEV,
           CODIGO,
           NUMCRED,
           DTDESCONTO,
           DTESTORNO,
           DTCOMPENSACAO,
           CONTACLIENTE,
           CONTABANCO,
           VALOR,
           VLMOVCR,
           DUPLIC,
           NUMNOTADEV,
           NUMERARIO,
           CODROTINA,
           NUMTRANS,
           NUMTRANS_MN,
           NUMTRANSBAIXA,
           NUMTRANSVENDADESC,
           NUMTRANSENTDEVCLI,
           NUMLANC,
           NUMLANCBAIXA,
           DT_UPDATE)
        VALUES
          (r.CODEMPRESA,
           r.CODFILIAL,
           r.CODCLI,
           r.CODCLIDEV,
           r.CODIGO,
           r.NUMCRED,
           r.DTDESCONTO,
           r.DTESTORNO,
           r.DTCOMPENSACAO,
           r.CONTACLIENTE,
           r.CONTABANCO,
           r.VALOR,
           r.VLMOVCR,
           r.DUPLIC,
           r.NUMNOTADEV,
           r.NUMERARIO,
           r.CODROTINA,
           r.NUMTRANS,
           r.NUMTRANS_MN,
           r.NUMTRANSBAIXA,
           r.NUMTRANSVENDADESC,
           r.NUMTRANSENTDEVCLI,
           r.NUMLANC,
           r.NUMLANCBAIXA,
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
