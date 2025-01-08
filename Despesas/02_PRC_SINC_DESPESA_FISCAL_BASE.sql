CREATE OR REPLACE PROCEDURE PRC_SINC_DESPESA_FISCAL_BASE AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 75;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

BEGIN
  FOR r IN (WITH LANCAMENTO AS
               (SELECT *
                 FROM (SELECT L.RECNUM,
                              L.ANO_COMPETENCIA,
                              L.CODFORNEC,
                              L.NUMNOTA,
                              ROW_NUMBER() OVER(PARTITION BY CODFORNEC, NUMNOTA ORDER BY RECNUM) AS RN
                         FROM BI_SINC_LANC_PAGAR_BASE L
                         JOIN BI_SINC_PLANO_CONTAS_JC P ON P.CODGERENCIAL = L.CODCONTA
                        WHERE L.TIPOPARCEIRO = 'F'
                          AND L.NUMNOTA > 0
                          AND P.CODDRE IS NOT NULL)
                WHERE RN = 1),
              
              FRETES AS
               (SELECT E.NUMTRANSENT,
                      V.CODSUPERVISOR
                 FROM BI_SINC_DESPESA_FISCAL E
                 JOIN PCCONHECIMENTOFRETEI F ON F.NUMTRANSCONHEC = E.NUMTRANSENT
                 JOIN PCNFSAID S ON S.CHAVENFE = F.CHAVENFE
                 LEFT JOIN BI_SINC_VENDEDOR V ON V.CODUSUR = S.CODUSUR
                WHERE E.ESPECIE = 'CT'),
              
              DESPESA_FISCAL_BASE AS
               (SELECT E.CODEMPRESA,
                      E.CODFILIAL,
                      E.DATA,
                      E.NUMTRANSENT,
                      F.CODSUPERVISOR,
                      E.NUMNOTA,
                      E.CODCONTA,
                      E.CODFORNEC,
                      E.FORNECEDOR,
                      E.ESPECIE,
                      E.CFOP,
                      NVL(E.VALOR, 0) VALOR,
                      NVL(ROUND((E.VALOR * NVL(C.PERCRATEIO, 100) / 100), 2), NVL(E.VALOR, 0)) VLRATEIO,
                      NVL(ROUND((E.VLICMS * NVL(C.PERCRATEIO, 100) / 100), 6), NVL(E.VLICMS, 0)) VLICMS,
                      NVL(ROUND((E.VLPIS * NVL(C.PERCRATEIO, 100) / 100), 6), NVL(E.VLPIS, 0)) VLPIS,
                      NVL(ROUND((E.VLCOFINS * NVL(C.PERCRATEIO, 100) / 100), 6), NVL(E.VLCOFINS, 0)) VLCOFINS,
                      NVL(ROUND((E.VLDIFAL * NVL(C.PERCRATEIO, 100) / 100), 2), NVL(E.VLDIFAL, 0)) VLDIFAL,
                      NVL(L.RECNUM, 0) RECNUM,
                      NVL(C.VLIMPOSTO, 0) VLIMPOSTO,
                      NVL(C.CODCC, '0') CODCC,
                      NVL(ROUND(C.PERCRATEIO, 2), 0) PERCRATEIO
                 FROM BI_SINC_DESPESA_FISCAL E
                 LEFT JOIN FRETES F ON F.NUMTRANSENT = E.NUMTRANSENT
                 LEFT JOIN LANCAMENTO L ON L.NUMNOTA = E.NUMNOTA
                                       AND L.CODFORNEC = E.CODFORNEC
                                       AND L.ANO_COMPETENCIA = E.ANO
                 LEFT JOIN BI_SINC_LANC_PAGAR_BASE C ON C.RECNUM = L.RECNUM
                WHERE E.DATA >= vDATA_MOV_INCREMENTAL)
              
              SELECT E.*
                FROM DESPESA_FISCAL_BASE E
                LEFT JOIN BI_SINC_DESPESA_FISCAL_BASE S ON S.NUMTRANSENT = E.NUMTRANSENT
                                                       AND S.NUMNOTA = E.NUMNOTA
                                                       AND S.CODCC = E.CODCC
               WHERE S.DT_UPDATE IS NULL
                  OR S.CODEMPRESA <> E.CODEMPRESA
                  OR S.CODFILIAL <> E.CODFILIAL
                  OR S.DATA <> E.DATA
                  OR NVL(S.CODSUPERVISOR, 0) <> NVL(E.CODSUPERVISOR, 0)
                  OR S.NUMNOTA <> E.NUMNOTA
                  OR NVL(S.CODCONTA, 0) <> NVL(E.CODCONTA, 0)
                  OR NVL(S.CODFORNEC, 0) <> NVL(E.CODFORNEC, 0)
                  OR NVL(S.FORNECEDOR, '0') <> NVL(E.FORNECEDOR, 0)
                  OR S.ESPECIE <> E.ESPECIE
                  OR S.CFOP <> E.CFOP
                  OR S.VALOR <> E.VALOR
                  OR S.VLRATEIO <> E.VLRATEIO
                  OR S.VLICMS <> E.VLICMS
                  OR S.VLPIS <> E.VLPIS
                  OR S.VLCOFINS <> E.VLCOFINS
                  OR S.VLDIFAL <> E.VLDIFAL
                  OR S.RECNUM <> E.RECNUM
                  OR S.VLIMPOSTO <> E.VLIMPOSTO
                  OR S.CODCC <> E.CODCC
                  OR S.PERCRATEIO <> E.PERCRATEIO)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_DESPESA_FISCAL_BASE
         SET CODEMPRESA    = r.CODEMPRESA,
             CODFILIAL     = r.CODFILIAL,
             DATA          = r.DATA,
             CODSUPERVISOR = r.CODSUPERVISOR,
             NUMNOTA       = r.NUMNOTA,
             CODCONTA      = r.CODCONTA,
             CODFORNEC     = r.CODFORNEC,
             FORNECEDOR    = r.FORNECEDOR,
             ESPECIE       = r.ESPECIE,
             CFOP          = r.CFOP,
             VALOR         = r.VALOR,
             VLRATEIO      = r.VLRATEIO,
             VLICMS        = r.VLICMS,
             VLPIS         = r.VLPIS,
             VLCOFINS      = r.VLCOFINS,
             VLDIFAL       = r.VLDIFAL,
             RECNUM        = r.RECNUM,
             VLIMPOSTO     = r.VLIMPOSTO,
             PERCRATEIO    = r.PERCRATEIO,
             DT_UPDATE     = SYSDATE
       WHERE NUMTRANSENT = r.NUMTRANSENT
         AND NUMNOTA = r.NUMNOTA
         AND CODCC = r.CODCC;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_DESPESA_FISCAL_BASE
          (CODEMPRESA,
           CODFILIAL,
           DATA,
           NUMTRANSENT,
           CODSUPERVISOR,
           NUMNOTA,
           CODCONTA,
           CODFORNEC,
           FORNECEDOR,
           ESPECIE,
           CFOP,
           VALOR,
           VLRATEIO,
           VLICMS,
           VLPIS,
           VLCOFINS,
           VLDIFAL,
           RECNUM,
           VLIMPOSTO,
           CODCC,
           PERCRATEIO,
           DT_UPDATE)
        VALUES
          (r.CODEMPRESA,
           r.CODFILIAL,
           r.DATA,
           r.NUMTRANSENT,
           r.CODSUPERVISOR,
           r.NUMNOTA,
           r.CODCONTA,
           r.CODFORNEC,
           r.FORNECEDOR,
           r.ESPECIE,
           r.CFOP,
           r.VALOR,
           r.VLRATEIO,
           r.VLICMS,
           r.VLPIS,
           r.VLCOFINS,
           r.VLDIFAL,
           r.RECNUM,
           r.VLIMPOSTO,
           r.CODCC,
           r.PERCRATEIO,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

  ---EXCLUIR REGISTROS QUE JÁ NAO EXISTEM MAIS NA BI_SINC_DESPESA_FISCAL
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_DESPESA_FISCAL_BASE
 WHERE (NUMTRANSENT, NUMNOTA, CODCC) IN (SELECT S.NUMTRANSENT,
                                                S.NUMNOTA,
                                                S.CODCC
                                           FROM BI_SINC_DESPESA_FISCAL_BASE S
                                           LEFT JOIN BI_SINC_DESPESA_FISCAL E ON E.NUMTRANSENT = S.NUMTRANSENT
                                          WHERE E.NUMTRANSENT IS NULL)';
  END;

  COMMIT;

  ---EXCLUIR REGISTROS QUE JÁ NAO EXISTEM MAIS NA BI_SINC_LANC_PAGAR_BASE
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_DESPESA_FISCAL_BASE
 WHERE (NUMTRANSENT, NUMNOTA, CODCC) IN (SELECT S.NUMTRANSENT,
          S.NUMNOTA,
          S.CODCC
     FROM BI_SINC_DESPESA_FISCAL_BASE S
     LEFT JOIN BI_SINC_LANC_PAGAR_BASE L ON L.RECNUM = S.RECNUM
                                        AND L.CODCC = S.CODCC
    WHERE S.CODCC <> ''0''
      AND L.RECNUM IS NULL)';
  END;

  COMMIT;
END;
