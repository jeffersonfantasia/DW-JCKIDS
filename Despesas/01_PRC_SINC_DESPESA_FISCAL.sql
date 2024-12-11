CREATE OR REPLACE PROCEDURE PRC_SINC_DESPESA_FISCAL AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 75;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2014', 'DD/MM/YYYY');

  -----------------------CONTAS
  vCONTA_FRETE_SISTEMA NUMBER := 100002;
  vCONTA_FRETE         NUMBER := 3202;

BEGIN
  FOR r IN (WITH MOV_ENT AS
               (SELECT NUMTRANSENT FROM PCMOV GROUP BY NUMTRANSENT),
              
              NFBASE AS
               (SELECT B.NUMTRANSENT,
                      B.CODFISCAL,
                      LPAD(B.SITTRIBUT, 2, 0) CST_ICMS,
                      B.VLBASE VLBASEICMS,
                      B.ALIQUOTA,
                      B.VLICMS
                 FROM PCNFBASE B
                 LEFT JOIN MOV_ENT M ON M.NUMTRANSENT = B.NUMTRANSENT
                WHERE M.NUMTRANSENT IS NULL
                  AND B.NUMTRANSENT IS NOT NULL),
              
              NFENTPISCOFINS AS
               (SELECT P.NUMTRANSENT,
                      P.VLBASEPIS VLBASEPISCOFINS,
                      LPAD(P.CODTRIBPISCOFINS, 2, 0) CST_PISCOFINS,
                      P.PERPIS,
                      P.PERCOFINS,
                      P.VLPIS,
                      P.VLCOFINS
                 FROM PCNFENTPISCOFINS P
                 LEFT JOIN MOV_ENT M ON M.NUMTRANSENT = P.NUMTRANSENT
                WHERE M.NUMTRANSENT IS NULL),
              
              DESPESA_FISCAL AS
               (SELECT L.CODEMPRESA,
                      E.CODFILIAL,
                      E.DTENT DATA,
                      EXTRACT(YEAR FROM E.DTENT) ANO,
                      E.NUMTRANSENT,
                      E.NUMNOTA,
                      (CASE
                        WHEN E.CODCONT = vCONTA_FRETE_SISTEMA THEN
                         vCONTA_FRETE
                        ELSE
                         E.CODCONT
                      END) CODCONTA,
                      E.CODFORNEC,
                      E.FORNECEDOR,
                      E.ESPECIE,
                      B.CODFISCAL CFOP,
                      E.VLTOTAL VALOR,
                      B.CST_ICMS,
                      B.VLBASEICMS,
                      B.ALIQUOTA PERCICMS,
                      B.VLICMS,
                      NVL(P.CST_PISCOFINS, '98') CST_PISCOFINS,
                      NVL(P.VLBASEPISCOFINS, 0) VLBASEPISCOFINS,
                      NVL(P.PERPIS, 0) PERCPIS,
                      NVL(P.PERCOFINS, 0) PERCCOFINS,
                      NVL(P.VLPIS, 0) VLPIS,
                      NVL(P.VLCOFINS, 0) VLCOFINS,
                      E.VLOUTRAS VLDIFAL
                 FROM PCNFENT E
                 LEFT JOIN MOV_ENT M ON M.NUMTRANSENT = E.NUMTRANSENT
                 LEFT JOIN NFBASE B ON B.NUMTRANSENT = E.NUMTRANSENT
                 LEFT JOIN NFENTPISCOFINS P ON P.NUMTRANSENT = E.NUMTRANSENT
                 LEFT JOIN BI_SINC_FILIAL L ON L.CODFILIAL = E.CODFILIAL
                WHERE M.NUMTRANSENT IS NULL
                  AND E.DTCANCEL IS NULL
                  AND E.ESPECIE <> 'OE'
                  AND E.DTENT >= vDATA_MOV_INCREMENTAL)
              
              SELECT E.*
                FROM DESPESA_FISCAL E
                LEFT JOIN BI_SINC_DESPESA_FISCAL S ON S.NUMTRANSENT = E.NUMTRANSENT
               WHERE S.DT_UPDATE IS NULL
                  OR S.CODEMPRESA <> E.CODEMPRESA
                  OR S.CODFILIAL <> E.CODFILIAL
                  OR S.DATA <> E.DATA
                  OR S.ANO <> E.ANO
                  OR S.NUMNOTA <> E.NUMNOTA
                  OR NVL(S.CODCONTA, 0) <> E.CODCONTA
                  OR NVL(S.CODFORNEC, 0) <> E.CODFORNEC
                  OR NVL(S.FORNECEDOR, '0') <> E.FORNECEDOR
                  OR S.ESPECIE <> E.ESPECIE
                  OR S.CFOP <> E.CFOP
                  OR S.VALOR <> E.VALOR
                  OR S.CST_ICMS <> E.CST_ICMS
                  OR S.VLBASEICMS <> E.VLBASEICMS
                  OR S.PERCICMS <> E.PERCICMS
                  OR S.VLICMS <> E.VLICMS
                  OR S.CST_PISCOFINS <> E.CST_PISCOFINS
                  OR S.VLBASEPISCOFINS <> E.VLBASEPISCOFINS
                  OR S.PERCPIS <> E.PERCPIS
                  OR S.PERCCOFINS <> E.PERCCOFINS
                  OR S.VLPIS <> E.VLPIS
                  OR S.VLCOFINS <> E.VLCOFINS
                  OR S.VLDIFAL <> E.VLDIFAL)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_DESPESA_FISCAL
         SET CODEMPRESA      = r.CODEMPRESA,
             CODFILIAL       = r.CODFILIAL,
             DATA            = r.DATA,
             ANO             = r.ANO,
             NUMNOTA         = r.NUMNOTA,
             CODCONTA        = r.CODCONTA,
             CODFORNEC       = r.CODFORNEC,
             FORNECEDOR      = r.FORNECEDOR,
             ESPECIE         = r.ESPECIE,
             CFOP            = r.CFOP,
             VALOR           = r.VALOR,
             CST_ICMS        = r.CST_ICMS,
             VLBASEICMS      = r.VLBASEICMS,
             PERCICMS        = r.PERCICMS,
             VLICMS          = r.VLICMS,
             CST_PISCOFINS   = r.CST_PISCOFINS,
             VLBASEPISCOFINS = r.VLBASEPISCOFINS,
             PERCPIS         = r.PERCPIS,
             PERCCOFINS      = r.PERCCOFINS,
             VLPIS           = r.VLPIS,
             VLCOFINS        = r.VLCOFINS,
             VLDIFAL         = r.VLDIFAL,
             DT_UPDATE       = SYSDATE
       WHERE NUMTRANSENT = r.NUMTRANSENT;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_DESPESA_FISCAL
          (CODEMPRESA,
           CODFILIAL,
           DATA,
           ANO,
           NUMTRANSENT,
           NUMNOTA,
           CODCONTA,
           CODFORNEC,
           FORNECEDOR,
           ESPECIE,
           CFOP,
           VALOR,
           CST_ICMS,
           VLBASEICMS,
           PERCICMS,
           VLICMS,
           CST_PISCOFINS,
           VLBASEPISCOFINS,
           PERCPIS,
           PERCCOFINS,
           VLPIS,
           VLCOFINS,
           VLDIFAL,
           DT_UPDATE)
        VALUES
          (r.CODEMPRESA,
           r.CODFILIAL,
           r.DATA,
           r.ANO,
           r.NUMTRANSENT,
           r.NUMNOTA,
           r.CODCONTA,
           r.CODFORNEC,
           r.FORNECEDOR,
           r.ESPECIE,
           r.CFOP,
           r.VALOR,
           r.CST_ICMS,
           r.VLBASEICMS,
           r.PERCICMS,
           r.VLICMS,
           r.CST_PISCOFINS,
           r.VLBASEPISCOFINS,
           r.PERCPIS,
           r.PERCCOFINS,
           r.VLPIS,
           r.VLCOFINS,
           r.VLDIFAL,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_DESPESA_FISCAL
 WHERE NUMTRANSENT IN
       (SELECT S.NUMTRANSENT
          FROM BI_SINC_DESPESA_FISCAL S
          JOIN PCNFENT E ON E.NUMTRANSENT = S.NUMTRANSENT
         WHERE  NOT(E.DTCANCEL IS NULL AND E.ESPECIE <> ''OE''))';
  END;

  COMMIT;
END;
