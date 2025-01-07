CREATE OR REPLACE PROCEDURE PRC_SINC_MOV_PROD_BASE_AGG AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 75;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

BEGIN
  FOR r IN (WITH MOV_AGG AS
               (SELECT M.CODFILIAL,
                      M.DATA,
                      M.MOVIMENTO,
                      M.TIPOMOV,
                      M.NUMTRANSACAO,
                      M.TEMVENDAORIG,
                      M.NUMNOTA,
                      M.DTCANCEL,
                      M.CODFORNEC,
                      M.CODCLI,
                      M.CODUSUR,
                      ROUND(SUM(M.CUSTOCONTABIL), 2) CUSTOCONTABIL,
                      ROUND(SUM(M.VLCONTABIL), 2) VALORCONTABIL,
                      ROUND(SUM(M.VLST), 2) VALORST,
                      ROUND(SUM(M.VLSTGUIA), 2) VALORSTGUIA,
                      ROUND(SUM(M.VLICMS), 2) VALORICMS,
                      ROUND(SUM(M.VLICMSBENEFICIO), 2) VALORICMSBENEFICIO,
                      ROUND(SUM(M.VLICMSDIFAL), 2) VALORICMSDIFAL,
                      ROUND(SUM(M.VLPIS), 2) VALORPIS,
                      ROUND(SUM(M.VLCOFINS), 2) VALORCOFINS
                 FROM BI_SINC_MOV_PRODUTO M
                GROUP BY M.CODFILIAL,
                         M.DATA,
                         M.MOVIMENTO,
                         M.TIPOMOV,
                         M.NUMTRANSACAO,
                         M.TEMVENDAORIG,
                         M.NUMNOTA,
                         M.DTCANCEL,
                         M.CODFORNEC,
                         M.CODCLI,
                         M.CODUSUR),
              
              MOVIMENTACAO AS
               (SELECT L.CODEMPRESA,
                      M.CODFILIAL,
                      M.DATA,
                      M.MOVIMENTO,
                      M.TIPOMOV,
                      M.NUMTRANSACAO,
                      M.TEMVENDAORIG,
                      M.NUMNOTA,
                      V.CODSUPERVISOR,
                      V.CODGERENTE,
                      M.DTCANCEL,
                      M.CODFORNEC,
                      (F.FORNECEDOR || ' - Cód. ' || M.CODFORNEC) FORNECEDOR,
                      (C.CLIENTE || ' - Cód. ' || M.CODCLI) CLIENTE,
                      M.CUSTOCONTABIL,
                      M.VALORCONTABIL,
                      M.VALORST,
                      M.VALORSTGUIA,
                      M.VALORICMS,
                      M.VALORICMSBENEFICIO,
                      M.VALORICMSDIFAL,
                      M.VALORPIS,
                      M.VALORCOFINS
                 FROM MOV_AGG M
                 LEFT JOIN BI_SINC_FILIAL L ON L.CODFILIAL = M.CODFILIAL
                 LEFT JOIN BI_SINC_VENDEDOR V ON V.CODUSUR = M.CODUSUR
                 LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = M.CODFORNEC
                 LEFT JOIN BI_SINC_CLIENTE C ON C.CODCLI = M.CODCLI
                WHERE 1 = 1
                  AND M.DATA >= vDATA_MOV_INCREMENTAL
                  AND M.DTCANCEL IS NULL)
              
              SELECT M.*
                FROM MOVIMENTACAO M
                LEFT JOIN BI_SINC_MOV_PROD_BASE_AGG S ON S.NUMTRANSACAO = M.NUMTRANSACAO
                                                     AND S.NUMNOTA = M.NUMNOTA
               WHERE S.DT_UPDATE IS NULL
                  OR S.CODEMPRESA <> M.CODEMPRESA
                  OR S.CODFILIAL <> M.CODFILIAL
                  OR S.DATA <> M.DATA
                  OR S.MOVIMENTO <> M.MOVIMENTO
                  OR S.TIPOMOV <> M.TIPOMOV
                  OR NVL(S.TEMVENDAORIG, '0') <> M.TEMVENDAORIG
                  OR S.CODSUPERVISOR <> M.CODSUPERVISOR
                  OR S.CODGERENTE <> M.CODGERENTE
                  OR NVL(S.DTCANCEL, TO_DATE('01/01/1889', 'DD/MM/YYYY')) <> M.DTCANCEL
                  OR S.CODFORNEC <> M.CODFORNEC
                  OR S.FORNECEDOR <> M.FORNECEDOR
                  OR S.CLIENTE <> M.CLIENTE
                  OR S.CUSTOCONTABIL <> M.CUSTOCONTABIL
                  OR S.VALORCONTABIL <> M.VALORCONTABIL
                  OR S.VALORST <> M.VALORST
                  OR S.VALORSTGUIA <> M.VALORSTGUIA
                  OR S.VALORICMS <> M.VALORICMS
                  OR S.VALORICMSBENEFICIO <> M.VALORICMSBENEFICIO
                  OR S.VALORICMSDIFAL <> M.VALORICMSDIFAL
                  OR S.VALORPIS <> M.VALORPIS
                  OR S.VALORCOFINS <> M.VALORCOFINS)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_MOV_PROD_BASE_AGG
         SET CODEMPRESA         = r.CODEMPRESA,
             CODFILIAL          = r.CODFILIAL,
             DATA               = r.DATA,
             MOVIMENTO          = r.MOVIMENTO,
             TIPOMOV            = r.TIPOMOV,
             TEMVENDAORIG       = r.TEMVENDAORIG,
             CODSUPERVISOR      = r.CODSUPERVISOR,
             CODGERENTE         = r.CODGERENTE,
             DTCANCEL           = r.DTCANCEL,
             CODFORNEC          = r.CODFORNEC,
             FORNECEDOR         = r.FORNECEDOR,
             CLIENTE            = r.CLIENTE,
             CUSTOCONTABIL      = r.CUSTOCONTABIL,
             VALORCONTABIL      = r.VALORCONTABIL,
             VALORST            = r.VALORST,
             VALORSTGUIA        = r.VALORSTGUIA,
             VALORICMS          = r.VALORICMS,
             VALORICMSBENEFICIO = r.VALORICMSBENEFICIO,
             VALORICMSDIFAL     = r.VALORICMSDIFAL,
             VALORPIS           = r.VALORPIS,
             VALORCOFINS        = r.VALORCOFINS,
             DT_UPDATE          = SYSDATE
       WHERE NUMTRANSACAO = r.NUMTRANSACAO
         AND NUMNOTA = r.NUMNOTA;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_MOV_PROD_BASE_AGG
          (CODEMPRESA,
           CODFILIAL,
           DATA,
           MOVIMENTO,
           TIPOMOV,
           NUMTRANSACAO,
           TEMVENDAORIG,
           NUMNOTA,
           CODSUPERVISOR,
           CODGERENTE,
           DTCANCEL,
           CODFORNEC,
           FORNECEDOR,
           CLIENTE,
           CUSTOCONTABIL,
           VALORCONTABIL,
           VALORST,
           VALORSTGUIA,
           VALORICMS,
           VALORICMSBENEFICIO,
           VALORICMSDIFAL,
           VALORPIS,
           VALORCOFINS,
           DT_UPDATE)
        VALUES
          (r.CODEMPRESA,
           r.CODFILIAL,
           r.DATA,
           r.MOVIMENTO,
           r.TIPOMOV,
           r.NUMTRANSACAO,
           r.TEMVENDAORIG,
           r.NUMNOTA,
           r.CODSUPERVISOR,
           r.CODGERENTE,
           r.DTCANCEL,
           r.CODFORNEC,
           r.FORNECEDOR,
           r.CLIENTE,
           r.CUSTOCONTABIL,
           r.VALORCONTABIL,
           r.VALORST,
           r.VALORSTGUIA,
           r.VALORICMS,
           r.VALORICMSBENEFICIO,
           r.VALORICMSDIFAL,
           r.VALORPIS,
           r.VALORCOFINS,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;
	
	---DELETAR OS REGISTROS QUE NÃO EXISTEM 
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_MOV_PROD_BASE_AGG
 WHERE NUMTRANSACAO IN
       (SELECT S.NUMTRANSACAO
          FROM BI_SINC_MOV_PROD_BASE_AGG S
          JOIN BI_SINC_MOV_PRODUTO M ON M.NUMTRANSACAO = S.NUMTRANSACAO
         WHERE M.DTCANCEL IS NOT NULL
         GROUP BY S.NUMTRANSACAO)';
  END;

  COMMIT;
END;
