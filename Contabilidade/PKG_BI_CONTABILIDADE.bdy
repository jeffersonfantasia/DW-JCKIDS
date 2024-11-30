CREATE OR REPLACE PACKAGE BODY PKG_BI_CONTABILIDADE IS

  FUNCTION FN_MOV_PROD_BASE RETURN T_MOV_PROD_BASE_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT L.CODEMPRESA,
                     M.CODFILIAL,
                     M.DATA,
                     M.TIPOMOV,
                     M.NUMTRANSACAO,
                     M.TEMVENDAORIG,
                     M.NUMNOTA,
                     V.CODSUPERVISOR,
                     V.CODGERENTE,
                     M.DTCANCEL,
                     (M.CODFORNEC || ' - ' || F.FORNECEDOR) FORNECEDOR,
                     (M.CODCLI || ' - ' || C.CLIENTE) CLIENTE,
                     ROUND(SUM(M.CUSTOCONTABIL * M.QT), 2) CUSTOCONTABIL,
                     ROUND(SUM(M.VLCONTABIL), 2) VALORCONTABIL,
                     ROUND(SUM(M.VLST), 2) VALORST,
                     ROUND(SUM(M.VLSTGUIA), 2) VALORSTGUIA,
                     ROUND(SUM(M.VLICMS), 2) VALORICMS,
                     ROUND(SUM(M.VLICMSBENEFICIO), 2) VALORICMSBENEFICIO,
                     ROUND(SUM(M.VLICMSDIFAL), 2) VALORICMSDIFAL,
                     ROUND(SUM(M.VLPIS), 2) VALORPIS,
                     ROUND(SUM(M.VLCOFINS), 2) VALORCOFINS
                FROM BI_SINC_MOV_PRODUTO M
                JOIN BI_SINC_FILIAL L ON L.CODFILIAL = M.CODFILIAL
                LEFT JOIN BI_SINC_VENDEDOR V ON V.CODUSUR = M.CODUSUR
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = M.CODFORNEC
                LEFT JOIN BI_SINC_CLIENTE C ON C.CODCLI = M.CODCLI
               GROUP BY L.CODEMPRESA,
                        M.CODFILIAL,
                        M.DATA,
                        M.TIPOMOV,
                        M.NUMTRANSACAO,
                        M.TEMVENDAORIG,
                        M.NUMNOTA,
                        V.CODSUPERVISOR,
                        V.CODGERENTE,
                        M.DTCANCEL,
                        M.CODFORNEC,
                        F.FORNECEDOR,
                        M.CODCLI,
                        C.CLIENTE)
    LOOP
      PIPE ROW(T_MOV_PROD_BASE_RECORD(r.CODEMPRESA,
                                      r.CODFILIAL,
                                      r.DATA,
                                      r.TIPOMOV,
                                      r.NUMTRANSACAO,
                                      r.TEMVENDAORIG,
                                      r.NUMNOTA,
                                      r.CODSUPERVISOR,
                                      r.CODGERENTE,
                                      r.DTCANCEL,
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
                                      r.VALORCOFINS));
    END LOOP;
  
  END FN_MOV_PROD_BASE;

END PKG_BI_CONTABILIDADE;
/
