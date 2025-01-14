CREATE OR REPLACE VIEW VIEW_BI_SINC_MOV_PROD_AGG AS
    
    WITH MOV_AGG AS
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
                M.CODUSUR)
    
    SELECT L.CODEMPRESA,
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
