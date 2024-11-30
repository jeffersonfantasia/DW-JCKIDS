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
                     (F.FORNECEDOR || ' - Cód. ' || M.CODFORNEC) FORNECEDOR,
                     (C.CLIENTE || ' - Cód. ' || M.CODCLI) CLIENTE,
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
	
	/*  -----------------------CENTROS DE CUSTO
   vCC_SPMARKET         VARCHAR(3) := '1.1';
   vCC_PARQUE           VARCHAR(3) := '1.2';
   vCC_JUNDIAI          VARCHAR(3) := '1.3';
   vCC_TRIMAIS          VARCHAR(3) := '1.4';
   vCC_CAMPINAS         VARCHAR(3) := '1.5';
   vCC_DISTRIBUICAO_SP  VARCHAR(3) := '2.1';
	 vCC_DISTRIBUICAO_ES  VARCHAR(3) := '2.2';
	 vCC_ECOMMERCE_SP     VARCHAR(3) := '3.1';
	 vCC_CORPORATIVO_SP   VARCHAR(3) := '4.1';*/

END PKG_BI_CONTABILIDADE;
/
