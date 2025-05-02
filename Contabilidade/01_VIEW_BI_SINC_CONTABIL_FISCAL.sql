CREATE OR REPLACE VIEW VIEW_BI_SINC_CONTABIL_FISCAL AS
    
    WITH PRODUTOS AS
     (SELECT M.CODFILIAL,
             M.DATA,
             M.MOVIMENTO,
             'PROD' TIPO,
             M.NUMTRANSITEM,
             M.NUMTRANSACAO,
             M.NUMNOTA,
             'NF' ESPECIE,
             V.CODGERENTE,
             M.CODPROD,
             ('Cod.: ' || M.CODPROD || ' - ' || P.PRODUTO) PRODUTO,
             (CASE
               WHEN M.CODFORNEC = 0 THEN
                ('CNPJ: ' || C.CNPJ || ' - ' || C.CLIENTE)
               WHEN (M.CODFORNEC <> 0 AND F.FORNECEDOR IS NULL) THEN
                ('CNPJ: ' || C2.CNPJ || ' - ' || C2.CLIENTE)
               ELSE
                ('CNPJ: ' || F.CNPJ || ' - ' || F.FORNECEDOR)
             END) RAZAOSOCIAL,
             M.CFOP,
             (M.PERCICMS * 100) PERCICMS,
             M.CST_ICMS,
             (M.PERCPIS * 100) PERCPIS,
             (M.PERCCOFINS * 100) PERCCOFINS,
             M.CST_PISCOFINS,
             M.VLCONTABIL VALORCONTABIL,
             --M.VALORST,
             --M.VALORSTGUIA,
             0 VLDIFALCONSUM,
             M.VLBASEICMS,
             M.VLICMS VALORICMS,
             M.VLBASEPISCOFINS,
             M.VLPIS VALORPIS,
             M.VLCOFINS VALORCOFINS
        FROM BI_SINC_MOV_PRODUTO M
        LEFT JOIN BI_SINC_VENDEDOR V ON V.CODUSUR = M.CODUSUR
        LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = M.CODFORNEC
        LEFT JOIN BI_SINC_CLIENTE C ON C.CODCLI = M.CODCLI
        LEFT JOIN BI_SINC_CLIENTE C2 ON C2.CODCLI = M.CODFORNEC
        LEFT JOIN BI_SINC_FILIAL L ON L.CODFILIAL = M.CODFILIAL
        LEFT JOIN BI_SINC_PRODUTO P ON P.CODPROD = M.CODPROD
       WHERE M.DTCANCEL IS NULL
         AND L.CODEMPRESA = 1),
    
    DESP_FISCAL AS
     (SELECT M.CODFILIAL,
             M.DATA,
             'E' MOVIMENTO,
             'DESP' TIPO,
             M.NUMTRANSENT NUMTRANSITEM,
             M.NUMTRANSENT NUMTRANSACAO,
             M.NUMNOTA,
             M.ESPECIE,
             0 CODGERENTE,
             0 CODPROD,
             ('DESP: ' || C.CONTA) PRODUTO,
             ('CNPJ: ' || F.CNPJ || ' - ' || M.FORNECEDOR) RAZAOSOCIAL,
             M.CFOP,
             M.PERCICMS,
             (CASE
               WHEN M.VLICMS = 0
                    AND M.CST_ICMS IS NULL THEN
                '99'
               ELSE
                M.CST_ICMS
             END) CST_ICMS,
             M.PERCPIS,
             M.PERCCOFINS,
             M.CST_PISCOFINS,
             M.VALOR VALORCONTABIL,
             --0 VALORST,
             --0 VALORSTGUIA,
             M.VLDIFAL VLDIFALCONSUM,
             M.VLBASEICMS,
             M.VLICMS VALORICMS,
             M.VLBASEPISCOFINS,
             M.VLPIS VALORPIS,
             M.VLCOFINS VALORCOFINS
        FROM BI_SINC_DESPESA_FISCAL M
        LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = M.CODFORNEC
        LEFT JOIN BI_SINC_FILIAL L ON L.CODFILIAL = M.CODFILIAL
        LEFT JOIN PCCONTA C ON C.CODCONTA = M.CODCONTA
       WHERE L.CODEMPRESA = 1)
    
    SELECT *
      FROM PRODUTOS
    UNION ALL
    SELECT * FROM DESP_FISCAL
