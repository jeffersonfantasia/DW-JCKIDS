CREATE OR REPLACE PROCEDURE PRC_SINC_MOV_PRODUTO AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_MOV_PRODUTO
    (NUMTRANSITEM,
     MOVIMENTO,
     TIPOMOV,
     CODFILIAL,
     NUMTRANSACAO,
     CODCOB,
     PARCELAS,
     PRAZO,
     CODUSUR,
     CODFORNEC,
     CODCLI,
     DATA,
     CODPROD,
     QT,
     CUSTOFINANCEIRO,
     CUSTOREPOSICAO,
     CUSTOCONTABIL,
     VLCONTABIL,
     PUNIT,
     PTABELA,
     VLPRODUTO,
     VLDESCONTO,
     CST_ICMS,
     CFOP,
     VLBASEICMS,
     PERCICMS,
     VLICMS,
     VLICMSBENEFICIO,
     VLST,
     VLSTGUIA,
     PERCIPI,
     VLIPI,
     CST_PISCOFINS,
     VLBASEPISCOFINS,
     PERCPIS,
     PERCCOFINS,
     VLPIS,
     VLCOFINS,
     VLFRETE,
     VLOUTRASDESP,
     VLICMSDIFAL,
     DTCANCEL)
    WITH ENTRADAS AS
     (SELECT M.NUMTRANSITEM,
             'E' AS MOVIMENTO,
             (CASE
               WHEN M.CODFISCAL IN (1910, 2910, 2911) THEN 'ENTRADA BONIFICADA'
               WHEN M.CODFISCAL IN (1915, 1916, 2915, 2916) THEN 'ENTRADA CONSERTO'
               WHEN M.CODFISCAL IN (1912, 1913, 2912, 2913) THEN 'ENTRADA DEMONSTRACAO'
               WHEN M.CODFISCAL IN (1202, 1411, 2202, 2411) THEN 'ENTRADA DEVOLUCAO'
               WHEN M.CODFISCAL IN (1922, 2922) THEN 'ENTRADA FAT ENTREGA FUTURA'
               WHEN M.CODFISCAL IN (1101, 1102, 1403, 2102, 2401, 2403) THEN 'ENTRADA COMPRA'
               WHEN M.CODFISCAL IN (1917, 1918, 2917, 2918) THEN 'ENTRADA CONSIGNADO'
               WHEN M.CODFISCAL IN (1113, 2113, 1114, 2114) THEN 'ENTRADA COMPRA CONSIGNADO'
               WHEN M.CODFISCAL IN (1923, 2923) THEN 'ENTRADA REM CONTA E ORDEM'
               WHEN M.CODFISCAL IN (1116, 1117, 2116, 2117) THEN 'ENTRADA REM ENTREGA FUTURA'
               WHEN M.CODFISCAL IN (1908, 1949, 2949) THEN 'ENTRADA SIMPLES REMESSA'
               WHEN M.CODFISCAL IN (1152, 1409, 2152, 2409) THEN 'ENTRADA TRANSFERENCIA'
               WHEN M.CODFISCAL IN (1118, 1119, 1121, 2118, 2119, 2121) THEN 'ENTRADA COMPRA TRIANGULAR'
               ELSE 'ENTRADA NAO INFORMADA'
             END) TIPOMOV,
             NVL(E.CODFILIALNF, E.CODFILIAL) CODFILIAL,
             M.NUMTRANSENT NUMTRANSACAO,
             '' CODCOB,
             0 PARCELAS,
             NVL(E.PRAZO,0) PRAZO,
             NVL(M.CODUSUR,0) CODUSUR,
             (CASE WHEN M.CODFISCAL IN (1202, 1411, 2202, 2411) THEN 0 ELSE E.CODFORNEC END) CODFORNEC,
             (CASE WHEN M.CODFISCAL IN (1202, 1411, 2202, 2411) THEN E.CODFORNEC ELSE 0 END) CODCLI,
             M.DTMOV DATA,
             M.CODPROD,
             M.QTCONT QT,
             (CASE
               WHEN E.TIPODESCARGA IN ('6', '8', 'C', 'T') THEN ROUND(NVL(M.CUSTOFINEST, 0),4)
               ELSE ROUND(NVL(M.CUSTOFIN, 0),4)
             END) CUSTOFINANCEIRO,
             ROUND(NVL(M.CUSTOREP, 0),4) CUSTOREPOSICAO,
             ROUND(NVL(M.CUSTOCONT, 0),4) CUSTOCONTABIL,
             (CASE
               WHEN (E.TIPODESCARGA IN ('6', '8', 'C', 'T')) THEN ROUND(M.QTCONT * (M.PUNITCONT + NVL(M.VLFRETE, 0) + NVL(M.VLOUTROS, 0)), 2)
               WHEN E.TIPODESCARGA IN ('N', 'F', 'I') THEN ROUND((M.QTCONT * M.PUNITCONT),2)
               ELSE ROUND(M.QTCONT * (M.PUNITCONT + NVL(M.VLIPI, 0) + NVL(M.ST, 0) + NVL(M.VLFRETE, 0) + NVL(M.VLOUTRASDESP, 0) - NVL(M.VLDESCONTO, 0) - NVL(M.VLSUFRAMA, 0)),2)
             END) VLCONTABIL,
             ROUND(M.PUNITCONT,4) PUNIT,
             ROUND(M.PTABELA,4) PTABELA,
             (CASE
               WHEN (E.TIPODESCARGA IN ('6', '8', 'C', 'T')) THEN ROUND(M.QTCONT * (M.PUNITCONT + NVL(E.VLDESCONTO, 0) - NVL(M.VLFRETE, 0) - NVL(M.VLOUTRASDESP, 0) - NVL(M.VLIPI, 0) - NVL(M.ST, 0)), 2)
               WHEN E.TIPODESCARGA IN ('N', 'F', 'I') THEN ROUND(M.QTCONT * (M.PUNITCONT + NVL(M.VLDESCONTO, 0) - NVL(M.VLFRETE, 0) - NVL(M.VLOUTRASDESP, 0) - NVL(M.ST, 0)),2)
               ELSE M.QTCONT * M.PUNITCONT
             END) VLPRODUTO,
             CASE
               WHEN (E.TIPODESCARGA IN ('6', '8', 'C', 'T')) THEN ROUND(M.QTCONT * NVL(E.VLDESCONTO, 0), 2)
               ELSE (M.QTCONT * NVL(M.VLDESCONTO, 0))
             END VLDESCONTO,
             (CASE
               WHEN LENGTH(M.SITTRIBUT) = 1 THEN LPAD(M.SITTRIBUT, 2, '0')
               ELSE M.SITTRIBUT
             END) CST_ICMS,
             M.CODFISCAL CFOP,
             (CASE
               WHEN E.TIPODESCARGA IN ('6', '8', 'C', 'T') THEN ROUND(M.QTCONT * (NVL(M.BASEICMS, 0) + NVL(MC.VLBASEFRETE, 0) + NVL(MC.VLBASEOUTROS, 0)), 2)
               ELSE ROUND(M.QTCONT * NVL(M.BASEICMS, 0), 2)
             END) VLBASEICMS,
             ROUND((NVL(M.PERCICM, 0) / 100),4) PERCICMS,
             (CASE
               WHEN E.TIPODESCARGA IN ('6', '8', 'C', 'T') THEN ROUND(M.QTCONT * (NVL(M.BASEICMS, 0) + NVL(MC.VLBASEFRETE, 0) + NVL(MC.VLBASEOUTROS, 0)) * NVL(M.PERCICM, 0) / 100, 2)
               ELSE ROUND(M.QTCONT * NVL(MC.VLICMS, (NVL(M.BASEICMS, 0) * NVL(M.PERCICM, 0) / 100)),2)
             END) VLICMS,
             0 VLICMSBENEFICIO,
             ROUND(M.QTCONT * NVL(M.ST, 0),4) VLST,
             ROUND(M.QTCONT * NVL(M.VLDESPADICIONAL, 0),4) VLSTGUIA,
             ROUND((NVL(M.PERCIPI, 0) / 100),4) PERCIPI,
             ROUND(M.QTCONT * NVL(M.VLIPI, 0), 2) VLIPI,
             (CASE
               WHEN LENGTH(M.CODSITTRIBPISCOFINS) = 1 THEN LPAD(TO_CHAR(M.CODSITTRIBPISCOFINS), 2, '0')
               ELSE TO_CHAR(M.CODSITTRIBPISCOFINS)
             END) CST_PISCOFINS,
             ROUND(M.QTCONT * NVL(M.VLBASEPISCOFINS, 0), 2) VLBASEPISCOFINS,
             ROUND((M.PERPIS / 100),4) PERCPIS,
             ROUND((M.PERCOFINS / 100),4) PERCCOFINS,
             ROUND(M.QTCONT * NVL(M.VLCREDPIS, 0), 2) VLPIS,
             ROUND(M.QTCONT * NVL(M.VLCREDCOFINS, 0), 2) VLCOFINS,
             ROUND((M.QTCONT * NVL(M.VLFRETE, 0)),4) VLFRETE,
             ROUND(M.QTCONT * NVL(M.VLOUTRASDESP, 0),4) VLOUTRASDESP,
             (ROUND(NVL(M.QTCONT, 0) * NVL(MC.VLICMSPARTREM, 0), 2) + ROUND(NVL(M.QTCONT, 0) * NVL(MC.VLICMSPARTDEST, 0), 2) + ROUND(NVL(M.QTCONT, 0) * NVL(MC.VLFCPPART, 0), 2)) VLICMSDIFAL,
             M.DTCANCEL
        FROM PCNFENT E
        JOIN PCMOV M ON M.NUMTRANSENT = E.NUMTRANSENT
        JOIN BI_SINC_PRODUTO PR ON PR.CODPROD = M.CODPROD
        LEFT JOIN PCPRODUT P ON P.CODPROD = M.CODPROD
        LEFT JOIN PCMOVCOMPLE MC ON MC.NUMTRANSITEM = M.NUMTRANSITEM
       WHERE E.ESPECIE IN ('NF', 'NC')
         AND M.STATUS IN ('A', 'AB')
         AND M.NUMTRANSITEM IS NOT NULL),
    BASE_VLOUTRASDESP AS
     (SELECT NUMTRANSVENDA,
             (CASE
               WHEN NVL(MAX(VLOUTRASDESP), 0) > 0 THEN
                'S'
               ELSE
                'N'
             END) AS CONSIDERA_OUTRASDESP
        FROM PCNFBASESAID
       WHERE TIPOVENDA = 'DF'
       GROUP BY NUMTRANSVENDA),
    PARCELAMENTO_PLPAG AS
     (SELECT PL.CODPLPAG,
             ROUND((NVL(PL.PRAZO1, 0) + NVL(PL.PRAZO2, 0) + NVL(PL.PRAZO3, 0) +
                   NVL(PL.PRAZO4, 0) + NVL(PL.PRAZO5, 0) + NVL(PL.PRAZO6, 0) +
                   NVL(PL.PRAZO7, 0) + NVL(PL.PRAZO8, 0) + NVL(PL.PRAZO9, 0) +
                   NVL(PL.PRAZO10, 0) + NVL(PL.PRAZO11, 0) + NVL(PL.PRAZO12, 0)) /
                   DECODE(PL.NUMDIAS, 0, 1, PL.NUMDIAS)) QTPARCELA
        FROM PCPLPAG PL),
    PARCELAMENTO_PRESTECF AS
     (SELECT E.NUMPED,
             COUNT(E.PRESTECF) QTPARCELA
        FROM PCPRESTECF E
       WHERE E.CODCOB NOT IN ('CRED')
       GROUP BY E.NUMPED),
    COBRANCA_CARTAO AS
     (SELECT B.CODCOB,
             B.CODCOBCC,
             NVL(MAX(B.PRAZOCC), 0) PRAZOPARCELA
        FROM PCCOB B
       WHERE B.CODCOBCC IS NOT NULL
       GROUP BY B.CODCOB,B.CODCOBCC),
    SAIDAS AS
     (SELECT M.NUMTRANSITEM,
             'S' MOVIMENTO,
             (CASE
               WHEN M.CODFISCAL IN (5910, 6910) THEN 'SAIDA BONIFICADA'
               WHEN M.CODFISCAL IN (5915, 5916, 6915, 6916) THEN 'SAIDA CONSERTO'
               WHEN M.CODFISCAL IN (5912, 6912) THEN 'SAIDA DEMONSTRACAO'
               WHEN M.CODFISCAL IN (5904, 5908, 5929) THEN 'SAIDA DESCONSIDERAR'
               WHEN M.CODFISCAL IN (5202, 5209, 5411, 6202, 6411) THEN 'SAIDA DEVOLUCAO'
               WHEN M.CODFISCAL IN (5918, 5919, 6918, 6919) THEN 'SAIDA DEVOLUCAO CONSIGNADO'
               WHEN M.CODFISCAL IN (5119, 6119) THEN 'SAIDA FAT CONTA E ORDEM'
               WHEN M.CODFISCAL IN (5922, 6922) THEN 'SAIDA FAT ENTREGA FUTURA'
               WHEN M.CODFISCAL IN (5927) THEN 'SAIDA PERDA MERCADORIA'
               WHEN M.CODFISCAL IN (5923, 6923) THEN 'SAIDA REM CONTA E ORDEM'
               WHEN M.CODFISCAL IN (5117, 6117) THEN 'SAIDA REM ENTREGA FUTURA'
               WHEN M.CODFISCAL IN (5909, 6909, 5949, 6949) THEN 'SAIDA SIMPLES REMESSA'
               WHEN M.CODFISCAL IN (5152, 5409, 6152, 6409) THEN 'SAIDA TRANSFERENCIA'
               WHEN M.CODFISCAL IN (5115, 6115, 5102, 5109, 5403, 5405, 6102, 6108, 6403, 5120, 6120) THEN 'SAIDA VENDA'
               ELSE 'SAIDA NAO INFORMADA'
             END) TIPOMOV,
             NVL(M.CODFILIALNF, M.CODFILIAL) CODFILIAL,
             M.NUMTRANSVENDA NUMTRANSACAO,
             S.CODCOB,
             (CASE
               WHEN S.CODCOB = 'CRED' THEN 1
               WHEN S.SERIE = 'SF' AND (S.CODCOB IN ('CONV', 'D', 'PIXL') OR CC.CODCOBCC = 'CADB') THEN 1
               WHEN S.SERIE = 'SF' THEN PE.QTPARCELA
               ELSE PL.QTPARCELA
             END) PARCELAS,
             (CASE
               WHEN S.CODCOB = 'CRED' THEN 0
               WHEN S.SERIE = 'SF' THEN (PE.QTPARCELA * CC.PRAZOPARCELA)
               ELSE S.PRAZOMEDIO
             END) PRAZO,
             NVL(M.CODUSUR,0) CODUSUR,
             (CASE WHEN M.CODFISCAL IN (5202, 5209, 5411, 6202, 6411) THEN S.CODCLI ELSE 0 END) CODFORNEC,
             (CASE WHEN M.CODFISCAL IN (5202, 5209, 5411, 6202, 6411) THEN 0 ELSE S.CODCLI END) CODCLI,
             M.DTMOV DATA,
             M.CODPROD,
             M.QTCONT QT,
             ROUND(NVL(M.CUSTOFINEST, M.CUSTOFIN),4) CUSTOFINANCEIRO,
             ROUND(NVL(M.CUSTOREP, 0),4) CUSTOREPOSICAO,
             ROUND(NVL(M.CUSTOCONT, 0),4) CUSTOCONTABIL,
             (CASE
               WHEN (NVL(M.CODOPER, 'X') <> 'SD') THEN ROUND(M.QTCONT * (M.PUNITCONT + NVL(M.VLFRETE, 0) + NVL(M.VLOUTROS, 0)), 2)
               ELSE
                (CASE WHEN B.CONSIDERA_OUTRASDESP = 'S' THEN  ROUND(M.QTCONT * (ROUND(M.PUNITCONT, 6) + NVL(ROUND(M.VLOUTRASDESP, 2), 0)), 2)
                      ELSE ROUND(M.QTCONT * ROUND(M.PUNITCONT, 6), 2)
                END)
             END) AS VLCONTABIL,
             ROUND(M.PUNITCONT,4) PUNIT,
             ROUND(M.PTABELA,4) PTABELA,
             (CASE
               WHEN (NVL(M.CODOPER, 'X') <> 'SDF') THEN ROUND(M.QTCONT * (M.PUNITCONT - NVL(M.ST, 0) - NVL(M.VLIPI, 0)), 2)
               ELSE ROUND(M.QTCONT * (M.PUNITCONT - NVL(M.ST, 0) - NVL(M.VLIPI, 0) - NVL(M.VLFRETE, 0)), 2)
             END) VLPRODUTO,
             ROUND(M.QTCONT * NVL(M.VLDESCONTO, 0), 2) VLDESCONTO,
             (CASE
               WHEN LENGTH(M.SITTRIBUT) = 1 THEN LPAD(M.SITTRIBUT, 2, '0')
               ELSE M.SITTRIBUT
             END) CST_ICMS,
             M.CODFISCAL CFOP,
             ROUND(M.QTCONT * (NVL(M.BASEICMS, 0) + NVL(MC.VLBASEFRETE, 0) + NVL(MC.VLBASEOUTROS, 0)), 2) VLBASEICMS,
             ROUND((NVL(M.PERCICM, 0) / 100),4) PERCICMS,
             ROUND(M.QTCONT * NVL(MC.VLICMS, (NVL(M.BASEICMS, 0) + NVL(MC.VLBASEFRETE, 0) + NVL(MC.VLBASEOUTROS, 0)) * NVL(M.PERCICM, 0) / 100), 2) VLICMS,
             (CASE
               WHEN (M.DTMOV >= TO_DATE('01/09/2023', 'DD/MM/YYYY') AND M.CODFILIAL = '11' AND M.PERCICM > 0) THEN ROUND(0.011385 * (M.QTCONT * (NVL(M.BASEICMS, 0) + NVL(MC.VLBASEFRETE, 0) + NVL(MC.VLBASEOUTROS, 0))),4)
               ELSE 0
             END) VLICMSBENEFICIO,
             ROUND(M.QTCONT * NVL(M.ST, 0), 2) VLST,
             ROUND(M.QTCONT * NVL(M.VLDESPADICIONAL, 0), 2) VLSTGUIA,
             ROUND((NVL(M.PERCIPI, 0) / 100),4) PERCIPI,
             ROUND(M.QTCONT * NVL(M.VLIPI, 0), 2) VLIPI,
             (CASE
               WHEN LENGTH(M.CODSITTRIBPISCOFINS) = 1 THEN LPAD(TO_CHAR(M.CODSITTRIBPISCOFINS), 2, '0')
               ELSE TO_CHAR(M.CODSITTRIBPISCOFINS)
             END) CST_PISCOFINS,
             ROUND(M.QTCONT * NVL(M.VLBASEPISCOFINS, 0), 2) VLBASEPISCOFINS,
             ROUND((M.PERPIS / 100),4) PERCPIS,
             ROUND((M.PERCOFINS / 100),4) PERCCOFINS,
             ROUND(M.QTCONT * NVL(M.VLPIS, 0), 2) VLPIS,
             ROUND(M.QTCONT * NVL(M.VLCOFINS, 0), 2) VLCOFINS,
             ROUND(M.QTCONT * NVL(M.VLFRETE, 0), 2) VLFRETE,
             ROUND(M.QTCONT * NVL(M.VLOUTRASDESP, 0), 2) VLOUTRASDESP,
             (ROUND(NVL(M.QTCONT, 0) * NVL(MC.VLICMSPARTREM, 0), 2) + ROUND(NVL(M.QTCONT, 0) * NVL(MC.VLICMSPARTDEST, 0), 2) + ROUND(NVL(M.QTCONT, 0) * NVL(MC.VLFCPPART, 0), 2)) VLICMSDIFAL,
             M.DTCANCEL
    FROM PCNFSAID S
    JOIN PCMOV M ON M.NUMTRANSVENDA = S.NUMTRANSVENDA
    JOIN BI_SINC_PRODUTO PR ON PR.CODPROD = M.CODPROD
    LEFT JOIN PCPRODUT P ON P.CODPROD = M.CODPROD
    LEFT JOIN PCMOVCOMPLE MC ON MC.NUMTRANSITEM = M.NUMTRANSITEM
    LEFT JOIN BASE_VLOUTRASDESP B ON B.NUMTRANSVENDA = S.NUMTRANSVENDA
    LEFT JOIN PARCELAMENTO_PLPAG PL ON PL.CODPLPAG = S.CODPLPAG
    LEFT JOIN PARCELAMENTO_PRESTECF PE ON PE.NUMPED = S.NUMPED
    LEFT JOIN COBRANCA_CARTAO CC ON CC.CODCOB = S.CODCOB
   WHERE M.STATUS IN ('A', 'AB')
     AND M.NUMTRANSITEM IS NOT NULL),
    MOVIMENTACAO AS
     (SELECT * FROM ENTRADAS UNION ALL SELECT * FROM SAIDAS)
    SELECT M.*
      FROM MOVIMENTACAO M
      LEFT JOIN BI_SINC_MOV_PRODUTO S ON S.NUMTRANSITEM = M.NUMTRANSITEM
     WHERE 1 = 1 
       AND M.DATA >= TO_DATE('01/02/2024', 'DD/MM/YYYY')
       AND (S.DT_UPDATE IS NULL
        OR S.CFOP <> M.CFOP
        OR NVL(S.CODUSUR,999) <> M.CODUSUR
        OR S.CUSTOFINANCEIRO <> M.CUSTOFINANCEIRO
        OR S.CUSTOREPOSICAO <> M.CUSTOREPOSICAO
        OR S.CUSTOCONTABIL <> M.CUSTOCONTABIL
        OR S.VLPRODUTO <> M.VLPRODUTO
        OR S.CST_ICMS <> M.CST_ICMS
        OR S.VLBASEICMS <> M.VLBASEICMS
        OR S.PERCICMS <> M.PERCICMS
        OR S.VLICMSBENEFICIO <> M.VLICMSBENEFICIO
        OR S.PERCIPI <> M.PERCIPI
        OR S.CST_PISCOFINS <> M.CST_PISCOFINS
        OR S.VLBASEPISCOFINS <> M.VLBASEPISCOFINS
        OR S.PERCPIS <> M.PERCPIS
        OR S.PERCCOFINS <> M.PERCCOFINS
        OR S.VLICMSDIFAL <> M.VLICMSDIFAL);

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_MOV_PRODUTO)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_MOV_PRODUTO
         SET MOVIMENTO       = temp_rec.MOVIMENTO,
             TIPOMOV         = temp_rec.TIPOMOV,
             CODFILIAL       = temp_rec.CODFILIAL,
             NUMTRANSACAO    = temp_rec.NUMTRANSACAO,
             CODCOB          = temp_rec.CODCOB,
             PARCELAS        = temp_rec.PARCELAS,
             PRAZO           = temp_rec.PRAZO,
             CODUSUR         = temp_rec.CODUSUR,
             CODFORNEC       = temp_rec.CODFORNEC,
             CODCLI          = temp_rec.CODCLI,
             DATA            = temp_rec.DATA,
             CODPROD         = temp_rec.CODPROD,
             QT              = temp_rec.QT,
             CUSTOFINANCEIRO = temp_rec.CUSTOFINANCEIRO,
             CUSTOREPOSICAO  = temp_rec.CUSTOREPOSICAO,
             CUSTOCONTABIL   = temp_rec.CUSTOCONTABIL,
             VLCONTABIL      = temp_rec.VLCONTABIL,
             PUNIT           = temp_rec.PUNIT,
             PTABELA         = temp_rec.PTABELA,
             VLPRODUTO       = temp_rec.VLPRODUTO,
             VLDESCONTO      = temp_rec.VLDESCONTO,
             CST_ICMS        = temp_rec.CST_ICMS,
             CFOP            = temp_rec.CFOP,
             VLBASEICMS      = temp_rec.VLBASEICMS,
             PERCICMS        = temp_rec.PERCICMS,
             VLICMS          = temp_rec.VLICMS,
             VLICMSBENEFICIO = temp_rec.VLICMSBENEFICIO,
             VLST            = temp_rec.VLST,
             VLSTGUIA        = temp_rec.VLSTGUIA,
             PERCIPI         = temp_rec.PERCIPI,
             VLIPI           = temp_rec.VLIPI,
             CST_PISCOFINS   = temp_rec.CST_PISCOFINS,
             VLBASEPISCOFINS = temp_rec.VLBASEPISCOFINS,
             PERCPIS         = temp_rec.PERCPIS,
             PERCCOFINS      = temp_rec.PERCCOFINS,
             VLPIS           = temp_rec.VLPIS,
             VLCOFINS        = temp_rec.VLCOFINS,
             VLFRETE         = temp_rec.VLFRETE,
             VLOUTRASDESP    = temp_rec.VLOUTRASDESP,
             VLICMSDIFAL     = temp_rec.VLICMSDIFAL,
             DT_UPDATE       = SYSDATE
       WHERE NUMTRANSITEM = temp_rec.NUMTRANSITEM;
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_MOV_PRODUTO
          (NUMTRANSITEM,
           MOVIMENTO,
           TIPOMOV,
           CODFILIAL,
           NUMTRANSACAO,
           CODCOB,
           PARCELAS,
           PRAZO,
           CODUSUR,
           CODFORNEC,
           CODCLI,
           DATA,
           CODPROD,
           QT,
           CUSTOFINANCEIRO,
           CUSTOREPOSICAO,
           CUSTOCONTABIL,
           VLCONTABIL,
           PUNIT,
           PTABELA,
           VLPRODUTO,
           VLDESCONTO,
           CST_ICMS,
           CFOP,
           VLBASEICMS,
           PERCICMS,
           VLICMS,
           VLICMSBENEFICIO,
           VLST,
           VLSTGUIA,
           PERCIPI,
           VLIPI,
           CST_PISCOFINS,
           VLBASEPISCOFINS,
           PERCPIS,
           PERCCOFINS,
           VLPIS,
           VLCOFINS,
           VLFRETE,
           VLOUTRASDESP,
           VLICMSDIFAL,
           DTCANCEL,
           DT_UPDATE)
        VALUES
          (temp_rec.NUMTRANSITEM,
           temp_rec.MOVIMENTO,
           temp_rec.TIPOMOV,
           temp_rec.CODFILIAL,
           temp_rec.NUMTRANSACAO,
           temp_rec.CODCOB,
           temp_rec.PARCELAS,
           temp_rec.PRAZO,
           temp_rec.CODUSUR,
           temp_rec.CODFORNEC,
           temp_rec.CODCLI,
           temp_rec.DATA,
           temp_rec.CODPROD,
           temp_rec.QT,
           temp_rec.CUSTOFINANCEIRO,
           temp_rec.CUSTOREPOSICAO,
           temp_rec.CUSTOCONTABIL,
           temp_rec.VLCONTABIL,
           temp_rec.PUNIT,
           temp_rec.PTABELA,
           temp_rec.VLPRODUTO,
           temp_rec.VLDESCONTO,
           temp_rec.CST_ICMS,
           temp_rec.CFOP,
           temp_rec.VLBASEICMS,
           temp_rec.PERCICMS,
           temp_rec.VLICMS,
           temp_rec.VLICMSBENEFICIO,
           temp_rec.VLST,
           temp_rec.VLSTGUIA,
           temp_rec.PERCIPI,
           temp_rec.VLIPI,
           temp_rec.CST_PISCOFINS,
           temp_rec.VLBASEPISCOFINS,
           temp_rec.PERCPIS,
           temp_rec.PERCCOFINS,
           temp_rec.VLPIS,
           temp_rec.VLCOFINS,
           temp_rec.VLFRETE,
           temp_rec.VLOUTRASDESP,
           temp_rec.VLICMSDIFAL,
           temp_rec.DTCANCEL,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000,
                                'Erro durante a insercao na tabela: ' ||
                                SQLERRM);
    END;
  END LOOP;

  COMMIT;
    -- Exclui os registros da tabela BI_SINC_MOV_PRODUTO que possuem DTCANCEL NOT NULL
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_MOV_PRODUTO WHERE DTCANCEL IS NOT NULL';

  -- Exclui os registros da tabela temporária TEMP criada;
    EXECUTE IMMEDIATE 'DELETE TEMP_MOV_PRODUTO';
END;
