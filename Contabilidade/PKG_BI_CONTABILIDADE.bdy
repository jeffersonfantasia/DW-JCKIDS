CREATE OR REPLACE PACKAGE BODY PKG_BI_CONTABILIDADE IS

  FUNCTION FN_MOV_PROD_BASE RETURN T_MOV_PROD_BASE_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT L.CODEMPRESA,
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
                        M.MOVIMENTO,
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
                                      r.VALORCOFINS));
    END LOOP;
  
  END FN_MOV_PROD_BASE;

  FUNCTION FN_MOV_PROD_VLCONTABIL_INTEIRO RETURN T_MOV_PROD_CONTABIL_TABLE
    PIPELINED IS

   -----------------------CENTROS DE CUSTO
   vCC_SPMARKET         VARCHAR(3) := '1.1';
   vCC_PARQUE           VARCHAR(3) := '1.2';
   vCC_JUNDIAI          VARCHAR(3) := '1.3';
   vCC_TRIMAIS          VARCHAR(3) := '1.4';
   vCC_CAMPINAS         VARCHAR(3) := '1.5';
   vCC_DISTRIBUICAO_SP  VARCHAR(3) := '2.1';
   vCC_DISTRIBUICAO_ES  VARCHAR(3) := '2.2';
   vCC_ECOMMERCE_SP     VARCHAR(3) := '3.1';
   vCC_CORPORATIVO_SP   VARCHAR(3) := '4.1' ;

    -----------------------CODIGO CONTAS
    vCLIENTES_NACIONAIS              NUMBER := 1152;
    vFATURAMENTO_BRUTO               NUMBER := 3101;
    vFATURAMENTO_ANTECIPADO          NUMBER := 1182;
    vCMV                             NUMBER := 3110;
    vESTOQUE                         NUMBER := 1174;
    vICMS_VENDA                      NUMBER := 3104;
    vPIS_VENDA                       NUMBER := 3105;
    vCOFINS_VENDA                    NUMBER := 3106;
    vST_VENDA                        NUMBER := 3107;
    vICMS_RECOLHER                   NUMBER := 2251;
    vICMS_RECOLHER_ES                NUMBER := 2261;
    vPIS_RECOLHER                    NUMBER := 2254;
    vCOFINS_RECOLHER                 NUMBER := 2255;
    vST_RECOLHER                     NUMBER := 2260;
    vDIFAL_RECOLHER                  NUMBER := 2252;
    vESTOQUE_CONSIGANDO              NUMBER := 1176;
    vMERCADORIA_RECEBIDA_CONSIGADO   NUMBER := 2200;
    vDEVOLUCAO_RECEBER               NUMBER := 1178;
    vICMS_RECUPERAR                  NUMBER := 1201;
    vICMS_RECUPERAR_ES               NUMBER := 1204;
    vPIS_RECUPERAR                   NUMBER := 1202;
    vCOFINS_RECUPERAR                NUMBER := 1203;
    vVENDA_BONIFICADA                NUMBER := 3113;
    vTRANSFERENCIA_MERCADORIA        NUMBER := 1183;
    vREMESSA_CONSERTO                NUMBER := 3117;
    vRETORNO_CONSERTO                NUMBER := 3120;
    vESTOQUE_TRANSITO                NUMBER := 1175;
    vREMESSA_DEMOSTRACAO             NUMBER := 3119;
    vRETORNO_DEMOSTRACAO             NUMBER := 3122;
    vENVIO_SIMPLES_REMESSA           NUMBER := 3118;
    vRECEBIMENTO_SIMPLES_REMESSA     NUMBER := 3121;
    vPERDA_MERCADORIA                NUMBER := 3114;
    vENTRADA_BONIFICADA              NUMBER := 3115;
    vDEVOLUCAO_PRODUTO               NUMBER := 3102;
    vDEVOLUCAO_CLIENTE               NUMBER := 2201;
    vENTRADA_INVENTARIO              NUMBER := 3116;

    -----------------------FORNECEDOR SEM CONTA CONTABIL
    vOUTRO_FORNECEDOR  NUMBER := 99999;
	
	BEGIN
    FOR r IN (
        WITH 
		
		vFORNECEDORES_JCBROTHERS AS (SELECT DISTINCT CODFORNEC FROM BI_SINC_FILIAL WHERE INSTR(EMPRESA, 'JC BROTHERS') > 0),

				        FILIAL_CC AS
                 (SELECT CODFILIAL,
                        (CASE CODFILIAL
                          WHEN '1' THEN vCC_SPMARKET
                          WHEN '2' THEN vCC_DISTRIBUICAO_SP
                          WHEN '5' THEN vCC_CORPORATIVO_SP
                          WHEN '6' THEN vCC_CORPORATIVO_SP
                          WHEN '7' THEN vCC_ECOMMERCE_SP
                          WHEN '8' THEN vCC_PARQUE
                          WHEN '9' THEN vCC_ECOMMERCE_SP
                          WHEN '10' THEN vCC_ECOMMERCE_SP
                          WHEN '11' THEN vCC_DISTRIBUICAO_ES
                          WHEN '12' THEN vCC_JUNDIAI
                          WHEN '13' THEN vCC_TRIMAIS
                          WHEN '14' THEN vCC_CAMPINAS
                          ELSE
                           NULL
                        END) CODCC
                   FROM BI_SINC_FILIAL F),
                
                VENDEDOR_CC AS
                 (SELECT DISTINCT CODSUPERVISOR,
                                 (CASE
                                   WHEN CODSUPERVISOR IN (1, 2, 4, 11, 12, 6) THEN vCC_DISTRIBUICAO_SP
                                   WHEN CODSUPERVISOR IN (3, 9) THEN vCC_CORPORATIVO_SP
                                   WHEN CODSUPERVISOR IN (7, 8, 10) THEN vCC_ECOMMERCE_SP
                                   WHEN CODSUPERVISOR IN (5) THEN vCC_SPMARKET
                                   WHEN CODSUPERVISOR IN (13) THEN vCC_PARQUE
                                   WHEN CODSUPERVISOR IN (14) THEN vCC_JUNDIAI
                                   WHEN CODSUPERVISOR IN (15) THEN vCC_TRIMAIS
                                   WHEN CODSUPERVISOR IN (16) THEN vCC_CAMPINAS
                                   ELSE
                                    NULL
                                 END) CODCC
                   FROM BI_SINC_VENDEDOR V)
                
                SELECT M.CODEMPRESA,
                       M.CODFILIAL,
                       M.CODSUPERVISOR,
                       M.CODGERENTE,
                       M.DATA,
                       ---------TIPOLANCAMENTO
                       (CASE
                         WHEN M.TIPOMOV IN ('ENTRADA SIMPLES REMESSA') THEN
                         (CASE  
                           WHEN M.TEMVENDAORIG = 'S' THEN 2 
                           WHEN M.CODFORNEC IN (SELECT CODFORNEC FROM vFORNECEDORES_JCBROTHERS) THEN 2 
                           ELSE 3
                          END)
                        WHEN M.TIPOMOV IN
                              ('SAIDA DEVOLUCAO', 'SAIDA TRANSFERENCIA') THEN
                          1
                         WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                            'ENTRADA COMPRA CONSIGNADO',
                                            'ENTRADA COMPRA TRIANGULAR',
                                            'ENTRADA BONIFICADA',
                                            'ENTRADA TRANSFERENCIA',
                                            'ENTRADA REM ENTREGA FUTURA') THEN
                          2
                       
                         ELSE
                          3
                       END) TIPOLANCAMENTO,
                       
                       M.NUMTRANSACAO IDENTIFICADOR,
                       M.NUMNOTA DOCUMENTO,
                       
                       ----------CONTADEBITO
                       (CASE
                         WHEN M.TIPOMOV IN
                              ('SAIDA VENDA',
                               'SAIDA FAT CONTA E ORDEM',
                               'SAIDA FAT ENTREGA FUTURA') THEN
                          vCLIENTES_NACIONAIS
                         WHEN M.TIPOMOV IN ('SAIDA REM ENTREGA FUTURA') THEN
                          vFATURAMENTO_ANTECIPADO
                         WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO CONSIGNADO') THEN
                          vMERCADORIA_RECEBIDA_CONSIGADO
                         WHEN M.TIPOMOV IN ('SAIDA SIMPLES REMESSA') THEN
                          vENVIO_SIMPLES_REMESSA
                         WHEN M.TIPOMOV IN ('SAIDA CONSERTO') THEN
                          vREMESSA_CONSERTO
                         WHEN M.TIPOMOV IN ('SAIDA DEMONSTRACAO') THEN
                          vREMESSA_DEMOSTRACAO
                         WHEN M.TIPOMOV IN ('SAIDA PERDA MERCADORIA') THEN
                          vPERDA_MERCADORIA
                         WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO') THEN
                          vDEVOLUCAO_RECEBER
                         WHEN M.TIPOMOV IN ('SAIDA TRANSFERENCIA') THEN
                          vTRANSFERENCIA_MERCADORIA
                         WHEN M.TIPOMOV IN ('ENTRADA CONSIGNADO') THEN
                          vESTOQUE_CONSIGANDO
                         WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                          vDEVOLUCAO_PRODUTO
                         WHEN M.TIPOMOV IN ('ENTRADA DEMONSTRACAO') THEN
                          vRETORNO_DEMOSTRACAO
                         WHEN M.TIPOMOV IN ('ENTRADA SIMPLES REMESSA') THEN
                         (CASE  
                           WHEN M.TEMVENDAORIG = 'S' THEN NULL
                           WHEN M.CODFORNEC IN (SELECT CODFORNEC FROM vFORNECEDORES_JCBROTHERS) THEN NULL 
                           ELSE vRECEBIMENTO_SIMPLES_REMESSA
                          END)
                         WHEN M.TIPOMOV IN ('ENTRADA FAT ENTREGA FUTURA') THEN
                          vESTOQUE_TRANSITO
                         ELSE
                          NULL
                       END) CONTADEBITO,
                       
                       ----------CONTACREDITO
                       (CASE
                         WHEN M.TIPOMOV IN
                              ('SAIDA VENDA',
                               'SAIDA FAT CONTA E ORDEM',
                               'SAIDA REM ENTREGA FUTURA') THEN
                          vFATURAMENTO_BRUTO
                         WHEN M.TIPOMOV IN ('SAIDA FAT ENTREGA FUTURA') THEN
                          vFATURAMENTO_ANTECIPADO
                         WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO CONSIGNADO') THEN
                          vESTOQUE_CONSIGANDO
                         WHEN M.TIPOMOV IN ('SAIDA SIMPLES REMESSA') THEN
                          vRECEBIMENTO_SIMPLES_REMESSA
                         WHEN M.TIPOMOV IN ('SAIDA CONSERTO') THEN
                          vRETORNO_CONSERTO
                         WHEN M.TIPOMOV IN ('SAIDA DEMONSTRACAO') THEN
                          vRETORNO_DEMOSTRACAO
                         WHEN M.TIPOMOV IN ('SAIDA PERDA MERCADORIA') THEN
                          vESTOQUE
                         WHEN M.TIPOMOV IN
                              ('ENTRADA COMPRA',
                               'ENTRADA COMPRA CONSIGNADO',
                               'ENTRADA COMPRA TRIANGULAR',
                               'ENTRADA FAT ENTREGA FUTURA') THEN
                          NVL(C.CODCONTABIL, vOUTRO_FORNECEDOR)
                         WHEN M.TIPOMOV IN ('ENTRADA BONIFICADA') THEN
                          vENTRADA_BONIFICADA
                         WHEN M.TIPOMOV IN ('ENTRADA CONSIGNADO') THEN
                          vMERCADORIA_RECEBIDA_CONSIGADO
                         WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                          vDEVOLUCAO_CLIENTE
                         WHEN M.TIPOMOV IN ('ENTRADA TRANSFERENCIA') THEN
                          vTRANSFERENCIA_MERCADORIA
                         WHEN M.TIPOMOV IN ('ENTRADA DEMONSTRACAO') THEN
                          vREMESSA_DEMOSTRACAO
                         WHEN M.TIPOMOV IN ('ENTRADA SIMPLES REMESSA') THEN
                         (CASE  
                           WHEN M.TEMVENDAORIG = 'S' THEN vDEVOLUCAO_RECEBER
                           WHEN M.CODFORNEC IN (SELECT CODFORNEC FROM vFORNECEDORES_JCBROTHERS) THEN vENTRADA_INVENTARIO 
                           ELSE vENVIO_SIMPLES_REMESSA
                          END)
                          WHEN M.TIPOMOV IN ('ENTRADA REM ENTREGA FUTURA') THEN
                          vESTOQUE_TRANSITO
                         ELSE
                          NULL
                       END) CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       (CASE
                         WHEN M.TIPOMOV IN ('SAIDA PERDA MERCADORIA') THEN
                          F.CODCC
                         ELSE
                          NULL
                       END) CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       (CASE
                         WHEN M.TIPOMOV IN
                              ('SAIDA VENDA',
                               'SAIDA FAT CONTA E ORDEM',
                               'SAIDA REM ENTREGA FUTURA',
                               'ENTRADA DEVOLUCAO') THEN
                          (CASE
                            WHEN M.CODGERENTE IN (1, 8, 9, 10) AND
                                 M.CODFILIAL = '11' THEN vCC_DISTRIBUICAO_ES
                            ELSE
                             V.CODCC
                          END)
                         WHEN M.TIPOMOV IN ('ENTRADA BONIFICADA') THEN
                          F.CODCC
                         WHEN M.TIPOMOV IN ('ENTRADA SIMPLES REMESSA') THEN
                         (CASE  
                           WHEN M.TEMVENDAORIG = 'S' THEN NULL
                           WHEN M.CODFORNEC IN (SELECT CODFORNEC FROM vFORNECEDORES_JCBROTHERS) THEN F.CODCC
                           ELSE NULL
                          END)
                         ELSE
                          NULL
                       END) CODCC_CREDITO,
                       
                       (M.TIPOMOV || ' - Nº TRANS: ' || M.NUMTRANSACAO) ATIVIDADE,
                       
                       ----------HISTORICO
                       (CASE
                         WHEN (M.MOVIMENTO = 'S' OR M.TIPOMOV IN ('ENTRADA DEVOLUCAO','ENTRADA DEMONSTRACAO'))THEN
                          ('NF ' || M.NUMNOTA || ' - ' || M.CLIENTE)
                         ELSE
                          ('NF ' || M.NUMNOTA || ' - ' || M.FORNECEDOR)
                       END) HISTORICO,
                       
                       M.VALORCONTABIL VALOR,
                       ('FN_MOV_PROD_VALOR_INTEIRO') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       (CASE
                         WHEN M.TIPOMOV IN ('SAIDA PERDA MERCADORIA') THEN
                          'S'
                         ELSE
                          'N'
                       END) ENVIAR_CONTABIL,
                       
                       M.DTCANCEL
                  FROM TABLE(PKG_BI_CONTABILIDADE.FN_MOV_PROD_BASE()) M
                  LEFT JOIN FILIAL_CC F ON F.CODFILIAL = M.CODFILIAL
                  LEFT JOIN VENDEDOR_CC V ON V.CODSUPERVISOR = M.CODSUPERVISOR
                  LEFT JOIN BI_SINC_FORNECEDOR_CONTA C ON C.CODEMPRESA =
                                                          M.CODEMPRESA
                                                      AND C.CODFORNEC =
                                                          M.CODFORNEC
                 WHERE 1 = 1
                   /*AND M.TIPOMOV IN ('SAIDA VENDA',
                                     'SAIDA FAT CONTA E ORDEM',
                                     'SAIDA FAT ENTREGA FUTURA',
                                     'SAIDA REM ENTREGA FUTURA',
                                     'SAIDA DEVOLUCAO CONSIGNADO',
                                     'SAIDA SIMPLES REMESSA',
                                     'SAIDA CONSERTO',
                                     'SAIDA DEMONSTRACAO',
                                     'SAIDA PERDA MERCADORIA',
                                     'SAIDA DEVOLUCAO',
                                     'SAIDA TRANSFERENCIA',
                                     'ENTRADA COMPRA',
                                     'ENTRADA COMPRA CONSIGNADO',
                                     'ENTRADA COMPRA TRIANGULAR',
                                     'ENTRADA BONIFICADA',
                                     'ENTRADA CONSIGNADO',
                                     'ENTRADA DEMONSTRACAO')*/)
    
    LOOP
      PIPE ROW(T_MOV_PROD_CONTABIL_RECORD(r.CODEMPRESA,
                                          r.DATA,
                                          r.TIPOLANCAMENTO,
                                          r.IDENTIFICADOR,
                                          r.DOCUMENTO,
                                          r.CONTADEBITO,
                                          r.CONTACREDITO,
                                          r.CODCC_DEBITO,
                                          r.CODCC_CREDITO,
                                          r.ATIVIDADE,
                                          r.HISTORICO,
                                          r.VALOR,
                                          r.ORIGEM,
                                          r.ENVIAR_CONTABIL,
                                          r.DTCANCEL));
    
    END LOOP;

 END FN_MOV_PROD_VLCONTABIL_INTEIRO;

END PKG_BI_CONTABILIDADE;
/
