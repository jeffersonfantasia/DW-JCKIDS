CREATE OR REPLACE PACKAGE BODY PKG_BI_CONTABILIDADE IS

  -----------------------CENTROS DE CUSTO
  vCC_SPMARKET        VARCHAR(3) := '1.1';
  vCC_PARQUE          VARCHAR(3) := '1.2';
  vCC_JUNDIAI         VARCHAR(3) := '1.3';
  vCC_TRIMAIS         VARCHAR(3) := '1.4';
  vCC_CAMPINAS        VARCHAR(3) := '1.5';
  vCC_DISTRIBUICAO_SP VARCHAR(3) := '2.1';
  vCC_DISTRIBUICAO_ES VARCHAR(3) := '2.2';
  vCC_ECOMMERCE_SP    VARCHAR(3) := '3.1';
  vCC_CORPORATIVO_SP  VARCHAR(3) := '4.1';

  -----------------------CODIGO CONTAS
  vCLIENTES_NACIONAIS            NUMBER := 1152;
  vFATURAMENTO_BRUTO             NUMBER := 3101;
  vFATURAMENTO_ANTECIPADO        NUMBER := 1182;
  vCMV                           NUMBER := 3110;
  vESTOQUE                       NUMBER := 1174;
  vICMS_VENDA                    NUMBER := 3104;
  vPIS_VENDA                     NUMBER := 3105;
  vCOFINS_VENDA                  NUMBER := 3106;
  vST_VENDA                      NUMBER := 3107;
  vICMS_RECOLHER                 NUMBER := 2251;
  vICMS_RECOLHER_ES              NUMBER := 2261;
  vPIS_RECOLHER                  NUMBER := 2254;
  vCOFINS_RECOLHER               NUMBER := 2255;
  vST_RECOLHER                   NUMBER := 2260;
  vDIFAL_RECOLHER                NUMBER := 2252;
  vESTOQUE_RECEBIDO_CONSIGNADO   NUMBER := 1176;
  vREMESSA_MERCADORIA_CONSIGNADO NUMBER := 2200;
  vDEVOLUCAO_RECEBER             NUMBER := 1178;
  vICMS_RECUPERAR                NUMBER := 1201;
  vICMS_RECUPERAR_ES             NUMBER := 1204;
  vPIS_RECUPERAR                 NUMBER := 1202;
  vCOFINS_RECUPERAR              NUMBER := 1203;
  vVENDA_BONIFICADA              NUMBER := 3113;
  vTRANSFERENCIA_MERCADORIA      NUMBER := 1183;
  vREMESSA_CONSERTO              NUMBER := 3117;
  vRETORNO_CONSERTO              NUMBER := 3120;
  vESTOQUE_TRANSITO              NUMBER := 1175;
  vREMESSA_DEMOSTRACAO           NUMBER := 3119;
  vRETORNO_DEMOSTRACAO           NUMBER := 3122;
  vENVIO_SIMPLES_REMESSA         NUMBER := 3118;
  vRECEBIMENTO_SIMPLES_REMESSA   NUMBER := 3121;
  vPERDA_MERCADORIA              NUMBER := 3114;
  vENTRADA_BONIFICADA            NUMBER := 3115;
  vDEVOLUCAO_PRODUTO             NUMBER := 3102;
  vDEVOLUCAO_CLIENTE             NUMBER := 2201;
  vENTRADA_INVENTARIO            NUMBER := 3116;
  vAQUISICAOIMOBILIZADO          NUMBER := 1451;
  vFRETE                         NUMBER := 3202;
  vDIFAL_MATERIAL_OPERACAO       NUMBER := 3555;
  vDIFAL_EQUIPAMENTO             NUMBER := 3653;

  ----------------OUTROS CODIGO CONTAS
  vOUTROSESTOQUES NUMBER := 200159;

  ----------------GRUPO CONTA
  vGRUPO_MATERIAL_OPERACAO NUMBER := 355;

  ----FORNECEDOR SEM CONTA CONTABIL
  vOUTRO_FORNECEDOR NUMBER := 99999;

  ----FILIAL ESPIRITO SANTO
  vCODFILIAL_ES VARCHAR2(2) := '11';

  ----FILIAL DEPOSITO SP
  vCODFILIAL_DEPOSITO_SP VARCHAR2(2) := '2';

  ----DATA MUDANCA DISTRIBUICAO FAT PARA ES
  vDT_MUDANCA_FAT_DISTRIB_ES DATE := TO_DATE('01/04/2024', 'DD/MM/YYYY');

  ---- GERENTES DA DISTRBUICAO

  ----CODIGO FORNECEDORES DAS FILIAIS DA JC BROTHERS
  FUNCTION FN_FORNEC_JCBROTHERS RETURN T_FORNEC_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT DISTINCT CODFORNEC FROM BI_SINC_FILIAL WHERE INSTR(EMPRESA, 'JC BROTHERS') > 0)
    LOOP
      PIPE ROW(T_FORNEC_RECORD(r.CODFORNEC));
    END LOOP;
  
  END FN_FORNEC_JCBROTHERS;

  ----CODIGO FORNECEDORES DESPESAS GERENCIAIS
  FUNCTION FN_FORNEC_DESP_GER RETURN T_FORNEC_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODFORNEC
                FROM BI_SINC_FORNECEDOR
               WHERE CODFORNEC IN (15,
                                   143,
                                   87,
                                   223,
                                   8549,
                                   7170,
                                   9211,
                                   9266,
                                   9272,
                                   9391,
                                   9681,
                                   9786,
                                   9837,
                                   9974,
                                   10336,
                                   10360,
                                   10567,
                                   10579,
                                   10620,
                                   10621))
    LOOP
      PIPE ROW(T_FORNEC_RECORD(r.CODFORNEC));
    END LOOP;
  
  END FN_FORNEC_DESP_GER;

  ----CODIGO CONTA GERENCIAL - DESPESAS IMPOSTOS A RECOLHER 
  FUNCTION FN_CONTA_IMPOSTO_DESP_GER RETURN T_CONTA_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODGERENCIAL CODCONTA
                FROM BI_SINC_PLANO_CONTAS_JC
               WHERE CODGERENCIAL IN (2253, 2256, 2257, 2258, 2261, 2262, 2266, 2267))
    LOOP
      PIPE ROW(T_CONTA_RECORD(r.CODCONTA));
    END LOOP;
  
  END FN_CONTA_IMPOSTO_DESP_GER;

  ----CENTRO DE CUSTO POR FILIAL
  FUNCTION FN_CC_FILIAL RETURN T_CC_FILIAL_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODFILIAL,
                     (CASE CODFILIAL
                       WHEN '1' THEN
                        vCC_SPMARKET
                       WHEN '2' THEN
                        vCC_DISTRIBUICAO_SP
                       WHEN '3' THEN
                        vCC_CORPORATIVO_SP
                       WHEN '5' THEN
                        vCC_CORPORATIVO_SP
                       WHEN '6' THEN
                        vCC_CORPORATIVO_SP
                       WHEN '7' THEN
                        vCC_ECOMMERCE_SP
                       WHEN '8' THEN
                        vCC_PARQUE
                       WHEN '9' THEN
                        vCC_ECOMMERCE_SP
                       WHEN '10' THEN
                        vCC_ECOMMERCE_SP
                       WHEN '11' THEN
                        vCC_DISTRIBUICAO_ES
                       WHEN '12' THEN
                        vCC_JUNDIAI
                       WHEN '13' THEN
                        vCC_TRIMAIS
                       WHEN '14' THEN
                        vCC_CAMPINAS
                     END) CODCC
                FROM BI_SINC_FILIAL F)
    LOOP
      PIPE ROW(T_CC_FILIAL_RECORD(r.CODFILIAL, r.CODCC));
    END LOOP;
  
  END FN_CC_FILIAL;

  ----CENTRO DE CUSTO POR SUPERVISOR
  FUNCTION FN_CC_VENDEDOR RETURN T_CC_VENDEDOR_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT DISTINCT CODSUPERVISOR,
                              (CASE
                                WHEN CODSUPERVISOR IN (1, 2, 4, 11, 12, 6) THEN
                                 vCC_DISTRIBUICAO_SP
                                WHEN CODSUPERVISOR IN (3, 9) THEN
                                 vCC_CORPORATIVO_SP
                                WHEN CODSUPERVISOR IN (7, 8, 10) THEN
                                 vCC_ECOMMERCE_SP
                                WHEN CODSUPERVISOR IN (5) THEN
                                 vCC_SPMARKET
                                WHEN CODSUPERVISOR IN (13) THEN
                                 vCC_PARQUE
                                WHEN CODSUPERVISOR IN (14) THEN
                                 vCC_JUNDIAI
                                WHEN CODSUPERVISOR IN (15) THEN
                                 vCC_TRIMAIS
                                WHEN CODSUPERVISOR IN (16) THEN
                                 vCC_CAMPINAS
                              END) CODCC
                FROM BI_SINC_VENDEDOR V)
    LOOP
      PIPE ROW(T_CC_VENDEDOR_RECORD(r.CODSUPERVISOR, r.CODCC));
    END LOOP;
  END FN_CC_VENDEDOR;

  ----MOVIMENTACAO PRODUTOS - VALOR CONTABIL INTEIRO
  FUNCTION FN_MOV_PROD_VLCONTABIL_INTEIRO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'P01' CODLANC,
                     M.CODEMPRESA,
                     M.DATA,
                     ---------TIPOLANCAMENTO
                     (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA SIMPLES REMESSA') THEN
                        (CASE
                          WHEN M.TEMVENDAORIG = 'S' THEN
                           2
                          WHEN M.CODFORNEC IN (SELECT CODFORNEC FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNEC_JCBROTHERS())) THEN
                           2
                          ELSE
                           3
                        END)
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO', 'SAIDA TRANSFERENCIA') THEN
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
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA FAT ENTREGA FUTURA') THEN
                        vCLIENTES_NACIONAIS
                       WHEN M.TIPOMOV IN ('SAIDA REM ENTREGA FUTURA') THEN
                        vFATURAMENTO_ANTECIPADO
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO CONSIGNADO') THEN
                        vREMESSA_MERCADORIA_CONSIGNADO
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
                        vESTOQUE_RECEBIDO_CONSIGNADO
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        vDEVOLUCAO_PRODUTO
                       WHEN M.TIPOMOV IN ('ENTRADA DEMONSTRACAO') THEN
                        vRETORNO_DEMOSTRACAO
                       WHEN M.TIPOMOV IN ('ENTRADA SIMPLES REMESSA') THEN
                        (CASE
                          WHEN M.TEMVENDAORIG = 'S' THEN
                           NULL
                          WHEN M.CODFORNEC IN (SELECT CODFORNEC FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNEC_JCBROTHERS())) THEN
                           NULL
                          ELSE
                           vRECEBIMENTO_SIMPLES_REMESSA
                        END)
                       WHEN M.TIPOMOV IN ('ENTRADA FAT ENTREGA FUTURA') THEN
                        vESTOQUE_TRANSITO
                       ELSE
                        NULL
                     END) CONTADEBITO,
                     
                     ----------CONTACREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA') THEN
                        vFATURAMENTO_BRUTO
                       WHEN M.TIPOMOV IN ('SAIDA FAT ENTREGA FUTURA') THEN
                        vFATURAMENTO_ANTECIPADO
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO CONSIGNADO') THEN
                        vESTOQUE_RECEBIDO_CONSIGNADO
                       WHEN M.TIPOMOV IN ('SAIDA SIMPLES REMESSA') THEN
                        vRECEBIMENTO_SIMPLES_REMESSA
                       WHEN M.TIPOMOV IN ('SAIDA CONSERTO') THEN
                        vRETORNO_CONSERTO
                       WHEN M.TIPOMOV IN ('SAIDA DEMONSTRACAO') THEN
                        vRETORNO_DEMOSTRACAO
                       WHEN M.TIPOMOV IN ('SAIDA PERDA MERCADORIA') THEN
                        vESTOQUE
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                          'ENTRADA COMPRA CONSIGNADO',
                                          'ENTRADA COMPRA TRIANGULAR',
                                          'ENTRADA FAT ENTREGA FUTURA') THEN
                        NVL(M.CODFORNEC, vOUTRO_FORNECEDOR)
                       WHEN M.TIPOMOV IN ('ENTRADA BONIFICADA') THEN
                        vENTRADA_BONIFICADA
                       WHEN M.TIPOMOV IN ('ENTRADA CONSIGNADO') THEN
                        vREMESSA_MERCADORIA_CONSIGNADO
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        vDEVOLUCAO_CLIENTE
                       WHEN M.TIPOMOV IN ('ENTRADA TRANSFERENCIA') THEN
                        vTRANSFERENCIA_MERCADORIA
                       WHEN M.TIPOMOV IN ('ENTRADA DEMONSTRACAO') THEN
                        vREMESSA_DEMOSTRACAO
                       WHEN M.TIPOMOV IN ('ENTRADA SIMPLES REMESSA') THEN
                        (CASE
                          WHEN M.TEMVENDAORIG = 'S' THEN
                           vDEVOLUCAO_RECEBER
                          WHEN M.CODFORNEC IN (SELECT CODFORNEC FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNEC_JCBROTHERS())) THEN
                           vENTRADA_INVENTARIO
                          ELSE
                           vENVIO_SIMPLES_REMESSA
                        END)
                       WHEN M.TIPOMOV IN ('ENTRADA REM ENTREGA FUTURA') THEN
                        vESTOQUE_TRANSITO
                       ELSE
                        NULL
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        (CASE
                          WHEN M.CODGERENTE IN (1, 8, 9, 10)
                               AND M.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          ELSE
                           V.CODCC
                        END)
                       WHEN M.TIPOMOV IN ('SAIDA PERDA MERCADORIA') THEN
                        F.CODCC
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA') THEN
                        (CASE
                          WHEN M.CODGERENTE IN (1, 8, 9, 10)
                               AND M.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          ELSE
                           V.CODCC
                        END)
                       WHEN M.TIPOMOV IN ('ENTRADA BONIFICADA') THEN
                        F.CODCC
                       WHEN M.TIPOMOV IN ('ENTRADA SIMPLES REMESSA') THEN
                        (CASE
                          WHEN M.TEMVENDAORIG = 'S' THEN
                           NULL
                          WHEN M.CODFORNEC IN (SELECT CODFORNEC FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNEC_JCBROTHERS())) THEN
                           F.CODCC
                          ELSE
                           NULL
                        END)
                       ELSE
                        NULL
                     END) CODCC_CREDITO,
                     
                     (M.TIPOMOV || ' - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANSACAO: ' || M.NUMTRANSACAO) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN (M.MOVIMENTO = 'S' OR M.TIPOMOV IN ('ENTRADA DEVOLUCAO', 'ENTRADA DEMONSTRACAO')) THEN
                        ('NF ' || M.NUMNOTA || ' - ' || M.CLIENTE)
                       ELSE
                        ('NF ' || M.NUMNOTA || ' - ' || M.FORNECEDOR)
                     END) HISTORICO,
                     
                     M.VALORCONTABIL VALOR,
                     ('MOVPROD_VL_INTEIRO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA PERDA MERCADORIA')
                            OR (M.TIPOMOV IN ('ENTRADA SIMPLES REMESSA') AND
                            M.CODFORNEC IN (SELECT CODFORNEC FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNEC_JCBROTHERS()))) THEN
                        'S'
                       ELSE
                        'N'
                     END) ENVIAR_CONTABIL,
                     
                     M.DTCANCEL
                FROM BI_SINC_MOV_PROD_BASE_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = M.CODFILIAL
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.TIPOMOV NOT IN ('SAIDA BONIFICADA', 'SAIDA DESCONSIDERAR', 'SAIDA REM CONTA E ORDEM'))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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

  ----MOVIMENTACAO PRODUTOS - VALOR CONTABIL PARCIAL
  FUNCTION FN_MOV_PROD_VLCONTABIL_PARCIAL RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'P02' CODLANC,
                     M.CODEMPRESA,
                     M.DATA,
                     ---------TIPOLANCAMENTO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO', 'SAIDA TRANSFERENCIA') THEN
                        2
                       ELSE
                        1
                     END) TIPOLANCAMENTO,
                     
                     M.NUMTRANSACAO IDENTIFICADOR,
                     M.NUMNOTA DOCUMENTO,
                     
                     ----------CONTADEBITO
                     (CASE
                     
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                          'ENTRADA COMPRA CONSIGNADO',
                                          'ENTRADA COMPRA TRIANGULAR',
                                          'ENTRADA BONIFICADA',
                                          'ENTRADA TRANSFERENCIA',
                                          'ENTRADA REM ENTREGA FUTURA') THEN
                        vESTOQUE
                       WHEN M.TIPOMOV IN ('ENTRADA SIMPLES REMESSA')
                            AND
                            (M.TEMVENDAORIG = 'S' OR
                            M.CODFORNEC IN (SELECT CODFORNEC FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNEC_JCBROTHERS()))) THEN
                        vESTOQUE
                       ELSE
                        NULL
                     END) CONTADEBITO,
                     
                     ----------CONTACREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO', 'SAIDA TRANSFERENCIA') THEN
                        vESTOQUE
                       ELSE
                        NULL
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     (M.TIPOMOV || ' - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANSACAO: ' || M.NUMTRANSACAO) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN (M.MOVIMENTO = 'S' OR M.TIPOMOV IN ('ENTRADA DEVOLUCAO')) THEN
                        ('NF ' || M.NUMNOTA || ' - ' || M.CLIENTE)
                       ELSE
                        ('NF ' || M.NUMNOTA || ' - ' || M.FORNECEDOR)
                     END) HISTORICO,
                     
                     (CASE
                       WHEN (M.VALORCONTABIL - NVL(M.VALORICMS, 0) - NVL(M.VALORPIS, 0) - NVL(M.VALORCOFINS, 0)) > 0 THEN
                        M.VALORCONTABIL - NVL(M.VALORICMS, 0) - NVL(M.VALORPIS, 0) - NVL(M.VALORCOFINS, 0)
                       ELSE
                        0
                     END) VALOR,
                     ('MOVPROD_VL_PARCIAL') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     M.DTCANCEL
                FROM BI_SINC_MOV_PROD_BASE_AGG M
               WHERE 1 = 1
                 AND M.TIPOMOV IN ('SAIDA DEVOLUCAO',
                                   'SAIDA TRANSFERENCIA',
                                   'ENTRADA COMPRA',
                                   'ENTRADA COMPRA CONSIGNADO',
                                   'ENTRADA COMPRA TRIANGULAR',
                                   'ENTRADA BONIFICADA',
                                   'ENTRADA TRANSFERENCIA',
                                   'ENTRADA SIMPLES REMESSA',
                                   'ENTRADA REM ENTREGA FUTURA'))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_MOV_PROD_VLCONTABIL_PARCIAL;

  ----MOVIMENTACAO PRODUTOS - CUSTO CONTABIL
  FUNCTION FN_MOV_PROD_CUSTO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'P03' CODLANC,
                     M.CODEMPRESA,
                     M.DATA,
                     ---------TIPOLANCAMENTO
                     3 TIPOLANCAMENTO,
                     M.NUMTRANSACAO IDENTIFICADOR,
                     M.NUMNOTA DOCUMENTO,
                     
                     ----------CONTADEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA REM CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA') THEN
                        vCMV
                       WHEN M.TIPOMOV IN ('SAIDA BONIFICADA') THEN
                        vVENDA_BONIFICADA
                       WHEN M.TIPOMOV IN ('SAIDA CONSERTO', 'SAIDA DEMONSTRACAO') THEN
                        vESTOQUE_TRANSITO
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO', 'ENTRADA CONSERTO', 'ENTRADA DEMONSTRACAO') THEN
                        vESTOQUE
                       ELSE
                        NULL
                     END) CONTADEBITO,
                     
                     ----------CONTACREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA',
                                          'SAIDA REM CONTA E ORDEM',
                                          'SAIDA REM ENTREGA FUTURA',
                                          'SAIDA BONIFICADA',
                                          'SAIDA CONSERTO',
                                          'SAIDA DEMONSTRACAO') THEN
                        vESTOQUE
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        vCMV
                       WHEN M.TIPOMOV IN ('ENTRADA CONSERTO', 'ENTRADA DEMONSTRACAO') THEN
                        vESTOQUE_TRANSITO
                       ELSE
                        NULL
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN M.TIPOMOV IN
                            ('SAIDA VENDA', 'SAIDA REM CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA', 'SAIDA BONIFICADA') THEN
                        (CASE
                          WHEN M.CODGERENTE IN (1, 8, 9, 10)
                               AND M.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          ELSE
                           V.CODCC
                        END)
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        (CASE
                          WHEN M.CODGERENTE IN (1, 8, 9, 10)
                               AND M.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          ELSE
                           V.CODCC
                        END)
                       ELSE
                        NULL
                     END) CODCC_CREDITO,
                     
                     (M.TIPOMOV || ' - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANSACAO: ' || M.NUMTRANSACAO) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN (M.MOVIMENTO = 'S' OR M.TIPOMOV IN ('ENTRADA DEVOLUCAO', 'ENTRADA DEMONSTRACAO')) THEN
                        ('NF ' || M.NUMNOTA || ' - ' || M.CLIENTE)
                       ELSE
                        ('NF ' || M.NUMNOTA || ' - ' || M.FORNECEDOR)
                     END) HISTORICO,
                     
                     ----------VALOR
                     (CASE
                       WHEN M.TIPOMOV IN
                            ('SAIDA CONSERTO', 'SAIDA DEMONSTRACAO', 'ENTRADA CONSERTO', 'ENTRADA DEMONSTRACAO') THEN
                        M.VALORCONTABIL
                       ELSE
                        M.CUSTOCONTABIL
                     END) VALOR,
                     ('MOVPROD_CUSTO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     M.DTCANCEL
                FROM BI_SINC_MOV_PROD_BASE_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.TIPOMOV IN ('SAIDA VENDA',
                                   'SAIDA REM CONTA E ORDEM',
                                   'SAIDA REM ENTREGA FUTURA',
                                   'SAIDA BONIFICADA',
                                   'SAIDA CONSERTO',
                                   'SAIDA DEMONSTRACAO',
                                   'ENTRADA DEVOLUCAO',
                                   'ENTRADA CONSERTO',
                                   'ENTRADA DEMONSTRACAO'))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_MOV_PROD_CUSTO;

  ----MOVIMENTACAO PRODUTOS - ICMS
  FUNCTION FN_MOV_PROD_ICMS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'P04' CODLANC,
                     M.CODEMPRESA,
                     M.DATA,
                     ---------TIPOLANCAMENTO
                     (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                          'ENTRADA COMPRA CONSIGNADO',
                                          'ENTRADA COMPRA TRIANGULAR',
                                          'ENTRADA BONIFICADA',
                                          'ENTRADA TRANSFERENCIA',
                                          'ENTRADA SIMPLES REMESSA',
                                          'ENTRADA REM ENTREGA FUTURA') THEN
                        1
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO', 'SAIDA TRANSFERENCIA') THEN
                        2
                       ELSE
                        3
                     END) TIPOLANCAMENTO,
                     
                     M.NUMTRANSACAO IDENTIFICADOR,
                     M.NUMNOTA DOCUMENTO,
                     
                     ----------CONTADEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA',
                                          'SAIDA FAT CONTA E ORDEM',
                                          'SAIDA REM ENTREGA FUTURA',
                                          'SAIDA BONIFICADA',
                                          'SAIDA DEMONSTRACAO',
                                          'SAIDA DEVOLUCAO CONSIGNADO') THEN
                        vICMS_VENDA
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                          'ENTRADA COMPRA CONSIGNADO',
                                          'ENTRADA COMPRA TRIANGULAR',
                                          'ENTRADA BONIFICADA',
                                          'ENTRADA TRANSFERENCIA',
                                          'ENTRADA SIMPLES REMESSA',
                                          'ENTRADA REM ENTREGA FUTURA',
                                          'ENTRADA CONSIGNADO') THEN
                        (CASE
                          WHEN M.CODFILIAL = vCODFILIAL_ES THEN
                           vICMS_RECUPERAR_ES
                          ELSE
                           vICMS_RECUPERAR
                        END)
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO', 'ENTRADA DEMONSTRACAO') THEN
                        (CASE
                          WHEN M.CODFILIAL = vCODFILIAL_ES THEN
                           vICMS_RECOLHER_ES
                          ELSE
                           vICMS_RECOLHER
                        END)
                       ELSE
                        NULL
                     END) CONTADEBITO,
                     
                     ----------CONTACREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA',
                                          'SAIDA FAT CONTA E ORDEM',
                                          'SAIDA REM ENTREGA FUTURA',
                                          'SAIDA BONIFICADA',
                                          'SAIDA TRANSFERENCIA',
                                          'SAIDA DEMONSTRACAO',
                                          'SAIDA DEVOLUCAO CONSIGNADO') THEN
                        (CASE
                          WHEN M.CODFILIAL = vCODFILIAL_ES THEN
                           vICMS_RECOLHER_ES
                          ELSE
                           vICMS_RECOLHER
                        END)
                     
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO') THEN
                        (CASE
                          WHEN M.CODFILIAL = vCODFILIAL_ES THEN
                           vICMS_RECUPERAR_ES
                          ELSE
                           vICMS_RECUPERAR
                        END)
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO', 'ENTRADA DEMONSTRACAO') THEN
                        vICMS_VENDA
                       WHEN M.TIPOMOV IN ('ENTRADA CONSIGNADO') THEN
                        vESTOQUE
                       ELSE
                        NULL
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA',
                                          'SAIDA FAT CONTA E ORDEM',
                                          'SAIDA REM ENTREGA FUTURA',
                                          'SAIDA BONIFICADA',
                                          'SAIDA DEMONSTRACAO') THEN
                        (CASE
                          WHEN M.CODGERENTE IN (1, 8, 9, 10)
                               AND M.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          ELSE
                           V.CODCC
                        END)
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO CONSIGNADO') THEN
                        F.CODCC
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO', 'ENTRADA DEMONSTRACAO') THEN
                        (CASE
                          WHEN M.CODGERENTE IN (1, 8, 9, 10)
                               AND M.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          ELSE
                           V.CODCC
                        END)
                       ELSE
                        NULL
                     END) CODCC_CREDITO,
                     
                     (M.TIPOMOV || ' - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANSACAO: ' || M.NUMTRANSACAO) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN (M.MOVIMENTO = 'S' OR M.TIPOMOV IN ('ENTRADA DEVOLUCAO', 'ENTRADA DEMONSTRACAO')) THEN
                        ('NF ' || M.NUMNOTA || ' - ' || M.CLIENTE)
                       ELSE
                        ('NF ' || M.NUMNOTA || ' - ' || M.FORNECEDOR)
                     END) HISTORICO,
                     
                     M.VALORICMS VALOR,
                     ('MOVPROD_VL_ICMS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     M.DTCANCEL
                FROM BI_SINC_MOV_PROD_BASE_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = M.CODFILIAL
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_MOV_PROD_ICMS;

  ----MOVIMENTACAO PRODUTOS - PIS
  FUNCTION FN_MOV_PROD_PIS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'P05' CODLANC,
                     M.CODEMPRESA,
                     M.DATA,
                     ---------TIPOLANCAMENTO
                     (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                          'ENTRADA COMPRA CONSIGNADO',
                                          'ENTRADA COMPRA TRIANGULAR',
                                          'ENTRADA REM ENTREGA FUTURA') THEN
                        1
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO') THEN
                        2
                       ELSE
                        3
                     END) TIPOLANCAMENTO,
                     
                     M.NUMTRANSACAO IDENTIFICADOR,
                     M.NUMNOTA DOCUMENTO,
                     
                     ----------CONTADEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA') THEN
                        vPIS_VENDA
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                          'ENTRADA COMPRA CONSIGNADO',
                                          'ENTRADA COMPRA TRIANGULAR',
                                          'ENTRADA REM ENTREGA FUTURA') THEN
                        vPIS_RECUPERAR
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        vPIS_RECOLHER
                       ELSE
                        NULL
                     END) CONTADEBITO,
                     
                     ----------CONTACREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA') THEN
                        vPIS_RECOLHER
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO') THEN
                        vPIS_RECUPERAR
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        vPIS_VENDA
                       ELSE
                        NULL
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA') THEN
                        (CASE
                          WHEN M.CODGERENTE IN (1, 8, 9, 10)
                               AND M.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          ELSE
                           V.CODCC
                        END)
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        (CASE
                          WHEN M.CODGERENTE IN (1, 8, 9, 10)
                               AND M.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          ELSE
                           V.CODCC
                        END)
                       ELSE
                        NULL
                     END) CODCC_CREDITO,
                     
                     (M.TIPOMOV || ' - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANSACAO: ' || M.NUMTRANSACAO) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN (M.MOVIMENTO = 'S' OR M.TIPOMOV IN ('ENTRADA DEVOLUCAO')) THEN
                        ('NF ' || M.NUMNOTA || ' - ' || M.CLIENTE)
                       ELSE
                        ('NF ' || M.NUMNOTA || ' - ' || M.FORNECEDOR)
                     END) HISTORICO,
                     
                     M.VALORPIS VALOR,
                     ('MOVPROD_VL_PIS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     M.DTCANCEL
                FROM BI_SINC_MOV_PROD_BASE_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_MOV_PROD_PIS;

  ----MOVIMENTACAO PRODUTOS - COFINS
  FUNCTION FN_MOV_PROD_COFINS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'P06' CODLANC,
                     M.CODEMPRESA,
                     M.DATA,
                     ---------TIPOLANCAMENTO
                     (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                          'ENTRADA COMPRA CONSIGNADO',
                                          'ENTRADA COMPRA TRIANGULAR',
                                          'ENTRADA REM ENTREGA FUTURA') THEN
                        1
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO') THEN
                        2
                       ELSE
                        3
                     END) TIPOLANCAMENTO,
                     
                     M.NUMTRANSACAO IDENTIFICADOR,
                     M.NUMNOTA DOCUMENTO,
                     
                     ----------CONTADEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA') THEN
                        vCOFINS_VENDA
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                          'ENTRADA COMPRA CONSIGNADO',
                                          'ENTRADA COMPRA TRIANGULAR',
                                          'ENTRADA REM ENTREGA FUTURA') THEN
                        vCOFINS_RECUPERAR
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        vCOFINS_RECOLHER
                       ELSE
                        NULL
                     END) CONTADEBITO,
                     
                     ----------CONTACREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA') THEN
                        vCOFINS_RECOLHER
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO') THEN
                        vCOFINS_RECUPERAR
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        vCOFINS_VENDA
                       ELSE
                        NULL
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA') THEN
                        (CASE
                          WHEN M.CODGERENTE IN (1, 8, 9, 10)
                               AND M.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          ELSE
                           V.CODCC
                        END)
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        (CASE
                          WHEN M.CODGERENTE IN (1, 8, 9, 10)
                               AND M.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          ELSE
                           V.CODCC
                        END)
                       ELSE
                        NULL
                     END) CODCC_CREDITO,
                     
                     (M.TIPOMOV || ' - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANSACAO: ' || M.NUMTRANSACAO) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN (M.MOVIMENTO = 'S' OR M.TIPOMOV IN ('ENTRADA DEVOLUCAO')) THEN
                        ('NF ' || M.NUMNOTA || ' - ' || M.CLIENTE)
                       ELSE
                        ('NF ' || M.NUMNOTA || ' - ' || M.FORNECEDOR)
                     END) HISTORICO,
                     
                     M.VALORCOFINS VALOR,
                     ('MOVPROD_VL_COFINS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     M.DTCANCEL
                FROM BI_SINC_MOV_PROD_BASE_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_MOV_PROD_COFINS;

  ----MOVIMENTACAO PRODUTOS - ICMS-ST
  FUNCTION FN_MOV_PROD_ST RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'P07' CODLANC,
                     M.CODEMPRESA,
                     M.DATA,
                     3 TIPOLANCAMENTO,
                     M.NUMTRANSACAO IDENTIFICADOR,
                     M.NUMNOTA DOCUMENTO,
                     
                     ----------CONTADEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA', 'ENTRADA COMPRA CONSIGNADO', 'ENTRADA COMPRA TRIANGULAR') THEN
                        vESTOQUE
                       ELSE
                        vST_VENDA
                     END) CONTADEBITO,
                     
                     ----------CONTACREDITO
                     vST_RECOLHER CONTACREDITO,
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN M.CODGERENTE IN (1, 8, 9, 10)
                            AND M.CODFILIAL = vCODFILIAL_ES THEN
                        vCC_DISTRIBUICAO_ES
                       ELSE
                        V.CODCC
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     (M.TIPOMOV || ' - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANSACAO: ' || M.NUMTRANSACAO) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN M.MOVIMENTO = 'S' THEN
                        ('NF ' || M.NUMNOTA || ' - ' || M.CLIENTE)
                       ELSE
                        ('NF ' || M.NUMNOTA || ' - ' || M.FORNECEDOR)
                     END) HISTORICO,
                     ----------VALOR
                     (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA', 'ENTRADA COMPRA CONSIGNADO', 'ENTRADA COMPRA TRIANGULAR') THEN
                        M.VALORSTGUIA
                       ELSE
                        M.VALORST
                     END) VALOR,
                     ('MOVPROD_VL_ST') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     M.DTCANCEL
                FROM BI_SINC_MOV_PROD_BASE_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.TIPOMOV IN ('SAIDA VENDA',
                                   'SAIDA FAT CONTA E ORDEM',
                                   'SAIDA REM ENTREGA FUTURA',
                                   'ENTRADA COMPRA',
                                   'ENTRADA COMPRA CONSIGNADO',
                                   'ENTRADA COMPRA TRIANGULAR'))
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_MOV_PROD_ST;

  ----MOVIMENTACAO PRODUTOS - ICMS-DIFAL
  FUNCTION FN_MOV_PROD_DIFAL RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'P08' CODLANC,
                     M.CODEMPRESA,
                     M.DATA,
                     3 TIPOLANCAMENTO,
                     M.NUMTRANSACAO IDENTIFICADOR,
                     M.NUMNOTA DOCUMENTO,
                     vICMS_VENDA CONTADEBITO,
                     vDIFAL_RECOLHER CONTACREDITO,
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN M.CODGERENTE IN (1, 8, 9, 10)
                            AND M.CODFILIAL = vCODFILIAL_ES THEN
                        vCC_DISTRIBUICAO_ES
                       ELSE
                        V.CODCC
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     (M.TIPOMOV || ' - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANSACAO: ' || M.NUMTRANSACAO) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN (M.MOVIMENTO = 'S' OR M.TIPOMOV IN ('ENTRADA DEVOLUCAO')) THEN
                        ('NF ' || M.NUMNOTA || ' - ' || M.CLIENTE)
                       ELSE
                        ('NF ' || M.NUMNOTA || ' - ' || M.FORNECEDOR)
                     END) HISTORICO,
                     
                     M.VALORICMSDIFAL VALOR,
                     ('MOVPROD_VL_DIFAL') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     M.DTCANCEL
                FROM BI_SINC_MOV_PROD_BASE_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA'))
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_MOV_PROD_DIFAL;

  ----DESPESA FISCAL - VALOR CONTABIL INTEIRO
  FUNCTION FN_DESP_FISCAL_VLCONTABIL_INTEIRO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (WITH vENT_BONIFICADA AS
                 (SELECT CODFISCAL FROM PCCFO F WHERE F.CODFISCAL IN (1910, 2910, 1911)),
                vENT_REM_ENTREGA_FUTURA AS
                 (SELECT CODFISCAL FROM PCCFO F WHERE F.CODFISCAL IN (1116, 1117, 2116, 2117))
                
                SELECT 'D01' CODLANC,
                       E.CODEMPRESA,
                       E.DATA,
                       
                       ----------TIPO LANCAMENTO
                       (CASE
                         WHEN E.VLIMPOSTO > 0 THEN
                          1
                         WHEN E.CFOP IN (SELECT CODFISCAL FROM vENT_BONIFICADA) THEN
                          2
                         ELSE
                          3
                       END) TIPOLANCAMENTO,
                       
                       E.NUMTRANSENT IDENTIFICADOR,
                       E.NUMNOTA DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       (CASE
                         WHEN E.CODCONTA = vOUTROSESTOQUES THEN
                          NULL
                         ELSE
                          E.CODCONTA
                       END) CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       (CASE
                         WHEN E.CODCONTA = vOUTROSESTOQUES
                              AND E.CFOP IN (SELECT CODFISCAL FROM vENT_BONIFICADA) THEN
                          vENTRADA_BONIFICADA
                         WHEN E.VLIMPOSTO > 0 THEN
                          NULL
                         WHEN E.CFOP IN (SELECT CODFISCAL FROM vENT_REM_ENTREGA_FUTURA) THEN
                          vAQUISICAOIMOBILIZADO
                         ELSE
                          NVL(E.CODFORNEC, vOUTRO_FORNECEDOR)
                       END) CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       (CASE
                         WHEN E.CODCONTA = vOUTROSESTOQUES THEN
                          NULL
                         WHEN E.CODCONTA = vFRETE
                              AND E.CODCC = '0' THEN
                          (CASE
                            WHEN E.CODFILIAL = vCODFILIAL_ES THEN
                             vCC_DISTRIBUICAO_ES
                            WHEN E.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                                 AND E.CODFILIAL = vCODFILIAL_DEPOSITO_SP THEN
                             vCC_CORPORATIVO_SP
                            WHEN E.CODSUPERVISOR IS NOT NULL THEN
                             V.CODCC
                            ELSE
                             L.CODCC
                          END)
                         WHEN E.CODCC = '0' THEN
                          NULL
                         ELSE
                          E.CODCC
                       END) CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       (CASE
                         WHEN E.CODCONTA = vOUTROSESTOQUES
                              AND E.CFOP IN (SELECT CODFISCAL FROM vENT_BONIFICADA) THEN
                          L.CODCC
                         ELSE
                          NULL
                       END) CODCC_CREDITO,
                       
                       ----------ATIVIDADE
                       (CASE
                         WHEN E.RECNUM > 0 THEN
                          (UPPER(C.CONTA) || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                          TRIM(TRANSLATE(TO_CHAR(E.VALOR, '999,999.00'), '.,', ',.')) || ' - RAT: ' ||
                          REPLACE(TO_CHAR(E.PERCRATEIO, '999.00'), '.', ',') || '% - Nº TRANSENT: ' || E.NUMTRANSENT ||
                          ' - RECNUM: ' || E.RECNUM)
                         ELSE
                          (UPPER(C.CONTA) || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - Nº TRANSENT: ' || E.NUMTRANSENT)
                       END) ATIVIDADE,
                       
                       ----------HISTORICO
                       (E.ESPECIE || ' ' || E.NUMNOTA || ' - ' || F.CNPJ || ' - ' || E.FORNECEDOR || ' - Cód: ' ||
                       E.CODFORNEC) HISTORICO,
                       
                       ROUND(E.VLRATEIO, 2) VALOR,
                       
                       ('DESP_FISCAL_VL_INTEIRO') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'N' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM BI_SINC_DESPESA_FISCAL_BASE E
                  LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                  LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = E.CODFILIAL
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = E.CODSUPERVISOR
                 WHERE 1 = 1)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_DESP_FISCAL_VLCONTABIL_INTEIRO;

  ----DESPESA FISCAL - VALOR CONTABIL PARCIAL
  FUNCTION FN_DESP_FISCAL_VLCONTABIL_PARCIAL RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (WITH vENT_BONIFICADA AS
                 (SELECT CODFISCAL FROM PCCFO F WHERE F.CODFISCAL IN (1910, 2910, 1911))
                
                SELECT DISTINCT 'D02' CODLANC,
                                E.CODEMPRESA,
                                E.DATA,
                                
                                ----------TIPO LANCAMENTO
                                (CASE
                                  WHEN E.VLIMPOSTO > 0 THEN
                                   2
                                  ELSE
                                   1
                                END) TIPOLANCAMENTO,
                                
                                E.NUMTRANSENT IDENTIFICADOR,
                                E.NUMNOTA DOCUMENTO,
                                
                                ----------CONTA_DEBITO
                                (CASE
                                  WHEN E.CFOP IN (SELECT CODFISCAL FROM vENT_BONIFICADA) THEN
                                   vESTOQUE
                                  ELSE
                                   NULL
                                END) CONTADEBITO,
                                
                                ----------CONTA_CREDITO
                                (CASE
                                  WHEN E.VLIMPOSTO > 0 THEN
                                   NVL(E.CODFORNEC, vOUTRO_FORNECEDOR)
                                  ELSE
                                   NULL
                                END) CONTACREDITO,
                                
                                ----------CODCC_DEBITO
                                NULL CODCC_DEBITO,
                                
                                ----------CODCC_CREDITO
                                NULL CODCC_CREDITO,
                                
                                ----------ATIVIDADE
                                (CASE
                                  WHEN E.RECNUM > 0 THEN
                                   (UPPER(C.CONTA) || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                                   TRIM(TRANSLATE(TO_CHAR(E.VALOR, '999,999.00'), '.,', ',.')) || ' - Nº TRANSENT: ' ||
                                   E.NUMTRANSENT || ' - RECNUM: ' || E.RECNUM)
                                  ELSE
                                   ('REMESSA BONIFICADA - F' || LPAD(E.CODFILIAL, 2, 0) || ' - Nº TRANSENT: ' ||
                                   E.NUMTRANSENT)
                                END) ATIVIDADE,
                                
                                ----------HISTORICO
                                (E.ESPECIE || ' ' || E.NUMNOTA || ' - ' || F.CNPJ || ' - ' || E.FORNECEDOR || ' - Cód: ' ||
                                E.CODFORNEC) HISTORICO,
                                
                                ----------VALOR
                                (CASE
                                  WHEN E.VLIMPOSTO > 0 THEN
                                   ROUND(E.VALOR - E.VLIMPOSTO, 2)
                                  ELSE
                                   ROUND(E.VALOR - E.VLICMS, 2)
                                END) VALOR,
                                
                                ('DESP_FISCAL_VL_PARCIAL') ORIGEM,
                                
                                ----------ENVIAR_CONTABIL
                                'N' ENVIAR_CONTABIL,
                                
                                TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM BI_SINC_DESPESA_FISCAL_BASE E
                  LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                  LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                 WHERE E.VLIMPOSTO > 0
                    OR E.CFOP IN (SELECT CODFISCAL FROM vENT_BONIFICADA))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_DESP_FISCAL_VLCONTABIL_PARCIAL;

  ----DESPESA FISCAL - ICMS
  FUNCTION FN_DESP_FISCAL_ICMS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (WITH vENT_BONIFICADA AS
                 (SELECT CODFISCAL FROM PCCFO F WHERE F.CODFISCAL IN (1910, 2910, 1911)),
                vENT_SIMPLES_REMESSA AS
                 (SELECT CODFISCAL FROM PCCFO F WHERE F.CODFISCAL IN (1908, 1909, 1949, 2949))
                
                SELECT 'D03' CODLANC,
                       E.CODEMPRESA,
                       E.DATA,
                       
                       ----------TIPO LANCAMENTO
                       (CASE
                         WHEN E.CFOP IN (SELECT CODFISCAL FROM vENT_BONIFICADA) THEN
                          1
                         ELSE
                          3
                       END) TIPOLANCAMENTO,
                       
                       E.NUMTRANSENT IDENTIFICADOR,
                       E.NUMNOTA DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       (CASE
                         WHEN E.CODFILIAL = vCODFILIAL_ES THEN
                          vICMS_RECUPERAR_ES
                         ELSE
                          vICMS_RECUPERAR
                       END) CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       (CASE
                         WHEN (E.CODCONTA = vOUTROSESTOQUES AND E.CFOP IN (SELECT CODFISCAL FROM vENT_SIMPLES_REMESSA)) THEN
                          vESTOQUE
                         WHEN (E.CODCONTA = vOUTROSESTOQUES AND E.CFOP IN (SELECT CODFISCAL FROM vENT_BONIFICADA)) THEN
                          NULL
                         ELSE
                          E.CODCONTA
                       END) CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       NULL CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       (CASE
                         WHEN (E.CODCONTA = vFRETE AND E.CODCC = '0') THEN
                          (CASE
                            WHEN E.CODFILIAL = vCODFILIAL_ES THEN
                             vCC_DISTRIBUICAO_ES
                            WHEN E.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                                 AND E.CODFILIAL = vCODFILIAL_DEPOSITO_SP THEN
                             vCC_CORPORATIVO_SP
                            WHEN E.CODSUPERVISOR IS NOT NULL THEN
                             V.CODCC
                            ELSE
                             L.CODCC
                          END)
                         WHEN (E.CODCC = '0' OR E.CODCONTA = vOUTROSESTOQUES) THEN
                          NULL
                         ELSE
                          E.CODCC
                       END) CODCC_CREDITO,
                       
                       ----------ATIVIDADE
                       (CASE
                         WHEN E.RECNUM > 0 THEN
                          (UPPER(C.CONTA) || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                          TRIM(TRANSLATE(TO_CHAR(E.VALOR, '999,999.00'), '.,', ',.')) || ' - RAT: ' ||
                          REPLACE(TO_CHAR(E.PERCRATEIO, '999.00'), '.', ',') || '% - Nº TRANSENT: ' || E.NUMTRANSENT ||
                          ' - RECNUM: ' || E.RECNUM)
                         ELSE
                          (UPPER(C.CONTA) || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - Nº TRANSENT: ' || E.NUMTRANSENT)
                       END) ATIVIDADE,
                       
                       ----------HISTORICO
                       (E.ESPECIE || ' ' || E.NUMNOTA || ' - ' || F.CNPJ || ' - ' || E.FORNECEDOR || ' - Cód: ' ||
                       E.CODFORNEC) HISTORICO,
                       
                       ROUND(E.VLICMS, 2) VALOR,
                       
                       ('DESP_FISCAL_ICMS') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'N' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM BI_SINC_DESPESA_FISCAL_BASE E
                  LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                  LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = E.CODFILIAL
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = E.CODSUPERVISOR
                 WHERE E.VLICMS > 0
                   AND E.CODEMPRESA = 1)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_DESP_FISCAL_ICMS;

  ----DESPESA FISCAL - PIS
  FUNCTION FN_DESP_FISCAL_PIS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'D04' CODLANC,
                     E.CODEMPRESA,
                     E.DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     E.NUMTRANSENT IDENTIFICADOR,
                     E.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vPIS_RECUPERAR CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     E.CODCONTA CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN (E.CODCONTA = vFRETE AND E.CODCC = '0') THEN
                        (CASE
                          WHEN E.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          WHEN E.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                               AND E.CODFILIAL = vCODFILIAL_DEPOSITO_SP THEN
                           vCC_CORPORATIVO_SP
                          WHEN E.CODSUPERVISOR IS NOT NULL THEN
                           V.CODCC
                          ELSE
                           L.CODCC
                        END)
                       WHEN E.CODCC = '0' THEN
                        L.CODCC
                       ELSE
                        E.CODCC
                     END) CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN E.RECNUM > 0 THEN
                        (UPPER(C.CONTA) || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                        TRIM(TRANSLATE(TO_CHAR(E.VALOR, '999,999.00'), '.,', ',.')) || ' - RAT: ' ||
                        REPLACE(TO_CHAR(E.PERCRATEIO, '999.00'), '.', ',') || '% - Nº TRANSENT: ' || E.NUMTRANSENT ||
                        ' - RECNUM: ' || E.RECNUM)
                       ELSE
                        (UPPER(C.CONTA) || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - Nº TRANSENT: ' || E.NUMTRANSENT)
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (E.ESPECIE || ' ' || E.NUMNOTA || ' - ' || F.CNPJ || ' - ' || E.FORNECEDOR || ' - Cód: ' ||
                     E.CODFORNEC) HISTORICO,
                     
                     ROUND(E.VLPIS, 2) VALOR,
                     
                     ('DESP_FISCAL_PIS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_DESPESA_FISCAL_BASE E
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = E.CODFILIAL
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = E.CODSUPERVISOR
               WHERE E.VLPIS > 0
                 AND E.CODEMPRESA = 1)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_DESP_FISCAL_PIS;

  ----DESPESA FISCAL - COFINS
  FUNCTION FN_DESP_FISCAL_COFINS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'D05' CODLANC,
                     E.CODEMPRESA,
                     E.DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     E.NUMTRANSENT IDENTIFICADOR,
                     E.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vCOFINS_RECUPERAR CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     E.CODCONTA CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN (E.CODCONTA = vFRETE AND E.CODCC = '0') THEN
                        (CASE
                          WHEN E.CODFILIAL = vCODFILIAL_ES THEN
                           vCC_DISTRIBUICAO_ES
                          WHEN E.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                               AND E.CODFILIAL = vCODFILIAL_DEPOSITO_SP THEN
                           vCC_CORPORATIVO_SP
                          WHEN E.CODSUPERVISOR IS NOT NULL THEN
                           V.CODCC
                          ELSE
                           L.CODCC
                        END)
                       WHEN E.CODCC = '0' THEN
                        L.CODCC
                       ELSE
                        E.CODCC
                     END) CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN E.RECNUM > 0 THEN
                        (UPPER(C.CONTA) || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                        TRIM(TRANSLATE(TO_CHAR(E.VALOR, '999,999.00'), '.,', ',.')) || ' - RAT: ' ||
                        REPLACE(TO_CHAR(E.PERCRATEIO, '999.00'), '.', ',') || '% - Nº TRANSENT: ' || E.NUMTRANSENT ||
                        ' - RECNUM: ' || E.RECNUM)
                       ELSE
                        (UPPER(C.CONTA) || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - Nº TRANSENT: ' || E.NUMTRANSENT)
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (E.ESPECIE || ' ' || E.NUMNOTA || ' - ' || F.CNPJ || ' - ' || E.FORNECEDOR || ' - Cód: ' ||
                     E.CODFORNEC) HISTORICO,
                     
                     ROUND(E.VLCOFINS, 2) VALOR,
                     
                     ('DESP_FISCAL_COFINS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_DESPESA_FISCAL_BASE E
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = E.CODFILIAL
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = E.CODSUPERVISOR
               WHERE E.VLCOFINS > 0
                 AND E.CODEMPRESA = 1)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_DESP_FISCAL_COFINS;

  ----DESPESA FISCAL - DIFAL
  FUNCTION FN_DESP_FISCAL_DIFAL RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'D06' CODLANC,
                     E.CODEMPRESA,
                     E.DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     E.NUMTRANSENT IDENTIFICADOR,
                     E.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     (CASE
                       WHEN T.GRUPOCONTA = vGRUPO_MATERIAL_OPERACAO THEN
                        vDIFAL_MATERIAL_OPERACAO
                       ELSE
                        vDIFAL_EQUIPAMENTO
                     END) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vICMS_RECOLHER CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN E.CODCC = '0' THEN
                        L.CODCC
                       ELSE
                        E.CODCC
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN E.RECNUM > 0 THEN
                        (UPPER(C.CONTA) || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                        TRIM(TRANSLATE(TO_CHAR(E.VALOR, '999,999.00'), '.,', ',.')) || ' - RAT: ' ||
                        REPLACE(TO_CHAR(E.PERCRATEIO, '999.00'), '.', ',') || '% - Nº TRANSENT: ' || E.NUMTRANSENT ||
                        ' - RECNUM: ' || E.RECNUM)
                       ELSE
                        (UPPER(C.CONTA) || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - Nº TRANSENT: ' || E.NUMTRANSENT)
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (E.ESPECIE || ' ' || E.NUMNOTA || ' - ' || F.CNPJ || ' - ' || E.FORNECEDOR || ' - Cód: ' ||
                     E.CODFORNEC) HISTORICO,
                     
                     ROUND(E.VLDIFAL, 2) VALOR,
                     
                     ('DESP_FISCAL_DIFAL') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_DESPESA_FISCAL_BASE E
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                LEFT JOIN PCCONTA T ON T.CODCONTA = E.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = E.CODFILIAL
               WHERE E.VLDIFAL > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_DESP_FISCAL_DIFAL;

  ----DESPESA GERENCIAL - FORNECEDORES
  FUNCTION FN_DESP_GERENCIAL_FORNECEDOR RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'G01' CODLANC,
                     L.CODEMPRESA,
                     L.DTCOMPETENCIA DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     L.RECNUM IDENTIFICADOR,
                     L.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     L.CODCONTA CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     L.CODFORNEC CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     L.CODCC CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, '999,999.00'), '.,', ',.')) || ' - RAT: ' ||
                     REPLACE(TO_CHAR(L.PERCRATEIO, '999.00'), '.', ',') || '% - RECNUM: ' || L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('FATURA Nº ' || L.NUMNOTA || ' - ' || F.CNPJ || ' - ' || F.FORNECEDOR || ' - Cód: ' || L.CODFORNEC) HISTORICO,
                     
                     ROUND(L.VLRATEIO, 2) VALOR,
                     
                     ('DESP_GERENCIAL_FORNEC') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN BI_SINC_DESPESA_FISCAL_BASE E ON E.RECNUM = L.RECNUM
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC P ON P.CODGERENCIAL = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
               WHERE L.CODFORNEC IN (SELECT CODFORNEC FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNEC_DESP_GER()))
                 AND L.TIPO = 'CONFIRMADO'
                 AND L.VLRATEIO > 0
                 AND L.ADIANTAMENTO = 'N'
                 AND P.CODEBTIDA = 1
                 AND E.RECNUM IS NULL)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_DESP_GERENCIAL_FORNECEDOR;

  ----DESPESA GERENCIAL - IMPOSTO
  FUNCTION FN_DESP_GERENCIAL_IMPOSTO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'G02' CODLANC,
                     L.CODEMPRESA,
                     L.DTCOMPETENCIA DATA,
                     
                     ----------TIPO LANCAMENTO
                     (CASE
                       WHEN C.CODCONTACONTRAPARTIDA IS NULL THEN
                        2
                       ELSE
                        3
                     END) TIPOLANCAMENTO,
                     
                     L.RECNUM IDENTIFICADOR,
                     L.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     L.CODCONTA CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     C.CODCONTACONTRAPARTIDA CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, '999,999.00'), '.,', ',.')) || ' - RECNUM: ' || L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('IMPOSTO NS' || L.NUMNOTA || ' - ' || F.CNPJ || ' - ' || F.FORNECEDOR || ' - Cód: ' || L.CODFORNEC) HISTORICO,
                     
                     ROUND(L.VLRATEIO, 2) VALOR,
                     
                     ('DESP_GERENCIAL_IMPOSTO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
               WHERE L.CODCONTA IN (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_IMPOSTO_DESP_GER()))
                 AND L.TIPO = 'CONFIRMADO'
                 AND L.VLRATEIO > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_DESP_GERENCIAL_IMPOSTO;

  ----LANCAMENTOS PAGAMENTO - OUTROS
  FUNCTION FN_LANC_TIPO_OUTROS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'L01' CODLANC,
                     L.CODEMPRESA,
                     L.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     L.RECNUM IDENTIFICADOR,
                     L.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     (CASE
                       WHEN L.VALOR > 0 THEN
                        L.CODCONTA
                       ELSE
                        L.CONTABANCO
                     END) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     (CASE
                       WHEN L.VALOR > 0 THEN
                        L.CONTABANCO
                       ELSE
                        L.CODCONTA
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN L.VALOR > 0
                            AND C.CODDRE > 0 THEN
                        DECODE(L.CODCC, '0', FL.CODCC, L.CODCC)
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN L.VALOR < 0
                            AND C.CODDRE > 0 THEN
                        DECODE(L.CODCC, '0', FL.CODCC, L.CODCC)
                       ELSE
                        NULL
                     END) CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, '999,999.00'), '.,', ',.')) || ' - RECNUM: ' || L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN L.NUMNOTA > 0 THEN
                        ('DOC ' || L.NUMNOTA || ' - ' || UPPER(L.HISTORICO))
                       ELSE
                        UPPER(L.HISTORICO)
                     END) HISTORICO,
                     
                     ROUND(L.VLRATEIO, 2) VALOR,
                     
                     ('LANC_PAG_OUTROS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) FL ON FL.CODFILIAL = L.CODFILIAL
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = L.CODCONTA
               WHERE 1 = 1
                 AND L.CODCONTA <> L.CONTABANCO
                 AND L.CODBANCO NOT IN (17, 20, 22, 40, 41)
                 AND (L.GRUPOCONTA NOT IN (680, 900) OR L.CODCONTA = 9005)
                 AND L.NUMTRANS IS NOT NULL
                 AND L.DTCOMPENSACAO IS NOT NULL
                 AND L.ADIANTAMENTO = 'N'
                 AND L.TIPOPARCEIRO <> 'F')
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
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
  
  END FN_LANC_TIPO_OUTROS;

END PKG_BI_CONTABILIDADE;
/
