CREATE OR REPLACE PACKAGE BODY PKG_BI_CONTABILIDADE IS

  -----------------------DATA PARA ATUALIZACAO INCREMENTAL
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

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
  vOUTROSESTOQUES                NUMBER := 200159;
  ----------------------------------------------
  vTAXA_CARTAO               NUMBER := 3203;
  vJUROS_RECEBIDOS           NUMBER := 4003;
  vDESCONTOS_OBTIDOS         NUMBER := 4005;
  vDESCONTOS_CONCEDIDOS      NUMBER := 4107;
  vPREJUIZO_CLIENTE          NUMBER := 4108;
  vJUROS_PAGOS               NUMBER := 4109;
  vRECEITA_EXTRA_OPERACIONAL NUMBER := 4201;
  vCONTA_PAGTO_FRETE         NUMBER := 9005;

  ----------------GRUPO CONTA
  vGRUPO_MATERIAL_OPERACAO NUMBER := 355;
  vGRUPO_TRIBUTOS_RECOLHER NUMBER := 225;

  ----FORNECEDOR SEM CONTA CONTABIL
  vOUTRO_FORNECEDOR NUMBER := 99999;

  ----FILIAL ESPIRITO SANTO
  vCODFILIAL_ES VARCHAR2(2) := '11';

  ----FILIAL DEPOSITO SP
  vCODFILIAL_DEPOSITO_SP VARCHAR2(2) := '2';

  ----DATA MUDANCA DISTRIBUICAO FAT PARA ES
  vDT_MUDANCA_FAT_DISTRIB_ES DATE := TO_DATE('01/04/2024', 'DD/MM/YYYY');

  ----BANCOS
  vCAIXA_CARTAO_LOJA NUMBER := 12;

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

  ----CODIGO FORNECEDORES DESCONSIDERAR LANCAMENTO - CONSIDERAR CONTA
  FUNCTION FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA RETURN T_FORNEC_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODFORNEC
                FROM BI_SINC_FORNECEDOR
               WHERE CODFORNEC IN
                     (21, 162, 185, 9177, 9158, 9178, 9160, 9421, 9444, 9852, 9870, 9851, 10117, 10535, 10506))
    LOOP
      PIPE ROW(T_FORNEC_RECORD(r.CODFORNEC));
    END LOOP;
  
  END FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA;

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

  ----GRUPOS GERENCIAIS DESCONSIDERADOS DOS LANCAMENTOS
  FUNCTION FN_GRUPO_LANC_DESCONSIDERAR RETURN T_GRUPO_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODGRUPO FROM PCGRUPO WHERE CODGRUPO IN (680, 900))
    LOOP
      PIPE ROW(T_GRUPO_RECORD(r.CODGRUPO));
    END LOOP;
  
  END FN_GRUPO_LANC_DESCONSIDERAR;

  ----GRUPOS COM LANCAMENTOS TIPO FORNECEDORES - CONSIDERAR CONTA GERENCIAL
  FUNCTION FN_GRUPO_LANC_TIPO_FORNEC_CONSIDERA_CONTA RETURN T_GRUPO_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODGRUPO FROM PCGRUPO WHERE CODGRUPO IN (110, 210, 225, 230, 240, 245))
    LOOP
      PIPE ROW(T_GRUPO_RECORD(r.CODGRUPO));
    END LOOP;
  
  END FN_GRUPO_LANC_TIPO_FORNEC_CONSIDERA_CONTA;

  ----GRUPOS GERENCIAIS - RECEITA
  FUNCTION FN_GRUPO_LANC_RECEITA RETURN T_GRUPO_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODGRUPO FROM PCGRUPO WHERE CODGRUPO IN (400, 420))
    LOOP
      PIPE ROW(T_GRUPO_RECORD(r.CODGRUPO));
    END LOOP;
  
  END FN_GRUPO_LANC_RECEITA;

  ----CONTAS COM LANCAMENTOS TIPO FORNECEDORES - CONSIDERAR CONTA GERENCIAL
  FUNCTION FN_CONTA_LANC_TIPO_FORNEC_CONSIDERA_CONTA RETURN T_CONTA_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODCONTA FROM PCCONTA WHERE CODCONTA IN (3406, 3451, 3454, 3705, 3706))
    LOOP
      PIPE ROW(T_CONTA_RECORD(r.CODCONTA));
    END LOOP;
  
  END FN_CONTA_LANC_TIPO_FORNEC_CONSIDERA_CONTA;

  ----GRUPOS PARA DESCONSIDERAR - CAIXA CARTAO CORPORATIVO -LANCAMENTOS TIPO FORNECEDORES
  FUNCTION FN_GRUPO_DESCONSIDERA_CX_CARTAO_LANC_TIPO_FORNEC RETURN T_GRUPO_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODGRUPO FROM PCGRUPO WHERE CODGRUPO IN (100, 135, 900))
    LOOP
      PIPE ROW(T_GRUPO_RECORD(r.CODGRUPO));
    END LOOP;
  
  END FN_GRUPO_DESCONSIDERA_CX_CARTAO_LANC_TIPO_FORNEC;

  ----CODIGO CONTA GERENCIAL - DESCRICAO DE FATURA AO INVES DE NOTA
  FUNCTION FN_CONTA_DESCRICAO_FATURA RETURN T_CONTA_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODCONTA
                FROM PCCONTA
               WHERE CODCONTA IN (3406, 3450, 3451, 3452, 3453, 3454, 3455, 3456, 3705, 3706))
    LOOP
      PIPE ROW(T_CONTA_RECORD(r.CODCONTA));
    END LOOP;
  
  END FN_CONTA_DESCRICAO_FATURA;

  ----BANCOS DESCONSIDERADOS DOS LANCAMENTOS E DAS BAIXAS
  FUNCTION FN_BANCOS_DESCONSIDERAR RETURN T_BANCO_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODBANCO FROM PCBANCO WHERE CODBANCO IN (17, 20, 22, 54, 35, 50, 70, 71))
    LOOP
      PIPE ROW(T_BANCO_RECORD(r.CODBANCO));
    END LOOP;
  
  END FN_BANCOS_DESCONSIDERAR;

  ----BANCOS REF AO LANCAMENTO DE CARTAO CORPORATIVO
  FUNCTION FN_BANCOS_CARTAO_CORP RETURN T_BANCO_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODBANCO FROM PCBANCO WHERE CODBANCO IN (30, 41))
    LOOP
      PIPE ROW(T_BANCO_RECORD(r.CODBANCO));
    END LOOP;
  
  END FN_BANCOS_CARTAO_CORP;

  ----BANCOS REF AO LANCAMENTO DE COMISSAO MKT
  FUNCTION FN_BANCOS_COMISSAO_MKT RETURN T_BANCO_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODBANCO FROM PCBANCO WHERE CODBANCO IN (40, 72, 75))
    LOOP
      PIPE ROW(T_BANCO_RECORD(r.CODBANCO));
    END LOOP;
  
  END FN_BANCOS_COMISSAO_MKT;

  ----BANCOS CARTAO CORPORATIVO E CAIXA COMISSAO MKT
  FUNCTION FN_BANCOS_COMISSAO_MKT_CARTAO_CORP RETURN T_BANCO_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODBANCO FROM PCBANCO WHERE CODBANCO IN (30, 41, 40, 72, 75))
    LOOP
      PIPE ROW(T_BANCO_RECORD(r.CODBANCO));
    END LOOP;
  
  END FN_BANCOS_COMISSAO_MKT_CARTAO_CORP;

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

  -----------------------------------------------------------------------------------

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
                FROM VIEW_BI_SINC_MOV_PROD_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = M.CODFILIAL
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.DATA >= vDATA_MOV_INCREMENTAL
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
                FROM VIEW_BI_SINC_MOV_PROD_AGG M
               WHERE 1 = 1
                 AND M.DATA >= vDATA_MOV_INCREMENTAL
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
                FROM VIEW_BI_SINC_MOV_PROD_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.DATA >= vDATA_MOV_INCREMENTAL
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
                FROM VIEW_BI_SINC_MOV_PROD_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = M.CODFILIAL
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.DATA >= vDATA_MOV_INCREMENTAL)
    
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
                FROM VIEW_BI_SINC_MOV_PROD_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.DATA >= vDATA_MOV_INCREMENTAL)
    
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
                FROM VIEW_BI_SINC_MOV_PROD_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.DATA >= vDATA_MOV_INCREMENTAL)
    
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
                FROM VIEW_BI_SINC_MOV_PROD_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.DATA >= vDATA_MOV_INCREMENTAL
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
                FROM VIEW_BI_SINC_MOV_PROD_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.DATA >= vDATA_MOV_INCREMENTAL
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
                
                SELECT ('D01' || '.CC_' || DECODE(E.CODCC, '0', L.CODCC, E.CODCC)) CODLANC,
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
                  FROM VIEW_BI_SINC_DESPESA_CONTABIL E
                  LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                  LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = E.CODFILIAL
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = E.CODSUPERVISOR
                 WHERE 1 = 1
                   AND E.DATA >= vDATA_MOV_INCREMENTAL)
    
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
                
                SELECT DISTINCT ('D02' || '.CC_' || E.CODCC) CODLANC,
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
                  FROM VIEW_BI_SINC_DESPESA_CONTABIL E
                  LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                  LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                 WHERE 1 = 1
                   AND E.DATA >= vDATA_MOV_INCREMENTAL
                   AND (E.VLIMPOSTO > 0 OR E.CFOP IN (SELECT CODFISCAL FROM vENT_BONIFICADA)))
    
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
                
                SELECT ('D03' || '.CC_' || DECODE(E.CODCC, '0', L.CODCC, E.CODCC)) CODLANC,
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
                  FROM VIEW_BI_SINC_DESPESA_CONTABIL E
                  LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                  LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = E.CODFILIAL
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = E.CODSUPERVISOR
                 WHERE 1 = 1
                   AND E.DATA >= vDATA_MOV_INCREMENTAL
                   AND E.VLICMS > 0
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
    FOR r IN (SELECT ('D04' || '.CC_' || DECODE(E.CODCC, '0', L.CODCC, E.CODCC)) CODLANC,
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
                FROM VIEW_BI_SINC_DESPESA_CONTABIL E
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = E.CODFILIAL
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = E.CODSUPERVISOR
               WHERE 1 = 1
                 AND E.DATA >= vDATA_MOV_INCREMENTAL
                 AND E.VLPIS > 0
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
    FOR r IN (SELECT ('D05' || '.CC_' || DECODE(E.CODCC, '0', L.CODCC, E.CODCC)) CODLANC,
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
                FROM VIEW_BI_SINC_DESPESA_CONTABIL E
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = E.CODFILIAL
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = E.CODSUPERVISOR
               WHERE 1 = 1
                 AND E.DATA >= vDATA_MOV_INCREMENTAL
                 AND E.VLCOFINS > 0
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
    FOR r IN (SELECT ('D06' || '.CC_' || DECODE(E.CODCC, '0', L.CODCC, E.CODCC)) CODLANC,
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
                FROM VIEW_BI_SINC_DESPESA_CONTABIL E
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = E.CODCONTA
                LEFT JOIN PCCONTA T ON T.CODCONTA = E.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = E.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = E.CODFILIAL
               WHERE 1 = 1
                 AND E.DATA >= vDATA_MOV_INCREMENTAL
                 AND E.VLDIFAL > 0)
    
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
    FOR r IN (SELECT ('G01' || '.CC_' || L.CODCC) CODLANC,
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
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RAT: ' ||
                     REPLACE(TO_CHAR(L.PERCRATEIO, '999.00'), '.', ',') || '% - RECNUM: ' || L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('FATURA Nº ' || L.NUMNOTA || ' - ' || F.CNPJ || ' - ' || F.FORNECEDOR || ' - Cód: ' || L.CODFORNEC) HISTORICO,
                     
                     ROUND(L.VLRATEIO, 2) VALOR,
                     
                     ('DESP_GERENCIAL_FORNEC') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN VIEW_BI_SINC_DESPESA_CONTABIL E ON E.RECNUM = L.RECNUM
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC P ON P.CODGERENCIAL = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
               WHERE 1 = 1
                 AND L.DTCOMPETENCIA >= vDATA_MOV_INCREMENTAL
                 AND L.CODFORNEC IN (SELECT CODFORNEC FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNEC_DESP_GER()))
                 AND L.CODFLUXO = 3 --CONFIRMADOS
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
    FOR r IN (SELECT ('G02' || '.CC_' || L.CODCC) CODLANC,
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
                     C.CODCONTACONTRAPARTIDA CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     L.CODCONTA CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN C.CODCONTACONTRAPARTIDA IS NULL THEN
                        NULL
                       ELSE
                        DECODE(L.CODCC, '0', F.CODCC, NULL)
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RECNUM: ' || L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('IMPOSTO NS ' || L.NUMNOTA || ' - ' || F.CNPJ || ' - ' || F.FORNECEDOR || ' - Cód: ' ||
                     L.CODFORNEC) HISTORICO,
                     
                     ROUND(L.VLRATEIO, 2) VALOR,
                     
                     ('DESP_GERENCIAL_IMPOSTO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = L.CODFILIAL
               WHERE 1 = 1
                 AND L.DTCOMPETENCIA >= vDATA_MOV_INCREMENTAL
                 AND L.CODCONTA IN (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_IMPOSTO_DESP_GER()))
                 AND L.CODFLUXO = 3 --'CONFIRMADO'
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
    FOR r IN (SELECT ('L01' || '.CC_' || L.CODCC) CODLANC,
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
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RAT: ' ||
                     REPLACE(TO_CHAR(L.PERCRATEIO, '999.00'), '.', ',') || ' - RECNUM: ' || L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN L.NUMNOTA > 0 THEN
                        ('DOC ' || L.NUMNOTA || ' - ' || UPPER(L.HISTORICO))
                       ELSE
                        UPPER(L.HISTORICO)
                     END) HISTORICO,
                     
                     ----------VALOR
                     (CASE
                       WHEN L.GRUPOCONTA IN (SELECT CODGRUPO FROM TABLE(PKG_BI_CONTABILIDADE.FN_GRUPO_LANC_RECEITA())) THEN
                        ROUND(L.VLRATEIO, 2) * -1
                       ELSE
                        ROUND(L.VLRATEIO, 2)
                     END) VALOR,
                     
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
                 AND L.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND L.CODCONTA <> L.CONTABANCO
                 AND L.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND L.CODBANCO NOT IN
                     (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_COMISSAO_MKT_CARTAO_CORP()))
                 AND (L.GRUPOCONTA NOT IN
                     (SELECT CODGRUPO FROM TABLE(PKG_BI_CONTABILIDADE.FN_GRUPO_LANC_DESCONSIDERAR())) OR
                     L.CODCONTA = vCONTA_PAGTO_FRETE)
                 AND L.NUMTRANS IS NOT NULL
                 AND L.DTCOMPENSACAO IS NOT NULL
                 AND L.ADIANTAMENTO = 'N'
                 AND L.CODCONTA NOT IN (vDESCONTOS_OBTIDOS, vJUROS_PAGOS)
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

  ----LANCAMENTOS PAGAMENTO - FORNECEDORES
  FUNCTION FN_LANC_TIPO_FORNECEDOR RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('L02' || '.CC_' || L.CODCC) CODLANC,
                     L.CODEMPRESA,
                     L.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     L.RECNUM IDENTIFICADOR,
                     L.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     (CASE
                       WHEN L.VALOR > 0 THEN
                        (CASE
                          WHEN (L.GRUPOCONTA IN
                               (SELECT CODGRUPO FROM TABLE(PKG_BI_CONTABILIDADE.FN_GRUPO_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                               L.CODCONTA IN
                               (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                               L.CODFORNEC IN
                               (SELECT CODFORNEC
                                   FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA()))) THEN
                           L.CODCONTA
                          ELSE
                           L.CODFORNEC
                        END)
                       ELSE
                        L.CONTABANCO
                     END) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     (CASE
                       WHEN L.VALOR < 0 THEN
                        (CASE
                          WHEN (L.GRUPOCONTA IN
                               (SELECT CODGRUPO FROM TABLE(PKG_BI_CONTABILIDADE.FN_GRUPO_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                               L.CODCONTA IN
                               (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                               L.CODFORNEC IN
                               (SELECT CODFORNEC
                                   FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA()))) THEN
                           L.CODCONTA
                          ELSE
                           L.CODFORNEC
                        END)
                       ELSE
                        L.CONTABANCO
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN L.VALOR > 0
                            AND C.CODDRE > 0
                            AND
                            (L.GRUPOCONTA IN
                            (SELECT CODGRUPO FROM TABLE(PKG_BI_CONTABILIDADE.FN_GRUPO_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                            L.CODCONTA IN
                            (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                            L.CODFORNEC IN
                            (SELECT CODFORNEC
                                FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA()))) THEN
                        DECODE(L.CODCC, '0', FL.CODCC, L.CODCC)
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN L.VALOR < 0
                            AND C.CODDRE > 0
                            AND
                            (L.GRUPOCONTA IN
                            (SELECT CODGRUPO FROM TABLE(PKG_BI_CONTABILIDADE.FN_GRUPO_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                            L.CODCONTA IN
                            (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                            L.CODFORNEC IN
                            (SELECT CODFORNEC
                                FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA()))) THEN
                        DECODE(L.CODCC, '0', FL.CODCC, L.CODCC)
                       ELSE
                        NULL
                     END) CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN C.CODDRE > 0
                            AND
                            (L.GRUPOCONTA IN
                            (SELECT CODGRUPO FROM TABLE(PKG_BI_CONTABILIDADE.FN_GRUPO_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                            L.CODCONTA IN
                            (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                            L.CODFORNEC IN
                            (SELECT CODFORNEC
                                FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA()))) THEN
                        (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                        TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RAT: ' ||
                        REPLACE(TO_CHAR(L.PERCRATEIO, '999.00'), '.', ',') || ' - CC: ' || L.CODCC || ' - RECNUM: ' ||
                        L.RECNUM)
                       ELSE
                        (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                        TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RECNUM: ' || L.RECNUM)
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN L.GRUPOCONTA IN (vGRUPO_TRIBUTOS_RECOLHER) THEN
                        ('Nº ' || L.NUMNOTA || ' - ' || UPPER(L.HISTORICO) || ' - ' || F.CNPJ || ' - ' || F.FORNECEDOR ||
                        ' - Cód: ' || L.CODFORNEC)
                       WHEN L.CODCONTA IN (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_DESCRICAO_FATURA())) THEN
                        ('FATURA ' || L.NUMNOTA || ' - ' || F.CNPJ || ' - ' || F.FORNECEDOR || ' - Cód: ' || L.CODFORNEC)
                       WHEN (L.GRUPOCONTA IN
                            (SELECT CODGRUPO FROM TABLE(PKG_BI_CONTABILIDADE.FN_GRUPO_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                            L.CODCONTA IN
                            (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) OR
                            L.CODFORNEC IN
                            (SELECT CODFORNEC
                                FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA()))) THEN
                        (UPPER(C.CONTA) || ' - ' || UPPER(L.HISTORICO))
                       ELSE
                        ('Nº ' || L.NUMNOTA || ' - ' || F.CNPJ || ' - ' || F.FORNECEDOR || ' - Cód: ' || L.CODFORNEC)
                     END) HISTORICO,
                     
                     ----------VALOR
                     (CASE
                       WHEN L.GRUPOCONTA IN (SELECT CODGRUPO FROM TABLE(PKG_BI_CONTABILIDADE.FN_GRUPO_LANC_RECEITA())) THEN
                        ROUND(L.VLRATEIO, 2) * -1
                       ELSE
                        ROUND(L.VLRATEIO, 2)
                     END) VALOR,
                     
                     ('LANC_PAG_FORNECEDOR') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) FL ON FL.CODFILIAL = L.CODFILIAL
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = L.CODCONTA
               WHERE 1 = 1
                 AND L.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND L.CODCONTA <> L.CONTABANCO
                 AND L.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND L.CODBANCO NOT IN
                     (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_COMISSAO_MKT_CARTAO_CORP()))
                 AND (L.GRUPOCONTA NOT IN
                     (SELECT CODGRUPO FROM TABLE(PKG_BI_CONTABILIDADE.FN_GRUPO_LANC_DESCONSIDERAR())) OR
                     L.CODCONTA = vCONTA_PAGTO_FRETE)
                 AND L.NUMTRANS IS NOT NULL
                 AND L.DTCOMPENSACAO IS NOT NULL
                 AND L.ADIANTAMENTO = 'N'
                 AND L.CODCONTA NOT IN (vDESCONTOS_OBTIDOS, vJUROS_PAGOS)
                 AND L.TIPOPARCEIRO = 'F')
    
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
  
  END FN_LANC_TIPO_FORNECEDOR;

  ----LANCAMENTOS PAGAMENTO - JUROS PAGOS
  FUNCTION FN_LANC_JUROS_PAGOS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('L03' || '.CC_' || L.CODCC) CODLANC,
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
                       WHEN L.VALOR < 0 THEN
                        L.CODCONTA
                       ELSE
                        L.CONTABANCO
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
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RECNUM: ' || L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN L.NUMNOTA > 0 THEN
                        ('Nº ' || L.NUMNOTA || ' - ' || UPPER(L.HISTORICO) || ' - ' || F.CNPJ || ' - ' || F.FORNECEDOR ||
                        ' - Cód: ' || L.CODFORNEC)
                       ELSE
                        UPPER(L.HISTORICO)
                     END) HISTORICO,
                     
                     ROUND(L.VLRATEIO, 2) VALOR,
                     
                     ('LANC_JUROS_PAGOS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) FL ON FL.CODFILIAL = L.CODFILIAL
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = L.CODCONTA
               WHERE 1 = 1
                 AND L.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND L.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND L.CODCONTA = vJUROS_PAGOS)
    
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
  
  END FN_LANC_JUROS_PAGOS;

  ----LANCAMENTOS PAGAMENTO - DESCONTOS OBTIDOS
  FUNCTION FN_LANC_DESCONTO_OBTIDO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('L04' || '.CC_' || L.CODCC) CODLANC,
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
                       WHEN L.VALOR < 0 THEN
                        L.CODCONTA
                       ELSE
                        L.CONTABANCO
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
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RECNUM: ' || L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN L.NUMNOTA > 0 THEN
                        ('Nº ' || L.NUMNOTA || ' - ' || UPPER(L.HISTORICO) || ' - ' || F.CNPJ || ' - ' || F.FORNECEDOR ||
                        ' - Cód: ' || L.CODFORNEC)
                       ELSE
                        UPPER(L.HISTORICO)
                     END) HISTORICO,
                     
                     (ROUND(L.VLRATEIO, 2) * -1) VALOR,
                     
                     ('LANC_DESCONTO_OBTIDO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) FL ON FL.CODFILIAL = L.CODFILIAL
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = L.CODCONTA
               WHERE 1 = 1
                 AND L.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND L.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND L.CODCONTA = vDESCONTOS_OBTIDOS)
    
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
  
  END FN_LANC_DESCONTO_OBTIDO;

  ----LANCAMENTOS PAGAMENTO - CAIXA CARTAO CORP - FORNECEDOR
  FUNCTION FN_LANC_CAIXA_CARTAO_CORP_FORNEC RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('L05' || '.CC_' || L.CODCC) CODLANC,
                     L.CODEMPRESA,
                     L.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     L.RECNUM IDENTIFICADOR,
                     L.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     (CASE
                       WHEN L.VALOR < 0 THEN
                        L.CODFORNEC
                       ELSE
                        L.CONTABANCO
                     END) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     (CASE
                       WHEN L.VALOR > 0 THEN
                        L.CODFORNEC
                       ELSE
                        L.CONTABANCO
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RECNUM: ' || L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('PAG NF ' || L.NUMNOTA || ' - ' || L.HISTORICO || ' - ' || F.CNPJ || ' - ' || F.FORNECEDOR ||
                     ' - Cód: ' || L.CODFORNEC) HISTORICO,
                     
                     ROUND(L.VLRATEIO, 2) VALOR,
                     
                     ('LANC_CX_CARTAO_FORNEC') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) FL ON FL.CODFILIAL = L.CODFILIAL
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = L.CODCONTA
               WHERE 1 = 1
                 AND L.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND L.DTCOMPENSACAO IS NOT NULL
                 AND L.CODBANCO IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_CARTAO_CORP()))
                 AND L.CODCONTA NOT IN (vDESCONTOS_OBTIDOS, vJUROS_PAGOS)
                 AND L.GRUPOCONTA NOT IN
                     (SELECT CODGRUPO FROM TABLE(PKG_BI_CONTABILIDADE.FN_GRUPO_DESCONSIDERA_CX_CARTAO_LANC_TIPO_FORNEC()))
                 AND L.CODCC = '0'
                 AND L.TIPOPARCEIRO = 'F')
    
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
  
  END FN_LANC_CAIXA_CARTAO_CORP_FORNEC;

  ----LANCAMENTOS PAGAMENTO - CAIXA CARTAO CORP - OUTROS
  FUNCTION FN_LANC_CAIXA_CARTAO_CORP_OUTROS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('L06' || '.CC_' || L.CODCC) CODLANC,
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
                       WHEN L.VALOR < 0 THEN
                        L.CODCONTA
                       ELSE
                        L.CONTABANCO
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN L.VALOR > 0 THEN
                        L.CODCC
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN L.VALOR < 0 THEN
                        L.CODCC
                       ELSE
                        NULL
                     END) CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RAT: ' ||
                     REPLACE(TO_CHAR(L.PERCRATEIO, '999.00'), '.', ',') || ' - CC: ' || L.CODCC || ' - RECNUM: ' ||
                     L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('PAG NF ' || L.NUMNOTA || ' - ' || L.HISTORICO) HISTORICO,
                     
                     ROUND(L.VLRATEIO, 2) VALOR,
                     
                     ('LANC_CX_CARTAO_OUTROS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) FL ON FL.CODFILIAL = L.CODFILIAL
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = L.CODCONTA
               WHERE 1 = 1
                 AND L.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND L.DTCOMPENSACAO IS NOT NULL
                 AND L.CODBANCO IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_CARTAO_CORP()))
                 AND L.CODCONTA NOT IN (vDESCONTOS_OBTIDOS, vJUROS_PAGOS)
                 AND L.CODCC <> '0'
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
  
  END FN_LANC_CAIXA_CARTAO_CORP_OUTROS;

  ----LANCAMENTOS RECEBIMENTO - DESDOBRAMENTO DE CLIENTES PARA CARTAO / MKT
  FUNCTION FN_RECEB_DESDOBRAMENTO_CARTAO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (WITH CONTA_POR_COBRANCA AS
                 (SELECT B.CODCOB,
                        C.CODCONTA CONTA_COBRANCA
                   FROM PCCOB B
                   JOIN PCCONTA C ON C.CODCONTAMASTER = B.CODCLICC
                  WHERE B.CARTAO = 'S')
                
                SELECT ('R01') CODLANC,
                       P.CODEMPRESA,
                       P.DTDESD DATA,
                       
                       ----------TIPO LANCAMENTO
                       3 TIPOLANCAMENTO,
                       
                       P.NUMTRANSVENDA IDENTIFICADOR,
                       P.NUMNOTA DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       C.CONTA_COBRANCA CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       P.CONTACLIENTE CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       NULL CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       NULL CODCC_CREDITO,
                       
                       ----------ATIVIDADE
                       ('DESDOBRE DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' ||
                       LPAD(P.PREST, 2, 0)) ATIVIDADE,
                       
                       ----------HISTORICO
                       ('DESDOBRE NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE || ' - Cód. ' ||
                       T.CODCLI) HISTORICO,
                       
                       ROUND(P.VALOR, 2) VALOR,
                       
                       ('RECEB_DESDOBRE_CARTAO') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'S' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM BI_SINC_LANC_RECEBER_BASE P
                  JOIN CONTA_POR_COBRANCA C ON C.CODCOB = P.CODCOBORIG
                  LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
                 WHERE 1 = 1
                   AND P.DTDESD >= vDATA_MOV_INCREMENTAL
                   AND P.DTDESD IS NOT NULL
                   AND P.NUMTRANS IS NULL
                   AND P.DTPAGAMENTO IS NOT NULL
                   AND P.CONTACLIENTE = vCLIENTES_NACIONAIS
                   AND NVL(P.VLESTORNO, 0) = 0)
    
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
  
  END FN_RECEB_DESDOBRAMENTO_CARTAO;

  ----LANCAMENTOS RECEBIMENTO - INCLUSAO DUPLICATA MOVIMENTANDO BANCO
  FUNCTION FN_RECEB_INCLUSAO_DUP_BANCO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('R02') CODLANC,
                     P.CODEMPRESA,
                     P.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     P.NUMTRANSVENDA IDENTIFICADOR,
                     P.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     P.CONTACLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     P.CONTABANCO CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('INCLUSAO DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                     ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0)) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('INCLUSAO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE || ' - Cód. ' ||
                     T.CODCLI) HISTORICO,
                     
                     ROUND(P.VLRECEBIDO, 2) VALOR,
                     
                     ('RECEB_INCLUSAO_DUP_BANCO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_RECEBER_BASE P
                LEFT JOIN PCLANC L ON L.NUMNOTA = P.NUMNOTA
                                  AND L.DUPLIC = P.PREST
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
               WHERE 1 = 1
                 AND P.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND P.DTCOMPENSACAO IS NOT NULL
                 AND P.DTINCLUSAOMANUAL IS NOT NULL
                 AND P.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND P.NUMTRANS IS NOT NULL
                 AND NVL(P.VLESTORNO, 0) = 0
                 AND L.NUMNOTA IS NULL)
    
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
  
  END FN_RECEB_INCLUSAO_DUP_BANCO;

  ----LANCAMENTOS RECEBIMENTO - PAGAMENTO DA INCLUSAO DUPLICATA MOVIMENTANDO BANCO
  FUNCTION FN_RECEB_PAG_INCLUSAO_DUP_BANCO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('R03') CODLANC,
                     P.CODEMPRESA,
                     P.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     P.NUMTRANSVENDA IDENTIFICADOR,
                     P.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     P.CONTABANCO CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     P.CONTACLIENTE CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('PAG INCLUSAO DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                     ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0)) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('PAG INCLUSAO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                     ' - Cód. ' || T.CODCLI) HISTORICO,
                     
                     ROUND(P.VLRECEBIDO, 2) VALOR,
                     
                     ('RECEB_PAG_INCLUSAO_DUP_BANCO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_RECEBER_BASE P
                LEFT JOIN PCLANC L ON L.NUMNOTA = P.NUMNOTA
                                  AND L.DUPLIC = P.PREST
                                  AND L.CODROTINABAIXA = 1206
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
               WHERE 1 = 1
                 AND P.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND P.DTCOMPENSACAO IS NOT NULL
                 AND P.DTINCLUSAOMANUAL IS NOT NULL
                 AND P.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND P.NUMTRANS IS NOT NULL
                 AND NVL(P.VLESTORNO, 0) = 0
                 AND L.NUMNOTA IS NULL)
    
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
  
  END FN_RECEB_PAG_INCLUSAO_DUP_BANCO;

  ----LANCAMENTOS RECEBIMENTO - BAIXA INCLUSAO DUPLICATA MOVIMENTANDO CONTA GERENCIAL
  FUNCTION FN_RECEB_INCLUSAO_DUP_RECEITA RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('R04') CODLANC,
                     P.CODEMPRESA,
                     P.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     P.NUMTRANSVENDA IDENTIFICADOR,
                     P.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     P.CONTABANCO CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vRECEITA_EXTRA_OPERACIONAL CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     F.CODCC CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('BAIXA INCLUSAO DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                     ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0)) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('BAIXA INCLUSAO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                     ' - Cód. ' || T.CODCLI) HISTORICO,
                     
                     ROUND(P.VLRECEBIDO, 2) VALOR,
                     
                     ('RECEB_INCLUSAO_DUP_RECEITA') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_RECEBER_BASE P
                JOIN PCLANC L ON L.NUMNOTA = P.NUMNOTA
                             AND L.DUPLIC = P.PREST
                             AND L.CODROTINABAIXA = 1206
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = P.CODFILIAL
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
               WHERE 1 = 1
                 AND P.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND P.DTCOMPENSACAO IS NOT NULL
                 AND P.DTINCLUSAOMANUAL IS NOT NULL
                 AND P.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND P.NUMTRANS IS NOT NULL
                 AND NVL(P.VLESTORNO, 0) = 0)
    
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
  
  END FN_RECEB_INCLUSAO_DUP_RECEITA;

  ----LANCAMENTOS RECEBIMENTO - BAIXA DUPLICATA COMO PERDA
  FUNCTION FN_RECEB_BAIXA_DUP_PERDA RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('R05') CODLANC,
                     P.CODEMPRESA,
                     P.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     P.NUMTRANSVENDA IDENTIFICADOR,
                     P.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vPREJUIZO_CLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     P.CONTACLIENTE CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NVL(V.CODCC, F.CODCC) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('PERDA DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                     ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0)) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('PERDA NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE || ' - Cód. ' ||
                     T.CODCLI) HISTORICO,
                     
                     ROUND(P.VLRECEBIDO, 2) VALOR,
                     
                     ('RECEB_BAIXA_DUP_PERDA') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_RECEBER_BASE P
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = P.CODFILIAL
                LEFT JOIN BI_SINC_VENDEDOR S ON S.CODUSUR = P.CODUSUR
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = S.CODSUPERVISOR
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
               WHERE 1 = 1
                 AND P.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND P.DTCOMPENSACAO IS NOT NULL
                 AND P.DTINCLUSAOMANUAL IS NULL
                 AND P.CODCOB = 'PERD'
                 AND P.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR())))
    
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
  
  END FN_RECEB_BAIXA_DUP_PERDA;

  ----LANCAMENTOS RECEBIMENTO - TAXA DE TRANSACAO DE CARTÃO LOJAS
  FUNCTION FN_RECEB_TAXA_CARTAO_LOJA RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('R06') CODLANC,
                     P.CODEMPRESA,
                     P.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     P.NUMTRANSVENDA IDENTIFICADOR,
                     P.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     (CASE
                       WHEN P.VLDESCONTO > 0 THEN
                        vTAXA_CARTAO
                       ELSE
                        P.CONTACLIENTE
                     END) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     (CASE
                       WHEN P.VLDESCONTO > 0 THEN
                        P.CONTACLIENTE
                       ELSE
                        vTAXA_CARTAO
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN P.VLDESCONTO > 0 THEN
                        NVL(V.CODCC, F.CODCC)
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN P.VLDESCONTO > 0 THEN
                        NULL
                       ELSE
                        NVL(V.CODCC, F.CODCC)
                     END) CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN P.VLDESCONTO > 0 THEN
                        ('TAXA CARTAO DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                        ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                       ELSE
                        ('ESTORNO TAXA CARTAO DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                        ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN P.VLDESCONTO > 0 THEN
                        ('TAXA CARTAO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                        ' - Cód. ' || T.CODCLI)
                       ELSE
                        ('ESTORNO TAXA CARTAO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                        ' - Cód. ' || T.CODCLI)
                     END) HISTORICO,
                     
                     ROUND(P.VLDESCONTO, 2) VALOR,
                     
                     ('RECEB_TAXA_CARTAO_LOJA') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_RECEBER_BASE P
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = P.CODFILIAL
                LEFT JOIN BI_SINC_VENDEDOR S ON S.CODUSUR = P.CODUSUR
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = S.CODSUPERVISOR
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
               WHERE 1 = 1
                 AND P.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND P.DTCOMPENSACAO IS NOT NULL
                 AND P.DTINCLUSAOMANUAL IS NULL
                 AND P.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND P.VLDESCONTO <> 0
                 AND P.CODBANCO = vCAIXA_CARTAO_LOJA)
    
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
  
  END FN_RECEB_TAXA_CARTAO_LOJA;

  ----LANCAMENTOS RECEBIMENTO - DESCONTOS CONCEDIDOS
  FUNCTION FN_RECEB_DESC_CONCEDIDOS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (WITH COBRANCAS_CARTAO_MKT AS
                 (SELECT CODCOB FROM PCCOB WHERE CODCLICC IS NOT NULL)
                
                SELECT ('R07') CODLANC,
                       P.CODEMPRESA,
                       P.DTCOMPENSACAO DATA,
                       
                       ----------TIPO LANCAMENTO
                       3 TIPOLANCAMENTO,
                       
                       P.NUMTRANSVENDA IDENTIFICADOR,
                       P.NUMNOTA DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       (CASE
                         WHEN P.VLDESCONTO > 0 THEN
                          vDESCONTOS_CONCEDIDOS
                         ELSE
                          P.CONTACLIENTE
                       END) CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       (CASE
                         WHEN P.VLDESCONTO > 0 THEN
                          P.CONTACLIENTE
                         ELSE
                          vDESCONTOS_CONCEDIDOS
                       END) CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       (CASE
                         WHEN P.VLDESCONTO > 0 THEN
                          NVL(V.CODCC, F.CODCC)
                         ELSE
                          NULL
                       END) CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       (CASE
                         WHEN P.VLDESCONTO > 0 THEN
                          NULL
                         ELSE
                          NVL(V.CODCC, F.CODCC)
                       END) CODCC_CREDITO,
                       
                       ----------ATIVIDADE
                       (CASE
                         WHEN P.VLDESCONTO > 0 THEN
                          ('DESCONTO DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                          ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                         ELSE
                          ('ESTORNO DESCONTO DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                          ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                       END) ATIVIDADE,
                       
                       ----------HISTORICO
                       (CASE
                         WHEN P.VLDESCONTO > 0 THEN
                          ('DESCONTO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                          ' - Cód. ' || T.CODCLI)
                         ELSE
                          ('ESTORNO DESCONTO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                          ' - Cód. ' || T.CODCLI)
                       END) HISTORICO,
                       
                       ROUND(P.VLDESCONTO, 2) VALOR,
                       
                       ('RECEB_DESC_CONCEDIDO') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'S' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM BI_SINC_LANC_RECEBER_BASE P
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = P.CODFILIAL
                  LEFT JOIN BI_SINC_VENDEDOR S ON S.CODUSUR = P.CODUSUR
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = S.CODSUPERVISOR
                  LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
                  LEFT JOIN COBRANCAS_CARTAO_MKT C ON C.CODCOB = P.CODCOB
                 WHERE 1 = 1
                   AND P.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                   AND P.DTCOMPENSACAO IS NOT NULL
                   AND P.DTINCLUSAOMANUAL IS NULL
                   AND P.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                   AND P.VLDESCONTO <> 0
                   AND P.CODCOB NOT IN ('CARC', 'CADB', 'JUR')
                   AND C.CODCOB IS NULL)
    
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
  
  END FN_RECEB_DESC_CONCEDIDOS;

  ----LANCAMENTOS RECEBIMENTO - JUROS RECEBIDOS
  FUNCTION FN_RECEB_JUROS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (
              
              SELECT ('R08') CODLANC,
                      P.CODEMPRESA,
                      P.DTCOMPENSACAO DATA,
                      
                      ----------TIPO LANCAMENTO
                      3 TIPOLANCAMENTO,
                      
                      P.NUMTRANSVENDA IDENTIFICADOR,
                      P.NUMNOTA DOCUMENTO,
                      
                      ----------CONTA_DEBITO
                      (CASE
                        WHEN P.VLJUROS > 0 THEN
                         P.CONTABANCO
                        ELSE
                         vJUROS_RECEBIDOS
                      END) CONTADEBITO,
                      
                      ----------CONTA_CREDITO
                      (CASE
                        WHEN P.VLJUROS > 0 THEN
                         vJUROS_RECEBIDOS
                        ELSE
                         P.CONTABANCO
                      END) CONTACREDITO,
                      
                      ----------CODCC_DEBITO
                      (CASE
                        WHEN P.VLJUROS > 0 THEN
                         NULL
                        ELSE
                         NVL(V.CODCC, F.CODCC)
                      END) CODCC_DEBITO,
                      
                      ----------CODCC_CREDITO
                      (CASE
                        WHEN P.VLJUROS > 0 THEN
                         NVL(V.CODCC, F.CODCC)
                        ELSE
                         NULL
                      END) CODCC_CREDITO,
                      
                      ----------ATIVIDADE
                      (CASE
                        WHEN P.VLDESCONTO > 0 THEN
                         ('JUROS DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                         ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                        ELSE
                         ('ESTORNO JUROS DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                         ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                      END) ATIVIDADE,
                      
                      ----------HISTORICO
                      (CASE
                        WHEN P.VLDESCONTO > 0 THEN
                         ('JUROS NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE || ' - Cód. ' ||
                         T.CODCLI)
                        ELSE
                         ('ESTORNO JUROS NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                         ' - Cód. ' || T.CODCLI)
                      END) HISTORICO,
                      
                      ROUND(P.VLDESCONTO, 2) VALOR,
                      
                      ('RECEB_JUROS') ORIGEM,
                      
                      ----------ENVIAR_CONTABIL
                      'S' ENVIAR_CONTABIL,
                      
                      TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_RECEBER_BASE P
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = P.CODFILIAL
                LEFT JOIN BI_SINC_VENDEDOR S ON S.CODUSUR = P.CODUSUR
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = S.CODSUPERVISOR
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
               WHERE 1 = 1
                 AND P.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND P.DTCOMPENSACAO IS NOT NULL
                 AND P.DTINCLUSAOMANUAL IS NULL
                 AND P.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND NVL(P.VLJUROS, 0) <> 0
                 AND P.CODCOB NOT IN ('JUR'))
    
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
  
  END FN_RECEB_JUROS;

  ----LANCAMENTOS RECEBIMENTO - BAIXA DAS DUPLICATAS
  FUNCTION FN_RECEB_BAIXA_DUPLICATAS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (WITH ANALISE_ESTORNO AS --QUANDO TEMOS 2 REGISTROS O ESTORNO É DE CODCOB = 'PERD'
                 (SELECT NUMTRANS,
                        COUNT(NUMTRANS) REGISTROS
                   FROM PCMOVCR M
                  WHERE M.CODCOB = 'D'
                    AND (M.DTESTORNO IS NULL OR (M.DTESTORNO IS NOT NULL AND M.ESTORNO = 'N'))
                  GROUP BY M.NUMTRANS,
                           M.CODBANCO
                 HAVING COUNT(NUMTRANS) > 0)
                
                SELECT ('R09') CODLANC,
                       P.CODEMPRESA,
                       P.DTCOMPENSACAO DATA,
                       
                       ----------TIPO LANCAMENTO
                       3 TIPOLANCAMENTO,
                       
                       P.NUMTRANSVENDA IDENTIFICADOR,
                       P.NUMNOTA DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       (CASE
                         WHEN ((P.CODCOB = 'ESTR' AND P.CODCOBORIG = 'JUR') OR (P.CODCOB = 'JUR')) THEN
                          vJUROS_RECEBIDOS --ESTORNO JUROS
                         WHEN (P.CODCOB = 'ESTR' AND E.REGISTROS > 0) THEN
                          P.CONTACLIENTE --ESTORNO PERDAS E ESTORNOS GERAIS
                         ELSE
                          P.CONTABANCO
                       END) CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       (CASE
                         WHEN (P.CODCOB = 'ESTR' AND E.REGISTROS = 2) THEN
                          vPREJUIZO_CLIENTE --ESTORNO PERDAS
                         WHEN (P.CODCOB = 'ESTR') THEN
                          P.CONTABANCO --ESTORNO JUROS E ESTORNOS GERAIS
                         ELSE
                          P.CONTACLIENTE
                       END) CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       (CASE
                         WHEN ((P.CODCOB = 'ESTR' AND P.CODCOBORIG = 'JUR') OR (P.CODCOB = 'JUR')) THEN
                          NVL(V.CODCC, F.CODCC)
                         ELSE
                          NULL
                       END) CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       (CASE
                         WHEN (P.CODCOB = 'ESTR' AND E.REGISTROS = 2) THEN
                          NVL(V.CODCC, F.CODCC)
                         ELSE
                          NULL
                       END) CODCC_CREDITO,
                       
                       ----------ATIVIDADE
                       (CASE
                         WHEN (P.CODCOB = 'ESTR' AND E.REGISTROS = 2) THEN
                          ('ESTORNO PERDA DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                          ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                         WHEN (P.CODCOB = 'ESTR' AND P.CODCOBORIG = 'JUR') THEN
                          ('ESTORNO JUR DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                          ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                         WHEN (P.CODCOB = 'ESTR') THEN
                          ('ESTORNO BAIXA DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                          ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                         WHEN (P.CODCOB = 'JUR') THEN
                          ('JUROS RECEBIDO DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                          ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                         ELSE
                          ('BAIXA DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                          ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                       END) ATIVIDADE,
                       
                       ----------HISTORICO
                       (CASE
                         WHEN (P.CODCOB = 'ESTR' AND E.REGISTROS = 2) THEN
                          ('ESTORNO PERDA NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                          ' - Cód. ' || T.CODCLI)
                         WHEN (P.CODCOB = 'ESTR' AND P.CODCOBORIG = 'JUR') THEN
                          ('ESTORNO JUR NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                          ' - Cód. ' || T.CODCLI)
                         WHEN (P.CODCOB = 'ESTR') THEN
                          ('ESTORNO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                          ' - Cód. ' || T.CODCLI)
                         WHEN (P.CODCOB = 'JUR') THEN
                          ('JUROS RECEBIDO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                          ' - Cód. ' || T.CODCLI)
                         ELSE
                          ('NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE || ' - Cód. ' ||
                          T.CODCLI)
                       END) HISTORICO,
                       
                       ----------VALOR
                       (CASE
                         WHEN ((P.CODCOB = 'ESTR' AND P.CODCOBORIG = 'JUR') OR (P.CODCOB = 'JUR')) THEN
                          ROUND(P.VLRECEBIDO, 2)
                         WHEN (P.VLRECEBIDO > P.VALOR) THEN
                          ROUND(P.VALORLIQ, 2)
                         ELSE
                          ROUND(P.VLRECEBIDO, 2)
                       END) VALOR,
                       
                       ('RECEB_BAIXA_DUPLICATAS') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'S' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM BI_SINC_LANC_RECEBER_BASE P
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = P.CODFILIAL
                  LEFT JOIN BI_SINC_VENDEDOR S ON S.CODUSUR = P.CODUSUR
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = S.CODSUPERVISOR
                  LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
                  LEFT JOIN ANALISE_ESTORNO E ON E.NUMTRANS = P.NUMTRANS
                 WHERE 1 = 1
                   AND P.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                   AND P.DTCOMPENSACAO IS NOT NULL
                   AND P.DTINCLUSAOMANUAL IS NULL
                   AND P.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                   AND NVL(P.VLRECEBIDO, 0) <> 0)
    
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
  
  END FN_RECEB_BAIXA_DUPLICATAS;

END PKG_BI_CONTABILIDADE;
/
