CREATE OR REPLACE PACKAGE BODY PKG_BI_CONTABILIDADE IS

  -----------------------DATA PARA ATUALIZACAO INCREMENTAL
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

  -----------------------BENEFICIO FISCAL
  vDT_INICIO_BENEFICIO_ES DATE := TO_DATE('01/09/2023', 'DD/MM/YYYY');

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
  vESTOQUE                       NUMBER := 1174;
  vCMV                           NUMBER := 3110;
  vICMS_VENDA                    NUMBER := 3104;
  vPIS_VENDA                     NUMBER := 3105;
  vCOFINS_VENDA                  NUMBER := 3106;
  vST_VENDA                      NUMBER := 3107;
  vICMS_RECOLHER                 NUMBER := 2251;
  vICMS_RECOLHER_ES              NUMBER := 2261;
  vPIS_RECOLHER                  NUMBER := 2254;
  vCOFINS_RECOLHER               NUMBER := 2255;
  vIRRF_RECOLHER                 NUMBER := 2256;
  vCSRF_RECOLHER                 NUMBER := 2257;
  vST_RECOLHER                   NUMBER := 2260;
  vDIFAL_RECOLHER                NUMBER := 2252;
  vESTOQUE_RECEBIDO_CONSIGNADO   NUMBER := 1176;
  vREMESSA_MERCADORIA_CONSIGNADO NUMBER := 2200;
  vDEVOLUCAO_RECEBER             NUMBER := 1178;
  vICMS_TRANSFERENCIA            NUMBER := 1187;
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
  vSUBVENCAO_FISCAL_ES           NUMBER := 3109;
  vICMS_BENEFICIO_COMPETE        NUMBER := 3206;
  vAQUISICAOIMOBILIZADO          NUMBER := 1451;
  vFRETE                         NUMBER := 3202;
  vDIFAL_MATERIAL_OPERACAO       NUMBER := 3555;
  vDIFAL_EQUIPAMENTO             NUMBER := 3653;
  vOUTROSESTOQUES                NUMBER := 200159;
  vOUTROS_MOVESTOQUES            NUMBER := 100023;
  ----------------------------------------------

  vADIANTAMENTO_FORNECEDOR   NUMBER := 1177;
  vCOMPRAS_CARTAO_CREDITO    NUMBER := 2100;
  vADIANTAMENTO_CLIENTE      NUMBER := 2202;
  vTAXA_CARTAO               NUMBER := 3203;
  vJUROS_RECEBIDOS           NUMBER := 4003;
  vCREDITOS_NAO_UTILIZADOS   NUMBER := 4004;
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

  ----FORNECEDOR
  vFORNEC_ESTRELA NUMBER := 9720;

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
                                   7534,
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
               WHERE CODFORNEC IN (21,
                                   162,
                                   185,
                                   264,
                                   7331,
                                   7793,
                                   8958,
                                   8959,
                                   9177,
                                   9158,
                                   9178,
                                   9160,
                                   9346,
                                   9421,
                                   9444,
                                   9737,
                                   9772,
                                   9776,
                                   9811,
                                   9819,
                                   9851,
                                   9852,
                                   9870,
                                   10070,
                                   10073,
                                   10117,
                                   10156,
                                   10271,
                                   10353,
                                   10506,
                                   10534,
                                   10535,
                                   10580))
    LOOP
      PIPE ROW(T_FORNEC_RECORD(r.CODFORNEC));
    END LOOP;
  
  END FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA;

  ----CODIGO FORNECEDORES ESTRELA DAS VERBAS QUE DEVEMOS CONSIDERAR O PRINCIPAL
  FUNCTION FN_FORNECEDOR_ESTRELA_VERBA RETURN T_FORNEC_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODFORNEC FROM BI_SINC_FORNECEDOR WHERE CODFORNEC IN (272))
    LOOP
      PIPE ROW(T_FORNEC_RECORD(r.CODFORNEC));
    END LOOP;
  
  END FN_FORNECEDOR_ESTRELA_VERBA;

  ----CODIGO CONTA GERENCIAL - DESPESAS IMPOSTOS A RECOLHER 
  FUNCTION FN_CONTA_IMPOSTO_DESP_GER RETURN T_CONTA_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODGERENCIAL CODCONTA
                FROM BI_SINC_PLANO_CONTAS_JC
               WHERE CODGERENCIAL IN (2253, 2256, 2257, 2258, 2262, 2266, 2267))
    LOOP
      PIPE ROW(T_CONTA_RECORD(r.CODCONTA));
    END LOOP;
  
  END FN_CONTA_IMPOSTO_DESP_GER;

  ----CODIGO CONTA GERENCIAL - DESPESAS IMPOSTOS A RECOLHER 
  FUNCTION FN_CONTA_IMPOSTO_RECOLHER_RESULTADO RETURN T_CONTA_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT CODGERENCIAL CODCONTA FROM BI_SINC_PLANO_CONTAS_JC WHERE CODGERENCIAL IN (2262, 2266, 2267))
    LOOP
      PIPE ROW(T_CONTA_RECORD(r.CODCONTA));
    END LOOP;
  
  END FN_CONTA_IMPOSTO_RECOLHER_RESULTADO;

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
    FOR r IN (SELECT CODGRUPO FROM PCGRUPO WHERE CODGRUPO IN (110, 210, 225, 230, 240, 245, 260))
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
    FOR r IN (SELECT CODCONTA FROM PCCONTA WHERE CODCONTA IN (3406, 3451, 3454, 3705, 3706, 3903))
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

  ----CENTRO DE CUSTO POR GERENTE ES
  FUNCTION FN_CC_GERENTE_ES RETURN T_CC_GERENTE_TABLE
    PIPELINED IS
  BEGIN
    FOR r IN (SELECT DISTINCT CODGERENTE,
                              (CASE
                                WHEN CODGERENTE IN (1, 8, 9, 10) THEN
                                 vCC_DISTRIBUICAO_ES
                                WHEN CODGERENTE IN (2) THEN
                                 vCC_CORPORATIVO_SP
                                WHEN CODGERENTE IN (4) THEN
                                 vCC_ECOMMERCE_SP
                                ELSE
                                 NULL
                              END) CODCC
                FROM BI_SINC_VENDEDOR V)
    LOOP
      PIPE ROW(T_CC_GERENTE_RECORD(r.CODGERENTE, r.CODCC));
    END LOOP;
  END FN_CC_GERENTE_ES;

  -----------------------------------------------------------------------------------

  ----MOVIMENTACAO PRODUTOS - VALOR CONTABIL INTEIRO
  FUNCTION FN_MOV_PROD_VLCONTABIL_INTEIRO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA BONIFICADA') THEN
                        'P01_B'
                       ELSE
                        'P01'
                     END) CODLANC,
                     M.CODEMPRESA,
                     M.CODFILIAL,
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
                       WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO') THEN
                        1
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                          'ENTRADA COMPRA CONSIGNADO',
                                          'ENTRADA COMPRA TRIANGULAR',
                                          'ENTRADA BONIFICADA',
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
                       WHEN M.TIPOMOV IN ('ENTRADA TRANSFERENCIA') THEN
                        vESTOQUE
                       WHEN M.TIPOMOV IN ('ENTRADA CONSIGNADO') THEN
                        vESTOQUE_RECEBIDO_CONSIGNADO
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        vDEVOLUCAO_PRODUTO
                       WHEN M.TIPOMOV IN ('ENTRADA CONSERTO') THEN
                        vRETORNO_CONSERTO
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
                       WHEN M.TIPOMOV IN ('SAIDA PERDA MERCADORIA', 'SAIDA TRANSFERENCIA') THEN
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
                       WHEN M.TIPOMOV IN ('ENTRADA CONSERTO') THEN
                        vREMESSA_CONSERTO
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
                          WHEN M.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                               AND M.CODGERENTE IN (1, 8, 9, 10) THEN
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
                          WHEN M.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                               AND M.CODGERENTE IN (1, 8, 9, 10) THEN
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
                       WHEN (M.MOVIMENTO = 'S' OR M.TIPOMOV IN ('ENTRADA DEVOLUCAO')) THEN
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
                 AND M.TIPOMOV NOT IN
                     ('ENTRADA REM CONTA E ORDEM', 'SAIDA BONIFICADA', 'SAIDA DESCONSIDERAR', 'SAIDA REM CONTA E ORDEM'))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
    FOR r IN (SELECT (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA BONIFICADA') THEN
                        'P02_B'
                       ELSE
                        'P02'
                     END) CODLANC,
                     M.CODEMPRESA,
                     M.CODFILIAL,
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
                 AND NOT (M.TIPOMOV IN ('ENTRADA SIMPLES REMESSA') AND
                      (M.TEMVENDAORIG = 'N' AND
                      M.CODFORNEC NOT IN (SELECT CODFORNEC FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNEC_JCBROTHERS()))))
                 AND M.TIPOMOV IN ('SAIDA DEVOLUCAO',
                                   'ENTRADA COMPRA',
                                   'ENTRADA COMPRA CONSIGNADO',
                                   'ENTRADA COMPRA TRIANGULAR',
                                   'ENTRADA BONIFICADA',
                                   'ENTRADA SIMPLES REMESSA',
                                   'ENTRADA REM ENTREGA FUTURA'))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
                     M.CODFILIAL,
                     M.DATA,
                     ---------TIPOLANCAMENTO
                     3 TIPOLANCAMENTO,
                     M.NUMTRANSACAO IDENTIFICADOR,
                     M.NUMNOTA DOCUMENTO,
                     
                     ----------CONTADEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA BONIFICADA') THEN
                        vVENDA_BONIFICADA
                       WHEN M.TIPOMOV IN ('SAIDA CONSERTO') THEN
                        vESTOQUE_TRANSITO
                       WHEN M.TIPOMOV IN ('ENTRADA CONSERTO') THEN
                        vESTOQUE
                       ELSE
                        NULL
                     END) CONTADEBITO,
                     
                     ----------CONTACREDITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA BONIFICADA', 'SAIDA CONSERTO') THEN
                        vESTOQUE
                       WHEN M.TIPOMOV IN ('ENTRADA CONSERTO') THEN
                        vESTOQUE_TRANSITO
                       ELSE
                        NULL
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA BONIFICADA') THEN
                        (CASE
                          WHEN M.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                               AND M.CODGERENTE IN (1, 8, 9, 10) THEN
                           vCC_DISTRIBUICAO_ES
                          ELSE
                           V.CODCC
                        END)
                       ELSE
                        NULL
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
                     
                     ----------VALOR
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA CONSERTO', 'ENTRADA CONSERTO') THEN
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
                 AND M.TIPOMOV IN ('SAIDA BONIFICADA', 'SAIDA CONSERTO', 'ENTRADA CONSERTO'))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
    FOR r IN (SELECT (CASE
                       WHEN M.TIPOMOV IN ('ENTRADA BONIFICADA') THEN
                        'P04_B'
                       ELSE
                        'P04'
                     END) CODLANC,
                     M.CODEMPRESA,
                     M.CODFILIAL,
                     M.DATA,
                     ---------TIPOLANCAMENTO
                     (CASE
                       WHEN M.VALORCONTABIL = 0 THEN
                        3
                       WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                          'ENTRADA COMPRA CONSIGNADO',
                                          'ENTRADA COMPRA TRIANGULAR',
                                          'ENTRADA BONIFICADA',
                                          'ENTRADA SIMPLES REMESSA',
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
                                          'ENTRADA CONSIGNADO',
                                          'ENTRADA DEVOLUCAO',
                                          'ENTRADA DEMONSTRACAO') THEN
                        (CASE
                          WHEN M.CODFILIAL = vCODFILIAL_ES THEN
                           vICMS_RECUPERAR_ES
                          ELSE
                           vICMS_RECUPERAR
                        END)
                       WHEN M.TIPOMOV IN ('SAIDA TRANSFERENCIA') THEN
                        vICMS_TRANSFERENCIA
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
                                          'SAIDA DEVOLUCAO CONSIGNADO',
                                          'SAIDA DEVOLUCAO') THEN
                        (CASE
                          WHEN M.CODFILIAL = vCODFILIAL_ES THEN
                           vICMS_RECOLHER_ES
                          ELSE
                           vICMS_RECOLHER
                        END)
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO', 'ENTRADA DEMONSTRACAO') THEN
                        vICMS_VENDA
                       WHEN (M.TIPOMOV IN ('ENTRADA CONSIGNADO') OR
                            (M.TIPOMOV IN ('ENTRADA COMPRA') AND M.VALORCONTABIL = 0)) THEN
                        vESTOQUE
                       WHEN M.TIPOMOV IN ('ENTRADA TRANSFERENCIA') THEN
                        vICMS_TRANSFERENCIA
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
                          WHEN M.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                               AND M.CODGERENTE IN (1, 8, 9, 10) THEN
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
                          WHEN M.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                               AND M.CODGERENTE IN (1, 8, 9, 10) THEN
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
                     
                     M.VALORICMS VALOR,
                     ('MOVPROD_VL_ICMS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     M.DTCANCEL
                FROM VIEW_BI_SINC_MOV_PROD_AGG M
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = M.CODFILIAL
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = M.CODSUPERVISOR
               WHERE 1 = 1
                 AND M.DATA >= vDATA_MOV_INCREMENTAL
                 AND M.VALORICMS > 0
                 AND M.TIPOMOV NOT IN ('ENTRADA REM CONTA E ORDEM', 'SAIDA DESCONSIDERAR', 'SAIDA PERDA MERCADORIA'))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
                     M.CODFILIAL,
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
                                          'ENTRADA REM ENTREGA FUTURA',
                                          'ENTRADA DEVOLUCAO') THEN
                        vPIS_RECUPERAR
                       ELSE
                        NULL
                     END) CONTADEBITO,
                     
                     ----------CONTACREDITO
                     (CASE
                       WHEN M.TIPOMOV IN
                            ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA', 'SAIDA DEVOLUCAO') THEN
                        vPIS_RECOLHER
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        vPIS_VENDA
                       ELSE
                        NULL
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA') THEN
                        (CASE
                          WHEN M.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                               AND M.CODGERENTE IN (1, 8, 9, 10) THEN
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
                          WHEN M.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                               AND M.CODGERENTE IN (1, 8, 9, 10) THEN
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
                 AND M.DATA >= vDATA_MOV_INCREMENTAL
                 AND M.TIPOMOV NOT IN ('ENTRADA REM CONTA E ORDEM', 'SAIDA DESCONSIDERAR', 'SAIDA PERDA MERCADORIA'))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
                     M.CODFILIAL,
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
                                          'ENTRADA REM ENTREGA FUTURA',
                                          'ENTRADA DEVOLUCAO') THEN
                        vCOFINS_RECUPERAR
                       ELSE
                        NULL
                     END) CONTADEBITO,
                     
                     ----------CONTACREDITO
                     (CASE
                       WHEN M.TIPOMOV IN
                            ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA', 'SAIDA DEVOLUCAO') THEN
                        vCOFINS_RECOLHER
                       WHEN M.TIPOMOV IN ('ENTRADA DEVOLUCAO') THEN
                        vCOFINS_VENDA
                       ELSE
                        NULL
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN M.TIPOMOV IN ('SAIDA VENDA', 'SAIDA FAT CONTA E ORDEM', 'SAIDA REM ENTREGA FUTURA') THEN
                        (CASE
                          WHEN M.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                               AND M.CODGERENTE IN (1, 8, 9, 10) THEN
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
                          WHEN M.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                               AND M.CODGERENTE IN (1, 8, 9, 10) THEN
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
                 AND M.DATA >= vDATA_MOV_INCREMENTAL
                 AND M.TIPOMOV NOT IN ('ENTRADA REM CONTA E ORDEM', 'SAIDA DESCONSIDERAR', 'SAIDA PERDA MERCADORIA'))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
                     M.CODFILIAL,
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
                       WHEN M.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                            AND M.CODGERENTE IN (1, 8, 9, 10) THEN
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
                                 r.CODFILIAL,
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
                     M.CODFILIAL,
                     M.DATA,
                     3 TIPOLANCAMENTO,
                     M.NUMTRANSACAO IDENTIFICADOR,
                     M.NUMNOTA DOCUMENTO,
                     vICMS_VENDA CONTADEBITO,
                     vDIFAL_RECOLHER CONTACREDITO,
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN M.DATA >= vDT_MUDANCA_FAT_DISTRIB_ES
                            AND M.CODGERENTE IN (1, 8, 9, 10) THEN
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
                                 r.CODFILIAL,
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
                 (SELECT CODFISCAL FROM PCCFO F WHERE F.CODFISCAL IN (1116, 1117, 2116, 2117)),
                vENT_REMESSA AS
                 (SELECT CODFISCAL FROM PCCFO F WHERE F.CODFISCAL IN (1908, 1909, 1949, 2949, 1923, 2923))
                
                SELECT ('D01' || '.CC_' || DECODE(E.CODCC, '0', L.CODCC, E.CODCC)) CODLANC,
                       E.CODEMPRESA,
                       E.CODFILIAL,
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
                         WHEN E.CODCONTA IN (vOUTROSESTOQUES, vOUTROS_MOVESTOQUES) THEN
                          NULL
                         ELSE
                          E.CODCONTA
                       END) CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       (CASE
                         WHEN E.CODCONTA IN (vOUTROSESTOQUES, vOUTROS_MOVESTOQUES)
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
                         WHEN E.CODCONTA IN (vOUTROSESTOQUES, vOUTROS_MOVESTOQUES) THEN
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
                         WHEN E.CODCONTA IN (vOUTROSESTOQUES, vOUTROS_MOVESTOQUES)
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
                         WHEN E.CODCONTA IN (vOUTROSESTOQUES, vOUTROS_MOVESTOQUES)
                              AND E.CFOP IN (SELECT CODFISCAL FROM vENT_BONIFICADA) THEN
                          ('REMESSA BONIFICADA' || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - Nº TRANSENT: ' ||
                          E.NUMTRANSENT)
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
                   AND E.DATA >= vDATA_MOV_INCREMENTAL
                   AND NOT (E.CFOP IN (SELECT CODFISCAL FROM vENT_REMESSA) AND E.ESPECIE = 'NF'))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
                
                SELECT DISTINCT ('D02') CODLANC,
                                E.CODEMPRESA,
                                E.CODFILIAL,
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
                                 r.CODFILIAL,
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
                       E.CODFILIAL,
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
                         WHEN (E.CODCONTA IN (vOUTROSESTOQUES) AND E.CFOP IN (SELECT CODFISCAL FROM vENT_SIMPLES_REMESSA)) THEN
                          vESTOQUE
                         WHEN (E.CODCONTA IN (vOUTROSESTOQUES, vOUTROS_MOVESTOQUES) AND
                              E.CFOP IN (SELECT CODFISCAL FROM vENT_BONIFICADA)) THEN
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
                         WHEN (E.CODCC = '0' OR E.CODCONTA IN (vOUTROSESTOQUES, vOUTROS_MOVESTOQUES)) THEN
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
                         WHEN E.CODCONTA IN (vOUTROSESTOQUES, vOUTROS_MOVESTOQUES)
                              AND E.CFOP IN (SELECT CODFISCAL FROM vENT_BONIFICADA) THEN
                          ('ICMS REMESSA BONIFICADA' || ' - F' || LPAD(E.CODFILIAL, 2, 0) || ' - Nº TRANSENT: ' ||
                          E.NUMTRANSENT)
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
                                 r.CODFILIAL,
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
                     E.CODFILIAL,
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
                                 r.CODFILIAL,
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
                     E.CODFILIAL,
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
                                 r.CODFILIAL,
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
                     E.CODFILIAL,
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
                                 r.CODFILIAL,
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
                     L.CODFILIAL,
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
                     
                     ROUND(ABS(L.VLRATEIO), 2) VALOR,
                     
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
                                 r.CODFILIAL,
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
    FOR r IN (WITH DESPESA AS
                 (SELECT E.RECNUM,
                        E.DATA
                   FROM VIEW_BI_SINC_DESPESA_CONTABIL E
                  GROUP BY RECNUM,
                           DATA),
                
                DATA_DESPESA_LANC AS
                 (SELECT E.DATA,
                        L.RECNUM
                   FROM BI_SINC_LANC_PAGAR_BASE L
                   LEFT JOIN DESPESA E ON E.RECNUM = L.RECNUMPRINC
                  WHERE L.CODCONTA IN (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_IMPOSTO_DESP_GER()))),
                
                DATA_DESPESA_LIVRO AS
                 (SELECT E.DATA,
                        L.RECNUM
                   FROM BI_SINC_LANC_PAGAR_BASE L
                   LEFT JOIN PCLANC B ON B.RECNUM = L.RECNUM
                   LEFT JOIN VIEW_BI_SINC_DESPESA_CONTABIL E ON B.NUMTRANSENT = E.NUMTRANSENT
                  WHERE L.CODCONTA IN (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_IMPOSTO_DESP_GER())))
                
                SELECT ('G02' || '.CC_' || L.CODCC) CODLANC,
                       L.CODEMPRESA,
                       L.CODFILIAL,
                       ----------DATA
                       (CASE
                         WHEN (L.CODCONTA = vCSRF_RECOLHER OR (NVL(L.RECNUMPRINC, 0) = 0 OR L.RECNUMPRINC = L.RECNUM)) THEN
                          L.DTCOMPETENCIA
                         ELSE
                          COALESCE(EL.DATA, E.DATA, L.DTCOMPETENCIA)
                       END) DATA,
                       
                       ----------TIPO LANCAMENTO
                       (CASE
                         WHEN (L.CODCONTA = vCSRF_RECOLHER OR (NVL(L.RECNUMPRINC, 0) = 0 OR L.RECNUMPRINC = L.RECNUM)) THEN
                          3
                         WHEN C.CODCONTACONTRAPARTIDA IS NULL THEN
                          2
                         ELSE
                          3
                       END) TIPOLANCAMENTO,
                       
                       L.RECNUM IDENTIFICADOR,
                       L.NUMNOTA DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       (CASE
                         WHEN L.CODCONTA IN
                              (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_IMPOSTO_RECOLHER_RESULTADO())) THEN
                          C.CODCONTACONTRAPARTIDA
                         WHEN (L.CODCONTA = vCSRF_RECOLHER OR (NVL(L.RECNUMPRINC, 0) = 0 OR L.RECNUMPRINC = L.RECNUM)) THEN
                          L.CODFORNEC
                         ELSE
                          C.CODCONTACONTRAPARTIDA
                       END) CONTADEBITO,
                       
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
                       
                       ROUND(ABS(L.VLRATEIO), 2) VALOR,
                       
                       ('DESP_GERENCIAL_IMPOSTO') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'N' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM BI_SINC_LANC_PAGAR_BASE L
                  LEFT JOIN DATA_DESPESA_LANC E ON E.RECNUM = L.RECNUM
                  LEFT JOIN DATA_DESPESA_LIVRO EL ON EL.RECNUM = L.RECNUM
                  LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                  LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = L.CODFILIAL
                 WHERE 1 = 1
                   AND L.DTCOMPETENCIA >= vDATA_MOV_INCREMENTAL
                   AND L.CODCONTA IN (SELECT CODCONTA FROM TABLE(PKG_BI_CONTABILIDADE.FN_CONTA_IMPOSTO_DESP_GER()))
                   AND L.CODFLUXO = 3 --'CONFIRMADO'
                   AND L.VLRATEIO > 0
                   AND L.DTESTORNOBAIXA IS NULL
                   AND NOT (L.CODCONTA = vIRRF_RECOLHER AND L.CODFORNEC IN (9177)))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
                     L.CODFILIAL,
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
                            AND JC.CODDRE > 0 THEN
                        DECODE(L.CODCC, '0', FL.CODCC, L.CODCC)
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN L.VALOR < 0
                            AND JC.CODDRE > 0 THEN
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
                     ROUND(ABS(L.VLRATEIO), 2) VALOR,
                     
                     ('LANC_PAG_OUTROS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) FL ON FL.CODFILIAL = L.CODFILIAL
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC JC ON JC.CODGERENCIAL = L.CODCONTA
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
                                 r.CODFILIAL,
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
                     L.CODFILIAL,
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
                            AND JC.CODDRE > 0
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
                            AND JC.CODDRE > 0
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
                       WHEN JC.CODDRE > 0
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
                     ROUND(ABS(L.VLRATEIO), 2) - (L.PERCRATEIO * NVL(L.VLDESCONTO, 0) / 100) VALOR,
                     
                     ('LANC_PAG_FORNECEDOR') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) FL ON FL.CODFILIAL = L.CODFILIAL
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC JC ON JC.CODGERENCIAL = L.CODCONTA
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
                                 r.CODFILIAL,
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
                     L.CODFILIAL,
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
                            AND JC.CODDRE > 0 THEN
                        DECODE(L.CODCC, '0', FL.CODCC, L.CODCC)
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN L.VALOR < 0
                            AND JC.CODDRE > 0 THEN
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
                     
                     ROUND(ABS(L.VLRATEIO), 2) VALOR,
                     
                     ('LANC_JUROS_PAGOS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) FL ON FL.CODFILIAL = L.CODFILIAL
                LEFT JOIN BI_SINC_PLANO_CONTAS_JC JC ON JC.CODGERENCIAL = L.CODCONTA
               WHERE 1 = 1
                 AND L.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND L.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND L.CODCONTA = vJUROS_PAGOS)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
    FOR r IN (WITH LANC_DESCONTOS AS
                 (SELECT RECNUM,
                        CODFORNEC,
                        NUMNOTA
                   FROM BI_SINC_LANC_PAGAR_BASE L
                  WHERE L.CODCONTA = vDESCONTOS_OBTIDOS),
                
                LANC_DESCONTOS_CONTRA_CONTA AS
                 (SELECT MAX(L.CODCONTA) CODCONTA,
                        L.CODFORNEC,
                        L.NUMNOTA
                   FROM BI_SINC_LANC_PAGAR_BASE L
                   JOIN LANC_DESCONTOS D ON D.CODFORNEC = L.CODFORNEC
                                        AND D.NUMNOTA = L.NUMNOTA
                  WHERE L.CODCONTA <> vDESCONTOS_OBTIDOS
                    AND L.CODFORNEC IN
                        (SELECT CODFORNEC
                           FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA()))
                  GROUP BY L.CODFORNEC,
                           L.NUMNOTA)
                
                SELECT ('L04' || '.CC_' || L.CODCC) CODLANC,
                       L.CODEMPRESA,
                       L.CODFILIAL,
                       L.DTCOMPENSACAO DATA,
                       
                       ----------TIPO LANCAMENTO
                       3 TIPOLANCAMENTO,
                       
                       L.RECNUM IDENTIFICADOR,
                       L.NUMNOTA DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       (CASE
                         WHEN (L.VALOR > 0 OR
                              L.CODFORNEC IN
                              (SELECT CODFORNEC
                                  FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA()))) THEN
                          COALESCE(DC.CODCONTA, L.CODCONTA)
                         ELSE
                          L.CODFORNEC
                       END) CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       (CASE
                         WHEN (L.VALOR < 0 OR
                              L.CODFORNEC IN
                              (SELECT CODFORNEC
                                  FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA()))) THEN
                          L.CODCONTA
                         ELSE
                          L.CODFORNEC
                       END) CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       (CASE
                         WHEN (L.VALOR > 0 OR (L.CODFORNEC IN
                              (SELECT CODFORNEC
                                                  FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) AND
                              JC2.CODDRE > 0))
                              AND JC.CODDRE > 0 THEN
                          DECODE(L.CODCC, '0', FL.CODCC, L.CODCC)
                         ELSE
                          NULL
                       END) CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       (CASE
                         WHEN (L.VALOR < 0 OR (L.CODFORNEC IN
                              (SELECT CODFORNEC
                                                  FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_LANC_TIPO_FORNEC_CONSIDERA_CONTA())) AND
                              JC2.CODDRE > 0))
                              AND JC.CODDRE > 0 THEN
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
                       
                       ROUND(ABS(L.VLRATEIO), 2) VALOR,
                       
                       ('LANC_DESCONTO_OBTIDO') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'S' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM BI_SINC_LANC_PAGAR_BASE L
                  LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                  LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) FL ON FL.CODFILIAL = L.CODFILIAL
                  LEFT JOIN BI_SINC_PLANO_CONTAS_JC JC ON JC.CODGERENCIAL = L.CODCONTA
                  LEFT JOIN LANC_DESCONTOS_CONTRA_CONTA DC ON DC.CODFORNEC = L.CODFORNEC
                                                          AND DC.NUMNOTA = L.NUMNOTA
                  LEFT JOIN BI_SINC_PLANO_CONTAS_JC JC2 ON JC2.CODGERENCIAL = DC.CODCONTA
                 WHERE 1 = 1
                   AND L.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                   AND L.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                   AND L.CODCONTA = vDESCONTOS_OBTIDOS)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
                     L.CODFILIAL,
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
                     
                     ROUND(ABS(L.VLRATEIO), 2) VALOR,
                     
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
                                 r.CODFILIAL,
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
                     L.CODFILIAL,
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
                        NVL(L.CONTABANCO, vCOMPRAS_CARTAO_CREDITO)
                     END) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     (CASE
                       WHEN L.VALOR < 0 THEN
                        L.CODCONTA
                       ELSE
                        NVL(L.CONTABANCO, vCOMPRAS_CARTAO_CREDITO)
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
                     (CASE
                       WHEN NVL(L.NUMNOTA, 0) <> 0 THEN
                        ('PAG NF ' || L.NUMNOTA || ' - ' || L.HISTORICO)
                       ELSE
                        (UPPER(C.CONTA) || ' - ' || L.HISTORICO)
                     END) HISTORICO,
                     
                     ROUND(ABS(L.VLRATEIO), 2) VALOR,
                     
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
                                 r.CODFILIAL,
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

  ----LANCAMENTOS PAGAMENTO - CAIXA NOTA COMISSAO MKT
  FUNCTION FN_LANC_CAIXA_NF_MKT RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'L07' CODLANC,
                     L.CODEMPRESA,
                     L.CODFILIAL,
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
                        L.CODCONTA
                     END) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     (CASE
                       WHEN L.VALOR > 0 THEN
                        L.CODFORNEC
                       ELSE
                        L.CODCONTA
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN L.VALOR < 0 THEN
                        (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                        TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RECNUM: ' || L.RECNUM)
                       ELSE
                        ('ESTORNO - ' || UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                        TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RECNUM: ' || L.RECNUM)
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN L.VALOR < 0 THEN
                        ('PAG NF ' || L.NUMNOTA || ' - ' || L.HISTORICO || ' - ' || F.CNPJ || ' - ' || F.FORNECEDOR ||
                        ' - Cód: ' || L.CODFORNEC || ' - RECNUM: ' || L.RECNUM)
                       ELSE
                        ('ESTORNO PAG NF ' || L.NUMNOTA || ' - ' || L.HISTORICO || ' - ' || F.CNPJ || ' - ' ||
                        F.FORNECEDOR || ' - Cód: ' || L.CODFORNEC || ' - RECNUM: ' || L.RECNUM)
                     END) HISTORICO,
                     
                     ROUND(ABS(L.VLRATEIO), 2) VALOR,
                     
                     ('LANC_CX_COMISSAO_MKT') ORIGEM,
                     
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
                 AND L.CODBANCO IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_COMISSAO_MKT()))
                 AND L.CODCONTA NOT IN (vDESCONTOS_OBTIDOS, vJUROS_PAGOS)
                 AND L.CODCC = '0')
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_LANC_CAIXA_NF_MKT;

  ----LANCAMENTOS PAGAMENTO - ADIANTAMENTO A FORNECEDORES
  FUNCTION FN_LANC_ADIANT_FORNECEDORES RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('L08' || '.CC_' || L.CODCC) CODLANC,
                     L.CODEMPRESA,
                     L.CODFILIAL,
                     L.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     L.RECNUM IDENTIFICADOR,
                     L.NUMTRANS DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     (CASE
                       WHEN L.VALOR > 0 THEN
                        vADIANTAMENTO_FORNECEDOR
                       ELSE
                        L.CONTABANCO
                     END) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     (CASE
                       WHEN L.VALOR < 0 THEN
                        vADIANTAMENTO_FORNECEDOR
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
                     (CASE
                       WHEN L.VALOR > 0 THEN
                        ('ADIANT. FORNECEDOR - Nº TRANS: ' || L.NUMTRANS || ' - ' || F.FORNECEDOR || ' - CNPJ: ' ||
                        F.CNPJ || ' - Cod. ' || L.CODFORNEC)
                       ELSE
                        ('DEV. ADIANT. FORNECEDOR - Nº TRANS: ' || L.NUMTRANS || ' - ' || F.FORNECEDOR || ' - CNPJ: ' ||
                        F.CNPJ || ' - Cod. ' || L.CODFORNEC)
                     END) HISTORICO,
                     
                     ROUND(ABS(L.VLRATEIO), 2) VALOR,
                     
                     ('LANC_ADIANT_FORNECEDOR') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
               WHERE 1 = 1
                 AND L.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND L.DTCOMPENSACAO IS NOT NULL
                 AND L.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND L.ADIANTAMENTO = 'S'
                 AND L.TIPOPARCEIRO = 'F')
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_LANC_ADIANT_FORNECEDORES;

  ----LANCAMENTOS PAGAMENTO - BAIXA DO ADIANTAMENTO A FORNECEDORES
  FUNCTION FN_LANC_BAIXA_ADIANT_FORNEC RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (WITH ADIANTAMENTOS_REALIZADOS AS
                 (SELECT A.RECNUMPAGTO FROM PCLANCADIANTFORNEC A GROUP BY A.RECNUMPAGTO)
                
                SELECT ('L09' || '.CC_' || L.CODCC) CODLANC,
                       L.CODEMPRESA,
                       L.CODFILIAL,
                       (CASE
                         WHEN L.VALOR < 0 THEN
                          L.DTCOMPENSACAO
                         ELSE
                          L.DTPAGAMENTO
                       END) DATA,
                       
                       ----------TIPO LANCAMENTO
                       3 TIPOLANCAMENTO,
                       
                       L.RECNUM IDENTIFICADOR,
                       
                       ----------TIPO DOCUMENTO
                       (CASE
                         WHEN L.VALOR < 0 THEN
                          L.NUMTRANS
                         ELSE
                          L.NUMNOTA
                       END) DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       (CASE
                         WHEN L.VALOR > 0 THEN
                          L.CODFORNEC
                         ELSE
                          L.CONTABANCO
                       END) CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       vADIANTAMENTO_FORNECEDOR CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       NULL CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       NULL CODCC_CREDITO,
                       
                       ----------ATIVIDADE
                       (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                       TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RAT: ' ||
                       REPLACE(TO_CHAR(L.PERCRATEIO, '999.00'), '.', ',') || ' - CC: ' || L.CODCC || ' - RECNUM: ' ||
                       L.RECNUM) ATIVIDADE,
                       
                       ----------HISTORICO
                       (CASE
                         WHEN L.VALOR > 0 THEN
                          ('BAIXA ADIANT FORNEC. NF ' || L.NUMNOTA || ' - ' || F.FORNECEDOR || ' - CNPJ: ' || F.CNPJ ||
                          ' - Cod. ' || L.CODFORNEC)
                         ELSE
                          (' DEV. ADIANT. FORNEC. UTILIZADO - Nº TRANS: ' || L.NUMTRANS || ' - ' || F.FORNECEDOR ||
                          ' - CNPJ: ' || F.CNPJ || ' - Cod. ' || L.CODFORNEC)
                       END) HISTORICO,
                       
                       ROUND(ABS(L.VLRATEIO), 2) VALOR,
                       
                       ('LANC_BAIXA_ADIANT_FORNEC') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'S' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM BI_SINC_LANC_PAGAR_BASE L
                  JOIN ADIANTAMENTOS_REALIZADOS A ON A.RECNUMPAGTO = L.RECNUM
                  LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                  LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
                
                 WHERE 1 = 1
                   AND L.DTPAGAMENTO >= vDATA_MOV_INCREMENTAL
                   AND L.DTPAGAMENTO IS NOT NULL)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_LANC_BAIXA_ADIANT_FORNEC;

  ----LANCAMENTOS PAGAMENTO - ESTORNO DA BAIXA DO ADIANTAMENTO A FORNECEDORES
  FUNCTION FN_LANC_EST_BAIXA_ADIANT_FORNEC RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('L10' || '.CC_' || L.CODCC) CODLANC,
                     L.CODEMPRESA,
                     L.CODFILIAL,
                     L.DTPAGAMENTO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     L.RECNUM IDENTIFICADOR,
                     
                     L.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vADIANTAMENTO_FORNECEDOR CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     L.CODFORNEC CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RECNUM: ' || L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('ESTORNO BAIXA ADIANT FORNEC. NF ' || L.NUMNOTA || ' - ' || F.FORNECEDOR || ' - CNPJ: ' || F.CNPJ ||
                     ' - Cod. ' || L.CODFORNEC) HISTORICO,
                     
                     ROUND(ABS(L.VLRATEIO), 2) VALOR,
                     
                     ('LANC_ESTORNO_BAIXA_ADIANT_FORNEC') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
               WHERE 1 = 1
                 AND L.DTPAGAMENTO >= vDATA_MOV_INCREMENTAL
                 AND L.DTPAGAMENTO IS NOT NULL
                 AND L.DTESTORNOBAIXA IS NOT NULL
                 AND L.NUMTRANS IS NULL
                 AND L.CODCONTA NOT IN (vDESCONTOS_OBTIDOS, vJUROS_PAGOS)
                 AND L.CODROTINABAIXA = 746
                 AND L.VALOR < 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_LANC_EST_BAIXA_ADIANT_FORNEC;

  ----LANCAMENTOS PAGAMENTO - ABATIMENTO CONTAS A PAGAR DEVOLUCAO DE FORNECEDOR
  FUNCTION FN_LANC_DEV_FORNEC_BAIXA_DUPLIC RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('L11' || '.CC_' || L.CODCC) CODLANC,
                     L.CODEMPRESA,
                     L.CODFILIAL,
                     NVL(L.DTCOMPENSACAO, L.DTPAGAMENTO) DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     L.RECNUM IDENTIFICADOR,
                     
                     L.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     L.CODFORNEC CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vDEVOLUCAO_RECEBER CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (UPPER(C.CONTA) || ' - F' || LPAD(L.CODFILIAL, 2, 0) || ' - VLTOTAL: ' ||
                     TRIM(TRANSLATE(TO_CHAR(L.VALOR, 'FM999G990D00'), '.,', ',.')) || ' - RECNUM: ' || L.RECNUM) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('BAIXA DEV. FORNEC. NF ' || L.NUMNOTA || ' - ' || F.FORNECEDOR || ' - CNPJ: ' || F.CNPJ ||
                     ' - Cod. ' || L.CODFORNEC) HISTORICO,
                     
                     ROUND(ABS(L.VLDESCONTO), 2) VALOR,
                     
                     ('LANC_DEV_FORNEC_BAIXA_DUPLIC') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_PAGAR_BASE L
                LEFT JOIN PCCONTA C ON C.CODCONTA = L.CODCONTA
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = L.CODFORNEC
               WHERE 1 = 1
                 AND L.DTPAGAMENTO >= vDATA_MOV_INCREMENTAL
                 AND L.DTPAGAMENTO IS NOT NULL
                 AND NVL(L.NUMNOTADEV, 0) > 0
                 AND NVL(L.VLDESCONTO, 0) > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_LANC_DEV_FORNEC_BAIXA_DUPLIC;

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
                
                SELECT ('R01' || '.P_' || P.PREST) CODLANC,
                       P.CODEMPRESA,
                       P.CODFILIAL,
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
                       
                       ROUND(ABS(P.VALOR), 2) VALOR,
                       
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
                                 r.CODFILIAL,
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
    FOR r IN (SELECT ('R02' || '.P_' || P.PREST) CODLANC,
                     P.CODEMPRESA,
                     P.CODFILIAL,
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
                     
                     ROUND(ABS(P.VLRECEBIDO), 2) VALOR,
                     
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
                                 r.CODFILIAL,
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
    FOR r IN (SELECT ('R03' || '.P_' || P.PREST) CODLANC,
                     P.CODEMPRESA,
                     P.CODFILIAL,
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
                     
                     ROUND(ABS(P.VLRECEBIDO), 2) VALOR,
                     
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
                                 r.CODFILIAL,
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
    FOR r IN (SELECT ('R04' || '.P_' || P.PREST) CODLANC,
                     P.CODEMPRESA,
                     P.CODFILIAL,
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
                     (CASE
                       WHEN P.DTCOMPENSACAO >= vDT_MUDANCA_FAT_DISTRIB_ES
                            AND P.CODFILIAL = vCODFILIAL_DEPOSITO_SP THEN
                        vCC_CORPORATIVO_SP
                       ELSE
                        F.CODCC
                     END) CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('BAIXA INCLUSAO DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                     ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0)) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('BAIXA INCLUSAO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                     ' - Cód. ' || T.CODCLI) HISTORICO,
                     
                     ROUND(ABS(P.VLRECEBIDO), 2) VALOR,
                     
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
                                 r.CODFILIAL,
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
    FOR r IN (WITH ANALISE_ESTORNO AS --QUANDO TEMOS 2 REGISTROS O ESTORNO É DE CODCOB = 'PERD'
                 (SELECT M.NUMTRANS,
                        COUNT(M.NUMTRANS) REGISTROS
                   FROM PCMOVCR M
                   JOIN PCPREST P ON P.NUMTRANS = M.NUMTRANS
                  WHERE P.CODCOB = 'ESTR'
                    AND (M.DTESTORNO IS NULL OR (M.DTESTORNO IS NOT NULL AND M.ESTORNO = 'N'))
                  GROUP BY M.NUMTRANS,
                           M.CODBANCO
                 HAVING COUNT(M.NUMTRANS) = 2)
                
                SELECT ('R05' || '.P_' || P.PREST) CODLANC,
                       P.CODEMPRESA,
                       P.CODFILIAL,
                       P.DTCOMPENSACAO DATA,
                       
                       ----------TIPO LANCAMENTO
                       3 TIPOLANCAMENTO,
                       
                       P.NUMTRANSVENDA IDENTIFICADOR,
                       P.NUMNOTA DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       (CASE
                         WHEN CODCOB = 'PERD' THEN
                          vPREJUIZO_CLIENTE
                         ELSE
                          P.CONTACLIENTE
                       END) CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       (CASE
                         WHEN CODCOB = 'PERD' THEN
                          P.CONTACLIENTE
                         ELSE
                          vPREJUIZO_CLIENTE
                       END) CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       (CASE
                         WHEN CODCOB = 'PERD' THEN
                          NVL(V.CODCC, F.CODCC)
                         ELSE
                          NULL
                       END) CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       (CASE
                         WHEN CODCOB = 'PERD' THEN
                          NULL
                         ELSE
                          NVL(V.CODCC, F.CODCC)
                       END) CODCC_CREDITO,
                       
                       ----------ATIVIDADE
                       (CASE
                         WHEN CODCOB = 'PERD' THEN
                          ('PERDA DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                          ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                         ELSE
                          ('ESTORNO PERDA DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                          ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0))
                       END) ATIVIDADE,
                       
                       ----------HISTORICO
                       
                       (CASE
                         WHEN CODCOB = 'PERD' THEN
                          ('PERDA NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE || ' - Cód. ' ||
                          T.CODCLI)
                         ELSE
                          ('ESTORNO PERDA NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                          ' - Cód. ' || T.CODCLI)
                       END) HISTORICO,
                       
                       ----------VALOR
                       ROUND(ABS(P.VLRECEBIDO), 2) VALOR,
                       
                       ('RECEB_BAIXA_DUP_PERDA') ORIGEM,
                       
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
                   AND (P.CODCOB = 'PERD' OR (P.CODCOB = 'ESTR' AND E.REGISTROS > 0)))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
    FOR r IN (SELECT ('R06' || '.P_' || P.PREST) CODLANC,
                     P.CODEMPRESA,
                     P.CODFILIAL,
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
                       WHEN P.VLDESCONTO > 0
                            AND P.CODFILIAL = vCODFILIAL_ES THEN
                        NVL(G.CODCC, F.CODCC)
                       WHEN P.VLDESCONTO > 0 THEN
                        NVL(V.CODCC, F.CODCC)
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN P.VLDESCONTO > 0 THEN
                        NULL
                       WHEN P.CODFILIAL = vCODFILIAL_ES THEN
                        NVL(G.CODCC, F.CODCC)
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
                     
                     ROUND(ABS(P.VLDESCONTO), 2) VALOR,
                     
                     ('RECEB_TAXA_CARTAO_LOJA') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_RECEBER_BASE P
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = P.CODFILIAL
                LEFT JOIN BI_SINC_VENDEDOR S ON S.CODUSUR = P.CODUSUR
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = S.CODSUPERVISOR
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_GERENTE_ES()) G ON G.CODGERENTE = S.CODGERENTE
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
                                 r.CODFILIAL,
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
    FOR r IN (SELECT ('R07' || '.P_' || P.PREST) CODLANC,
                     P.CODEMPRESA,
                     P.CODFILIAL,
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
                       WHEN P.VLDESCONTO > 0
                            AND P.CODFILIAL = vCODFILIAL_ES THEN
                        NVL(G.CODCC, F.CODCC)
                       WHEN P.VLDESCONTO > 0 THEN
                        NVL(V.CODCC, F.CODCC)
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN P.VLDESCONTO > 0 THEN
                        NULL
                       WHEN P.CODFILIAL = vCODFILIAL_ES THEN
                        NVL(G.CODCC, F.CODCC)
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
                        ('DESCONTO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE || ' - Cód. ' ||
                        T.CODCLI)
                       ELSE
                        ('ESTORNO DESCONTO NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                        ' - Cód. ' || T.CODCLI)
                     END) HISTORICO,
                     
                     ROUND(ABS(P.VLDESCONTO), 2) VALOR,
                     
                     ('RECEB_DESC_CONCEDIDO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_RECEBER_BASE P
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = P.CODFILIAL
                LEFT JOIN BI_SINC_VENDEDOR S ON S.CODUSUR = P.CODUSUR
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = S.CODSUPERVISOR
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_GERENTE_ES()) G ON G.CODGERENTE = S.CODGERENTE
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
               WHERE 1 = 1
                 AND P.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND P.DTCOMPENSACAO IS NOT NULL
                 AND P.DTINCLUSAOMANUAL IS NULL
                 AND P.CONTACLIENTE = vCLIENTES_NACIONAIS
                 AND P.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                 AND P.VLDESCONTO <> 0
                 AND P.CODCOB NOT IN ('CARC', 'CADB', 'JUR'))
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
              
              SELECT ('R08' || '.P_' || P.PREST) CODLANC,
                      P.CODEMPRESA,
                      P.CODFILIAL,
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
                        WHEN P.CODFILIAL = vCODFILIAL_ES THEN
                         NVL(G.CODCC, F.CODCC)
                        ELSE
                         NVL(V.CODCC, F.CODCC)
                      END) CODCC_DEBITO,
                      
                      ----------CODCC_CREDITO
                      (CASE
                        WHEN P.VLJUROS > 0
                             AND P.CODFILIAL = vCODFILIAL_ES THEN
                         NVL(G.CODCC, F.CODCC)
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
                      
                      ROUND(ABS(P.VLJUROS), 2) VALOR,
                      
                      ('RECEB_JUROS') ORIGEM,
                      
                      ----------ENVIAR_CONTABIL
                      'S' ENVIAR_CONTABIL,
                      
                      TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_RECEBER_BASE P
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = P.CODFILIAL
                LEFT JOIN BI_SINC_VENDEDOR S ON S.CODUSUR = P.CODUSUR
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = S.CODSUPERVISOR
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_GERENTE_ES()) G ON G.CODGERENTE = S.CODGERENTE
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
                                 r.CODFILIAL,
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
                 (SELECT M.NUMTRANS,
                        COUNT(M.NUMTRANS) REGISTROS
                   FROM PCMOVCR M
                   JOIN PCPREST P ON P.NUMTRANS = M.NUMTRANS
                  WHERE P.CODCOB = 'ESTR'
                    AND (M.DTESTORNO IS NULL OR (M.DTESTORNO IS NOT NULL AND M.ESTORNO = 'N'))
                  GROUP BY M.NUMTRANS,
                           M.CODBANCO
                 HAVING COUNT(M.NUMTRANS) > 0)
                
                SELECT ('R09' || '.P_' || P.PREST) CODLANC,
                       P.CODEMPRESA,
                       P.CODFILIAL,
                       P.DTCOMPENSACAO DATA,
                       
                       ----------TIPO LANCAMENTO
                       3 TIPOLANCAMENTO,
                       
                       P.NUMTRANSVENDA IDENTIFICADOR,
                       P.NUMNOTA DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       (CASE
                         WHEN ((P.CODCOB = 'ESTR' AND P.CODCOBORIG = 'JUR') OR (P.CODCOB = 'JUR')) THEN
                          vJUROS_RECEBIDOS --ESTORNO JUROS
                         WHEN (P.CODCOB = 'ESTR') THEN
                          P.CONTACLIENTE --ESTORNOS GERAIS
                         ELSE
                          P.CONTABANCO
                       END) CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       (CASE
                         WHEN (P.CODCOB = 'ESTR') THEN
                          P.CONTABANCO --ESTORNO JUROS E ESTORNOS GERAIS
                         ELSE
                          P.CONTACLIENTE
                       END) CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       (CASE
                         WHEN (((P.CODCOB = 'ESTR' AND P.CODCOBORIG = 'JUR') OR (P.CODCOB = 'JUR')) AND
                              P.CODFILIAL = vCODFILIAL_ES) THEN
                          NVL(G.CODCC, F.CODCC)
                         WHEN ((P.CODCOB = 'ESTR' AND P.CODCOBORIG = 'JUR') OR (P.CODCOB = 'JUR')) THEN
                          NVL(V.CODCC, F.CODCC)
                         ELSE
                          NULL
                       END) CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       NULL CODCC_CREDITO,
                       
                       ----------ATIVIDADE
                       (CASE
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
                          ROUND(ABS(P.VLRECEBIDO), 2) --BAIXA E ESTORNO DE JUROS
                         WHEN P.VLJUROS <> 0 THEN
                          ABS(ROUND(P.VLRECEBIDO, 2) - ROUND(P.VLJUROS, 2)) --BAIXA E ESTORNO DE DUPLICATAS COM VLJUROS
                         WHEN (P.CODCOB = 'ESTR' AND P.VLDESCONTO <> 0 AND P.CONTACLIENTE = vCLIENTES_NACIONAIS) THEN
                          ABS(ROUND(P.VLRECEBIDO, 2) + ROUND(P.VLDESCONTO, 2)) --ESTORNO DE DUPLICATA COM DESCONTO SEM SER MKT
                         ELSE
                          ROUND(ABS(P.VLRECEBIDO), 2)
                       END) VALOR,
                       
                       ('RECEB_BAIXA_DUPLICATAS') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'S' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM BI_SINC_LANC_RECEBER_BASE P
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = P.CODFILIAL
                  LEFT JOIN BI_SINC_VENDEDOR S ON S.CODUSUR = P.CODUSUR
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_VENDEDOR()) V ON V.CODSUPERVISOR = S.CODSUPERVISOR
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_GERENTE_ES()) G ON G.CODGERENTE = S.CODGERENTE
                  LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
                  LEFT JOIN ANALISE_ESTORNO E ON E.NUMTRANS = P.NUMTRANS
                 WHERE 1 = 1
                   AND P.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                   AND P.DTCOMPENSACAO IS NOT NULL
                   AND P.DTINCLUSAOMANUAL IS NULL
                   AND P.CODCOB NOT IN ('PERD')
                   AND NOT (P.CODCOB = 'ESTR' AND E.REGISTROS > 1)
                   AND P.CODBANCO NOT IN (SELECT CODBANCO FROM TABLE(PKG_BI_CONTABILIDADE.FN_BANCOS_DESCONSIDERAR()))
                   AND NVL(P.VLRECEBIDO, 0) <> 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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

  ----LANCAMENTOS RECEBIMENTO - DEV. CLIENTE COM ABATIMENTO DIRETO NA DUPLICATA
  FUNCTION FN_RECEB_DEV_CLI_DUPLICATA RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('R10' || '.P_' || P.PREST) CODLANC,
                     P.CODEMPRESA,
                     P.CODFILIAL,
                     P.DTPAGAMENTO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     P.NUMTRANSVENDA IDENTIFICADOR,
                     P.NUMNOTA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vDEVOLUCAO_CLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     P.CONTACLIENTE CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('ABAT. DEV. CLIENTE DUPLIC. - F' || LPAD(P.CODFILIAL, 2, 0) || ' - Nº MOV: ' || P.NUMTRANS ||
                     ' - Nº TRANSACAO: ' || P.NUMTRANSVENDA || '-' || LPAD(P.PREST, 2, 0)) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('ABAT. DEV. CLIENTE NF ' || P.NUMNOTA || ' - ' || 'PREST: ' || P.PREST || ' - ' || T.CLIENTE ||
                     ' - Cód. ' || T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(P.VLRECEBIDO), 2) VALOR,
                     
                     ('RECEB_DEV_CLI_DUPLICATA') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_LANC_RECEBER_BASE P
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = P.CODCLI
               WHERE 1 = 1
                 AND P.DTPAGAMENTO >= vDATA_MOV_INCREMENTAL
                 AND P.DTPAGAMENTO IS NOT NULL
                 AND P.DTINCLUSAOMANUAL IS NULL
                 AND P.CODCOB IN ('DEVP', 'DEVT')
                 AND NVL(P.VLRECEBIDO, 0) > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_RECEB_DEV_CLI_DUPLICATA;

  ----CREDITOS DE CLIENTES - ADIANTAMENTO DE CLIENTE RECEBIDO
  FUNCTION FN_CRED_ADIANT_CLIENTE RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('C01' || '.' || C.CODIGO) CODLANC,
                     C.CODEMPRESA,
                     C.CODFILIAL,
                     C.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     C.CODIGO IDENTIFICADOR,
                     C.NUMTRANS DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     C.CONTABANCO CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vADIANTAMENTO_CLIENTE CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('ADIANTAMENTO CLIENTE - F' || LPAD(C.CODFILIAL, 2, 0) || ' - Nº MOV: ' || C.NUMTRANS ||
                     ' - CÓDIGO: ' || C.CODIGO) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('ADIANTAMENTO CLIENTE - Nº MOV: ' || C.NUMTRANS || ' - ' || T.CLIENTE || ' - Cód. ' || T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(C.VALOR), 2) VALOR,
                     
                     ('CRED_ADIANT_CLIENTE') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_CREDITO_CLIENTE C
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = C.CODCLI
               WHERE 1 = 1
                 AND C.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND C.DTCOMPENSACAO IS NOT NULL
                 AND NVL(C.NUMTRANS, 0) > 0
                 AND C.CODROTINA = 618
                 AND C.VALOR > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_CRED_ADIANT_CLIENTE;

  ----CREDITOS DE CLIENTES - ADIANTAMENTO DE CLIENTE ESTORNADOS
  FUNCTION FN_CRED_ADIANT_CLIENTE_ESTORNO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('C02' || '.' || C.CODIGO) CODLANC,
                     C.CODEMPRESA,
                     C.CODFILIAL,
                     C.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     C.CODIGO IDENTIFICADOR,
                     C.NUMTRANSBAIXA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vADIANTAMENTO_CLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     C.CONTABANCO CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('ESTORNO ADIANTAMENTO CLIENTE - F' || LPAD(C.CODFILIAL, 2, 0) || ' - Nº MOV: ' || C.NUMTRANSBAIXA ||
                     ' - Nº CRED: ' || C.NUMCRED || ' - Cód: ' || C.CODIGO) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('ESTORNO ADIANTAMENTO CLIENTE - Nº MOV: ' || C.NUMTRANSBAIXA || ' - ' || T.CLIENTE || ' - Cód. ' ||
                     T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(C.VALOR), 2) VALOR,
                     
                     ('CRED_ADIANT_CLIENTE_ESTORNO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_CREDITO_CLIENTE C
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = C.CODCLI
               WHERE 1 = 1
                 AND C.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND C.DTCOMPENSACAO IS NOT NULL
                 AND NVL(C.NUMTRANSBAIXA, 0) > 0
                 AND C.CODROTINA = 619
                 AND C.VALOR < 0
                 AND NVL(C.NUMTRANS_MN, 0) > 0
                 AND C.VALOR = C.VLMOVCR)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_CRED_ADIANT_CLIENTE_ESTORNO;

  ----CREDITOS DE CLIENTES - ADIANTAMENTO DE CLIENTE BAIXADOS COMO RECEITA
  FUNCTION FN_CRED_ADIANT_CLIENTE_RECEITA RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('C03' || '.' || C.CODIGO) CODLANC,
                     C.CODEMPRESA,
                     C.CODFILIAL,
                     C.DTDESCONTO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     C.CODIGO IDENTIFICADOR,
                     C.NUMLANCBAIXA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vADIANTAMENTO_CLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vCREDITOS_NAO_UTILIZADOS CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     F.CODCC CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('RECEITA ADIANT. CLIENTE - F' || LPAD(C.CODFILIAL, 2, 0) || ' - Nº LANC: ' || C.NUMLANCBAIXA ||
                     ' - Nº CRED: ' || C.NUMCRED || ' - Cód: ' || C.CODIGO) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('RECEITA ADIANT. CLIENTE - Nº LANC: ' || C.NUMLANCBAIXA || ' - ' || T.CLIENTE || ' - Cód. ' ||
                     T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(C.VALOR), 2) VALOR,
                     
                     ('CRED_ADIANT_CLIENTE_RECEITA') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_CREDITO_CLIENTE C
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = C.CODCLI
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = C.CODFILIAL
               WHERE 1 = 1
                 AND C.DTDESCONTO >= vDATA_MOV_INCREMENTAL
                 AND C.DTDESCONTO IS NOT NULL
                 AND NVL(C.NUMLANCBAIXA, 0) > 0
                 AND C.CODROTINA = 619
                 AND C.VALOR < 0
                 AND NVL(C.NUMTRANS_MN, 0) > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_CRED_ADIANT_CLIENTE_RECEITA;

  ----CREDITOS DE CLIENTES - ADIANTAMENTO DE CLIENTE BAIXADOS EM DUPLICATAS
  FUNCTION FN_CRED_ADIANT_CLIENTE_BAIXA_DUP RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('C04' || '.' || C.CODIGO) CODLANC,
                     C.CODEMPRESA,
                     C.CODFILIAL,
                     C.DTDESCONTO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     C.CODIGO IDENTIFICADOR,
                     C.DUPLIC DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vADIANTAMENTO_CLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     C.CONTACLIENTE CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('BAIXA DUP ADIANT. CLIENTE - F' || LPAD(C.CODFILIAL, 2, 0) || ' - Nº NOTA: ' || C.DUPLIC ||
                     ' - Nº CRED: ' || C.NUMCRED || ' - Cód: ' || C.CODIGO) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('BAIXA DUP ADIANT. CLIENTE - Nº NOTA: ' || C.DUPLIC || ' - ' || T.CLIENTE || ' - Cód. ' ||
                     T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(C.VALOR), 2) VALOR,
                     
                     ('CRED_ADIANT_CLIENTE_BAIXA_DUP') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_CREDITO_CLIENTE C
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = C.CODCLI
               WHERE 1 = 1
                 AND C.DTDESCONTO >= vDATA_MOV_INCREMENTAL
                 AND C.DTDESCONTO IS NOT NULL
                 AND NVL(C.NUMTRANS, 0) > 0
                 AND NVL(C.NUMTRANSVENDADESC, 0) > 0
                 AND C.VALOR > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_CRED_ADIANT_CLIENTE_BAIXA_DUP;

  ----CREDITOS DE CLIENTES - ADIANTAMENTO DE CLIENTE ESTORNADOS APÓS BAIXA EM DUPLICATAS
  FUNCTION FN_CRED_ADIANT_CLIENTE_DUP_ESTORNO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('C05' || '.' || C.CODIGO) CODLANC,
                     C.CODEMPRESA,
                     C.CODFILIAL,
                     C.DTESTORNO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     C.CODIGO IDENTIFICADOR,
                     C.DUPLIC DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     C.CONTACLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vADIANTAMENTO_CLIENTE CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('ESTORNO BAIXA DUP ADIANT. CLIENTE - F' || LPAD(C.CODFILIAL, 2, 0) || ' - Nº NOTA: ' || C.DUPLIC ||
                     ' - Nº CRED: ' || C.NUMCRED || ' - Cód: ' || C.CODIGO) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('ESTORNO BAIXA DUP ADIANT. CLIENTE - Nº NOTA: ' || C.DUPLIC || ' - ' || T.CLIENTE || ' - Cód. ' ||
                     T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(C.VALOR), 2) VALOR,
                     
                     ('CRED_ADIANT_CLIENTE_DUP_ESTORNO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_CREDITO_CLIENTE C
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = C.CODCLI
               WHERE 1 = 1
                 AND C.DTESTORNO >= vDATA_MOV_INCREMENTAL
                 AND C.DTESTORNO IS NOT NULL
                 AND NVL(C.NUMTRANS, 0) > 0
                 AND NVL(C.NUMTRANSVENDADESC, 0) > 0
                 AND C.VALOR < 0
                 AND NVL(C.NUMERARIO, '0') = '0')
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_CRED_ADIANT_CLIENTE_DUP_ESTORNO;

  ----CREDITOS DE CLIENTES - DEVOLUCAO DE CLIENTE BAIXADOS COMO RECEITA
  FUNCTION FN_CRED_DEV_CLIENTE_RECEITA RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('C06' || '.' || C.CODIGO) CODLANC,
                     C.CODEMPRESA,
                     C.CODFILIAL,
                     C.DTDESCONTO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     C.CODIGO IDENTIFICADOR,
                     C.NUMNOTADEV DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vDEVOLUCAO_CLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vCREDITOS_NAO_UTILIZADOS CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     F.CODCC CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('RECEITA DEV. CLIENTE - F' || LPAD(C.CODFILIAL, 2, 0) || ' - NOTA DEV: ' || C.NUMNOTADEV ||
                     ' - CLI. DEV.: ' || C.CODCLIDEV || ' - Nº LANC: ' || C.NUMLANCBAIXA || ' - Nº CRED: ' || C.NUMCRED ||
                     ' - Cód: ' || C.CODIGO) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('RECEITA DEV. CLIENTE - Nº NOTA DEV: ' || C.NUMNOTADEV || ' - ' || T.CLIENTE || ' - Cód. ' ||
                     T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(C.VALOR), 2) VALOR,
                     
                     ('CRED_DEV_CLIENTE_RECEITA') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_CREDITO_CLIENTE C
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = C.CODCLI
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = C.CODFILIAL
               WHERE 1 = 1
                 AND C.DTDESCONTO >= vDATA_MOV_INCREMENTAL
                 AND C.DTDESCONTO IS NOT NULL
                 AND NVL(C.NUMTRANSENTDEVCLI, 0) > 0
                 AND NVL(C.NUMLANCBAIXA, 0) > 0
                 AND C.VALOR < 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_CRED_DEV_CLIENTE_RECEITA;

  ----CREDITOS DE CLIENTES - DEVOLUCAO DE CLIENTE BAIXADOS EM DUPLICATAS
  FUNCTION FN_CRED_DEV_CLIENTE_BAIXA_DUP RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('C07' || '.' || C.CODIGO) CODLANC,
                     C.CODEMPRESA,
                     C.CODFILIAL,
                     C.DTDESCONTO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     C.CODIGO IDENTIFICADOR,
                     C.DUPLIC DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vDEVOLUCAO_CLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     C.CONTACLIENTE CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('BAIXA DUP. DEV. CLIENTE - F' || LPAD(C.CODFILIAL, 2, 0) || ' - NOTA DEV: ' || C.NUMNOTADEV ||
                     ' - CLI. DEV.: ' || C.CODCLIDEV || ' - DUPLIC: ' || C.DUPLIC || ' - Nº CRED: ' || C.NUMCRED ||
                     ' - Cód: ' || C.CODIGO) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('BAIXA DUP. DEV. CLIENTE - DUPLIC ' || C.DUPLIC || ' - Nº NOTA DEV: ' || C.NUMNOTADEV || ' - ' ||
                     T.CLIENTE || ' - Cód. ' || T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(C.VALOR), 2) VALOR,
                     
                     ('CRED_DEV_CLIENTE_BAIXA_DUP') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_CREDITO_CLIENTE C
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = C.CODCLI
               WHERE 1 = 1
                 AND C.DTDESCONTO >= vDATA_MOV_INCREMENTAL
                 AND C.DTDESCONTO IS NOT NULL
                 AND NVL(C.NUMTRANSENTDEVCLI, 0) > 0
                 AND NVL(C.NUMTRANSVENDADESC, 0) > 0
                 AND C.VALOR > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_CRED_DEV_CLIENTE_BAIXA_DUP;

  ----CREDITOS DE CLIENTES - DEVOLUCAO DE CLIENTE ESTORNO DA BAIXA EM DUPLICATAS
  FUNCTION FN_CRED_DEV_CLIENTE_DUP_ESTORNO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('C08' || '.' || C.CODIGO) CODLANC,
                     C.CODEMPRESA,
                     C.CODFILIAL,
                     C.DTESTORNO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     C.CODIGO IDENTIFICADOR,
                     C.DUPLIC DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     C.CONTACLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vDEVOLUCAO_CLIENTE CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('ESTORNO BAIXA DUP. DEV. CLIENTE - F' || LPAD(C.CODFILIAL, 2, 0) || ' - NOTA DEV: ' ||
                     C.NUMNOTADEV || ' - CLI. DEV.: ' || C.CODCLIDEV || ' - DUPLIC: ' || C.DUPLIC || ' - Nº CRED: ' ||
                     C.NUMCRED || ' - Cód: ' || C.CODIGO) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('ESTORNO BAIXA DEV. CLIENTE - DUPLIC ' || C.DUPLIC || ' - Nº NOTA DEV: ' || C.NUMNOTADEV || ' - ' ||
                     T.CLIENTE || ' - Cód. ' || T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(C.VALOR), 2) VALOR,
                     
                     ('CRED_DEV_CLIENTE_DUP_ESTORNO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_CREDITO_CLIENTE C
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = C.CODCLI
               WHERE 1 = 1
                 AND C.DTESTORNO >= vDATA_MOV_INCREMENTAL
                 AND C.DTESTORNO IS NOT NULL
                 AND NVL(C.NUMTRANSENTDEVCLI, 0) > 0
                 AND NVL(C.NUMTRANSVENDADESC, 0) > 0
                 AND C.VALOR < 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_CRED_DEV_CLIENTE_DUP_ESTORNO;

  ----CREDITOS DE CLIENTES - DEVOLUCAO DE CLIENTE MOVIMENTANDO BANCO (DEV)
  FUNCTION FN_CRED_DEV_CLIENTE_MOV_BANCO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('C09' || '.' || C.CODIGO) CODLANC,
                     C.CODEMPRESA,
                     C.CODFILIAL,
                     C.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     C.CODIGO IDENTIFICADOR,
                     C.NUMTRANSBAIXA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vDEVOLUCAO_CLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     C.CONTABANCO CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('MOV. BANCO DEV. CLIENTE - F' || LPAD(C.CODFILIAL, 2, 0) || ' - NOTA DEV: ' || C.NUMNOTADEV ||
                     ' - CLI. DEV.: ' || C.CODCLIDEV || ' - Nº MOV: ' || C.NUMTRANSBAIXA || ' - Nº CRED: ' || C.NUMCRED ||
                     ' - Cód: ' || C.CODIGO) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('MOV. BANCO DEV. CLIENTE - Nº MOV ' || C.NUMTRANSBAIXA || ' - Nº NOTA DEV: ' || C.NUMNOTADEV ||
                     ' - ' || T.CLIENTE || ' - Cód. ' || T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(C.VALOR), 2) VALOR,
                     
                     ('CRED_DEV_CLIENTE_MOV_BANCO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_CREDITO_CLIENTE C
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = C.CODCLI
               WHERE 1 = 1
                 AND C.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND C.DTCOMPENSACAO IS NOT NULL
                 AND NVL(C.NUMTRANSENTDEVCLI, 0) > 0
                 AND NVL(C.NUMTRANSBAIXA, 0) > 0
                 AND C.CODROTINA = 619
                 AND C.VALOR < 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_CRED_DEV_CLIENTE_MOV_BANCO;

  ----CREDITOS DE CLIENTES - GERADOS EM CONTA GERENCIAL E USADOS NA BAIXA DUPLICATA
  FUNCTION FN_CRED_CONTA_GER_BAIXA_DUP RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('C10' || '.' || C.CODIGO) CODLANC,
                     C.CODEMPRESA,
                     C.CODFILIAL,
                     C.DTDESCONTO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     C.CODIGO IDENTIFICADOR,
                     C.DUPLIC DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vDESCONTOS_CONCEDIDOS CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     C.CONTACLIENTE CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     F.CODCC CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('DESC DUPLIC. CREDITO MANUAL - F' || LPAD(C.CODFILIAL, 2, 0) || ' - Nº NOTA: ' || C.DUPLIC ||
                     ' - Nº LANC: ' || C.NUMLANC || ' - Nº CRED: ' || C.NUMCRED || ' - Cód: ' || C.CODIGO) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('DESC DUPLIC. CREDITO MANUAL - Nº NOTA: ' || C.DUPLIC || ' - ' || T.CLIENTE || ' - Cód. ' ||
                     T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(C.VALOR), 2) VALOR,
                     
                     ('CRED_MANUAL_BAIXA_DUP') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_CREDITO_CLIENTE C
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = C.CODCLI
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = C.CODFILIAL
               WHERE 1 = 1
                 AND C.DTDESCONTO >= vDATA_MOV_INCREMENTAL
                 AND C.DTDESCONTO IS NOT NULL
                 AND NVL(C.NUMLANC, 0) > 0
                 AND NVL(C.NUMTRANSVENDADESC, 0) > 0
                 AND C.VALOR > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_CRED_CONTA_GER_BAIXA_DUP;

  ----CREDITOS DE CLIENTES - GERADOS EM CONTA GERENCIAL - ESTORNO DA BAIXA DUPLICATA
  FUNCTION FN_CRED_CONTA_GER_DUP_ESTORNO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('C11' || '.' || C.CODIGO) CODLANC,
                     C.CODEMPRESA,
                     C.CODFILIAL,
                     C.DTESTORNO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     C.CODIGO IDENTIFICADOR,
                     C.DUPLIC DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     C.CONTACLIENTE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vDESCONTOS_CONCEDIDOS CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     F.CODCC CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('ESTORNO DESC DUPLIC. CREDITO MANUAL - F' || LPAD(C.CODFILIAL, 2, 0) || ' - Nº NOTA: ' || C.DUPLIC ||
                     ' - Nº LANC: ' || C.NUMLANC || ' - Nº CRED: ' || C.NUMCRED || ' - Cód: ' || C.CODIGO) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('ESTORNO DESC DUPLIC. CREDITO MANUAL - Nº NOTA: ' || C.DUPLIC || ' - ' || T.CLIENTE || ' - Cód. ' ||
                     T.CODCLI) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(C.VALOR), 2) VALOR,
                     
                     ('CRED_MANUAL_DUP_ESTORNO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_CREDITO_CLIENTE C
                LEFT JOIN BI_SINC_CLIENTE T ON T.CODCLI = C.CODCLI
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) F ON F.CODFILIAL = C.CODFILIAL
               WHERE 1 = 1
                 AND C.DTESTORNO >= vDATA_MOV_INCREMENTAL
                 AND C.DTESTORNO IS NOT NULL
                 AND NVL(C.NUMLANC, 0) > 0
                 AND NVL(C.NUMTRANSVENDADESC, 0) > 0
                 AND C.VALOR < 0
                 AND NVL(C.NUMERARIO, '0') = '0')
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_CRED_CONTA_GER_DUP_ESTORNO;

  ----VERBA DE FORNECEDORES - ESTORNO DA NOTA DE DEVOLUCAO AO FORNECEDOR
  FUNCTION FN_VERBA_ESTORNO_DEVOLUCAO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'V01' CODLANC,
                     V.CODEMPRESA,
                     V.CODFILIAL,
                     V.DTPAGVERBA DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     V.NUMTRANSCRFOR IDENTIFICADOR,
                     V.NUMNOTADEV DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vCLIENTES_NACIONAIS CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vDEVOLUCAO_RECEBER CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('ESTORNO DEV. FORNECEDOR - F' || LPAD(V.CODFILIAL, 2, 0) || ' - NFD: ' || V.NUMNOTADEV ||
                     ' - Nº VERBA: ' || V.NUMVERBA || ' - Cód: ' || V.NUMTRANSCRFOR) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('ESTORNO DEV. FORNECEDOR - NFD: ' || V.NUMNOTADEV || ' - ' || F.FORNECEDOR || ' - Cód. ' ||
                     V.CODFORNEC) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(V.VALOR), 2) VALOR,
                     
                     ('VERBA_ESTORNO_DEVOLUCAO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_VERBA_FORNECEDOR V
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = V.CODFORNEC
               WHERE 1 = 1
                 AND V.DTPAGVERBA >= vDATA_MOV_INCREMENTAL
                 AND V.DTPAGVERBA IS NOT NULL
                 AND NVL(V.NUMLANC, 0) = 0
                 AND NVL(V.NUMTRANSENT, 0) > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_VERBA_ESTORNO_DEVOLUCAO;

  ----VERBA DE FORNECEDORES - DEVOLUCAO AO FORNECEDOR RECEBIDO EM DINHEIRO
  FUNCTION FN_VERBA_DEV_MOV_BANCO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'V02' CODLANC,
                     V.CODEMPRESA,
                     V.CODFILIAL,
                     V.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     V.NUMTRANSCRFOR IDENTIFICADOR,
                     V.NUMNOTADEV DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     NVL(V.CONTABANCO, vJUROS_PAGOS) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vDEVOLUCAO_RECEBER CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN NVL(V.CONTABANCO, vJUROS_PAGOS) = vJUROS_PAGOS THEN
                        L.CODCC
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('REC. DINHEIRO - DEV. FORNECEDOR - F' || LPAD(V.CODFILIAL, 2, 0) || ' - Nº TRANS: ' || V.NUMTRANS ||
                     ' - Nº VERBA: ' || V.NUMVERBA || ' - Cód: ' || V.NUMTRANSCRFOR) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('RECEITA DEV. FORNECEDOR - NFD: ' || V.NUMNOTADEV || ' - ' || F.FORNECEDOR || ' - Cód. ' ||
                     V.CODFORNEC) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(V.VALOR), 2) VALOR,
                     
                     ('VERBA_DEV_FORNEC_MOV_BANCO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_VERBA_FORNECEDOR V
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = V.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = V.CODFILIAL
               WHERE 1 = 1
                 AND V.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND V.DTCOMPENSACAO IS NOT NULL
                 AND NVL(V.NUMTRANSVENDADEV, 0) > 0
                 AND NVL(V.NUMTRANS, 0) > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_VERBA_DEV_MOV_BANCO;

  ----VERBA DE FORNECEDORES - DEVOLUCAO AO FORNECEDOR BAIXADO COMO DESCONTO EM DUPLICATA
  FUNCTION FN_VERBA_DEV_BAIXA_DUPLIC RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'V03' CODLANC,
                     V.CODEMPRESA,
                     V.CODFILIAL,
                     NVL(V.DTCOMPENSACAO, V.DTPAGLANC) DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     V.NUMTRANSCRFOR IDENTIFICADOR,
                     V.NUMNOTADEV DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     V.CODFORNEC CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vDEVOLUCAO_RECEBER CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('DESC. DUPLIC. - DEV. FORNECEDOR - F' || LPAD(V.CODFILIAL, 2, 0) || ' - DUPLIC: ' || V.NUMNOTADESC ||
                     ' - Nº VERBA: ' || V.NUMVERBA || ' - Cód: ' || V.NUMTRANSCRFOR) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('DESC. DUPLIC. DEV. FORNECEDOR - NFD: ' || V.NUMNOTADEV || ' - DUPLIC: ' || V.NUMNOTADESC || ' - ' ||
                     F.FORNECEDOR || ' - Cód. ' || V.CODFORNEC) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(V.VALOR), 2) VALOR,
                     
                     ('VERBA_DEV_DESC_DUPLIC') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_VERBA_FORNECEDOR V
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = V.CODFORNEC
               WHERE 1 = 1
                 AND NVL(V.DTCOMPENSACAO, V.DTPAGLANC) >= vDATA_MOV_INCREMENTAL
                 AND V.DTPAGLANC IS NOT NULL
                 AND NVL(V.NUMTRANSVENDADEV, 0) > 0
                 AND NVL(V.NUMLANC, 0) > 0
                 AND NVL(V.NUMTRANS, 0) = 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_VERBA_DEV_BAIXA_DUPLIC;

  ----VERBA DE FORNECEDORES - GERADO MANUAL BAIXADO COMO DESCONTO EM DUPLICATA
  FUNCTION FN_VERBA_MANUAL_BAIXA_DUPLIC RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'V04' CODLANC,
                     V.CODEMPRESA,
                     V.CODFILIAL,
                     NVL(V.DTCOMPENSACAO, V.DTPAGLANC) DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     V.NUMTRANSCRFOR IDENTIFICADOR,
                     V.NUMVERBA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     (CASE
                       WHEN V.CODFORNEC IN
                            (SELECT CODFORNEC FROM TABLE(PKG_BI_CONTABILIDADE.FN_FORNECEDOR_ESTRELA_VERBA())) THEN
                        vFORNEC_ESTRELA
                       ELSE
                        V.CODFORNEC
                     END) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vDESCONTOS_OBTIDOS CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     L.CODCC CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('VERBA GERADA DESC. DUPLIC. - F' || LPAD(V.CODFILIAL, 2, 0) || ' - DUPLIC: ' || V.NUMNOTADESC ||
                     ' - Nº VERBA: ' || V.NUMVERBA || ' - Cód: ' || V.NUMTRANSCRFOR) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('DESC. DUPLIC. - Nº VERBA: ' || V.NUMVERBA || ' - DUPLIC: ' || V.NUMNOTADESC || ' - ' ||
                     F.FORNECEDOR || ' - Cód. ' || V.CODFORNEC) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(V.VALOR), 2) VALOR,
                     
                     ('VERBA_MANUAL_DESC_DUPLIC') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_VERBA_FORNECEDOR V
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = V.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = V.CODFILIAL
               WHERE 1 = 1
                 AND NVL(V.DTCOMPENSACAO, V.DTPAGLANC) >= vDATA_MOV_INCREMENTAL
                 AND V.DTPAGLANC IS NOT NULL
                 AND NVL(V.NUMTRANSVENDADEV, 0) = 0
                 AND NVL(V.NUMLANC, 0) > 0
                 AND NVL(V.NUMTRANS, 0) = 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_VERBA_MANUAL_BAIXA_DUPLIC;

  ----VERBA DE FORNECEDORES - GERADO MANUAL RECEBIDO EM DINHEIRO
  FUNCTION FN_VERBA_MANUAL_MOV_BANCO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'V05' CODLANC,
                     V.CODEMPRESA,
                     V.CODFILIAL,
                     V.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     V.NUMTRANSCRFOR IDENTIFICADOR,
                     V.NUMVERBA DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     V.CONTABANCO CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vRECEITA_EXTRA_OPERACIONAL CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     L.CODCC CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     ('VERBA GERADA RECEBIDA EM DINHEIRO - F' || LPAD(V.CODFILIAL, 2, 0) || ' - Nº TRANS: ' ||
                     V.NUMTRANS || ' - Nº VERBA: ' || V.NUMVERBA || ' - Cód: ' || V.NUMTRANSCRFOR) ATIVIDADE,
                     
                     ----------HISTORICO
                     ('RECEITA - Nº VERBA: ' || V.NUMVERBA || ' - Nº TRANS: ' || V.NUMTRANS || ' - ' || F.FORNECEDOR ||
                     ' - Cód. ' || V.CODFORNEC) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(V.VALOR), 2) VALOR,
                     
                     ('VERBA_MANUAL_MOV_BANCO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_VERBA_FORNECEDOR V
                LEFT JOIN BI_SINC_FORNECEDOR F ON F.CODFORNEC = V.CODFORNEC
                LEFT JOIN TABLE(PKG_BI_CONTABILIDADE.FN_CC_FILIAL()) L ON L.CODFILIAL = V.CODFILIAL
               WHERE 1 = 1
                 AND V.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND V.DTCOMPENSACAO IS NOT NULL
                 AND NVL(V.NUMTRANSVENDADEV, 0) = 0
                 AND NVL(V.NUMTRANS, 0) > 0
                 AND NVL(V.CONTABANCO, 0) > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_VERBA_MANUAL_MOV_BANCO;

  ----MOVIMENTACAO ENTRE BANCOS - ENTRADA DE DINHEIRO
  FUNCTION FN_MOV_BANCO_ENTRADA RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'B01' CODLANC,
                     M.CODEMPRESA,
                     M.CODFILIAL,
                     M.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     1 TIPOLANCAMENTO,
                     
                     M.NUMSEQ IDENTIFICADOR,
                     M.NUMTRANS DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     NVL(M.CONTABANCO, vADIANTAMENTO_CLIENTE) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     NULL CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN B.OBSERVACAO = 'MARKETPLACE' THEN
                        ('MOV. BANCO - DEV. ADIANT. MKT - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANS: ' || M.NUMTRANS ||
                        ' - Cód: ' || M.NUMSEQ)
                       ELSE
                        ('MOV. BANCO ENTRADA - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANS: ' || M.NUMTRANS ||
                        ' - Cód: ' || M.NUMSEQ)
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN B.OBSERVACAO = 'MARKETPLACE' THEN
                        ('MOV. BANCO - DEV. ADIANT. MKT - Nº TRANS: ' || M.NUMTRANS || ' - ' || M.HISTORICO)
                       ELSE
                        ('MOV. BANCO ENTRADA - Nº TRANS: ' || M.NUMTRANS || ' - ' || M.HISTORICO)
                     END) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(M.VALOR), 2) VALOR,
                     
                     ('MOV_BANCO_ENTRADA') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_MOV_BANCO M
                LEFT JOIN BI_SINC_BANCO B ON B.CODBANCO = M.CODBANCO
               WHERE 1 = 1
                 AND M.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND M.DTCOMPENSACAO IS NOT NULL
                 AND M.TIPO = 'D')
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_MOV_BANCO_ENTRADA;

  ----MOVIMENTACAO ENTRE BANCOS - SAIDA DE DINHEIRO
  FUNCTION FN_MOV_BANCO_SAIDA RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'B02' CODLANC,
                     M.CODEMPRESA,
                     M.CODFILIAL,
                     M.DTCOMPENSACAO DATA,
                     
                     ----------TIPO LANCAMENTO
                     2 TIPOLANCAMENTO,
                     
                     M.NUMSEQ IDENTIFICADOR,
                     M.NUMTRANS DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     NULL CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     NVL(M.CONTABANCO, vADIANTAMENTO_CLIENTE) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN B.OBSERVACAO = 'MARKETPLACE' THEN
                        ('MOV. BANCO - ADIANT. MKT - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANS: ' || M.NUMTRANS ||
                        ' - Cód: ' || M.NUMSEQ)
                       ELSE
                        ('MOV. BANCO SAIDA - F' || LPAD(M.CODFILIAL, 2, 0) || ' - Nº TRANS: ' || M.NUMTRANS || ' - Cód: ' ||
                        M.NUMSEQ)
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN B.OBSERVACAO = 'MARKETPLACE' THEN
                        ('MOV. BANCO - ADIANT. MKT - Nº TRANS: ' || M.NUMTRANS || ' - ' || M.HISTORICO)
                       ELSE
                        ('MOV. BANCO SAIDA - Nº TRANS: ' || M.NUMTRANS || ' - ' || M.HISTORICO)
                     END) HISTORICO,
                     
                     ----------VALOR
                     ROUND(ABS(M.VALOR), 2) VALOR,
                     
                     ('MOV_BANCO_SAIDA') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'S' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_MOV_BANCO M
                LEFT JOIN BI_SINC_BANCO B ON B.CODBANCO = M.CODBANCO
               WHERE 1 = 1
                 AND M.DTCOMPENSACAO >= vDATA_MOV_INCREMENTAL
                 AND M.DTCOMPENSACAO IS NOT NULL
                 AND M.TIPO = 'C')
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_MOV_BANCO_SAIDA;

  ----APURACAO IMPOSTOS - ICMS
  FUNCTION FN_APURA_ICMS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('AP01' || '.F' || LPAD(A.CODFILIAL, 2, 0)) CODLANC,
                     '1' CODEMPRESA,
                     A.CODFILIAL,
                     A.DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) IDENTIFICADOR,
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vICMS_RECOLHER CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vICMS_RECUPERAR CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN A.VLRECUPERAR > 0 THEN
                        ('APURACAO ICMS - RECUPERAR - F' || LPAD(A.CODFILIAL, 2, 0))
                       ELSE
                        ('APURACAO ICMS - PAGAR - F' || LPAD(A.CODFILIAL, 2, 0))
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN A.VLRECUPERAR > 0 THEN
                        ('APURACAO ICMS - RECUPERAR - F' || LPAD(A.CODFILIAL, 2, 0))
                       ELSE
                        ('APURACAO ICMS - PAGAR - F' || LPAD(A.CODFILIAL, 2, 0))
                     END) HISTORICO,
                     
                     ----------VALOR
                     (CASE
                       WHEN A.VLRECUPERAR > 0 THEN
                        ROUND(A.VLDEBITO, 2)
                       ELSE
                        ROUND(A.VLCREDITO, 2)
                     END) VALOR,
                     
                     ('APURA_ICMS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_APURACAO_ICMS A
               WHERE 1 = 1
                 AND A.DATA >= vDATA_MOV_INCREMENTAL
                 AND A.DATA IS NOT NULL)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_APURA_ICMS;

  ----APURACAO IMPOSTOS - PIS
  FUNCTION FN_APURA_PIS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'AP02' CODLANC,
                     '1' CODEMPRESA,
                     '1' CODFILIAL,
                     A.DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) IDENTIFICADOR,
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vPIS_RECOLHER CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vPIS_RECUPERAR CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN A.VLRECUPERAR > 0 THEN
                        ('APURACAO PIS - RECUPERAR')
                       ELSE
                        ('APURACAO PIS - PAGAR')
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN A.VLRECUPERAR > 0 THEN
                        ('APURACAO PIS - RECUPERAR')
                       ELSE
                        ('APURACAO PIS - PAGAR')
                     END) HISTORICO,
                     
                     ----------VALOR
                     (CASE
                       WHEN A.VLRECUPERAR > 0 THEN
                        ROUND(A.VLDEBITO, 2)
                       ELSE
                        ROUND(A.VLCREDITO, 2)
                     END) VALOR,
                     
                     ('APURA_PIS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_APURACAO_PIS A
               WHERE 1 = 1
                 AND A.DATA >= vDATA_MOV_INCREMENTAL
                 AND A.DATA IS NOT NULL)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_APURA_PIS;

  ----APURACAO IMPOSTOS - COFINS
  FUNCTION FN_APURA_COFINS RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'AP03' CODLANC,
                     '1' CODEMPRESA,
                     '1' CODFILIAL,
                     A.DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) IDENTIFICADOR,
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vCOFINS_RECOLHER CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vCOFINS_RECUPERAR CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN A.VLRECUPERAR > 0 THEN
                        ('APURACAO COFINS - RECUPERAR')
                       ELSE
                        ('APURACAO COFINS - PAGAR')
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN A.VLRECUPERAR > 0 THEN
                        ('APURACAO COFINS - RECUPERAR')
                       ELSE
                        ('APURACAO COFINS - PAGAR')
                     END) HISTORICO,
                     
                     ----------VALOR
                     (CASE
                       WHEN A.VLRECUPERAR > 0 THEN
                        ROUND(A.VLDEBITO, 2)
                       ELSE
                        ROUND(A.VLCREDITO, 2)
                     END) VALOR,
                     
                     ('APURA_COFINS') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_APURACAO_COFINS A
               WHERE 1 = 1
                 AND A.DATA >= vDATA_MOV_INCREMENTAL
                 AND A.DATA IS NOT NULL)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_APURA_COFINS;

  ----APURACAO IMPOSTOS - COMPETE - ESTORNO CREDITO
  FUNCTION FN_APURA_COMPETE_EST_CREDITO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'AC01' CODLANC,
                     '1' CODEMPRESA,
                     '11' CODFILIAL,
                     A.DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) IDENTIFICADOR,
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vESTOQUE CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vICMS_RECOLHER_ES CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     'APURACAO COMPETE - ESTORNO DE CRÉDITO' ATIVIDADE,
                     
                     ----------HISTORICO
                     'APURACAO COMPETE - ESTORNO DE CRÉDITO' HISTORICO,
                     
                     ----------VALOR
                     ROUND(NVL(A.VLESTCRED_COMPETE, 0), 2) + ROUND(NVL(A.VLESTCRED_RED, 0), 2) VALOR,
                     
                     ('APURA_COMPETE_EST_CREDITO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_APURACAO_COMPETE A
               WHERE 1 = 1
                 AND A.DATA >= vDATA_MOV_INCREMENTAL
                 AND A.DATA >= vDT_INICIO_BENEFICIO_ES
                 AND A.DATA IS NOT NULL
                 AND NVL(A.VLESTCRED_COMPETE, 0) + NVL(A.VLESTCRED_RED, 0) > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_APURA_COMPETE_EST_CREDITO;

  ----APURACAO IMPOSTOS - COMPETE - CREDITO LIMITE 7%
  FUNCTION FN_APURA_COMPETE_CREDITO_LIMITE RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (WITH RATEIO_COMPETE AS
                 (SELECT A.DATA,
                        NVL(C.CODGERENTE, 1) CODGERENTE,
                        (A.VLCRED_ALIQPERC + A.VLCRED_RED) VLCRED,
                        NVL(C.PERC, 1) PERC
                   FROM BI_SINC_APURACAO_COMPETE A
                   LEFT JOIN VIEW_BI_SINC_APURA_COMPETE_CC C ON C.DATA = A.DATA),
                
                VALOR_RATEIO_COMPETE AS
                 (SELECT DATA,
                        CODGERENTE,
                        VLCRED,
                        PERC,
                        ROUND((VLCRED * PERC), 2) VALOR
                   FROM RATEIO_COMPETE)
                
                SELECT ('AC02_C.' || G.CODCC) CODLANC,
                       '1' CODEMPRESA,
                       '11' CODFILIAL,
                       A.DATA,
                       
                       ----------TIPO LANCAMENTO
                       3 TIPOLANCAMENTO,
                       
                       TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) IDENTIFICADOR,
                       TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       vICMS_RECUPERAR_ES CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       vSUBVENCAO_FISCAL_ES CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       NULL CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       G.CODCC CODCC_CREDITO,
                       
                       ----------ATIVIDADE
                       ('APURACAO COMPETE - CREDITO LIMITE 7% - RAT: ' || REPLACE(TO_CHAR(A.PERC, '999.00'), '.', ',')) ATIVIDADE,
                       
                       ----------HISTORICO
                       'APURACAO COMPETE - CREDITO LIMITE 7%' HISTORICO,
                       
                       ----------VALOR
                       ROUND(A.VALOR, 2) VALOR,
                       
                       ('APURA_COMPETE_CREDITO_LIMITE') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'N' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM VALOR_RATEIO_COMPETE A
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_GERENTE_ES()) G ON G.CODGERENTE = A.CODGERENTE
                 WHERE 1 = 1
                   AND A.DATA >= vDATA_MOV_INCREMENTAL
                   AND A.DATA >= vDT_INICIO_BENEFICIO_ES
                   AND A.DATA IS NOT NULL
                   AND A.VALOR > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_APURA_COMPETE_CREDITO_LIMITE;

  ----APURACAO IMPOSTOS - COMPETE - DESTINADAS A COMERCIO
  FUNCTION FN_APURA_COMPETE_DEST_COM RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'AC03' CODLANC,
                     '1' CODEMPRESA,
                     '11' CODFILIAL,
                     A.DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) IDENTIFICADOR,
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vICMS_RECUPERAR_ES CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vICMS_RECOLHER_ES CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     'APURACAO COMPETE - DESTINADAS A COMERCIO' ATIVIDADE,
                     
                     ----------HISTORICO
                     'APURACAO COMPETE - DESTINADAS A COMERCIO' HISTORICO,
                     
                     ----------VALOR
                     ROUND(NVL(A.VLCRED_DESTCOM, 0), 2) VALOR,
                     
                     ('APURA_COMPETE_DEST_COMERCIO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_APURACAO_COMPETE A
               WHERE 1 = 1
                 AND A.DATA >= vDATA_MOV_INCREMENTAL
                 AND A.DATA >= vDT_INICIO_BENEFICIO_ES
                 AND A.DATA IS NOT NULL
                 AND A.VLCRED_DESTCOM > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_APURA_COMPETE_DEST_COM;

  ----APURACAO IMPOSTOS - COMPETE - CREDITO PRESUMIDO 1.1%
  FUNCTION FN_APURA_COMPETE_CRED_PRESUMIDO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (WITH RATEIO_COMPETE AS
                 (SELECT A.DATA,
                        NVL(C.CODGERENTE, 1) CODGERENTE,
                        (A.VLCRED_PRESUMIDO) VLCRED,
                        NVL(C.PERC, 1) PERC
                   FROM BI_SINC_APURACAO_COMPETE A
                   LEFT JOIN VIEW_BI_SINC_APURA_COMPETE_CC C ON C.DATA = A.DATA),
                
                VALOR_RATEIO_COMPETE AS
                 (SELECT DATA,
                        CODGERENTE,
                        VLCRED,
                        PERC,
                        ROUND((VLCRED * PERC), 2) VALOR
                   FROM RATEIO_COMPETE)
                
                SELECT ('AC04_C.' || G.CODCC) CODLANC,
                       '1' CODEMPRESA,
                       '11' CODFILIAL,
                       A.DATA,
                       
                       ----------TIPO LANCAMENTO
                       3 TIPOLANCAMENTO,
                       
                       TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) IDENTIFICADOR,
                       TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       vICMS_RECUPERAR_ES CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       vSUBVENCAO_FISCAL_ES CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       NULL CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       G.CODCC CODCC_CREDITO,
                       
                       ----------ATIVIDADE
                       ('APURACAO COMPETE - CREDITO PRESUMIDO - RAT: ' || REPLACE(TO_CHAR(A.PERC, '999.00'), '.', ',')) ATIVIDADE,
                       
                       ----------HISTORICO
                       'APURACAO COMPETE - CREDITO PRESUMIDO' HISTORICO,
                       
                       ----------VALOR
                       ROUND(ABS(A.VALOR), 2) VALOR,
                       
                       ('APURA_COMPETE_CREDITO_PRESUMIDO') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'N' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM VALOR_RATEIO_COMPETE A
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_GERENTE_ES()) G ON G.CODGERENTE = A.CODGERENTE
                 WHERE 1 = 1
                   AND A.DATA >= vDATA_MOV_INCREMENTAL
                   AND A.DATA >= vDT_INICIO_BENEFICIO_ES
                   AND A.DATA IS NOT NULL
                   AND A.VALOR IS NOT NULL)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_APURA_COMPETE_CRED_PRESUMIDO;

  ----APURACAO IMPOSTOS - COMPETE - ADICIONAL INCENTIVO 3.5%
  FUNCTION FN_APURA_COMPETE_ADD_INCENTIVO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (WITH RATEIO_COMPETE AS
                 (SELECT A.DATA,
                        NVL(C.CODGERENTE, 1) CODGERENTE,
                        (A.VLADD_INCENTIVO) VLCRED,
                        NVL(C.PERC, 1) PERC
                   FROM BI_SINC_APURACAO_COMPETE A
                   LEFT JOIN VIEW_BI_SINC_APURA_COMPETE_CC C ON C.DATA = A.DATA),
                
                VALOR_RATEIO_COMPETE AS
                 (SELECT DATA,
                        CODGERENTE,
                        VLCRED,
                        PERC,
                        ROUND((VLCRED * PERC), 2) VALOR
                   FROM RATEIO_COMPETE)
                
                SELECT ('AC05_C.' || G.CODCC) CODLANC,
                       '1' CODEMPRESA,
                       '11' CODFILIAL,
                       A.DATA,
                       
                       ----------TIPO LANCAMENTO
                       3 TIPOLANCAMENTO,
                       
                       TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) IDENTIFICADOR,
                       TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) DOCUMENTO,
                       
                       ----------CONTA_DEBITO
                       vICMS_BENEFICIO_COMPETE CONTADEBITO,
                       
                       ----------CONTA_CREDITO
                       vICMS_RECOLHER_ES CONTACREDITO,
                       
                       ----------CODCC_DEBITO
                       G.CODCC CODCC_DEBITO,
                       
                       ----------CODCC_CREDITO
                       NULL CODCC_CREDITO,
                       
                       ----------ATIVIDADE
                       ('APURACAO COMPETE - ADICIONAL INCENTIVO - RAT: ' || REPLACE(TO_CHAR(A.PERC, '999.00'), '.', ',')) ATIVIDADE,
                       
                       ----------HISTORICO
                       'APURACAO COMPETE - ADICIONAL INCENTIVO ' HISTORICO,
                       
                       ----------VALOR
                       ROUND(A.VALOR, 2) VALOR,
                       
                       ('APURA_COMPETE_ADD_INCENTIVO') ORIGEM,
                       
                       ----------ENVIAR_CONTABIL
                       'N' ENVIAR_CONTABIL,
                       
                       TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                  FROM VALOR_RATEIO_COMPETE A
                  LEFT JOIN TABLE (PKG_BI_CONTABILIDADE.FN_CC_GERENTE_ES()) G ON G.CODGERENTE = A.CODGERENTE
                 WHERE 1 = 1
                   AND A.DATA >= vDATA_MOV_INCREMENTAL
                   AND A.DATA >= vDT_INICIO_BENEFICIO_ES
                   AND A.DATA IS NOT NULL
                   AND A.VALOR > 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_APURA_COMPETE_ADD_INCENTIVO;

  ----APURACAO IMPOSTOS - COMPETE - SALDO APURADO
  FUNCTION FN_APURA_COMPETE_SALDO RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT 'AC06' CODLANC,
                     '1' CODEMPRESA,
                     '11' CODFILIAL,
                     A.DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) IDENTIFICADOR,
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY')) DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     vICMS_RECOLHER_ES CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     vICMS_RECUPERAR_ES CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     NULL CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     NULL CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     'APURACAO COMPETE - SALDO APURADO' ATIVIDADE,
                     
                     ----------HISTORICO
                     'APURACAO COMPETE - SALDO APURADO' HISTORICO,
                     
                     ----------VALOR
                     (CASE
                       WHEN A.DATA < vDT_INICIO_BENEFICIO_ES THEN
                        ROUND(A.VLDEBITO, 2)
                       WHEN (A.VLSALDO > 0 AND A.VLRECUPERAR > 0) THEN
                        (ROUND(NVL(A.VLCREDITO, 0), 2) + ROUND(NVL(A.VLCRED_RED, 0), 2) +
                        ROUND(NVL(A.VLCRED_ALIQPERC, 0), 2) + ROUND(NVL(A.VLCRED_PRESUMIDO, 0), 2) +
                        ROUND(NVL(A.VLCRED_DESTCOM, 0), 2) - ROUND(NVL(A.VLSALDO, 0), 2))
                       ELSE
                        (ROUND(NVL(A.VLCREDITO, 0), 2) + ROUND(NVL(A.VLCRED_RED, 0), 2) +
                        ROUND(NVL(A.VLCRED_ALIQPERC, 0), 2) + ROUND(NVL(A.VLCRED_PRESUMIDO, 0), 2) +
                        ROUND(NVL(A.VLCRED_DESTCOM, 0), 2) + ROUND(NVL(ABS(A.VLSALDO), 0), 2))
                     END) VALOR,
                     
                     ('APURA_COMPETE_SALDO_APURADO') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM BI_SINC_APURACAO_COMPETE A
               WHERE 1 = 1
                 AND A.DATA >= vDATA_MOV_INCREMENTAL
                 AND A.DATA IS NOT NULL
                 AND NVL(A.VLSALDO, 0) <> 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_APURA_COMPETE_SALDO;

  ----APURACAO CMV
  FUNCTION FN_APURA_CMV RETURN T_CONTABIL_TABLE
    PIPELINED IS
  
  BEGIN
    FOR r IN (SELECT ('AV01' || '.F' || LPAD(A.CODFILIAL, 2, 0) || '.CC_' || A.CODCC) CODLANC,
                     A.CODEMPRESA,
                     A.CODFILIAL,
                     A.DATA,
                     
                     ----------TIPO LANCAMENTO
                     3 TIPOLANCAMENTO,
                     
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY') || LPAD(A.CODFILIAL, 2, 0)) IDENTIFICADOR,
                     TO_NUMBER(TO_CHAR(A.DATA, 'DDMMYYYY') || LPAD(A.CODFILIAL, 2, 0)) DOCUMENTO,
                     
                     ----------CONTA_DEBITO
                     (CASE
                       WHEN A.VLCMVRATEIO > 0 THEN
                        vCMV
                       ELSE
                        vESTOQUE
                     END) CONTADEBITO,
                     
                     ----------CONTA_CREDITO
                     (CASE
                       WHEN A.VLCMVRATEIO > 0 THEN
                        vESTOQUE
                       ELSE
                        vCMV
                     END) CONTACREDITO,
                     
                     ----------CODCC_DEBITO
                     (CASE
                       WHEN A.VLCMVRATEIO > 0 THEN
                        A.CODCC
                       ELSE
                        NULL
                     END) CODCC_DEBITO,
                     
                     ----------CODCC_CREDITO
                     (CASE
                       WHEN A.VLCMVRATEIO > 0 THEN
                        NULL
                       ELSE
                        A.CODCC
                     END) CODCC_CREDITO,
                     
                     ----------ATIVIDADE
                     (CASE
                       WHEN A.VLCMVRATEIO > 0 THEN
                        ('APURACAO CMV - F' || LPAD(A.CODFILIAL, 2, 0) || ' - RAT:' ||
                        REPLACE(TO_CHAR(A.PERC, '999.00'), '.', ','))
                       ELSE
                        ('APURACAO CMV DEVOLUÇÃO - F' || LPAD(A.CODFILIAL, 2, 0) || ' - RAT:' ||
                        REPLACE(TO_CHAR(A.PERC, '999.00'), '.', ','))
                     END) ATIVIDADE,
                     
                     ----------HISTORICO
                     (CASE
                       WHEN A.VLCMVRATEIO > 0 THEN
                        ('APURACAO CMV - F' || LPAD(A.CODFILIAL, 2, 0))
                       ELSE
                        ('APURACAO CMV DEVOLUCAO - F' || LPAD(A.CODFILIAL, 2, 0))
                     END) HISTORICO,
                     
                     ----------VALOR
                     ABS(A.VLCMVRATEIO) VALOR,
                     
                     ('APURA_CMV') ORIGEM,
                     
                     ----------ENVIAR_CONTABIL
                     'N' ENVIAR_CONTABIL,
                     
                     TO_DATE(NULL, 'DD/MM/YYYY') DTCANCEL
                FROM VIEW_BI_SINC_APURA_CMV A
               WHERE 1 = 1
                 AND A.DATA >= vDATA_MOV_INCREMENTAL
                 AND A.DATA IS NOT NULL
                 AND NVL(A.VLCMVRATEIO, 0) <> 0)
    
    LOOP
      PIPE ROW(T_CONTABIL_RECORD(r.CODLANC,
                                 r.CODEMPRESA,
                                 r.CODFILIAL,
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
  
  END FN_APURA_CMV;

END PKG_BI_CONTABILIDADE;
/
