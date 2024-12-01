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
    vCC_SPMARKET        VARCHAR(3) := '1.1';
    vCC_PARQUE          VARCHAR(3) := '1.2';
    vCC_JUNDIAI         VARCHAR(3) := '1.3';
    vCC_TRIMAIS         VARCHAR(3) := '1.4';
    vCC_CAMPINAS        VARCHAR(3) := '1.5';
    vCC_DISTRIBUICAO_SP VARCHAR(3) := '2.1';
    vCC_DISTRIBUICAO_ES VARCHAR(3) := '2.2';
    vCC_ECOMMERCE_SP    VARCHAR(3) := '3.1';
    vCC_CORPORATIVO_SP  VARCHAR(3) := '4.1';
  
  BEGIN
    FOR r IN (WITH FILIAL_CC AS
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
                         WHEN M.TIPOMOV IN
                              ('SAIDA DEVOLUCAO', 'SAIDA TRANSFERENCIA') THEN
                          1
                         WHEN M.TIPOMOV IN ('ENTRADA COMPRA',
                                            'ENTRADA COMPRA CONSIGNADO',
                                            'ENTRADA COMPRA TRIANGULAR',
                                            'ENTRADA BONIFICADA') THEN
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
                          1152
                         WHEN M.TIPOMOV IN ('SAIDA REM ENTREGA FUTURA') THEN
                          1182
                         WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO CONSIGNADO') THEN
                          2200
                         WHEN M.TIPOMOV IN ('SAIDA SIMPLES REMESSA') THEN
                          3118
                         WHEN M.TIPOMOV IN ('SAIDA CONSERTO') THEN
                          3117
                         WHEN M.TIPOMOV IN ('SAIDA DEMONSTRACAO') THEN
                          3119
                         WHEN M.TIPOMOV IN ('SAIDA PERDA MERCADORIA') THEN
                          3114
                         WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO') THEN
                          1178
                         WHEN M.TIPOMOV IN ('SAIDA TRANSFERENCIA') THEN
                          1183
                         ELSE
                          NULL
                       END) CONTADEBITO,
                       
                       ----------CONTACREDITO
                       (CASE
                         WHEN M.TIPOMOV IN
                              ('SAIDA VENDA',
                               'SAIDA FAT CONTA E ORDEM',
                               'SAIDA REM ENTREGA FUTURA') THEN
                          3101
                         WHEN M.TIPOMOV IN ('SAIDA FAT ENTREGA FUTURA') THEN
                          1182
                         WHEN M.TIPOMOV IN ('SAIDA DEVOLUCAO CONSIGNADO') THEN
                          1176
                         WHEN M.TIPOMOV IN ('SAIDA SIMPLES REMESSA') THEN
                          3121
                         WHEN M.TIPOMOV IN ('SAIDA CONSERTO') THEN
                          3120
                         WHEN M.TIPOMOV IN ('SAIDA DEMONSTRACAO') THEN
                          3122
                         WHEN M.TIPOMOV IN ('SAIDA PERDA MERCADORIA') THEN
                          1174
                         WHEN M.TIPOMOV IN
                              ('ENTRADA COMPRA',
                               'ENTRADA COMPRA CONSIGNADO',
                               'ENTRADA COMPRA TRIANGULAR') THEN
                          NVL(C.CODCONTABIL, 999999)
                         WHEN M.TIPOMOV IN ('ENTRADA BONIFICADA') THEN
                          3115
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
                               'SAIDA REM ENTREGA FUTURA') THEN
                          (CASE
                            WHEN M.CODGERENTE IN (1, 8, 9, 10) AND
                                 M.CODFILIAL = '11' THEN vCC_DISTRIBUICAO_ES
                            ELSE
                             V.CODCC
                          END)
                         WHEN M.TIPOMOV IN ('ENTRADA BONIFICADA') THEN
                          F.CODCC
                         ELSE
                          NULL
                       END) CODCC_CREDITO,
                       
                       (M.TIPOMOV || ' - Nº TRANS: ' || M.NUMTRANSACAO) ATIVIDADE,
                       
                       ----------ENVIAR_HISTORICO
                       (CASE
                         WHEN M.MOVIMENTO = 'S' THEN
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
                   AND M.TIPOMOV IN ('SAIDA VENDA',
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
                                     'ENTRADA BONIFICADA'))
    
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
