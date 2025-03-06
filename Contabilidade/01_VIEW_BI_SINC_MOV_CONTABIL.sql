CREATE OR REPLACE VIEW VIEW_BI_SINC_MOV_CONTABIL AS

     WITH MOV_CONTABIL AS
      (SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_MOV_PROD_VLCONTABIL_INTEIRO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_MOV_PROD_VLCONTABIL_PARCIAL())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_MOV_PROD_CUSTO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_MOV_PROD_ICMS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_MOV_PROD_PIS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_MOV_PROD_COFINS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_MOV_PROD_ST())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_MOV_PROD_DIFAL())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_DESP_FISCAL_VLCONTABIL_INTEIRO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_DESP_FISCAL_VLCONTABIL_PARCIAL())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_DESP_FISCAL_ICMS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_DESP_FISCAL_PIS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_DESP_FISCAL_COFINS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_DESP_FISCAL_DIFAL())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_DESP_GERENCIAL_FORNECEDOR())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_DESP_GERENCIAL_IMPOSTO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_LANC_TIPO_OUTROS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_LANC_TIPO_FORNECEDOR())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_LANC_JUROS_PAGOS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_LANC_DESCONTO_OBTIDO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_LANC_CAIXA_CARTAO_CORP_FORNEC())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_LANC_CAIXA_CARTAO_CORP_OUTROS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_RECEB_DESDOBRAMENTO_CARTAO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_RECEB_INCLUSAO_DUP_BANCO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_RECEB_PAG_INCLUSAO_DUP_BANCO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_RECEB_INCLUSAO_DUP_RECEITA())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_RECEB_BAIXA_DUP_PERDA())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_RECEB_TAXA_CARTAO_LOJA())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_RECEB_DESC_CONCEDIDOS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_RECEB_JUROS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_RECEB_BAIXA_DUPLICATAS())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_RECEB_DEV_CLI_DUPLICATA())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_CRED_ADIANT_CLIENTE())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_CRED_ADIANT_CLIENTE_ESTORNO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_CRED_ADIANT_CLIENTE_RECEITA())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_CRED_ADIANT_CLIENTE_BAIXA_DUP())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_CRED_ADIANT_CLIENTE_DUP_ESTORNO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_CRED_DEV_CLIENTE_RECEITA())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_CRED_DEV_CLIENTE_BAIXA_DUP())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_CRED_DEV_CLIENTE_DUP_ESTORNO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_CRED_DEV_CLIENTE_MOV_BANCO())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_CRED_CONTA_GER_BAIXA_DUP())
       UNION ALL
       SELECT *
         FROM TABLE(PKG_BI_CONTABILIDADE.FN_CRED_CONTA_GER_DUP_ESTORNO()))
     
     SELECT M.CODLANC,
            M.CODEMPRESA,
            M.DATA,
            M.TIPOLANCAMENTO,
            M.IDENTIFICADOR,
            M.DOCUMENTO,
            M.CONTADEBITO,
            M.CONTACREDITO,
            NVL(M.CODCC_DEBITO, '0') CODCC_DEBITO,
            NVL(M.CODCC_CREDITO, '0') CODCC_CREDITO,
            M.ATIVIDADE,
            M.HISTORICO,
            M.VALOR,
            M.ORIGEM,
            M.ENVIAR_CONTABIL,
            M.DTCANCEL
       FROM MOV_CONTABIL M
