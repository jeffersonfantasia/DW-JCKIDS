CREATE OR REPLACE VIEW BI_VIEW_MOV_CONTABIL AS

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
    FROM TABLE(PKG_BI_CONTABILIDADE.FN_DESP_FISCAL_DIFAL()))

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
