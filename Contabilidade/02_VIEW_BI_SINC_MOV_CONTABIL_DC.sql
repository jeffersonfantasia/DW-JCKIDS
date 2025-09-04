CREATE OR REPLACE VIEW VIEW_BI_SINC_MOV_CONTABIL_DC AS 

    WITH LANC_MOV AS
     (SELECT M.CODLANC,
             M.CODEMPRESA,
             M.CODFILIAL,
             M.DATA,
             M.TIPOLANCAMENTO,
             M.IDENTIFICADOR,
             M.DOCUMENTO,
             M.CONTADEBITO,
             M.CODCC_DEBITO,
             M.CONTACREDITO,
             M.CODCC_CREDITO,
             M.ATIVIDADE,
             M.HISTORICO,
             ROUND(ABS(M.VALOR), 2) VALOR_DEBITO,
             ROUND(ABS(M.VALOR), 2) VALOR_CREDITO,
             M.ORIGEM,
             M.ENVIAR_CONTABIL
        FROM VIEW_BI_SINC_MOV_CONTABIL M
       WHERE M.DTCANCEL IS NULL
         AND M.VALOR <> 0),
    
    CONTA_DEBITO AS
     (SELECT (M.CODLANC || '_I.' || M.IDENTIFICADOR || '_D.' || M.CONTADEBITO) CODLANC,
             M.CODEMPRESA,
             M.CODFILIAL,
             M.DATA,
             M.TIPOLANCAMENTO,
             M.IDENTIFICADOR,
             M.DOCUMENTO,
             'D' OPERACAO,
             M.CONTADEBITO CODGERENCIAL,
             'C' OPER_RAZAO,
             M.CONTACREDITO CONTARAZAO,
             M.CODCC_DEBITO CODCC,
             M.ATIVIDADE,
             M.HISTORICO,
             M.VALOR_DEBITO VALOR,
             M.ORIGEM,
             M.ENVIAR_CONTABIL
        FROM LANC_MOV M
       WHERE M.CONTADEBITO IS NOT NULL),
    
    CONTA_CREDITO AS
     (SELECT (M.CODLANC || '_I.' || M.IDENTIFICADOR || '_C.' || M.CONTACREDITO) CODLANC,
             M.CODEMPRESA,
             M.CODFILIAL,
             M.DATA,
             M.TIPOLANCAMENTO,
             M.IDENTIFICADOR,
             M.DOCUMENTO,
             'C' OPERACAO,
             M.CONTACREDITO CODGERENCIAL,
             'D' OPER_RAZAO,
             M.CONTADEBITO CONTARAZAO,
             M.CODCC_CREDITO CODCC,
             M.ATIVIDADE,
             M.HISTORICO,
             (VALOR_CREDITO * -1) VALOR,
             M.ORIGEM,
             M.ENVIAR_CONTABIL
        FROM LANC_MOV M
       WHERE M.CONTACREDITO IS NOT NULL),
    
    MOVIMENTO_CONTABIL AS
     (SELECT * FROM CONTA_DEBITO UNION ALL SELECT * FROM CONTA_CREDITO),
    
    RESULTADO AS
     (SELECT M.CODLANC,
             M.CODEMPRESA,
             M.CODFILIAL,
             M.DATA,
             M.TIPOLANCAMENTO,
             M.IDENTIFICADOR,
             M.DOCUMENTO,
             M.OPERACAO,
             M.CODGERENCIAL,
             M.OPER_RAZAO,
             M.CONTARAZAO,
             C2.CONTAN5 DESC_CONTARAZAO,
             M.CODCC,
             C.CODDRE,
             (CASE
               WHEN C.CODDFC = 18
                    AND M.OPERACAO = 'C' THEN
                17
               ELSE
                NVL(C.CODDFC, 0)
             END) CODDFC,
             NVL(C2.CODDFC, 0) CODDFC_CTARAZAO,
             C.CODCONTABIL,
             C2.CODCONTABIL RAZAOCONTABIL,
             (M.CODGERENCIAL || '-' || M.CODEMPRESA) IDGERENCIAL,
             (C.CODCONTABIL || '-' || M.CODEMPRESA) IDCONTABIL,
             M.ATIVIDADE,
             M.HISTORICO,
             M.VALOR,
             M.ORIGEM,
             M.ENVIAR_CONTABIL
        FROM MOVIMENTO_CONTABIL M
        LEFT JOIN VIEW_BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = M.CODGERENCIAL
                                                AND C.CODEMPRESA = M.CODEMPRESA
        LEFT JOIN VIEW_BI_SINC_PLANO_CONTAS_JC C2 ON C2.CODGERENCIAL = M.CONTARAZAO
                                                 AND C2.CODEMPRESA = M.CODEMPRESA)
    
    SELECT M.* FROM RESULTADO M
