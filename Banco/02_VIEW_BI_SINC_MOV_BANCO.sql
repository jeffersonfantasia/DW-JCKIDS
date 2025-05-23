CREATE OR REPLACE VIEW VIEW_BI_SINC_MOV_BANCO AS

    
    WITH MOV_CREDITO AS
     (SELECT M.NUMSEQ NUMSEQ_CRED,
             M.CODFILIAL FILIALCREDITO,
             M.NUMTRANS,
             UPPER(J.CONTA) BANCOCREDITO,
             M.CONTABANCO CONTACREDITO
        FROM BI_SINC_MOV_BANCO M
        LEFT JOIN BI_SINC_PLANO_CONTAS_JC J ON J.CODGERENCIAL = M.CONTABANCO
       WHERE M.TIPO = 'C')
    
    SELECT M.CODEMPRESA,
           M.CODFILIAL,
           C.FILIALCREDITO,
           M.NUMSEQ NUMSEQ_DEB,
           C.NUMSEQ_CRED,
           M.NUMTRANS,
           M.DTCOMPENSACAO,
           M.CONTABANCO CONTADEBITO,
           C.CONTACREDITO,
           UPPER(J.CONTA) BANCODEBITO,
           C.BANCOCREDITO,
           M.VALOR,
           M.HISTORICO,
           M.DT_UPDATE
      FROM BI_SINC_MOV_BANCO M
      LEFT JOIN BI_SINC_PLANO_CONTAS_JC J ON J.CODGERENCIAL = M.CONTABANCO
      LEFT JOIN MOV_CREDITO C ON C.NUMTRANS = M.NUMTRANS
     WHERE M.TIPO = 'D'
