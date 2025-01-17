CREATE OR REPLACE PROCEDURE PRC_SINC_CONTABILIDADE AS

BEGIN
  FOR r IN (WITH LANC_MOV AS
               (SELECT M.CODLANC,
                      M.CODEMPRESA,
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
                      (M.VALOR * -1) VALOR_DEBITO,
                      (M.VALOR) VALOR_CREDITO,
                      M.ORIGEM,
                      M.ENVIAR_CONTABIL
                 FROM VIEW_BI_SINC_MOV_CONTABIL M
                WHERE M.DTCANCEL IS NULL
                  AND M.VALOR <> 0),
              
              CONTA_DEBITO AS
               (SELECT M.CODLANC,
                      M.CODEMPRESA,
                      M.DATA,
                      M.TIPOLANCAMENTO,
                      M.IDENTIFICADOR,
                      M.DOCUMENTO,
                      'D' OPERACAO,
                      M.CONTADEBITO CODGERENCIAL,
                      M.CODCC_DEBITO CODCC,
                      M.ATIVIDADE,
                      M.HISTORICO,
                      M.VALOR_DEBITO VALOR,
                      M.ORIGEM,
                      M.ENVIAR_CONTABIL
                 FROM LANC_MOV M),
              
              CONTA_CREDITO AS
               (SELECT M.CODLANC,
                      M.CODEMPRESA,
                      M.DATA,
                      M.TIPOLANCAMENTO,
                      M.IDENTIFICADOR,
                      M.DOCUMENTO,
                      'C' OPERACAO,
                      M.CONTACREDITO CODGERENCIAL,
                      M.CODCC_CREDITO CODCC,
                      M.ATIVIDADE,
                      M.HISTORICO,
                      VALOR_CREDITO VALOR,
                      M.ORIGEM,
                      M.ENVIAR_CONTABIL
                 FROM LANC_MOV M),
              
              MOVIMENTO_CONTABIL AS
               (SELECT * FROM CONTA_DEBITO UNION ALL SELECT * FROM CONTA_CREDITO),
              
              RESULTADO AS
               (SELECT M.CODLANC,
                      M.CODEMPRESA,
                      M.DATA,
                      M.TIPOLANCAMENTO,
                      M.IDENTIFICADOR,
                      M.DOCUMENTO,
                      M.OPERACAO,
                      M.CODGERENCIAL,
                      M.CODCC,
                      C.CODDRE,
                      C.CODCONTABIL,
                      M.ATIVIDADE,
                      M.HISTORICO,
                      M.VALOR,
                      M.ORIGEM,
                      M.ENVIAR_CONTABIL
                 FROM MOVIMENTO_CONTABIL M
                 LEFT JOIN BI_SINC_PLANO_CONTAS_JC C ON C.CODGERENCIAL = M.CODGERENCIAL)
              
              SELECT M.*
                FROM RESULTADO M
                LEFT JOIN BI_SINC_CONTABILIDADE S ON M.CODLANC = S.CODLANC
                                                 AND S.IDENTIFICADOR = M.IDENTIFICADOR
                                                 AND S.OPERACAO = M.OPERACAO
               WHERE S.DT_UPDATE IS NULL
                  OR S.CODEMPRESA <> M.CODEMPRESA
                  OR NVL(S.DATA, TO_DATE('01/01/1889', 'DD/MM/YYYY')) <>
                     NVL(M.DATA, TO_DATE('01/01/1889', 'DD/MM/YYYY'))
                  OR NVL(S.TIPOLANCAMENTO, 0) <> NVL(M.TIPOLANCAMENTO, 0)
                  OR NVL(S.DOCUMENTO, 0) <> NVL(M.DOCUMENTO, 0)
                  OR NVL(S.OPERACAO, 0) <> NVL(M.OPERACAO, 0)
                  OR NVL(S.CODGERENCIAL, 0) <> NVL(M.CODGERENCIAL, 0)
                  OR NVL(S.CODCC, '99') <> NVL(M.CODCC, '0')
                  OR NVL(S.CODDRE, '0') <> NVL(M.CODDRE, '0')
                  OR NVL(S.CODCONTABIL, '0') <> NVL(M.CODCONTABIL, '0')
                  OR S.ATIVIDADE <> M.ATIVIDADE
                  OR S.HISTORICO <> M.HISTORICO
                  OR S.VALOR <> M.VALOR
                  OR S.ORIGEM <> M.ORIGEM
                  OR NVL(S.ENVIAR_CONTABIL, '0') <> M.ENVIAR_CONTABIL)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_CONTABILIDADE
         SET CODEMPRESA      = r.CODEMPRESA,
             DATA            = r.DATA,
             TIPOLANCAMENTO  = r.TIPOLANCAMENTO,
             DOCUMENTO       = r.DOCUMENTO,
             OPERACAO        = r.OPERACAO,
             CODGERENCIAL    = r.CODGERENCIAL,
             CODCC           = r.CODCC,
             CODDRE          = r.CODDRE,
             CODCONTABIL     = r.CODCONTABIL,
             ATIVIDADE       = r.ATIVIDADE,
             HISTORICO       = r.HISTORICO,
             VALOR           = r.VALOR,
             ORIGEM          = r.ORIGEM,
             ENVIAR_CONTABIL = r.ENVIAR_CONTABIL,
             DT_UPDATE       = SYSDATE
       WHERE CODLANC = r.CODLANC
         AND IDENTIFICADOR = r.IDENTIFICADOR
         AND OPERACAO = r.OPERACAO;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_CONTABILIDADE
          (CODLANC,
           CODEMPRESA,
           DATA,
           TIPOLANCAMENTO,
           IDENTIFICADOR,
           DOCUMENTO,
           OPERACAO,
           CODGERENCIAL,
           CODCC,
           CODDRE,
           CODCONTABIL,
           ATIVIDADE,
           HISTORICO,
           VALOR,
           ORIGEM,
           ENVIAR_CONTABIL,
           DT_UPDATE)
        VALUES
          (r.CODLANC,
           r.CODEMPRESA,
           r.DATA,
           r.TIPOLANCAMENTO,
           r.IDENTIFICADOR,
           r.DOCUMENTO,
           r.OPERACAO,
           r.CODGERENCIAL,
           r.CODCC,
           r.CODDRE,
           r.CODCONTABIL,
           r.ATIVIDADE,
           r.HISTORICO,
           r.VALOR,
           r.ORIGEM,
           r.ENVIAR_CONTABIL,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

  BEGIN
     EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_CONTABILIDADE
  WHERE (CODLANC, IDENTIFICADOR) IN
        (SELECT S.CODLANC,
                S.IDENTIFICADOR
           FROM BI_SINC_CONTABILIDADE S
           LEFT JOIN VIEW_BI_SINC_MOV_CONTABIL M ON M.CODLANC = S.CODLANC
                                               AND M.IDENTIFICADOR =
                                               S.IDENTIFICADOR
          WHERE M.CODLANC IS NULL)';
   END;
  
   COMMIT;

END;
