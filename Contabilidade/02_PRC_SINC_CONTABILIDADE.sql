CREATE OR REPLACE PROCEDURE PRC_SINC_CONTABILIDADE AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 90;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

BEGIN
  FOR r IN (WITH CONTABILIDADE AS
               (SELECT C.* FROM VIEW_BI_SINC_MOV_CONTABIL_DC C WHERE C.DATA >= vDATA_MOV_INCREMENTAL)
              
              SELECT M.*
                FROM CONTABILIDADE M
                LEFT JOIN BI_SINC_CONTABILIDADE S ON M.CODLANC = S.CODLANC
                                                 AND S.OPERACAO = M.OPERACAO
               WHERE S.DT_UPDATE IS NULL
                  OR S.CODEMPRESA <> M.CODEMPRESA
                  OR S.CODFILIAL <> M.CODFILIAL
                  OR NVL(S.DATA, TO_DATE('01/01/1889', 'DD/MM/YYYY')) <>
                     NVL(M.DATA, TO_DATE('01/01/1889', 'DD/MM/YYYY'))
                  OR NVL(S.TIPOLANCAMENTO, 0) <> NVL(M.TIPOLANCAMENTO, 0)
                  OR NVL(S.IDENTIFICADOR, 0) <> NVL(M.IDENTIFICADOR, 0)
                  OR NVL(S.DOCUMENTO, 0) <> NVL(M.DOCUMENTO, 0)
                  OR NVL(S.OPERACAO, '0') <> NVL(M.OPERACAO, '0')
                  OR NVL(S.CODGERENCIAL, 0) <> NVL(M.CODGERENCIAL, 0)
                  OR NVL(S.OPER_RAZAO, '0') <> NVL(M.OPER_RAZAO, '0')
                  OR NVL(S.CONTARAZAO, 0) <> NVL(M.CONTARAZAO, 0)
                  OR NVL(S.CODCC, '99') <> NVL(M.CODCC, '99')
                  OR NVL(S.CODDRE, 0) <> NVL(M.CODDRE, 0)
                  OR NVL(S.CODCONTABIL, '0') <> NVL(M.CODCONTABIL, '0')
                  OR NVL(S.RAZAOCONTABIL, '0') <> NVL(M.RAZAOCONTABIL, '0')
                  OR NVL(S.IDGERENCIAL, '0') <> NVL(M.IDGERENCIAL, '0')
                  OR NVL(S.IDCONTABIL, '0') <> NVL(M.IDCONTABIL, '0')
                  --OR S.ATIVIDADE <> M.ATIVIDADE
                  OR S.HISTORICO <> M.HISTORICO
                  OR S.VALOR <> M.VALOR
                  OR S.ORIGEM <> M.ORIGEM
                  OR NVL(S.ENVIAR_CONTABIL, '0') <> M.ENVIAR_CONTABIL)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_CONTABILIDADE
         SET CODEMPRESA      = r.CODEMPRESA,
             CODFILIAL       = r.CODFILIAL,
             DATA            = r.DATA,
             TIPOLANCAMENTO  = r.TIPOLANCAMENTO,
             IDENTIFICADOR   = r.IDENTIFICADOR,
             DOCUMENTO       = r.DOCUMENTO,
             CODGERENCIAL    = r.CODGERENCIAL,
             OPER_RAZAO      = r.OPER_RAZAO,
             CONTARAZAO      = r.CONTARAZAO,
             CODCC           = r.CODCC,
             CODDRE          = r.CODDRE,
             CODCONTABIL     = r.CODCONTABIL,
             RAZAOCONTABIL   = r.RAZAOCONTABIL,
             IDGERENCIAL     = r.IDGERENCIAL,
             IDCONTABIL      = r.IDCONTABIL,
             ATIVIDADE       = r.ATIVIDADE,
             HISTORICO       = r.HISTORICO,
             VALOR           = r.VALOR,
             ORIGEM          = r.ORIGEM,
             ENVIAR_CONTABIL = r.ENVIAR_CONTABIL,
             DT_UPDATE       = SYSDATE
       WHERE CODLANC = r.CODLANC
         AND OPERACAO = r.OPERACAO;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_CONTABILIDADE
          (CODLANC,
           CODEMPRESA,
           CODFILIAL,
           DATA,
           TIPOLANCAMENTO,
           IDENTIFICADOR,
           DOCUMENTO,
           OPERACAO,
           CODGERENCIAL,
           OPER_RAZAO,
           CONTARAZAO,
           CODCC,
           CODDRE,
           CODCONTABIL,
           RAZAOCONTABIL,
           IDGERENCIAL,
           IDCONTABIL,
           ATIVIDADE,
           HISTORICO,
           VALOR,
           ORIGEM,
           ENVIAR_CONTABIL,
           DT_UPDATE)
        VALUES
          (r.CODLANC,
           r.CODEMPRESA,
           r.CODFILIAL,
           r.DATA,
           r.TIPOLANCAMENTO,
           r.IDENTIFICADOR,
           r.DOCUMENTO,
           r.OPERACAO,
           r.CODGERENCIAL,
           r.OPER_RAZAO,
           r.CONTARAZAO,
           r.CODCC,
           r.CODDRE,
           r.CODCONTABIL,
           r.RAZAOCONTABIL,
           r.IDGERENCIAL,
           r.IDCONTABIL,
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
  WHERE (CODLANC) IN
        (SELECT S.CODLANC
           FROM BI_SINC_CONTABILIDADE S
           LEFT JOIN VIEW_BI_SINC_MOV_CONTABIL_DC M ON M.CODLANC = S.CODLANC
          WHERE M.CODLANC IS NULL)';
  END;

  COMMIT;

END;
