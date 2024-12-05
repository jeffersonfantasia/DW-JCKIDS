CREATE OR REPLACE PROCEDURE PRC_SINC_MOV_CONTABIL AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 75;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2014',
                                        'DD/MM/YYYY');

BEGIN
  FOR r IN (WITH MOV_CONTABIL AS
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
               
               )
              
              SELECT M.*
                FROM MOV_CONTABIL M
                LEFT JOIN BI_SINC_MOV_CONTABIL S ON M.CODLANC =
                                                    S.CODLANC
                                                    AND
                                                    S.IDENTIFICADOR =
                                                    M.IDENTIFICADOR
               WHERE 1 = 1
                     AND M.DATA >= vDATA_MOV_INCREMENTAL
                     AND
                     (S.DT_UPDATE IS NULL OR
                     S.CODEMPRESA <> M.CODEMPRESA OR
                     S.DATA <> M.DATA OR
                     NVL(S.DTCANCEL,
                          TO_DATE('01/01/1889', 'DD/MM/YYYY')) <>
                     M.DTCANCEL OR NVL(S.TIPOLANCAMENTO, 0) <>
                     M.TIPOLANCAMENTO OR
                     NVL(S.DOCUMENTO, 0) <> M.DOCUMENTO OR
                     NVL(S.CONTADEBITO, 0) <> NVL(M.CONTADEBITO, 0) OR
                     NVL(S.CONTACREDITO, 0) <> NVL(M.CONTACREDITO, 0) OR
                     NVL(S.CODCC_DEBITO, '0') <> NVL(M.CODCC_DEBITO, '0') OR
                     NVL(S.CODCC_CREDITO, '0') <> NVL(M.CODCC_CREDITO, '0') OR
                     S.ATIVIDADE <> M.ATIVIDADE OR
                     S.HISTORICO <> M.HISTORICO OR
                     S.VALOR <> M.VALOR OR S.ORIGEM <> M.ORIGEM OR
                     NVL(S.ENVIAR_CONTABIL, '0') <>
                     M.ENVIAR_CONTABIL))
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_MOV_CONTABIL
         SET CODEMPRESA      = r.CODEMPRESA,
             DATA            = r.DATA,
             TIPOLANCAMENTO  = r.TIPOLANCAMENTO,
             DOCUMENTO       = r.DOCUMENTO,
             CONTADEBITO     = r.CONTADEBITO,
             CONTACREDITO    = r.CONTACREDITO,
             CODCC_DEBITO    = r.CODCC_DEBITO,
             CODCC_CREDITO   = r.CODCC_CREDITO,
             ATIVIDADE       = r.ATIVIDADE,
             HISTORICO       = r.HISTORICO,
             VALOR           = r.VALOR,
             ORIGEM          = r.ORIGEM,
             ENVIAR_CONTABIL = r.ENVIAR_CONTABIL,
             DTCANCEL        = r.DTCANCEL,
             DT_UPDATE       = SYSDATE
       WHERE CODLANC = r.CODLANC
             AND IDENTIFICADOR = r.IDENTIFICADOR;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_MOV_CONTABIL
          (CODLANC,
           CODEMPRESA,
           DATA,
           TIPOLANCAMENTO,
           IDENTIFICADOR,
           DOCUMENTO,
           CONTADEBITO,
           CONTACREDITO,
           CODCC_DEBITO,
           CODCC_CREDITO,
           ATIVIDADE,
           HISTORICO,
           VALOR,
           ORIGEM,
           ENVIAR_CONTABIL,
           DTCANCEL,
           DT_UPDATE)
        VALUES
          (r.CODLANC,
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
           r.DTCANCEL,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000,
                                'Erro durante a insercao na tabela: ' ||
                                SQLERRM);
    END;
  END LOOP;

  COMMIT;
END;
