CREATE OR REPLACE PROCEDURE PRC_SINC_MOV_FOLHA AS

BEGIN
  FOR r IN (
            
              WITH FOLHA_EXCEL AS
               (SELECT TRIM(TO_CHAR(col001)) CODFILIAL,
                       TRIM(TO_DATE(col002, 'DD/MM/YYYY')) DATA,
                       TO_NUMBER(col003) NUMLANC,
                       TRIM(TO_CHAR(col004)) HISTORICO,
                       TO_NUMBER(col005) CONTADEBIRO,
                       TO_NUMBER(col006) CONTACREDITO,
                       TRIM(TO_CHAR(col007)) CODCC_DEBITO,
                       TRIM(TO_CHAR(col008)) CODCC_CREDITO,
                       TO_NUMBER(col010) VALOR
                  FROM apex_data_parser.parse(p_content           => apex_web_service.make_rest_request_b(p_url         => 'http://10.0.0.6:90/planilhas/Folha.xlsx',
                                                                                                          p_http_method => 'GET'),
                                              p_skip_rows         => 1,
                                              p_detect_data_types => 'S',
                                              p_file_name         => 'Folha.xlsx')),
              
              FOLHA AS
               (SELECT ('F01' || '.I_' || P.NUMLANC || '.CC_' || COALESCE(F.CODCC_DEBITO, F.CODCC_CREDITO)) CODLANC,
                       P.CODEMPRESA,
                       F.DATA,
                       3 TIPOLANCAMENTO,
                       F.NUMLANC IDENTIFICADOR,
                       F.NUMLANC DOCUMENTO,
                       F.CONTADEBITO,
                       F.CONTACREDITO,
                       F.CODCC_DEBITO,
                       F.CODCC_CREDITO,
                       ('MOV. FOLHA - F' || LPAD(F.CODFILIAL, 2, 0) || ' - RAT: ' ||
                       REPLACE(TO_CHAR(COALESCE(F.CODCC_DEBITO, F.CODCC_CREDITO), '999.00'), '.', ',') || '% - Nº LANC: ' ||
                       F.NUMLANC) ATIVIDADE,
                       ('F' || LPAD(F.CODFILIAL, 2, 0) || ' - ' || F.HISTORICO) HISTORICO,
                       F.VALOR,
                       ('MOV_FOLHA') ORIGEM,
                       'N' ENVIAR_CONTABIL
                  FROM FOLHA_EXCEL
                  LEFT JOIN BI_SINC_FILIAL P ON P.CODFILIAL = F.CODFILIAL)
              
              SELECT F.*
                FROM FOLHA F
                LEFT JOIN BI_SINC_MOV_FOLHA S ON S.NUMLANC = F.NUMLANC
                                             AND S.CODLANC = F.CODLANC
               WHERE S.DT_UPDATE IS NULL
                  OR S.CODEMPRESA <> F.CODEMPRESA
                  OR NVL(S.DATA, '01/01/1899') <> F.DATA
                  OR NVL(S.TIPOLANCAMENTO, 0) <> F.TIPOLANCAMENTO
                  OR NVL(S.IDENTIFICADOR, 0) <> F.IDENTIFICADOR
                  OR NVL(S.DOCUMENTO, 0) <> F.DOCUMENTO
                  OR NVL(S.CONTADEBITO, 0) <> F.CONTADEBITO
                  OR NVL(S.CONTACREDITO, 0) <> F.CONTACREDITO
                  OR NVL(S.CODCC_DEBITO, '0') <> NVL(F.CODCC_DEBITO, '0')
                  OR NVL(S.CODCC_CREDITO, '0') <> NVL(F.CODCC_CREDITO, '0')
                  OR NVL(S.ATIVIDADE, '0') <> NVL(F.ATIVIDADE, '0')
                  OR NVL(S.HISTORICO, '0') <> NVL(F.HISTORICO, '0')
                  OR NVL(S.VALOR, 0) <> NVL(F.VALOR, 0)
                  OR NVL(S.ORIGEM, '0') <> NVL(F.ORIGEM, '0')
                  OR NVL(S.ENVIAR_CONTABIL, '0') <> NVL(F.ENVIAR_CONTABIL, '0'))
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  LOOP
    BEGIN
      UPDATE BI_SINC_PLANO_CONTAS_JC
         SET CONTA        = r.CONTA,
             NIVEL        = r.NIVEL,
             TIPOCONTA    = r.TIPOCONTA,
             CODGERENCIAL = r.CODGERENCIAL,
             CODCONTABIL  = r.CODCONTABIL,
             CODBALANCO   = r.CODBALANCO,
             CODDRE       = r.CODDRE,
             CODEBTIDA    = r.CODEBTIDA,
             CONTAN1      = r.CONTAN1,
             CONTAN2      = r.CONTAN2,
             CONTAN3      = r.CONTAN3,
             CONTAN4      = r.CONTAN4,
             CONTAN5      = r.CONTAN5,
             DT_UPDATE    = TRUNC(SYSDATE)
       WHERE CODCLASSIFICA = r.CODCLASSIFICA;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_PLANO_CONTAS_JC
          (CODCLASSIFICA,
           CONTA,
           NIVEL,
           TIPOCONTA,
           CODGERENCIAL,
           CODCONTABIL,
           CODBALANCO,
           CODDRE,
           CODEBTIDA,
           CONTAN1,
           CONTAN2,
           CONTAN3,
           CONTAN4,
           CONTAN5,
           DT_UPDATE)
        VALUES
          (r.CODCLASSIFICA,
           r.CONTA,
           r.NIVEL,
           r.TIPOCONTA,
           r.CODGERENCIAL,
           r.CODCONTABIL,
           r.CODBALANCO,
           r.CODDRE,
           r.CODEBTIDA,
           r.CONTAN1,
           r.CONTAN2,
           r.CONTAN3,
           r.CONTAN4,
           r.CONTAN5,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

END;
