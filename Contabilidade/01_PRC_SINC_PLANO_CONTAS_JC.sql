CREATE OR REPLACE PROCEDURE PRC_SINC_PLANO_CONTAS_JC AS

BEGIN
  FOR temp_rec IN (
                   
                     WITH PLANOCONTAS AS
                      (SELECT TRIM(TO_CHAR(col001)) CODCLASSIFICA,
                              TRIM(TO_CHAR(col002)) CONTA,
                              TO_NUMBER(col003) NIVEL,
                              TO_NUMBER(col004) TIPOCONTA,
                              TO_NUMBER(col006) CODGERENCIAL,
                              TO_NUMBER(col007) CODCONTABIL,
                              TO_NUMBER(col008) CODBALANCO,
                              TO_NUMBER(col009) CODDRE,
                              TO_NUMBER(col010) CODEBTIDA,
                              TRIM(TO_CHAR(col011)) CONTAN1,
                              TRIM(TO_CHAR(col012)) CONTAN2,
                              TRIM(TO_CHAR(col013)) CONTAN3,
                              TRIM(TO_CHAR(col014)) CONTAN4,
                              TRIM(TO_CHAR(col015)) CONTAN5
                         FROM apex_data_parser.parse(p_content           => apex_web_service.make_rest_request_b(p_url         => 'http://10.0.0.6:90/planilhas/PlanoContas.xlsx',
                                                                                                                 p_http_method => 'GET'),
                                                     p_skip_rows         => 1,
                                                     p_detect_data_types => 'S',
                                                     p_file_name         => 'PlanoContas.xlsx'))
                     SELECT F.*
                       FROM PLANOCONTAS F
                       LEFT JOIN BI_SINC_PLANO_CONTAS_JC S ON S.CODCLASSIFICA =
                                                              F.CODCLASSIFICA
                      WHERE S.DT_UPDATE IS NULL
                         OR S.CONTA <> F.CONTA
                         OR S.NIVEL <> F.NIVEL
                         OR S.TIPOCONTA <> F.TIPOCONTA
                         OR S.CODGERENCIAL <> F.CODGERENCIAL
                         OR S.CODCONTABIL <> F.CODCONTABIL
                         OR S.CODBALANCO <> F.CODBALANCO
                         OR S.CODDRE <> F.CODDRE
                         OR S.CODEBTIDA <> F.CODEBTIDA
                         OR NVL(S.CONTAN1, 'S') <> F.CONTAN1
                         OR NVL(S.CONTAN2, 'S') <> F.CONTAN2
                         OR NVL(S.CONTAN3, 'S') <> F.CONTAN3
                         OR NVL(S.CONTAN4, 'S') <> F.CONTAN4
                         OR NVL(S.CONTAN5, 'S') <> F.CONTAN5)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  LOOP
    BEGIN
      UPDATE BI_SINC_PLANO_CONTAS_JC
         SET CONTA        = temp_rec.CONTA,
             NIVEL        = temp_rec.NIVEL,
             TIPOCONTA    = temp_rec.TIPOCONTA,
             CODGERENCIAL = temp_rec.CODGERENCIAL,
             CODCONTABIL  = temp_rec.CODCONTABIL,
             CODBALANCO   = temp_rec.CODBALANCO,
             CODDRE       = temp_rec.CODDRE,
             CODEBTIDA    = temp_rec.CODEBTIDA,
             CONTAN1      = temp_rec.CONTAN1,
             CONTAN2      = temp_rec.CONTAN2,
             CONTAN3      = temp_rec.CONTAN3,
             CONTAN4      = temp_rec.CONTAN4,
             CONTAN5      = temp_rec.CONTAN5,
             DT_UPDATE    = TRUNC(SYSDATE)
       WHERE CODCLASSIFICA = temp_rec.CODCLASSIFICA;
    
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
          (temp_rec.CODCLASSIFICA,
           temp_rec.CONTA,
           temp_rec.NIVEL,
           temp_rec.TIPOCONTA,
           temp_rec.CODGERENCIAL,
           temp_rec.CODCONTABIL,
           temp_rec.CODBALANCO,
           temp_rec.CODDRE,
           temp_rec.CODEBTIDA,
           temp_rec.CONTAN1,
           temp_rec.CONTAN2,
           temp_rec.CONTAN3,
           temp_rec.CONTAN4,
           temp_rec.CONTAN5,
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
