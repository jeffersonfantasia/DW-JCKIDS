CREATE OR REPLACE PROCEDURE PRC_SINC_PLANO_CONTAS_ESTILO AS

BEGIN
  FOR r IN (
                   
                     WITH PLANOCONTAS AS
                      (SELECT TRIM(TO_CHAR(col001)) ID,
                              TRIM(TO_CHAR(col002)) CONTA,
                              TRIM(TO_CHAR(col003)) CODCONTA,
                              TRIM(TO_CHAR(col004)) CONTAN1,
                              TRIM(TO_CHAR(col005)) CONTAN2,
                              TRIM(TO_CHAR(col006)) CONTAN3,
                              TRIM(TO_CHAR(col007)) CONTAN4
                         FROM apex_data_parser.parse(p_content           => apex_web_service.make_rest_request_b(p_url         => 'http://10.0.0.6:90/planilhas/PlanoContasEstilo.xlsx',
                                                                                                                 p_http_method => 'GET'),
                                                     p_skip_rows         => 1,
                                                     p_detect_data_types => 'S',
                                                     p_file_name         => 'PlanoContasEstilo.xlsx'))
                     SELECT F.*
                       FROM PLANOCONTAS F
                       LEFT JOIN BI_SINC_PLANO_CONTAS_ESTILO S ON S.ID =
                                                              F.ID
                      WHERE S.DT_UPDATE IS NULL
                         OR S.CONTA <> F.CONTA
                         OR NVL(S.CODCONTA,'0') <> F.CODCONTA
                         OR NVL(S.CONTAN1, 'S') <> F.CONTAN1
                         OR NVL(S.CONTAN2, 'S') <> F.CONTAN2
                         OR NVL(S.CONTAN3, 'S') <> F.CONTAN3
                         OR NVL(S.CONTAN4, 'S') <> F.CONTAN4)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  LOOP
    BEGIN
      UPDATE BI_SINC_PLANO_CONTAS_ESTILO
         SET CONTA        = r.CONTA,
             CODCONTA     = r.CODCONTA,
             CONTAN1      = r.CONTAN1,
             CONTAN2      = r.CONTAN2,
             CONTAN3      = r.CONTAN3,
             CONTAN4      = r.CONTAN4,
             DT_UPDATE    = TRUNC(SYSDATE)
       WHERE ID = r.ID;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_PLANO_CONTAS_ESTILO
          (ID,
           CONTA,
           CODCONTA,
           CONTAN1,
           CONTAN2,
           CONTAN3,
           CONTAN4,
           DT_UPDATE)
        VALUES
          (r.ID,
           r.CONTA,
           r.CODCONTA,
           r.CONTAN1,
           r.CONTAN2,
           r.CONTAN3,
           r.CONTAN4,
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
