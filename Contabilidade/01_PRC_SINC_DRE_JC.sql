CREATE OR REPLACE PROCEDURE PRC_SINC_DRE_JC AS

BEGIN
  FOR r IN (
            
              WITH DRE AS
               (SELECT TRIM(TO_CHAR(col001)) CODDRE,
                       TRIM(TO_CHAR(col002)) SUBCONTADRE,
                       TO_NUMBER(col003) SUBTOTAL,
                       TRIM(TO_CHAR(col004)) CONTADRE,
                       TRIM(TO_CHAR(col005)) GRUPODRE
                  FROM apex_data_parser.parse(p_content           => apex_web_service.make_rest_request_b(p_url         => 'http://10.122.152.4:90/planilhas/DRE.xlsx',
                                                                                                          p_http_method => 'GET'),
                                              p_skip_rows         => 1,
                                              p_detect_data_types => 'S',
                                              p_file_name         => 'DRE.xlsx'))
              SELECT F.*
                FROM DRE F
                LEFT JOIN BI_SINC_DRE_JC S ON S.CODDRE = F.CODDRE
               WHERE S.DT_UPDATE IS NULL
                  OR NVL(S.SUBCONTADRE, '') <> F.SUBCONTADRE
                  OR NVL(S.SUBTOTAL, 0) <> F.SUBTOTAL
                  OR NVL(S.CONTADRE, '') <> F.CONTADRE
                  OR NVL(S.GRUPODRE, '') <> F.GRUPODRE)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condi��es mencionadas
  LOOP
    BEGIN
      UPDATE BI_SINC_DRE_JC
         SET SUBCONTADRE = r.SUBCONTADRE,
             SUBTOTAL    = r.SUBTOTAL,
             CONTADRE    = r.CONTADRE,
             GRUPODRE    = r.GRUPODRE,
             DT_UPDATE   = TRUNC(SYSDATE)
       WHERE CODDRE = r.CODDRE;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_DRE_JC
          (CODDRE,
           SUBCONTADRE,
           SUBTOTAL,
           CONTADRE,
           GRUPODRE,
           DT_UPDATE)
        VALUES
          (r.CODDRE,
           r.SUBCONTADRE,
           r.SUBTOTAL,
           r.CONTADRE,
           r.GRUPODRE,
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
