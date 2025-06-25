create or replace PROCEDURE PRC_SINC_DRE_JC AS
 v_mensagem VARCHAR2(4000);
BEGIN
  FOR r IN (
            
              WITH DRE AS
               (SELECT TRIM(TO_CHAR(col001)) CODDRE,
                       TRIM(TO_CHAR(col002)) SUBCONTADRE,
                       TO_NUMBER(col003) SUBTOTAL,
                       TRIM(TO_CHAR(col004)) CODCONTADRE,
                       TRIM(TO_CHAR(col005)) CONTADRE,
                       TRIM(TO_CHAR(col006)) CODGRUPODRE,
                       TRIM(TO_CHAR(col007)) GRUPODRE
                  FROM apex_data_parser.parse(p_content           => apex_web_service.make_rest_request_b(p_url         => 'http://10.122.152.7:90/planilhas/DRE.xlsx',
                                                                                                          p_http_method => 'GET'),
                                              p_skip_rows         => 1,
                                              p_detect_data_types => 'S',
                                              p_file_name         => 'DRE.xlsx'))
              SELECT F.*
                FROM DRE F
                LEFT JOIN JEFFERSON.BI_SINC_DRE_JC@DBLINK S ON S.CODDRE = F.CODDRE
               WHERE S.DT_UPDATE IS NULL
                  OR NVL(S.SUBCONTADRE, '') <> F.SUBCONTADRE
                  OR NVL(S.SUBTOTAL, 0) <> F.SUBTOTAL
                  OR NVL(S.CODCONTADRE, '') <> F.CODCONTADRE
                  OR NVL(S.CONTADRE, '') <> F.CONTADRE
                  OR NVL(S.CODGRUPODRE, '') <> F.CODGRUPODRE
                  OR NVL(S.GRUPODRE, '') <> F.GRUPODRE)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  LOOP
    BEGIN
      UPDATE JEFFERSON.BI_SINC_DRE_JC@DBLINK
         SET SUBCONTADRE = r.SUBCONTADRE,
             SUBTOTAL    = r.SUBTOTAL,
             CODCONTADRE = r.CODCONTADRE,
             CONTADRE    = r.CONTADRE,
             CODGRUPODRE = r.CODGRUPODRE,
             GRUPODRE    = r.GRUPODRE,
             DT_UPDATE   = TRUNC(SYSDATE)
       WHERE CODDRE = r.CODDRE;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO JEFFERSON.BI_SINC_DRE_JC@DBLINK
          (CODDRE,
           SUBCONTADRE,
           SUBTOTAL,
           CODCONTADRE,
           CONTADRE,
           CODGRUPODRE,
           GRUPODRE,
           DT_UPDATE)
        VALUES
          (r.CODDRE,
           r.SUBCONTADRE,
           r.SUBTOTAL,
           r.CODCONTADRE,
           r.CONTADRE,
           r.CODGRUPODRE,
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
 -- Se chegou até aqui, não houve erro
  v_mensagem := 'Processo concluído com sucesso!';
  APEX_UTIL.SET_SESSION_STATE('P11000_RETORNO', v_mensagem);

EXCEPTION
  WHEN OTHERS THEN
    v_mensagem := 'Erro durante a execução: ' || SQLERRM;
    APEX_UTIL.SET_SESSION_STATE('P11000_RETORNO', v_mensagem);
    ROLLBACK;
END;
