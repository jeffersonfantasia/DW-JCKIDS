create or replace PROCEDURE PRC_SINC_DFC_JC AS
  v_mensagem VARCHAR2(4000);
BEGIN
  FOR r IN (
            
              WITH DFC AS
               (SELECT TO_NUMBER(TRIM(TO_CHAR(col001))) CODDFC,
                       TRIM(TO_CHAR(col002)) CONTADFC,
                       TO_NUMBER(TRIM(TO_CHAR(col003))) CODTIPO,
                       TRIM(TO_CHAR(col004)) TIPO,
                       TO_NUMBER(TRIM(TO_CHAR(col005))) CODGRUPODFC,
                       TRIM(TO_CHAR(col006)) GRUPODFC
                  FROM apex_data_parser.parse(p_content           => apex_web_service.make_rest_request_b(p_url         => 'http://10.122.152.7:90/planilhas/DFC.xlsx',
                                                                                                          p_http_method => 'GET'),
                                              p_skip_rows         => 1,
                                              p_detect_data_types => 'S',
                                              p_file_name         => 'DFC.xlsx'))
              SELECT F.*
                FROM DFC F
                LEFT JOIN JEFFERSON.BI_SINC_DFC_JC@DBLINK S ON S.CODDFC = F.CODDFC
               WHERE S.DT_UPDATE IS NULL
                  OR NVL(S.CONTADFC, '') <> F.CONTADFC
                  OR NVL(S.CODTIPO, 0) <> F.CODTIPO
                  OR NVL(S.TIPO, '') <> F.TIPO
                  OR NVL(S.CODGRUPODFC, 0) <> F.CODGRUPODFC
                  OR NVL(S.GRUPODFC, '') <> F.GRUPODFC)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  LOOP
    BEGIN
      UPDATE JEFFERSON.BI_SINC_DFC_JC@DBLINK
         SET CONTADFC    = r.CONTADFC,
             CODTIPO     = r.CODTIPO,
             TIPO        = r.TIPO,
             CODGRUPODFC = r.CODGRUPODFC,
             GRUPODFC    = r.GRUPODFC,
             DT_UPDATE   = SYSDATE
       WHERE CODDFC = r.CODDFC;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO JEFFERSON.BI_SINC_DFC_JC@DBLINK
          (CODDFC,
           CONTADFC,
           CODTIPO,
           TIPO,
           CODGRUPODFC,
           GRUPODFC,
           DT_UPDATE)
        VALUES
          (r.CODDFC,
           r.CONTADFC,
           r.CODTIPO,
           r.TIPO,
           r.CODGRUPODFC,
           r.GRUPODFC,
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
/
