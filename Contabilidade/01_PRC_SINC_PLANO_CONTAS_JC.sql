create or replace PROCEDURE PRC_SINC_PLANO_CONTAS_JC AS
  v_mensagem VARCHAR2(4000);

BEGIN
  FOR r IN (
                   
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
                              TO_NUMBER(col011) CODDFC,
                              TRIM(TO_CHAR(col012)) CONTAN1,
                              TRIM(TO_CHAR(col013)) CONTAN2,
                              TRIM(TO_CHAR(col014)) CONTAN3,
                              TRIM(TO_CHAR(col015)) CONTAN4,
                              TRIM(TO_CHAR(col016)) CONTAN5
                         FROM apex_data_parser.parse(p_content           => apex_web_service.make_rest_request_b(p_url         => 'http://10.122.152.7:90/planilhas/PlanoContas.xlsx',
                                                                                                                 p_http_method => 'GET'),
                                                     p_skip_rows         => 1,
                                                     p_detect_data_types => 'S',
                                                     p_file_name         => 'PlanoContas.xlsx'))
                     SELECT F.*
                       FROM PLANOCONTAS F
                       LEFT JOIN JEFFERSON.BI_SINC_PLANO_CONTAS_JC@DBLINK S ON S.CODCLASSIFICA =
                                                              F.CODCLASSIFICA
                      WHERE S.DT_UPDATE IS NULL
                         OR S.CONTA <> F.CONTA
                         OR NVL(S.NIVEL,0) <> F.NIVEL
                         OR NVL(S.TIPOCONTA,0) <> F.TIPOCONTA
                         OR NVL(S.CODGERENCIAL,0) <> F.CODGERENCIAL
                         OR NVL(S.CODCONTABIL,0) <> F.CODCONTABIL
                         OR NVL(S.CODBALANCO,0) <> F.CODBALANCO
                         OR NVL(S.CODDRE,0) <> F.CODDRE
                         OR NVL(S.CODEBTIDA,0) <> F.CODEBTIDA
                         OR NVL(S.CODDFC,0) <> F.CODDFC
                         OR NVL(S.CONTAN1, 'S') <> F.CONTAN1
                         OR NVL(S.CONTAN2, 'S') <> F.CONTAN2
                         OR NVL(S.CONTAN3, 'S') <> F.CONTAN3
                         OR NVL(S.CONTAN4, 'S') <> F.CONTAN4
                         OR NVL(S.CONTAN5, 'S') <> F.CONTAN5)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  LOOP
    BEGIN
      UPDATE JEFFERSON.BI_SINC_PLANO_CONTAS_JC@DBLINK
         SET CONTA        = r.CONTA,
             NIVEL        = r.NIVEL,
             TIPOCONTA    = r.TIPOCONTA,
             CODGERENCIAL = r.CODGERENCIAL,
             CODCONTABIL  = r.CODCONTABIL,
             CODBALANCO   = r.CODBALANCO,
             CODDRE       = r.CODDRE,
             CODEBTIDA    = r.CODEBTIDA,
             CODDFC       = r.CODDFC,
             CONTAN1      = r.CONTAN1,
             CONTAN2      = r.CONTAN2,
             CONTAN3      = r.CONTAN3,
             CONTAN4      = r.CONTAN4,
             CONTAN5      = r.CONTAN5,
             DT_UPDATE    = TRUNC(SYSDATE)
       WHERE CODCLASSIFICA = r.CODCLASSIFICA;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO JEFFERSON.BI_SINC_PLANO_CONTAS_JC@DBLINK
          (CODCLASSIFICA,
           CONTA,
           NIVEL,
           TIPOCONTA,
           CODGERENCIAL,
           CODCONTABIL,
           CODBALANCO,
           CODDRE,
           CODEBTIDA,
           CODDFC,
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
           r.CODDFC,
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
        RAISE_APPLICATION_ERROR(-20000,
                                'Erro durante a insercao na tabela: ' ||
                                SQLERRM);
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
