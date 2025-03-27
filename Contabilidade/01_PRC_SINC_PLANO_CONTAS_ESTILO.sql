CREATE OR REPLACE PROCEDURE PRC_SINC_PLANO_CONTAS_ESTILO AS
  v_mensagem VARCHAR2(4000);
BEGIN
  FOR r IN (
    WITH PLANOCONTAS AS (
      SELECT TRIM(TO_CHAR(col001)) IDCONTABIL,
             TRIM(TO_CHAR(col002)) CONTA,
             TRIM(TO_CHAR(col003)) CODCONTA,
             TRIM(TO_CHAR(col004)) CONTAN1,
             TRIM(TO_CHAR(col005)) CONTAN2,
             TRIM(TO_CHAR(col006)) CONTAN3,
             TRIM(TO_CHAR(col007)) CONTAN4
        FROM apex_data_parser.parse(
               p_content           => apex_web_service.make_rest_request_b(
                                        p_url         => 'http://10.122.152.7:90/planilhas/PlanoContasEstilo.xlsx',
                                        p_http_method => 'GET'),
               p_skip_rows         => 1,
               p_detect_data_types => 'S',
               p_file_name         => 'PlanoContasEstilo.xlsx')
    )
    SELECT F.*
      FROM PLANOCONTAS F
      LEFT JOIN JEFFERSON.BI_SINC_PLANO_CONTAS_ESTILO@DBLINK S 
        ON S.IDCONTABIL = F.IDCONTABIL
      WHERE S.DT_UPDATE IS NULL
         OR S.CONTA <> F.CONTA
         OR NVL(S.CODCONTA,'0') <> F.CODCONTA
         OR NVL(S.CONTAN1, 'S') <> F.CONTAN1
         OR NVL(S.CONTAN2, 'S') <> F.CONTAN2
         OR NVL(S.CONTAN3, 'S') <> F.CONTAN3
         OR NVL(S.CONTAN4, 'S') <> F.CONTAN4
  )
  LOOP
    BEGIN
      UPDATE JEFFERSON.BI_SINC_PLANO_CONTAS_ESTILO@DBLINK
         SET CONTA        = r.CONTA,
             CODCONTA     = r.CODCONTA,
             CONTAN1      = r.CONTAN1,
             CONTAN2      = r.CONTAN2,
             CONTAN3      = r.CONTAN3,
             CONTAN4      = r.CONTAN4,
             DT_UPDATE    = TRUNC(SYSDATE)
       WHERE IDCONTABIL = r.IDCONTABIL;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO JEFFERSON.BI_SINC_PLANO_CONTAS_ESTILO@DBLINK
          (IDCONTABIL, CONTA, CODCONTA, CONTAN1, CONTAN2, CONTAN3, CONTAN4, DT_UPDATE)
        VALUES
          (r.IDCONTABIL, r.CONTA, r.CODCONTA, r.CONTAN1, r.CONTAN2, r.CONTAN3, r.CONTAN4, SYSDATE);
      END IF;
      
    EXCEPTION
      WHEN OTHERS THEN
        v_mensagem := 'Erro encontrado: ' || SQLERRM;
        APEX_UTIL.SET_SESSION_STATE('P11000_RETORNO', v_mensagem);
        RAISE;
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
