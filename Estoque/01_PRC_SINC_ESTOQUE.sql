CREATE OR REPLACE PROCEDURE PRC_SINC_ESTOQUE AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCEST
    (CODFILIAL,
     CODPROD,
     QTCONTABIL,
     QTGERENCIAL,
     QTBLOQUEADA,
     QTRESERVADA,
     QTAVARIADA,
     QTFRENTELOJA,
     VALORULTENT,
     CUSTOREPOSICAO,
     CUSTOFINANCEIRO,
     CUSTOCONTABIL,
     CODBLOQUEIO,
     MOTIVOBLOQUEIO)
    SELECT E.CODFILIAL,
           E.CODPROD,
           E.QTEST,
           E.QTESTGER,
           E.QTBLOQUEADA,
           E.QTRESERV,
           E.QTINDENIZ,
           E.QTFRENTELOJA,
           E.VALORULTENT,
           E.CUSTOREP,
           E.CUSTOFIN,
           E.CUSTOCONT,
           E.CODDEVOL,
           D.MOTIVO
      FROM PCEST E
      LEFT JOIN PCTABDEV D ON D.CODDEVOL = E.CODDEVOL
      LEFT JOIN BI_SINC_ESTOQUE S ON S.CODFILIAL = E.CODFILIAL
                                 AND S.CODPROD = E.CODPROD
     WHERE S.DT_UPDATE IS NULL
        OR S.QTCONTABIL <> E.QTEST
        OR S.QTGERENCIAL <> E.QTESTGER
        OR S.QTBLOQUEADA <> E.QTBLOQUEADA
        OR S.QTRESERVADA <> E.QTRESERV
        OR S.QTAVARIADA <> E.QTINDENIZ
        OR S.QTFRENTELOJA <> E.QTFRENTELOJA
        OR S.VALORULTENT <> E.VALORULTENT
        OR S.CUSTOREPOSICAO <> E.CUSTOREP
        OR S.CUSTOFINANCEIRO <> E.CUSTOFIN
        OR S.CUSTOCONTABIL <> E.CUSTOCONT
        OR S.CODBLOQUEIO <> E.CODDEVOL
        OR S.MOTIVOBLOQUEIO <> D.MOTIVO;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCEST)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_ESTOQUE
         SET QTCONTABIL      = temp_rec.QTCONTABIL,
             QTGERENCIAL     = temp_rec.QTGERENCIAL,
             QTBLOQUEADA     = temp_rec.QTBLOQUEADA,
             QTRESERVADA     = temp_rec.QTRESERVADA,
             QTAVARIADA      = temp_rec.QTAVARIADA,
             QTFRENTELOJA    = temp_rec.QTFRENTELOJA,
             VALORULTENT     = temp_rec.VALORULTENT,
             CUSTOREPOSICAO  = temp_rec.CUSTOREPOSICAO,
             CUSTOFINANCEIRO = temp_rec.CUSTOFINANCEIRO,
             CUSTOCONTABIL   = temp_rec.CUSTOCONTABIL,
             CODBLOQUEIO     = temp_rec.CODBLOQUEIO,
             MOTIVOBLOQUEIO  = temp_rec.MOTIVOBLOQUEIO,
             DT_UPDATE       = SYSDATE
       WHERE CODFILIAL = temp_rec.CODFILIAL
         AND CODPROD = temp_rec.CODPROD;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_ESTOQUE
          (CODFILIAL,
           CODPROD,
           QTCONTABIL,
           QTGERENCIAL,
           QTBLOQUEADA,
           QTRESERVADA,
           QTAVARIADA,
           QTFRENTELOJA,
           VALORULTENT,
           CUSTOREPOSICAO,
           CUSTOFINANCEIRO,
           CUSTOCONTABIL,
           CODBLOQUEIO,
           MOTIVOBLOQUEIO,
           DT_UPDATE,
           DT_SINC)
        VALUES
          (temp_rec.CODFILIAL,
           temp_rec.CODPROD,
           temp_rec.QTCONTABIL,
           temp_rec.QTGERENCIAL,
           temp_rec.QTBLOQUEADA,
           temp_rec.QTRESERVADA,
           temp_rec.QTAVARIADA,
           temp_rec.QTFRENTELOJA,
           temp_rec.VALORULTENT,
           temp_rec.CUSTOREPOSICAO,
           temp_rec.CUSTOFINANCEIRO,
           temp_rec.CUSTOCONTABIL,
           temp_rec.CODBLOQUEIO,
           temp_rec.MOTIVOBLOQUEIO,
           SYSDATE,
           NULL);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000,
                                'Erro durante a criação da tabela: ' ||
                                SQLERRM);
    END;
  END LOOP;

  COMMIT;

  -- Exclui os registros da tabela temporária TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_PCEST';
END;
