CREATE OR REPLACE PROCEDURE PRC_SINC_ESTOQUE AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCEST
    (CODFILIAL,
     CODPROD,
     QTCONTABIL,
     QTGERENCIAL,
     QTBLOQUEADA,
     QTPENDENTE,
     QTRESERVADA,
     QTAVARIADA,
     QTDISPONIVEL,
     QTFRENTELOJA,
     QTDEPOSITO,
     VALORULTENT,
     CUSTOREPOSICAO,
     CUSTOFINANCEIRO,
     CUSTOCONTABIL,
     CODBLOQUEIO,
     MOTIVOBLOQUEIO)
    WITH ESTOQUE AS
     (SELECT E.CODFILIAL,
             E.CODPROD,
             NVL(E.QTEST, 0) QTCONTABIL,
             NVL(E.QTESTGER, 0) QTGERENCIAL,
             NVL(E.QTBLOQUEADA, 0) QTBLOQUEADA,
             NVL(E.QTPENDENTE, 0) QTPENDENTE,
             NVL(E.QTRESERV, 0) QTRESERVADA,
             NVL(E.QTINDENIZ, 0) QTAVARIADA,
             (NVL(E.QTESTGER, 0) - NVL(E.QTBLOQUEADA, 0) -
             NVL(E.QTRESERV, 0)) QTDISPONIVEL,
             NVL(E.QTFRENTELOJA, 0) QTFRENTELOJA,
             (NVL(E.QTESTGER, 0) - NVL(E.QTFRENTELOJA, 0)) QTDEPOSITO,
             NVL(E.VALORULTENT, 0) VALORULTENT,
             NVL(E.CUSTOREP, 0) CUSTOREPOSICAO,
             NVL(E.CUSTOFIN, 0) CUSTOFINANCEIRO,
             NVL(E.CUSTOCONT, 0) CUSTOCONTABIL,
             (CASE
               WHEN (E.CODDEVOL = 29 AND NVL(E.QTBLOQUEADA, 0) > 0 AND  NVL(E.QTINDENIZ, 0) <= 0) THEN E.CODDEVOL
               WHEN (NVL(E.CODDEVOL, 0) IN (0, 29) OR NVL(E.QTINDENIZ, 0) <= 0 OR NVL(E.QTBLOQUEADA, 0) <= 0) THEN 0
               ELSE E.CODDEVOL
             END) CODBLOQUEIO,
             (CASE
               WHEN (E.CODDEVOL = 29 AND NVL(E.QTBLOQUEADA, 0) > 0 AND NVL(E.QTINDENIZ, 0) <= 0) THEN 'ENTRADA MERCADORIA'
               WHEN (NVL(E.CODDEVOL, 0) IN (0, 29) OR NVL(E.QTINDENIZ, 0) <= 0 OR NVL(E.QTBLOQUEADA, 0) <= 0) THEN 'SEM BLOQUEIO'
               ELSE D.MOTIVO
             END) MOTIVOBLOQUEIO
        FROM PCEST E
        JOIN BI_SINC_PRODUTO PR ON PR.CODPROD = E.CODPROD
        LEFT JOIN PCTABDEV D ON D.CODDEVOL = E.CODDEVOL)
    SELECT E.*
      FROM ESTOQUE E
      LEFT JOIN BI_SINC_ESTOQUE S ON S.CODFILIAL = E.CODFILIAL
                                 AND S.CODPROD = E.CODPROD
     WHERE S.DT_UPDATE IS NULL
        OR S.QTCONTABIL <> E.QTCONTABIL
        OR S.QTGERENCIAL <> E.QTGERENCIAL
        OR S.QTBLOQUEADA <> E.QTBLOQUEADA
        OR S.QTPENDENTE <> E.QTPENDENTE
        OR S.QTRESERVADA <> E.QTRESERVADA
        OR S.QTAVARIADA <> E.QTAVARIADA
        OR S.QTFRENTELOJA <> E.QTFRENTELOJA
        OR S.VALORULTENT <> E.VALORULTENT
        OR S.CUSTOREPOSICAO <> E.CUSTOREPOSICAO
        OR S.CUSTOFINANCEIRO <> E.CUSTOFINANCEIRO
        OR S.CUSTOCONTABIL <> E.CUSTOCONTABIL
        OR S.CODBLOQUEIO <> E.CODBLOQUEIO
        OR S.MOTIVOBLOQUEIO <> E.MOTIVOBLOQUEIO;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condi��es mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCEST)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_ESTOQUE
         SET QTCONTABIL      = temp_rec.QTCONTABIL,
             QTGERENCIAL     = temp_rec.QTGERENCIAL,
             QTBLOQUEADA     = temp_rec.QTBLOQUEADA,
             QTPENDENTE      = temp_rec.QTPENDENTE,
             QTRESERVADA     = temp_rec.QTRESERVADA,
             QTAVARIADA      = temp_rec.QTAVARIADA,
             QTDISPONIVEL    = temp_rec.QTDISPONIVEL,
             QTFRENTELOJA    = temp_rec.QTFRENTELOJA,
             QTDEPOSITO      = temp_rec.QTDEPOSITO,
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
           QTPENDENTE,
           QTRESERVADA,
           QTAVARIADA,
           QTDISPONIVEL,
           QTFRENTELOJA,
           QTDEPOSITO,
           VALORULTENT,
           CUSTOREPOSICAO,
           CUSTOFINANCEIRO,
           CUSTOCONTABIL,
           CODBLOQUEIO,
           MOTIVOBLOQUEIO,
           DT_UPDATE)
        VALUES
          (temp_rec.CODFILIAL,
           temp_rec.CODPROD,
           temp_rec.QTCONTABIL,
           temp_rec.QTGERENCIAL,
           temp_rec.QTBLOQUEADA,
           temp_rec.QTPENDENTE,
           temp_rec.QTRESERVADA,
           temp_rec.QTAVARIADA,
           temp_rec.QTDISPONIVEL,
           temp_rec.QTFRENTELOJA,
           temp_rec.QTDEPOSITO,
           temp_rec.VALORULTENT,
           temp_rec.CUSTOREPOSICAO,
           temp_rec.CUSTOFINANCEIRO,
           temp_rec.CUSTOCONTABIL,
           temp_rec.CODBLOQUEIO,
           temp_rec.MOTIVOBLOQUEIO,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000,
                                'Erro durante insercao na tabela: ' ||
                                SQLERRM);
    END;
  END LOOP;

  COMMIT;

  -- Exclui os registros da tabela tempor�ria TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_PCEST';
END;
