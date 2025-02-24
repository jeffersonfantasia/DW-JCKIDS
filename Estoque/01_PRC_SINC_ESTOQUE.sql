CREATE OR REPLACE PROCEDURE PRC_SINC_ESTOQUE AS
BEGIN
  FOR r IN (
            
              WITH FILIAL_REGIAO AS
               (SELECT E.CODFILIAL,
                       (CASE
                         WHEN E.CODFILIAL IN ('1', '8', '12', '13', '14') THEN
                          100
                         WHEN E.CODFILIAL IN ('3', '4') THEN
                          2
                         WHEN E.CODFILIAL IN ('2', '7') THEN
                          102
                         WHEN E.CODFILIAL = '11' THEN
                          4
                         ELSE
                          NULL
                       END) NUMREGIAO
                  FROM PCEST E
                 WHERE E.CODFILIAL IN ('1', '2', '3', '4', '7', '8', '11', '12', '13', '14')
                 GROUP BY E.CODFILIAL),
              
              PRECOVENDA_FILIAL AS
               (SELECT F.CODFILIAL,
                       V.CODPROD,
                       V.NUMREGIAO,
                       NVL(MAX(P.PRECOPROMOCIONAL), V.PRECOVENDA) PRECOVENDA
                  FROM BI_SINC_PRECO_VENDA V
                  JOIN FILIAL_REGIAO F ON F.NUMREGIAO = V.NUMREGIAO
                  LEFT JOIN BI_SINC_PRECO_VENDA_PROMOCIONAL P ON V.CODPROD = P.CODPROD
                                                             AND V.NUMREGIAO = P.NUMREGIAO
                                                             AND P.ATIVO = 'S'
                 GROUP BY F.CODFILIAL,
                          V.CODPROD,
                          V.NUMREGIAO,
                          V.PRECOVENDA),
              ESTOQUE AS
               (SELECT E.CODFILIAL,
                       E.CODPROD,
                       NVL(E.QTEST, 0) QTCONTABIL,
                       NVL(E.QTESTGER, 0) QTGERENCIAL,
                       NVL(E.QTBLOQUEADA, 0) QTBLOQUEADA,
                       NVL(E.QTPENDENTE, 0) QTPENDENTE,
                       NVL(E.QTRESERV, 0) QTRESERVADA,
                       NVL(E.QTINDENIZ, 0) QTAVARIADA,
                       (NVL(E.QTESTGER, 0) - NVL(E.QTBLOQUEADA, 0)) QTDISPONIVEL,
                       NVL(E.QTFRENTELOJA, 0) QTFRENTELOJA,
                       (NVL(E.QTESTGER, 0) - NVL(E.QTFRENTELOJA, 0) - NVL(E.QTINDENIZ, 0)) QTDEPOSITO,
                       NVL(E.VALORULTENT, 0) VALORULTENT,
                       NVL(E.CUSTOREP, 0) CUSTOREPOSICAO,
                       NVL(E.CUSTOFIN, 0) CUSTOFINANCEIRO,
                       NVL(E.CUSTOCONT, 0) CUSTOCONTABIL,
                       NVL(E.QTEST, 0) * NVL(E.CUSTOCONT, 0) VLESTOQUECONTABIL,
                       NVL(E.QTESTGER, 0) * NVL(E.CUSTOFIN, 0) VLESTOQUEFINANCEIRO,
                       NVL(E.QTESTGER, 0) * NVL(E.CUSTOREP, 0) VLESTOQUEGERENCIAL,
                       NVL(E.QTFRENTELOJA, 0) * NVL(E.CUSTOREP, 0) VLESTOQUELOJA,
                       (NVL(E.QTESTGER, 0) - NVL(E.QTFRENTELOJA, 0) - NVL(E.QTINDENIZ, 0)) * NVL(E.CUSTOREP, 0) VLESTOQUEDEPOSITO,
                       (NVL(E.QTESTGER, 0) - NVL(E.QTBLOQUEADA, 0)) * NVL(E.CUSTOREP, 0) VLESTOQUEDISPONIVEL,
                       NVL(E.QTINDENIZ, 0) * NVL(E.CUSTOREP, 0) VLESTOQUEAVARIADO,
                       NVL(E.QTBLOQUEADA, 0) * NVL(E.CUSTOREP, 0) VLESTOQUEBLOQUEADO,
                       NVL(E.QTESTGER, 0) * NVL(F.PRECOVENDA, 0) VLESTOQUEVENDA,
                       (CASE
                         WHEN (NVL(E.CODDEVOL, 0) IN (0, 29) AND NVL(E.QTBLOQUEADA, 0) > 0 AND NVL(E.QTINDENIZ, 0) <= 0) THEN
                          0
                         WHEN (NVL(E.CODDEVOL, 0) IN (0, 29) AND NVL(E.QTINDENIZ, 0) > 0 AND NVL(E.QTBLOQUEADA, 0) = NVL(E.QTINDENIZ, 0)) THEN
                          0
                         WHEN (NVL(E.QTINDENIZ, 0) <= 0 AND NVL(E.QTBLOQUEADA, 0) <= 0) THEN
                          0
                         ELSE
                          E.CODDEVOL
                       END) CODBLOQUEIO,
                       (CASE
                         WHEN (NVL(E.CODDEVOL, 0) IN (0, 29) AND NVL(E.QTBLOQUEADA, 0) > 0 AND NVL(E.QTINDENIZ, 0) <= 0) THEN
                          'ENTRADA MERCADORIA'
                         WHEN (NVL(E.CODDEVOL, 0) IN (0, 29) AND NVL(E.QTINDENIZ, 0) > 0 AND NVL(E.QTBLOQUEADA, 0) = NVL(E.QTINDENIZ, 0)) THEN
                          'AVARIADO'
                         WHEN  (NVL(E.QTINDENIZ, 0) <= 0 AND NVL(E.QTBLOQUEADA, 0) <= 0) THEN
                          'SEM BLOQUEIO'
                         ELSE
                          D.MOTIVO
                       END) MOTIVOBLOQUEIO
                  FROM PCEST E
                  JOIN BI_SINC_PRODUTO PR ON PR.CODPROD = E.CODPROD
                  LEFT JOIN PCTABDEV D ON D.CODDEVOL = E.CODDEVOL
                  LEFT JOIN PRECOVENDA_FILIAL F ON F.CODPROD = E.CODPROD
                                               AND F.CODFILIAL = E.CODFILIAL)
              
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
                  OR S.VLESTOQUECONTABIL <> E.VLESTOQUECONTABIL
                  OR S.VLESTOQUEFINANCEIRO <> E.VLESTOQUEFINANCEIRO
                  OR S.VLESTOQUEGERENCIAL <> E.VLESTOQUEGERENCIAL
                  OR S.VLESTOQUELOJA <> E.VLESTOQUELOJA
                  OR S.VLESTOQUEDEPOSITO <> E.VLESTOQUEDEPOSITO
                  OR S.VLESTOQUEDISPONIVEL <> E.VLESTOQUEDISPONIVEL
                  OR S.VLESTOQUEAVARIADO <> E.VLESTOQUEAVARIADO
                  OR S.VLESTOQUEBLOQUEADO <> E.VLESTOQUEBLOQUEADO
                  OR S.VLESTOQUEVENDA <> E.VLESTOQUEVENDA
                  OR S.CODBLOQUEIO <> E.CODBLOQUEIO
                  OR S.MOTIVOBLOQUEIO <> E.MOTIVOBLOQUEIO)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_ESTOQUE
         SET QTCONTABIL          = r.QTCONTABIL,
             QTGERENCIAL         = r.QTGERENCIAL,
             QTBLOQUEADA         = r.QTBLOQUEADA,
             QTPENDENTE          = r.QTPENDENTE,
             QTRESERVADA         = r.QTRESERVADA,
             QTAVARIADA          = r.QTAVARIADA,
             QTDISPONIVEL        = r.QTDISPONIVEL,
             QTFRENTELOJA        = r.QTFRENTELOJA,
             QTDEPOSITO          = r.QTDEPOSITO,
             VALORULTENT         = r.VALORULTENT,
             CUSTOREPOSICAO      = r.CUSTOREPOSICAO,
             CUSTOFINANCEIRO     = r.CUSTOFINANCEIRO,
             CUSTOCONTABIL       = r.CUSTOCONTABIL,
             VLESTOQUECONTABIL   = r.VLESTOQUECONTABIL,
             VLESTOQUEFINANCEIRO = r.VLESTOQUEFINANCEIRO,
             VLESTOQUEGERENCIAL  = r.VLESTOQUEGERENCIAL,
             VLESTOQUELOJA       = r.VLESTOQUELOJA,
             VLESTOQUEDEPOSITO   = r.VLESTOQUEDEPOSITO,
             VLESTOQUEDISPONIVEL = r.VLESTOQUEDISPONIVEL,
             VLESTOQUEAVARIADO   = r.VLESTOQUEAVARIADO,
             VLESTOQUEBLOQUEADO  = r.VLESTOQUEBLOQUEADO,
             VLESTOQUEVENDA      = r.VLESTOQUEVENDA,
             CODBLOQUEIO         = r.CODBLOQUEIO,
             MOTIVOBLOQUEIO      = r.MOTIVOBLOQUEIO,
             DT_UPDATE           = SYSDATE
       WHERE CODFILIAL = r.CODFILIAL
         AND CODPROD = r.CODPROD;
    
      IF SQL%NOTFOUND THEN
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
           VLESTOQUECONTABIL,
           VLESTOQUEFINANCEIRO,
           VLESTOQUEGERENCIAL,
           VLESTOQUELOJA,
           VLESTOQUEDEPOSITO,
           VLESTOQUEDISPONIVEL,
           VLESTOQUEAVARIADO,
           VLESTOQUEBLOQUEADO,
           VLESTOQUEVENDA,
           CODBLOQUEIO,
           MOTIVOBLOQUEIO,
           DT_UPDATE)
        VALUES
          (r.CODFILIAL,
           r.CODPROD,
           r.QTCONTABIL,
           r.QTGERENCIAL,
           r.QTBLOQUEADA,
           r.QTPENDENTE,
           r.QTRESERVADA,
           r.QTAVARIADA,
           r.QTDISPONIVEL,
           r.QTFRENTELOJA,
           r.QTDEPOSITO,
           r.VALORULTENT,
           r.CUSTOREPOSICAO,
           r.CUSTOFINANCEIRO,
           r.CUSTOCONTABIL,
           r.VLESTOQUECONTABIL,
           r.VLESTOQUEFINANCEIRO,
           r.VLESTOQUEGERENCIAL,
           r.VLESTOQUELOJA,
           r.VLESTOQUEDEPOSITO,
           r.VLESTOQUEDISPONIVEL,
           r.VLESTOQUEAVARIADO,
           r.VLESTOQUEBLOQUEADO,
           r.VLESTOQUEVENDA,
           r.CODBLOQUEIO,
           r.MOTIVOBLOQUEIO,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

END;
