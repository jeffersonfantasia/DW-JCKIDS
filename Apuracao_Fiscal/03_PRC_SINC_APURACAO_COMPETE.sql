CREATE OR REPLACE PROCEDURE PRC_SINC_APURACAO_COMPETE AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 90;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

  -----------------------BENEFICIO FISCAL
  vDT_INICIO_BENEFICIO_ES       DATE := TO_DATE('01/09/2023', 'DD/MM/YYYY');
  vDT_AJUSTE_SALDO_RECUPERAR    DATE := TO_DATE('01/03/2024', 'DD/MM/YYYY');
  vVALOR_AJUSTE_SALD0_RECUPERAR NUMBER := 4046.17;
  vALIQ_RATEIO                  NUMBER := 0.07;
  vALIQ_CREDITO_PRESUMIDO       NUMBER := 0.011;
  vALIQ_CREDITO_ADICIONAL       NUMBER := 0.035;

  -----------------------VARIAVEIS PARA CALCULO
  vVALOR_SALDO_ANTERIOR NUMBER := 0;
  vVALOR_SALDO_ATUAL    NUMBER;
  vVALOR_APURADO        NUMBER;
  vVALOR_PAGAR          NUMBER;
  vVALOR_RECUPERAR      NUMBER;
  vVALOR_ANT_SALDO      NUMBER;

  vVALOR_ICMS_ENT_NAC             NUMBER;
  vVALOR_ICMS_ENT_IMP             NUMBER;
  vVALOR_BASEICMS_ENT_NAC         NUMBER;
  vVALOR_SAID_RED                 NUMBER;
  vVALOR_SAID_COMPETE             NUMBER;
  vVALOR_SAID_TRANSF_FORA_COMPETE NUMBER;
  vVALOR_ICMS_SAID_COMPETE        NUMBER;

  vVALOR_ICMS_ENT_TOTAL     NUMBER;
  vVALOR_SAID_COMPETE_TOTAL NUMBER;

  vPERC_VENDA_RED     NUMBER;
  vPERC_VENDA_COMPETE NUMBER;

  vVALOR_ESTORNO_CRED_RED     NUMBER;
  vVALOR_ESTORNO_CRED_COMPETE NUMBER;
  vVALOR_ESTORNO_CRED_TOTAL   NUMBER;

  vVALOR_ENTRADA_PROPORCIONAL_RED     NUMBER;
  vVALOR_ENTRADA_PROPORCIONAL_COMPETE NUMBER;
  vVALOR_CREDITO_7PERC_RED            NUMBER;
  vVALOR_CREDITO_7PERC                NUMBER;
  vVALOR_CREDITO_7PERC_AJUST          NUMBER;
  vVALOR_CREDITO_ESCOLHIDO            NUMBER;
  vVALOR_CREDITO_RED_ESCOLHIDO        NUMBER;

  vVALOR_CRED_DESTINADO_COMERCIO NUMBER;
  vVALOR_CRED_PRESUMIDO          NUMBER;
  vVALOR_CRED_PRESUMIDO_AJUST    NUMBER;
  vVALOR_ADICIONAL_INCENTIVO     NUMBER;
  vVALOR_APURADO_COMPETE         NUMBER;

BEGIN
  FOR r IN (SELECT M.* FROM VIEW_BI_SINC_APURA_COMPETE M WHERE M.DATA >= vDATA_MOV_INCREMENTAL ORDER BY M.RN)
  
  LOOP
    BEGIN
      IF r.RN = 1 THEN
        vVALOR_SALDO_ANTERIOR := 0;
      END IF;
    
      vVALOR_ANT_SALDO := vVALOR_SALDO_ANTERIOR;
    
      -- ATRIBUI OS RESULTADOS DA CONSULTA AS VARIAVEIS
      vVALOR_BASEICMS_ENT_NAC         := r.VLBASEICMS_ENT_NAC;
      vVALOR_ICMS_ENT_NAC             := r.VLICMS_ENT_NAC;
      vVALOR_ICMS_ENT_IMP             := r.VLICMS_ENT_IMP;
      vVALOR_SAID_RED                 := r.VLCONT_SAID_RED;
      vVALOR_SAID_COMPETE             := r.VLCONT_SAID_COMPETE;
      vVALOR_SAID_TRANSF_FORA_COMPETE := r.VLCONT_SAID_FORA_TRANSF;
      vVALOR_ICMS_SAID_COMPETE        := r.VLICMS_SAID_COMPETE;
    
      -- PREPARACAO DAS VARIAVEIS PARA OS CALCULOS
      vVALOR_ICMS_ENT_TOTAL     := NVL(vVALOR_ICMS_ENT_NAC, 0) + NVL(vVALOR_ICMS_ENT_IMP, 0);
      vVALOR_SAID_COMPETE_TOTAL := NVL(vVALOR_SAID_RED, 0) + NVL(vVALOR_SAID_COMPETE, 0) +
                                   NVL(vVALOR_SAID_TRANSF_FORA_COMPETE, 0);
    
      -- CALCULO DO PERCENTIAL DO TOTAL DE VENDAS TRIBUTADAS
      vPERC_VENDA_RED := CASE
                           WHEN NVL(vVALOR_SAID_COMPETE_TOTAL, 0) = 0 THEN
                            0
                           ELSE
                            NVL(vVALOR_SAID_RED, 0) / vVALOR_SAID_COMPETE_TOTAL
                         END;
    
      vPERC_VENDA_COMPETE := CASE
                               WHEN NVL(vVALOR_SAID_COMPETE_TOTAL, 0) = 0 THEN
                                0
                               ELSE
                                NVL(vVALOR_SAID_COMPETE, 0) / vVALOR_SAID_COMPETE_TOTAL
                             END;
      -- CALCULO DO ESTORNO DO CREDITO
      vVALOR_ESTORNO_CRED_RED     := NVL(vVALOR_ICMS_ENT_TOTAL, 0) * NVL(vPERC_VENDA_RED, 0);
      vVALOR_ESTORNO_CRED_COMPETE := NVL(vVALOR_ICMS_ENT_TOTAL, 0) * NVL(vPERC_VENDA_COMPETE, 0);
      vVALOR_ESTORNO_CRED_TOTAL   := NVL(vVALOR_ESTORNO_CRED_RED, 0) + NVL(vVALOR_ESTORNO_CRED_COMPETE, 0);
    
      -- CALCULO DO CREDITO 7% + RATEIO
      vVALOR_ENTRADA_PROPORCIONAL_RED     := NVL(vPERC_VENDA_RED, 0) * NVL(vVALOR_BASEICMS_ENT_NAC, 0);
      vVALOR_ENTRADA_PROPORCIONAL_COMPETE := NVL(vPERC_VENDA_COMPETE, 0) * NVL(vVALOR_BASEICMS_ENT_NAC, 0);
    
      vVALOR_CREDITO_7PERC_RED := (vALIQ_RATEIO * NVL(vVALOR_ENTRADA_PROPORCIONAL_RED, 0)) +
                                  (NVL(vPERC_VENDA_RED, 0) * NVL(vVALOR_ICMS_ENT_IMP, 0));
      vVALOR_CREDITO_7PERC     := (vALIQ_RATEIO * NVL(vVALOR_ENTRADA_PROPORCIONAL_COMPETE, 0)) +
                                  (NVL(vPERC_VENDA_COMPETE, 0) * NVL(vVALOR_ICMS_ENT_IMP, 0));
    
      vVALOR_CREDITO_ESCOLHIDO := CASE
                                    WHEN NVL(vVALOR_CREDITO_7PERC, 0) < NVL(vVALOR_ESTORNO_CRED_TOTAL, 0) THEN
                                     NVL(vVALOR_CREDITO_7PERC, 0)
                                    ELSE
                                     NVL(vVALOR_ESTORNO_CRED_TOTAL, 0)
                                  END;
    
      -- CALCULO DO CREDITO 7% + RATEIO - BASE REDUZIDA
      vVALOR_CREDITO_RED_ESCOLHIDO := CASE
                                        WHEN NVL(vVALOR_CREDITO_7PERC_RED, 0) < NVL(vVALOR_ESTORNO_CRED_RED, 0) THEN
                                         NVL(vVALOR_CREDITO_7PERC_RED, 0)
                                        ELSE
                                         NVL(vVALOR_ESTORNO_CRED_RED, 0)
                                      END;
    
      -- CALCULO DO CREDITO DESTINADO A COMERCIO OU INDUSTRIA
      vVALOR_CRED_DESTINADO_COMERCIO := vALIQ_CREDITO_PRESUMIDO * NVL(vVALOR_SAID_COMPETE, 0);
    
      -- CALCULO DO CREDITO PRESUMIDO DE 1.1%
      vVALOR_CRED_PRESUMIDO := NVL(vVALOR_ICMS_SAID_COMPETE, 0) - NVL(vVALOR_CREDITO_ESCOLHIDO, 0) -
                               NVL(vVALOR_CRED_DESTINADO_COMERCIO, 0);
    
      -- CALCULO AJUSTADO DO CREDITO PRESUMIDO DE 1.1% E CREDITO 7% + RATEIO
      vVALOR_CREDITO_7PERC_AJUST := CASE
                                      WHEN vVALOR_CRED_PRESUMIDO < 0 THEN
                                       vVALOR_CREDITO_7PERC + vVALOR_CRED_PRESUMIDO
                                      ELSE
                                       vVALOR_CREDITO_7PERC
                                    END;
    
      vVALOR_CRED_PRESUMIDO_AJUST := CASE
                                       WHEN vVALOR_CRED_PRESUMIDO < 0 THEN
                                        0
                                       ELSE
                                        vVALOR_CRED_PRESUMIDO
                                     END;
    
      -- CALCULO DO ADICIONAL DE 3.5% DO INCENTIVO
      vVALOR_ADICIONAL_INCENTIVO := vALIQ_CREDITO_ADICIONAL * NVL(vVALOR_CRED_DESTINADO_COMERCIO, 0);
    
      -- APURACAO COMPETE
      vVALOR_APURADO_COMPETE := ((r.VLCREDITO + NVL(vVALOR_CREDITO_RED_ESCOLHIDO, 0) +
                                NVL(vVALOR_CREDITO_7PERC_AJUST, 0) + NVL(vVALOR_CRED_PRESUMIDO_AJUST, 0) +
                                NVL(vVALOR_CRED_DESTINADO_COMERCIO, 0)) -
                                (r.VLDEBITO + NVL(vVALOR_ESTORNO_CRED_RED, 0) + NVL(vVALOR_ESTORNO_CRED_COMPETE, 0)));
    
      -- CALCULA O RESULTADO ATUAL
      vVALOR_SALDO_ATUAL := CASE
                              WHEN r.DATA = vDT_AJUSTE_SALDO_RECUPERAR THEN
                               vVALOR_APURADO_COMPETE + vVALOR_AJUSTE_SALD0_RECUPERAR
                              WHEN r.DATA >= vDT_INICIO_BENEFICIO_ES THEN
                               vVALOR_APURADO_COMPETE
                              ELSE
                               (r.VLCREDITO - r.VLDEBITO)
                            END;
      vVALOR_APURADO     := vVALOR_SALDO_ANTERIOR + vVALOR_SALDO_ATUAL;
    
      -- ATUALIZA OS VALORES A PAGAR E A RECUPERAR
      vVALOR_PAGAR := CASE
                        WHEN vVALOR_APURADO < 0 THEN
                         ABS(vVALOR_APURADO)
                        ELSE
                         0
                      END;
      vVALOR_RECUPERAR := CASE
                            WHEN vVALOR_APURADO > 0 THEN
                             vVALOR_APURADO
                            ELSE
                             0
                          END;
    
      -- ATUALIZA O VALOR_RECUPERAR_ANTERIOR PARA PROXIMA ITERACAO
      vVALOR_SALDO_ANTERIOR := CASE
                                 WHEN vVALOR_APURADO > 0 THEN
                                  vVALOR_APURADO
                                 ELSE
                                  0
                               END;
    
      -- FAZ O UPSERT
      UPDATE BI_SINC_APURACAO_COMPETE
         SET VLSALDOANT        = vVALOR_ANT_SALDO,
             VLCREDITO         = r.VLCREDITO,
             VLDEBITO          = r.VLDEBITO,
             VLCRED_RED        = vVALOR_CREDITO_RED_ESCOLHIDO,
             VLCRED_ALIQPERC   = vVALOR_CREDITO_7PERC_AJUST,
             VLCRED_PRESUMIDO  = vVALOR_CRED_PRESUMIDO_AJUST,
             VLCRED_DESTCOM    = vVALOR_CRED_DESTINADO_COMERCIO,
             VLESTCRED_RED     = vVALOR_ESTORNO_CRED_RED,
             VLESTCRED_COMPETE = vVALOR_ESTORNO_CRED_COMPETE,
             VLADD_INCENTIVO   = vVALOR_ADICIONAL_INCENTIVO,
             VLSALDO           = vVALOR_SALDO_ATUAL,
             VLPAGAR           = vVALOR_PAGAR,
             VLRECUPERAR       = vVALOR_RECUPERAR,
             DT_UPDATE         = SYSDATE
       WHERE DATA = r.DATA;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_APURACAO_COMPETE
          (DATA,
           VLSALDOANT,
           VLCREDITO,
           VLDEBITO,
           VLCRED_RED,
           VLCRED_ALIQPERC,
           VLCRED_PRESUMIDO,
           VLCRED_DESTCOM,
           VLESTCRED_RED,
           VLESTCRED_COMPETE,
           VLADD_INCENTIVO,
           VLSALDO,
           VLPAGAR,
           VLRECUPERAR,
           DT_UPDATE)
        VALUES
          (r.DATA,
           vVALOR_ANT_SALDO,
           r.VLCREDITO,
           r.VLDEBITO,
           vVALOR_CREDITO_RED_ESCOLHIDO,
           vVALOR_CREDITO_7PERC_AJUST,
           vVALOR_CRED_PRESUMIDO_AJUST,
           vVALOR_CRED_DESTINADO_COMERCIO,
           vVALOR_ESTORNO_CRED_RED,
           vVALOR_ESTORNO_CRED_COMPETE,
           vVALOR_ADICIONAL_INCENTIVO,
           vVALOR_SALDO_ATUAL,
           vVALOR_PAGAR,
           vVALOR_RECUPERAR,
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
