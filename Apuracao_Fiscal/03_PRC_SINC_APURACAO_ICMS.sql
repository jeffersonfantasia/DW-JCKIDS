CREATE OR REPLACE PROCEDURE PRC_SINC_APURACAO_ICMS AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 90;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

  -----------------------VARIAVEIS PARA CALCULO
  vVALOR_SALDO_ANTERIOR NUMBER := 0;
  vVALOR_SALDO_ATUAL    NUMBER;
  vVALOR_APURADO        NUMBER;
  vVALOR_PAGAR          NUMBER;
  vVALOR_RECUPERAR      NUMBER;

BEGIN
  FOR r IN (SELECT M.*
              FROM VIEW_BI_SINC_APURA_ICMS M
             WHERE M.DATA >= vDATA_MOV_INCREMENTAL
             ORDER BY M.CODFILIAL,
                      M.DATA,
                      M.RN)
  
  LOOP
    BEGIN
      IF r.RN = 1 THEN
        vVALOR_SALDO_ANTERIOR := 0;
      END IF;
    
      -- Calcula o resultado atual
      vVALOR_SALDO_ATUAL := r.VLSALDO;
      vVALOR_APURADO     := vVALOR_SALDO_ANTERIOR + vVALOR_SALDO_ATUAL;
    
      -- Atualiza os valores a pagar e a recuperar
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
    
      -- Atualiza o valor_recuperar_anterior para a pr�xima itera��o
      vVALOR_SALDO_ANTERIOR := CASE
                                 WHEN vVALOR_APURADO > 0 THEN
                                  vVALOR_APURADO
                                 ELSE
                                  0
                               END;
    
      UPDATE BI_SINC_APURACAO_ICMS
         SET VLCREDITO   = r.VLCREDITO,
             VLDEBITO    = r.VLDEBITO,
             VLSALDO     = r.VLSALDO,
             VLPAGAR     = vVALOR_PAGAR,
             VLRECUPERAR = vVALOR_RECUPERAR,
             DT_UPDATE   = SYSDATE
       WHERE DATA = r.DATA
         AND CODFILIAL = r.CODFILIAL;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_APURACAO_ICMS
          (CODFILIAL,
           DATA,
           VLCREDITO,
           VLDEBITO,
           VLSALDO,
           VLPAGAR,
           VLRECUPERAR,
           DT_UPDATE)
        VALUES
          (r.CODFILIAL,
           r.DATA,
           r.VLCREDITO,
           r.VLDEBITO,
           r.VLSALDO,
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
