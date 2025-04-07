CREATE OR REPLACE PROCEDURE PRC_SINC_APURACAO_COFINS AS

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
  FOR r IN (WITH APURA_FISCAL AS
               (SELECT M.* FROM VIEW_BI_SINC_APURA_COFINS M WHERE M.DATA >= vDATA_MOV_INCREMENTAL)
              
              SELECT M.*
                FROM APURA_FISCAL M
                LEFT JOIN BI_SINC_APURACAO_COFINS S ON S.DATA = M.DATA
               WHERE S.DT_UPDATE IS NULL
                  OR NVL(S.VLCREDITO, 0) <> NVL(M.VLCREDITO, 0)
                  OR NVL(S.VLDEBITO, 0) <> NVL(M.VLDEBITO, 0)
                  OR NVL(S.VLSALDO, 0) <> NVL(M.VLSALDO, 0))
  
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
    
      -- Atualiza o valor_recuperar_anterior para a próxima iteração
      vVALOR_SALDO_ANTERIOR := CASE
                                 WHEN vVALOR_APURADO > 0 THEN
                                  vVALOR_APURADO
                                 ELSE
                                  0
                               END;
    
      UPDATE BI_SINC_APURACAO_COFINS
         SET VLCREDITO   = r.VLCREDITO,
             VLDEBITO    = r.VLDEBITO,
             VLSALDO     = r.VLSALDO,
             VLPAGAR     = vVALOR_PAGAR,
             VLRECUPERAR = vVALOR_RECUPERAR,
             DT_UPDATE   = SYSDATE
       WHERE DATA = r.DATA;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_APURACAO_COFINS
          (DATA,
           VLCREDITO,
           VLDEBITO,
           VLSALDO,
           VLPAGAR,
           VLRECUPERAR,
           DT_UPDATE)
        VALUES
          (r.DATA,
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
