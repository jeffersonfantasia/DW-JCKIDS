CREATE OR REPLACE PROCEDURE PRC_SINC_CALENDARIO AS
  -- Inicializa variáveis
  vDTHOJE    DATE := SYSDATE;
  vDTINICIO  DATE := TO_DATE('01/01/2010', 'DD/MM/YYYY');
  vDTFIM     DATE := TO_DATE('31/01/2027', 'DD/MM/YYYY');
  vDIAFIM_DC NUMBER := 12;

BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_CALENDARIO
    (DATA,
     DIA,
     ANO,
     NUM_MES,
     NOME_MES,
     NOME_MES_ABREV,
     NUM_MES_ANO,
     NUM_MES_ANO_SEQ,
     MES_ANO,
     NUM_DIA_ANO,
     NUM_TRIMESTRE,
     NOME_TRIMESTRE,
     NOME_TRIMESTRE_ABREV,
     NUM_TRIMESTRE_ANO,
     NOME_TRIMESTRE_ANO,
     NUM_TRIMESTRE_JC,
     NOME_TRIMESTRE_JC,
     NOME_TRIMESTRE_ABREV_JC,
     NUM_TRIMESTRE_ANO_JC,
     NOME_TRIMESTRE_ANO_JC,
     NUM_SEMANA_MES,
     NOME_SEMANA_MES,
     NUM_SEMANA_ANO,
     NOME_SEMANA_ANO,
     NUM_DIA_SEMANA,
     NOME_DIA_SEMANA,
     NOME_DIA_SEMANA_ABREV,
     CALCULO_MES_ATUAL,
     CALCULO_TRIMESTRE_ATUAL,
     CALCULO_ANO_ATUAL,
     DIA_UTIL_FINANCEIRO)
    WITH DATAS AS
     (SELECT (vDTINICIO + LEVEL - 1) DT
        FROM DUAL
      CONNECT BY LEVEL <= (vDTFIM - vDTINICIO) + 1),
    CALENDARIO AS
     (SELECT DT AS DATA,
             EXTRACT(DAY FROM DT) DIA,
             EXTRACT(YEAR FROM DT) ANO,
             EXTRACT(MONTH FROM DT) NUM_MES,
             INITCAP(TO_CHAR(DT, 'MONTH', 'NLS_DATE_LANGUAGE=PORTUGUESE')) NOME_MES,
             INITCAP(TO_CHAR(DT, 'MON', 'NLS_DATE_LANGUAGE=PORTUGUESE')) NOME_MES_ABREV,
             (EXTRACT(YEAR FROM DT) * 100) + EXTRACT(MONTH FROM DT) NUM_MES_ANO,
             (EXTRACT(YEAR FROM DT) * 12) + (EXTRACT(MONTH FROM DT) - 1) NUM_MES_ANO_SEQ,
             INITCAP(TO_CHAR(DT, 'MON', 'NLS_DATE_LANGUAGE=PORTUGUESE')) || '/' ||
             TO_CHAR(DT, 'YYYY') MES_ANO,
             TO_NUMBER(TO_CHAR(DT, 'DDD')) NUM_DIA_ANO,
             CEIL(EXTRACT(MONTH FROM DT) / 3) NUM_TRIMESTRE,
             CEIL(EXTRACT(MONTH FROM DT) / 3) || 'º ' || 'Trimestre' NOME_TRIMESTRE,
             CEIL(EXTRACT(MONTH FROM DT) / 3) || 'º ' || 'Tri' NOME_TRIMESTRE_ABREV,
             (EXTRACT(YEAR FROM DT) * 100) +
             CEIL(EXTRACT(MONTH FROM DT) / 3) NUM_TRIMESTRE_ANO,
             CEIL(EXTRACT(MONTH FROM DT) / 3) || 'º ' || 'Tri/' ||
             EXTRACT(YEAR FROM DT) NOME_TRIMESTRE_ANO,
             (CASE
               WHEN (CEIL(EXTRACT(MONTH FROM DT) / 3) = 4 AND
                    EXTRACT(MONTH FROM DT) = 10 AND
                    EXTRACT(DAY FROM DT) <= vDIAFIM_DC) THEN
                3
               ELSE
                CEIL(EXTRACT(MONTH FROM DT) / 3)
             END) NUM_TRIMESTRE_JC,
             TO_NUMBER(TO_CHAR(DT, 'W')) NUM_SEMANA_MES,
             TO_NUMBER(TO_CHAR(DT, 'W')) || 'º ' || 'Semana' NOME_SEMANA_MES,
             TO_NUMBER(TO_CHAR(DT, 'WW')) NUM_SEMANA_ANO,
             TO_NUMBER(TO_CHAR(DT, 'WW')) || 'º ' || 'Sem. Ano' NOME_SEMANA_ANO,
             TO_NUMBER(TO_CHAR(DT, 'D')) NUM_DIA_SEMANA,
             INITCAP(TO_CHAR(DT, 'DAY', 'NLS_DATE_LANGUAGE=PORTUGUESE')) NOME_DIA_SEMANA,
             INITCAP(TO_CHAR(DT, 'DY', 'NLS_DATE_LANGUAGE=PORTUGUESE')) NOME_DIA_SEMANA_ABREV,
             (EXTRACT(YEAR FROM DT) - EXTRACT(YEAR FROM vDTHOJE)) * 12 +
             EXTRACT(MONTH FROM DT) - EXTRACT(MONTH FROM vDTHOJE) CALCULO_MES_ATUAL,
             (EXTRACT(YEAR FROM DT) - EXTRACT(YEAR FROM vDTHOJE)) * 4 +
             CEIL(EXTRACT(MONTH FROM DT) / 3) -
             CEIL(EXTRACT(MONTH FROM vDTHOJE) / 3) CALCULO_TRIMESTRE_ATUAL,
             (EXTRACT(YEAR FROM DT) - EXTRACT(YEAR FROM vDTHOJE)) CALCULO_ANO_ATUAL,
             (FN_BI_DIA_UTIL_FINANCEIRO(DT)) DIA_UTIL_FINANCEIRO
        FROM DATAS)
    SELECT C.DATA,
           C.DIA,
           C.ANO,
           C.NUM_MES,
           C.NOME_MES,
           C.NOME_MES_ABREV,
           C.NUM_MES_ANO,
           C.NUM_MES_ANO_SEQ,
           C.MES_ANO,
           C.NUM_DIA_ANO,
           C.NUM_TRIMESTRE,
           C.NOME_TRIMESTRE,
           C.NOME_TRIMESTRE_ABREV,
           C.NUM_TRIMESTRE_ANO,
           C.NOME_TRIMESTRE_ANO,
           C.NUM_TRIMESTRE_JC,
           (C.NUM_TRIMESTRE_JC || 'º ' || 'Trimestre') NOME_TRIMESTRE_JC,
           (C.NUM_TRIMESTRE_JC || 'º ' || 'Tri') NOME_TRIMESTRE_ABREV_JC,
           ((C.ANO * 100) + C.NUM_TRIMESTRE_JC) NUM_TRIMESTRE_ANO_JC,
           C.NUM_TRIMESTRE_JC || 'º ' || 'Tri/' || C.ANO NOME_TRIMESTRE_ANO_JC,
           C.NUM_SEMANA_MES,
           C.NOME_SEMANA_MES,
           C.NUM_SEMANA_ANO,
           C.NOME_SEMANA_ANO,
           C.NUM_DIA_SEMANA,
           C.NOME_DIA_SEMANA,
           C.NOME_DIA_SEMANA_ABREV,
           C.CALCULO_MES_ATUAL,
           C.CALCULO_TRIMESTRE_ATUAL,
           C.CALCULO_ANO_ATUAL,
           C.DIA_UTIL_FINANCEIRO
      FROM CALENDARIO C
      LEFT JOIN BI_SINC_CALENDARIO S ON S.DATA = C.DATA
     WHERE S.DT_UPDATE IS NULL
        OR S.NUM_TRIMESTRE_JC <> C.NUM_TRIMESTRE_JC
        OR S.CALCULO_MES_ATUAL <> C.CALCULO_MES_ATUAL
        OR S.CALCULO_TRIMESTRE_ATUAL <> C.CALCULO_TRIMESTRE_ATUAL
        OR S.CALCULO_ANO_ATUAL <> C.CALCULO_ANO_ATUAL
        OR S.DIA_UTIL_FINANCEIRO <> C.DIA_UTIL_FINANCEIRO;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_CALENDARIO)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_CALENDARIO
         SET DIA                     = temp_rec.DIA,
             ANO                     = temp_rec.ANO,
             NUM_MES                 = temp_rec.NUM_MES,
             NOME_MES                = temp_rec.NOME_MES,
             NOME_MES_ABREV          = temp_rec.NOME_MES_ABREV,
             NUM_MES_ANO             = temp_rec.NUM_MES_ANO,
             NUM_MES_ANO_SEQ         = temp_rec.NUM_MES_ANO_SEQ,
             MES_ANO                 = temp_rec.MES_ANO,
             NUM_DIA_ANO             = temp_rec.NUM_DIA_ANO,
             NUM_TRIMESTRE           = temp_rec.NUM_TRIMESTRE,
             NOME_TRIMESTRE          = temp_rec.NOME_TRIMESTRE,
             NOME_TRIMESTRE_ABREV    = temp_rec.NOME_TRIMESTRE_ABREV,
             NUM_TRIMESTRE_ANO       = temp_rec.NUM_TRIMESTRE_ANO,
             NOME_TRIMESTRE_ANO      = temp_rec.NOME_TRIMESTRE_ANO,
             NUM_TRIMESTRE_JC        = temp_rec.NUM_TRIMESTRE_JC,
             NOME_TRIMESTRE_JC       = temp_rec.NOME_TRIMESTRE_JC,
             NOME_TRIMESTRE_ABREV_JC = temp_rec.NOME_TRIMESTRE_ABREV_JC,
             NUM_TRIMESTRE_ANO_JC    = temp_rec.NUM_TRIMESTRE_ANO_JC,
             NOME_TRIMESTRE_ANO_JC   = temp_rec.NOME_TRIMESTRE_ANO_JC,
             NUM_SEMANA_MES          = temp_rec.NUM_SEMANA_MES,
             NOME_SEMANA_MES         = temp_rec.NOME_SEMANA_MES,
             NUM_SEMANA_ANO          = temp_rec.NUM_SEMANA_ANO,
             NOME_SEMANA_ANO         = temp_rec.NOME_SEMANA_ANO,
             NUM_DIA_SEMANA          = temp_rec.NUM_DIA_SEMANA,
             NOME_DIA_SEMANA         = temp_rec.NOME_DIA_SEMANA,
             NOME_DIA_SEMANA_ABREV   = temp_rec.NOME_DIA_SEMANA_ABREV,
             CALCULO_MES_ATUAL       = temp_rec.CALCULO_MES_ATUAL,
             CALCULO_TRIMESTRE_ATUAL = temp_rec.CALCULO_TRIMESTRE_ATUAL,
             CALCULO_ANO_ATUAL       = temp_rec.CALCULO_ANO_ATUAL,
             DIA_UTIL_FINANCEIRO     = temp_rec.DIA_UTIL_FINANCEIRO,
             DT_UPDATE               = SYSDATE
       WHERE DATA = temp_rec.DATA;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_CALENDARIO
          (DATA,
           DIA,
           ANO,
           NUM_MES,
           NOME_MES,
           NOME_MES_ABREV,
           NUM_MES_ANO,
           NUM_MES_ANO_SEQ,
           MES_ANO,
           NUM_DIA_ANO,
           NUM_TRIMESTRE,
           NOME_TRIMESTRE,
           NOME_TRIMESTRE_ABREV,
           NUM_TRIMESTRE_ANO,
           NOME_TRIMESTRE_ANO,
           NUM_TRIMESTRE_JC,
           NOME_TRIMESTRE_JC,
           NOME_TRIMESTRE_ABREV_JC,
           NUM_TRIMESTRE_ANO_JC,
           NOME_TRIMESTRE_ANO_JC,
           NUM_SEMANA_MES,
           NOME_SEMANA_MES,
           NUM_SEMANA_ANO,
           NOME_SEMANA_ANO,
           NUM_DIA_SEMANA,
           NOME_DIA_SEMANA,
           NOME_DIA_SEMANA_ABREV,
           CALCULO_MES_ATUAL,
           CALCULO_TRIMESTRE_ATUAL,
           CALCULO_ANO_ATUAL,
           DIA_UTIL_FINANCEIRO,
           DT_UPDATE)
        VALUES
          (temp_rec.DATA,
           temp_rec.DIA,
           temp_rec.ANO,
           temp_rec.NUM_MES,
           temp_rec.NOME_MES,
           temp_rec.NOME_MES_ABREV,
           temp_rec.NUM_MES_ANO,
           temp_rec.NUM_MES_ANO_SEQ,
           temp_rec.MES_ANO,
           temp_rec.NUM_DIA_ANO,
           temp_rec.NUM_TRIMESTRE,
           temp_rec.NOME_TRIMESTRE,
           temp_rec.NOME_TRIMESTRE_ABREV,
           temp_rec.NUM_TRIMESTRE_ANO,
           temp_rec.NOME_TRIMESTRE_ANO,
           temp_rec.NUM_TRIMESTRE_JC,
           temp_rec.NOME_TRIMESTRE_JC,
           temp_rec.NOME_TRIMESTRE_ABREV_JC,
           temp_rec.NUM_TRIMESTRE_ANO_JC,
           temp_rec.NOME_TRIMESTRE_ANO_JC,
           temp_rec.NUM_SEMANA_MES,
           temp_rec.NOME_SEMANA_MES,
           temp_rec.NUM_SEMANA_ANO,
           temp_rec.NOME_SEMANA_ANO,
           temp_rec.NUM_DIA_SEMANA,
           temp_rec.NOME_DIA_SEMANA,
           temp_rec.NOME_DIA_SEMANA_ABREV,
           temp_rec.CALCULO_MES_ATUAL,
           temp_rec.CALCULO_TRIMESTRE_ATUAL,
           temp_rec.CALCULO_ANO_ATUAL,
           temp_rec.DIA_UTIL_FINANCEIRO,
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

  -- Exclui os registros da tabela temporária TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_CALENDARIO';
END;
