CREATE OR REPLACE PROCEDURE PRC_SINC_CALENDARIO AS
  -- Inicializa variáveis
  vDTHOJE    DATE := SYSDATE;
  vDTINICIO  DATE := TO_DATE('01/01/2010', 'DD/MM/YYYY');
  vDTFIM     DATE := TO_DATE('31/12/2030', 'DD/MM/YYYY');
  vDIAFIM_DC NUMBER := 12;

BEGIN

  FOR r IN (
            
              WITH DATAS AS
               (SELECT (vDTINICIO + LEVEL - 1) DT FROM DUAL CONNECT BY LEVEL <= (vDTFIM - vDTINICIO) + 1),
              CALENDARIO AS
               (SELECT DT AS DATA,
                       EXTRACT(DAY FROM DT) DIA,
                       EXTRACT(YEAR FROM DT) ANO,
                       EXTRACT(MONTH FROM DT) NUM_MES,
                       INITCAP(TO_CHAR(DT, 'MONTH', 'NLS_DATE_LANGUAGE=PORTUGUESE')) NOME_MES,
                       INITCAP(TO_CHAR(DT, 'MON', 'NLS_DATE_LANGUAGE=PORTUGUESE')) NOME_MES_ABREV,
                       (EXTRACT(YEAR FROM DT) * 100) + EXTRACT(MONTH FROM DT) NUM_MES_ANO,
                       (EXTRACT(YEAR FROM DT) * 12) + (EXTRACT(MONTH FROM DT) - 1) NUM_MES_ANO_SEQ,
                       INITCAP(TO_CHAR(DT, 'MON', 'NLS_DATE_LANGUAGE=PORTUGUESE')) || '/' || TO_CHAR(DT, 'YYYY') MES_ANO,
                       TO_NUMBER(TO_CHAR(DT, 'DDD')) NUM_DIA_ANO,
                       CEIL(EXTRACT(MONTH FROM DT) / 3) NUM_TRIMESTRE,
                       CEIL(EXTRACT(MONTH FROM DT) / 3) || 'º ' || 'Trimestre' NOME_TRIMESTRE,
                       CEIL(EXTRACT(MONTH FROM DT) / 3) || 'º ' || 'Tri' NOME_TRIMESTRE_ABREV,
                       (EXTRACT(YEAR FROM DT) * 100) + CEIL(EXTRACT(MONTH FROM DT) / 3) NUM_TRIMESTRE_ANO,
                       CEIL(EXTRACT(MONTH FROM DT) / 3) || 'º ' || 'Tri/' || EXTRACT(YEAR FROM DT) NOME_TRIMESTRE_ANO,
                       (CASE
                         WHEN (CEIL(EXTRACT(MONTH FROM DT) / 3) = 4 AND EXTRACT(MONTH FROM DT) = 10 AND
                              EXTRACT(DAY FROM DT) <= vDIAFIM_DC) THEN
                          3
                         ELSE
                          CEIL(EXTRACT(MONTH FROM DT) / 3)
                       END) NUM_TRIMESTRE_JC,
                       CEIL((EXTRACT(DAY FROM DT) + TO_NUMBER(TO_CHAR(TRUNC(DT, 'MM'), 'D')) - 2) / 7) NUM_SEMANA_MES,
                       CEIL((EXTRACT(DAY FROM DT) + TO_NUMBER(TO_CHAR(TRUNC(DT, 'MM'), 'D')) - 2) / 7) || 'º ' || 'Semana' NOME_SEMANA_MES,
                       TO_NUMBER(TO_CHAR(DT, 'IW')) NUM_SEMANA_ANO,
                       TO_NUMBER(TO_CHAR(DT, 'IW')) || 'º ' || 'Sem. Ano' NOME_SEMANA_ANO,
                       TO_NUMBER(TO_CHAR(DT, 'D')) NUM_DIA_SEMANA,
                       INITCAP(TO_CHAR(DT, 'DAY', 'NLS_DATE_LANGUAGE=PORTUGUESE')) NOME_DIA_SEMANA,
                       INITCAP(TO_CHAR(DT, 'DY', 'NLS_DATE_LANGUAGE=PORTUGUESE')) NOME_DIA_SEMANA_ABREV,
                       TRUNC(DT, 'MM') DATA_INICIO_MES,
                       TRUNC(DT, 'Q') DATA_INICIO_TRIMESTRE,
                       TRUNC(DT, 'YYYY') DATA_INICIO_ANO,
                       (EXTRACT(YEAR FROM DT) - EXTRACT(YEAR FROM vDTHOJE)) * 12 + EXTRACT(MONTH FROM DT) -
                       EXTRACT(MONTH FROM vDTHOJE) CALCULO_MES_ATUAL,
                       (EXTRACT(YEAR FROM DT) - EXTRACT(YEAR FROM vDTHOJE)) * 4 + CEIL(EXTRACT(MONTH FROM DT) / 3) -
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
                     C.DATA_INICIO_MES,
                     C.DATA_INICIO_TRIMESTRE,
                     C.DATA_INICIO_ANO,
                     C.CALCULO_MES_ATUAL,
                     C.CALCULO_TRIMESTRE_ATUAL,
                     C.CALCULO_ANO_ATUAL,
                     C.DIA_UTIL_FINANCEIRO
                FROM CALENDARIO C
                LEFT JOIN BI_SINC_CALENDARIO S ON S.DATA = C.DATA
               WHERE S.DT_UPDATE IS NULL
                  OR S.NUM_TRIMESTRE_JC <> C.NUM_TRIMESTRE_JC
                  OR S.NUM_SEMANA_MES <> C.NUM_SEMANA_MES
                  OR S.NUM_SEMANA_ANO <> C.NUM_SEMANA_ANO
                  OR S.CALCULO_MES_ATUAL <> C.CALCULO_MES_ATUAL
                  OR S.CALCULO_TRIMESTRE_ATUAL <> C.CALCULO_TRIMESTRE_ATUAL
                  OR S.CALCULO_ANO_ATUAL <> C.CALCULO_ANO_ATUAL
                  OR S.DIA_UTIL_FINANCEIRO <> C.DIA_UTIL_FINANCEIRO)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_CALENDARIO
         SET DIA                     = r.DIA,
             ANO                     = r.ANO,
             NUM_MES                 = r.NUM_MES,
             NOME_MES                = r.NOME_MES,
             NOME_MES_ABREV          = r.NOME_MES_ABREV,
             NUM_MES_ANO             = r.NUM_MES_ANO,
             NUM_MES_ANO_SEQ         = r.NUM_MES_ANO_SEQ,
             MES_ANO                 = r.MES_ANO,
             NUM_DIA_ANO             = r.NUM_DIA_ANO,
             NUM_TRIMESTRE           = r.NUM_TRIMESTRE,
             NOME_TRIMESTRE          = r.NOME_TRIMESTRE,
             NOME_TRIMESTRE_ABREV    = r.NOME_TRIMESTRE_ABREV,
             NUM_TRIMESTRE_ANO       = r.NUM_TRIMESTRE_ANO,
             NOME_TRIMESTRE_ANO      = r.NOME_TRIMESTRE_ANO,
             NUM_TRIMESTRE_JC        = r.NUM_TRIMESTRE_JC,
             NOME_TRIMESTRE_JC       = r.NOME_TRIMESTRE_JC,
             NOME_TRIMESTRE_ABREV_JC = r.NOME_TRIMESTRE_ABREV_JC,
             NUM_TRIMESTRE_ANO_JC    = r.NUM_TRIMESTRE_ANO_JC,
             NOME_TRIMESTRE_ANO_JC   = r.NOME_TRIMESTRE_ANO_JC,
             NUM_SEMANA_MES          = r.NUM_SEMANA_MES,
             NOME_SEMANA_MES         = r.NOME_SEMANA_MES,
             NUM_SEMANA_ANO          = r.NUM_SEMANA_ANO,
             NOME_SEMANA_ANO         = r.NOME_SEMANA_ANO,
             NUM_DIA_SEMANA          = r.NUM_DIA_SEMANA,
             NOME_DIA_SEMANA         = r.NOME_DIA_SEMANA,
             NOME_DIA_SEMANA_ABREV   = r.NOME_DIA_SEMANA_ABREV,
             DATA_INICIO_MES         = r.DATA_INICIO_MES,
             DATA_INICIO_TRIMESTRE   = r.DATA_INICIO_TRIMESTRE,
             DATA_INICIO_ANO         = r.DATA_INICIO_ANO,
             CALCULO_MES_ATUAL       = r.CALCULO_MES_ATUAL,
             CALCULO_TRIMESTRE_ATUAL = r.CALCULO_TRIMESTRE_ATUAL,
             CALCULO_ANO_ATUAL       = r.CALCULO_ANO_ATUAL,
             DIA_UTIL_FINANCEIRO     = r.DIA_UTIL_FINANCEIRO,
             DT_UPDATE               = SYSDATE
       WHERE DATA = r.DATA;
    
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
           DATA_INICIO_MES,
           DATA_INICIO_TRIMESTRE,
           DATA_INICIO_ANO,
           CALCULO_MES_ATUAL,
           CALCULO_TRIMESTRE_ATUAL,
           CALCULO_ANO_ATUAL,
           DIA_UTIL_FINANCEIRO,
           DT_UPDATE)
        VALUES
          (r.DATA,
           r.DIA,
           r.ANO,
           r.NUM_MES,
           r.NOME_MES,
           r.NOME_MES_ABREV,
           r.NUM_MES_ANO,
           r.NUM_MES_ANO_SEQ,
           r.MES_ANO,
           r.NUM_DIA_ANO,
           r.NUM_TRIMESTRE,
           r.NOME_TRIMESTRE,
           r.NOME_TRIMESTRE_ABREV,
           r.NUM_TRIMESTRE_ANO,
           r.NOME_TRIMESTRE_ANO,
           r.NUM_TRIMESTRE_JC,
           r.NOME_TRIMESTRE_JC,
           r.NOME_TRIMESTRE_ABREV_JC,
           r.NUM_TRIMESTRE_ANO_JC,
           r.NOME_TRIMESTRE_ANO_JC,
           r.NUM_SEMANA_MES,
           r.NOME_SEMANA_MES,
           r.NUM_SEMANA_ANO,
           r.NOME_SEMANA_ANO,
           r.NUM_DIA_SEMANA,
           r.NOME_DIA_SEMANA,
           r.NOME_DIA_SEMANA_ABREV,
           r.DATA_INICIO_MES,
           r.DATA_INICIO_TRIMESTRE,
           r.DATA_INICIO_ANO,
           r.CALCULO_MES_ATUAL,
           r.CALCULO_TRIMESTRE_ATUAL,
           r.CALCULO_ANO_ATUAL,
           r.DIA_UTIL_FINANCEIRO,
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
