CREATE OR REPLACE PROCEDURE PRC_SINC_FISCAL AS

  -----------------------DATAS DE ATUALIZACAO
  --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 90;
  vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY');

BEGIN
  FOR r IN (WITH MOV_FISCAL AS
               (SELECT M.* FROM VIEW_BI_SINC_CONTABIL_FISCAL M WHERE M.DATA >= vDATA_MOV_INCREMENTAL)
              
              SELECT M.*
                FROM MOV_FISCAL M
                LEFT JOIN BI_SINC_FISCAL S ON S.NUMTRANSITEM = M.NUMTRANSITEM
                                          AND S.TIPO = M.TIPO
               WHERE S.DT_UPDATE IS NULL
                  OR NVL(S.CODFILIAL, '0') <> NVL(M.CODFILIAL, '0')
                  OR NVL(S.DATA, TO_DATE('01/01/1889', 'DD/MM/YYYY')) <>
                     NVL(M.DATA, TO_DATE('01/01/1889', 'DD/MM/YYYY'))
                  OR NVL(S.NUMTRANSACAO, 0) <> NVL(M.NUMTRANSACAO, 0)
                  OR NVL(S.NUMNOTA, 0) <> NVL(M.NUMNOTA, 0)
                  OR NVL(S.ESPECIE, '0') <> NVL(M.ESPECIE, '0')
                  OR NVL(S.CODGERENTE, 0) <> NVL(M.CODGERENTE, 0)
                  OR NVL(S.CODPROD, 0) <> NVL(M.CODPROD, 0)
                  OR NVL(S.PRODUTO, '0') <> NVL(M.PRODUTO, '0')
                  OR NVL(S.RAZAOSOCIAL, '0') <> NVL(M.RAZAOSOCIAL, '0')
                  OR NVL(S.CFOP, 0) <> NVL(M.CFOP, 0)
                  OR NVL(S.PERCICMS, 0) <> NVL(M.PERCICMS, 0)
                  OR NVL(S.CST_ICMS, '0') <> NVL(M.CST_ICMS, '0')
                  OR NVL(S.PERCPIS, 0) <> NVL(M.PERCPIS, 0)
                  OR NVL(S.PERCCOFINS, 0) <> NVL(M.PERCCOFINS, 0)
                  OR NVL(S.CST_PISCOFINS, '0') <> NVL(M.CST_PISCOFINS, '0')
                  OR NVL(S.QT, 0) <> NVL(M.QT, 0)
                  OR NVL(S.VALORCONTABIL, 0) <> NVL(M.VALORCONTABIL, 0)
                  OR NVL(S.VLDIFALCONSUM, 0) <> NVL(M.VLDIFALCONSUM, 0)
                  OR NVL(S.VLBASEICMS, 0) <> NVL(M.VLBASEICMS, 0)
                  OR NVL(S.VALORICMS, 0) <> NVL(M.VALORICMS, 0)
                  OR NVL(S.VLBASEPISCOFINS, 0) <> NVL(M.VLBASEPISCOFINS, 0)
                  OR NVL(S.VALORPIS, 0) <> NVL(M.VALORPIS, 0)
                  OR NVL(S.VALORCOFINS, 0) <> NVL(M.VALORCOFINS, 0))
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_FISCAL
         SET CODFILIAL       = r.CODFILIAL,
             DATA            = r.DATA,
             MOVIMENTO       = r.MOVIMENTO,
             NUMTRANSACAO    = r.NUMTRANSACAO,
             NUMNOTA         = r.NUMNOTA,
             ESPECIE         = r.ESPECIE,
             CODGERENTE      = r.CODGERENTE,
             CODPROD         = r.CODPROD,
             PRODUTO         = r.PRODUTO,
             RAZAOSOCIAL     = r.RAZAOSOCIAL,
             CFOP            = r.CFOP,
             PERCICMS        = r.PERCICMS,
             CST_ICMS        = r.CST_ICMS,
             PERCPIS         = r.PERCPIS,
             PERCCOFINS      = r.PERCCOFINS,
             CST_PISCOFINS   = r.CST_PISCOFINS,
             QT              = r.QT,
             VALORCONTABIL   = r.VALORCONTABIL,
             VLDIFALCONSUM   = r.VLDIFALCONSUM,
             VLBASEICMS      = r.VLBASEICMS,
             VALORICMS       = r.VALORICMS,
             VLBASEPISCOFINS = r.VLBASEPISCOFINS,
             VALORPIS        = r.VALORPIS,
             VALORCOFINS     = r.VALORCOFINS,
             DT_UPDATE       = SYSDATE
       WHERE NUMTRANSITEM = r.NUMTRANSITEM
         AND TIPO = r.TIPO;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_FISCAL
          (CODFILIAL,
           DATA,
           MOVIMENTO,
           TIPO,
           NUMTRANSITEM,
           NUMTRANSACAO,
           NUMNOTA,
           ESPECIE,
           CODGERENTE,
           CODPROD,
           PRODUTO,
           RAZAOSOCIAL,
           CFOP,
           PERCICMS,
           CST_ICMS,
           PERCPIS,
           PERCCOFINS,
           CST_PISCOFINS,
           QT,
           VALORCONTABIL,
           VLDIFALCONSUM,
           VLBASEICMS,
           VALORICMS,
           VLBASEPISCOFINS,
           VALORPIS,
           VALORCOFINS,
           DT_UPDATE)
        VALUES
          (r.CODFILIAL,
           r.DATA,
           r.MOVIMENTO,
           r.TIPO,
           r.NUMTRANSITEM,
           r.NUMTRANSACAO,
           r.NUMNOTA,
           r.ESPECIE,
           r.CODGERENTE,
           r.CODPROD,
           r.PRODUTO,
           r.RAZAOSOCIAL,
           r.CFOP,
           r.PERCICMS,
           r.CST_ICMS,
           r.PERCPIS,
           r.PERCCOFINS,
           r.CST_PISCOFINS,
           r.QT,
           r.VALORCONTABIL,
           r.VLDIFALCONSUM,
           r.VLBASEICMS,
           r.VALORICMS,
           r.VLBASEPISCOFINS,
           r.VALORPIS,
           r.VALORCOFINS,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_FISCAL
  WHERE (NUMTRANSITEM, TIPO) IN
        (SELECT S.NUMTRANSITEM, S.TIPO
  FROM BI_SINC_FISCAL S
  LEFT JOIN VIEW_BI_SINC_CONTABIL_FISCAL M ON M.NUMTRANSITEM = S.NUMTRANSITEM AND M.TIPO = S.TIPO
    WHERE M.NUMTRANSITEM IS NULL)';
  END;

  COMMIT;

END;
