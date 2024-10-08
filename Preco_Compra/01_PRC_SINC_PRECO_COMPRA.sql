CREATE OR REPLACE PROCEDURE PRC_SINC_PRECO_COMPRA AS
BEGIN

  FOR temp_rec IN (
                   
                     WITH TRIBUTACAO_ENTRADA AS
                      (SELECT F.CODFIGURA,
                              FN_JF_TRANSFORMA_EM_PERCENTUAL(F.PERPIS) PERCPIS,
                              FN_JF_TRANSFORMA_EM_PERCENTUAL(F.PERCOFINS) PERCCOFINS,
                              FN_JF_TRANSFORMA_EM_PERCENTUAL(F.PERCICMRED) PERCICMSRED,
                              FN_JF_TRANSFORMA_EM_PERCENTUAL(F.PERCICM) PERCICMS,
                              FN_JF_TRANSFORMA_EM_PERCENTUAL(F.PERCREDICMS) PERCCREDICMS,
                              FN_JF_TRANSFORMA_EM_PERCENTUAL(F.PERIPI) PERCIPI,
                              FN_JF_TRANSFORMA_EM_PERCENTUAL(F.PERCIVA) PERCIVA,
                              FN_JF_TRANSFORMA_EM_PERCENTUAL(F.PERCALIQEXTGUIA) PERCALIQEXTGUIA,
                              FN_JF_TRANSFORMA_EM_PERCENTUAL(F.PERCALIQEXT) PERCALIQEXT,
                              FN_JF_TRANSFORMA_EM_PERCENTUAL(F.PERCALIQINT) PERCALIQINT,
                              FN_JF_TRANSFORMA_EM_PERCENTUAL(F.REDBASEALIQEXT) REDBASEALIQEXT
                         FROM PCTRIBFIGURA F),
                     CUSTO_COMPRA AS
                      (SELECT P.CODFILIAL,
                              P.CODPROD,
                              P.CUSTOREP PTABELA,
                              ROUND(NVL((P.CUSTOREP * (1 - P.PERCDESC)), 0), 4) AS PCOMPRA
                         FROM (SELECT P.CODFILIAL,
                                      P.CODPROD,
                                      P.CUSTOREP,
                                      FN_JF_SOMADESCONTO(P.PERCDESC1,
                                                         P.PERCDESC2,
                                                         P.PERCDESC3,
                                                         P.PERCDESC4,
                                                         P.PERCDESC5,
                                                         P.PERCDESC6,
                                                         P.PERCDESC7,
                                                         P.PERCDESC8,
                                                         P.PERCDESC9,
                                                         P.PERCDESC10) PERCDESC
                                 FROM PCPRODFILIAL P) P),
                     BASE_FINAL_LC AS
                      (SELECT E.CODFILIAL,
                              P.CODPROD,
                              C.PTABELA,
                              C.PCOMPRA,
                              FN_JF_BASE_ENT_ICMS(C.PCOMPRA,
                                                  T.PERCICMSRED,
                                                  T.PERCICMS) BASEICMS,
                              T.PERCPIS,
                              T.PERCCOFINS,
                              T.PERCICMS,
                              T.PERCICMSRED,
                              T.PERCCREDICMS,
                              (CASE
                                WHEN (F.EQUIPINDUSTRIA = 'N' AND
                                     F.TIPOFORNEC = 'D') THEN
                                 0
                                ELSE
                                 T.PERCIPI
                              END) AS PERCIPI,
                              T.PERCIVA,
                              T.PERCALIQEXT,
                              T.PERCALIQINT,
                              T.PERCALIQEXTGUIA,
                              T.REDBASEALIQEXT,
                              (T.PERCALIQINT * T.REDBASEALIQEXT) AS PERCALIQSTRED
                         FROM PCPRODUT P
                         JOIN BI_SINC_PRODUTO PR ON PR.CODPROD = P.CODPROD
                         JOIN PCFORNEC F ON P.CODFORNEC = F.CODFORNEC
                         JOIN PCTRIBENTRADA E ON P.CODNCMEX = E.NCM
                                             AND F.TIPOFORNEC = E.TIPOFORNEC
                                             AND F.ESTADO = E.UFORIGEM
                         LEFT JOIN TRIBUTACAO_ENTRADA T ON E.CODFIGURA =
                                                           T.CODFIGURA
                         LEFT JOIN CUSTO_COMPRA C ON P.CODPROD = C.CODPROD
                                                 AND E.CODFILIAL = C.CODFILIAL
                        WHERE F.TIPOFORNEC NOT IN ('C', 'O')),
                     BASE_FINAL_SN AS
                      (SELECT E.CODFILIAL,
                              P.CODPROD,
                              C.PTABELA,
                              C.PCOMPRA,
                              FN_JF_BASE_ENT_ICMS(C.PCOMPRA,
                                                  T.PERCICMSRED,
                                                  T.PERCICMS) BASEICMS,
                              T.PERCPIS,
                              T.PERCCOFINS,
                              T.PERCICMS,
                              T.PERCICMSRED,
                              T.PERCCREDICMS,
                              (CASE
                                WHEN (F.EQUIPINDUSTRIA = 'N' AND
                                     F.TIPOFORNEC = 'D') THEN
                                 0
                                ELSE
                                 T.PERCIPI
                              END) AS PERCIPI,
                              T.PERCIVA,
                              T.PERCALIQEXT,
                              T.PERCALIQINT,
                              T.PERCALIQEXTGUIA,
                              T.REDBASEALIQEXT,
                              (T.PERCALIQINT * T.REDBASEALIQEXT) AS PERCALIQSTRED
                         FROM PCPRODUT P
                         JOIN BI_SINC_PRODUTO PR ON PR.CODPROD = P.CODPROD
                         JOIN PCFORNEC F ON F.CODFORNEC = 1
                         JOIN PCTRIBENTRADA E ON P.CODNCMEX = E.NCM
                                             AND F.TIPOFORNEC = E.TIPOFORNEC
                                             AND F.ESTADO = E.UFORIGEM
                         LEFT JOIN TRIBUTACAO_ENTRADA T ON E.CODFIGURA =
                                                           T.CODFIGURA
                         LEFT JOIN CUSTO_COMPRA C ON P.CODPROD = C.CODPROD
                                                 AND E.CODFILIAL = C.CODFILIAL
                        WHERE E.CODFILIAL IN ('5', '6', '9', '10')),
                     PRECO_COMPRA AS
                      (SELECT T.CODFILIAL,
                              T.CODPROD,
                              ROUND(NVL(T.PTABELA, 0), 6) PTABELA,
                              ROUND(NVL(T.PCOMPRA, 0), 6) PCOMPRA,
                              ROUND(T.PCOMPRA * T.PERCIPI, 6) VLIPI,
                              ROUND(FN_JF_VALOR_ICMS_ST(T.PCOMPRA,
                                                        T.PERCIPI,
                                                        T.PERCIVA,
                                                        T.PERCALIQSTRED,
                                                        T.PERCALIQINT,
                                                        T.PERCALIQEXT),
                                    6) VLICMSST,
                              ROUND(FN_JF_VALOR_BRUTO(T.PCOMPRA,
                                                      T.PERCIPI,
                                                      FN_JF_VALOR_ICMS_ST(T.PCOMPRA,
                                                                          T.PERCIPI,
                                                                          T.PERCIVA,
                                                                          T.PERCALIQSTRED,
                                                                          T.PERCALIQINT,
                                                                          T.PERCALIQEXT)),
                                    6) PBRUTO,
                              ROUND(T.BASEICMS * T.PERCCREDICMS, 6) VLCREDICMS,
                              ROUND(FN_JF_BASE_ENT_PIS_COFINS(T.PCOMPRA,
                                                              T.BASEICMS,
                                                              T.PERCCREDICMS,
                                                              T.PERCIPI) *
                                    PERCPIS,
                                    6) VLPIS,
                              ROUND(FN_JF_BASE_ENT_PIS_COFINS(T.PCOMPRA,
                                                              T.BASEICMS,
                                                              T.PERCCREDICMS,
                                                              T.PERCIPI) *
                                    PERCCOFINS,
                                    6) VLCOFINS,
                              ROUND(FN_JF_CUSTO_LIQ(T.PCOMPRA,
                                                    T.BASEICMS,
                                                    T.PERCCREDICMS,
                                                    T.PERCIPI,
                                                    T.PERCPIS,
                                                    T.PERCCOFINS,
                                                    FN_JF_BASE_ENT_PIS_COFINS(T.PCOMPRA,
                                                                              T.BASEICMS,
                                                                              T.PERCCREDICMS,
                                                                              T.PERCIPI),
                                                    FN_JF_VALOR_ICMS_ST(T.PCOMPRA,
                                                                        T.PERCIPI,
                                                                        T.PERCIVA,
                                                                        T.PERCALIQSTRED,
                                                                        T.PERCALIQINT,
                                                                        T.PERCALIQEXT),
                                                    FN_JF_VALOR_BRUTO(T.PCOMPRA,
                                                                      T.PERCIPI,
                                                                      FN_JF_VALOR_ICMS_ST(T.PCOMPRA,
                                                                                          T.PERCIPI,
                                                                                          T.PERCIVA,
                                                                                          T.PERCALIQSTRED,
                                                                                          T.PERCALIQINT,
                                                                                          T.PERCALIQEXT))),
                                    6) CUSTOLIQ,
                              ROUND(T.BASEICMS, 6) BASEICMS,
                              ROUND(FN_JF_BASE_ICMS_ST(T.PCOMPRA,
                                                       T.PERCIPI,
                                                       T.PERCIVA),
                                    6) BASEICMSST,
                              ROUND(FN_JF_BASE_ENT_PIS_COFINS(T.PCOMPRA,
                                                              T.BASEICMS,
                                                              T.PERCCREDICMS,
                                                              T.PERCIPI),
                                    6) BASEPISCOFINS,
                              ROUND(T.PERCPIS, 6) PERCPIS,
                              ROUND(T.PERCCOFINS, 6) PERCCOFINS,
                              ROUND(T.PERCIPI, 6) PERCIPI,
                              ROUND(T.PERCICMS, 6) PERCICMS,
                              ROUND(T.PERCICMSRED, 6) PERCICMSRED,
                              ROUND(T.PERCCREDICMS, 6) PERCCREDICMS,
                              ROUND(T.PERCIVA, 6) PERCIVA,
                              ROUND(T.PERCALIQEXT, 6) PERCALIQEXT,
                              ROUND(T.PERCALIQINT, 6) PERCALIQINT,
                              ROUND(T.PERCALIQEXTGUIA, 6) PERCALIQEXTGUIA,
                              ROUND(T.REDBASEALIQEXT, 6) REDBASEALIQEXT,
                              ROUND(T.PERCALIQSTRED, 6) PERCALIQSTRED,
                              FN_JF_VALOR_ICMS_ST(T.PCOMPRA,
                                                  T.PERCIPI,
                                                  T.PERCIVA,
                                                  T.PERCALIQSTRED,
                                                  T.PERCALIQINT,
                                                  T.PERCALIQEXT) VLST
                         FROM (SELECT *
                                 FROM BASE_FINAL_LC
                               UNION
                               SELECT *
                                 FROM BASE_FINAL_SN) T)
                     
                     SELECT T.*
                       FROM PRECO_COMPRA T
                       LEFT JOIN BI_SINC_PRECO_COMPRA S ON S.CODFILIAL =
                                                           T.CODFILIAL
                                                       AND S.CODPROD =
                                                           T.CODPROD
                      WHERE S.DT_UPDATE IS NULL
                         OR S.PTABELA <> T.PTABELA
                         OR S.PCOMPRA <> T.PCOMPRA
                         OR S.PERCIPI <> T.PERCIPI
                         OR S.PERCICMS <> T.PERCICMS
                         OR S.PERCICMSRED <> T.PERCICMSRED
                         OR S.PERCCREDICMS <> T.PERCCREDICMS
                         OR S.PERCPIS <> T.PERCPIS
                         OR S.PERCCOFINS <> T.PERCCOFINS
                         OR S.PERCIVA <> T.PERCIVA
                         OR S.PERCALIQSTRED <> T.PERCALIQSTRED
                         OR S.PERCALIQINT <> T.PERCALIQINT
                         OR S.PERCALIQEXT <> T.PERCALIQEXT
                         OR S.PERCALIQEXTGUIA <> T.PERCALIQEXTGUIA
                         OR S.REDBASEALIQEXT <> T.REDBASEALIQEXT
                         OR S.PERCALIQSTRED <> T.PERCALIQSTRED
                         OR S.VLST <> T.VLST)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condi��es mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_PRECO_COMPRA
         SET PTABELA         = temp_rec.PTABELA,
             PCOMPRA         = temp_rec.PCOMPRA,
             VLIPI           = temp_rec.VLIPI,
             VLST            = temp_rec.VLST,
             PBRUTO          = temp_rec.PBRUTO,
             VLCREDICMS      = temp_rec.VLCREDICMS,
             VLPIS           = temp_rec.VLPIS,
             VLCOFINS        = temp_rec.VLCOFINS,
             CUSTOLIQ        = temp_rec.CUSTOLIQ,
             BASEICMS        = temp_rec.BASEICMS,
             BASEPISCOFINS   = temp_rec.BASEPISCOFINS,
             PERCPIS         = temp_rec.PERCPIS,
             PERCCOFINS      = temp_rec.PERCCOFINS,
             PERCIPI         = temp_rec.PERCIPI,
             PERCICMS        = temp_rec.PERCICMS,
             PERCICMSRED     = temp_rec.PERCICMSRED,
             PERCCREDICMS    = temp_rec.PERCCREDICMS,
             PERCIVA         = temp_rec.PERCIVA,
             PERCALIQEXT     = temp_rec.PERCALIQEXT,
             PERCALIQINT     = temp_rec.PERCALIQINT,
             PERCALIQEXTGUIA = temp_rec.PERCALIQEXTGUIA,
             REDBASEALIQEXT  = temp_rec.REDBASEALIQEXT,
             PERCALIQSTRED   = temp_rec.PERCALIQSTRED,
             DT_UPDATE       = SYSDATE
       WHERE CODFILIAL = temp_rec.CODFILIAL
         AND CODPROD = temp_rec.CODPROD;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_PRECO_COMPRA
          (CODFILIAL,
           CODPROD,
           PTABELA,
           PCOMPRA,
           VLIPI,
           VLST,
           PBRUTO,
           VLCREDICMS,
           VLPIS,
           VLCOFINS,
           CUSTOLIQ,
           BASEICMS,
           BASEPISCOFINS,
           PERCPIS,
           PERCCOFINS,
           PERCIPI,
           PERCICMS,
           PERCICMSRED,
           PERCCREDICMS,
           PERCIVA,
           PERCALIQEXT,
           PERCALIQINT,
           PERCALIQEXTGUIA,
           REDBASEALIQEXT,
           PERCALIQSTRED,
           DT_UPDATE)
        VALUES
          (temp_rec.CODFILIAL,
           temp_rec.CODPROD,
           temp_rec.PTABELA,
           temp_rec.PCOMPRA,
           temp_rec.VLIPI,
           temp_rec.VLST,
           temp_rec.PBRUTO,
           temp_rec.VLCREDICMS,
           temp_rec.VLPIS,
           temp_rec.VLCOFINS,
           temp_rec.CUSTOLIQ,
           temp_rec.BASEICMS,
           temp_rec.BASEPISCOFINS,
           temp_rec.PERCPIS,
           temp_rec.PERCCOFINS,
           temp_rec.PERCIPI,
           temp_rec.PERCICMS,
           temp_rec.PERCICMSRED,
           temp_rec.PERCCREDICMS,
           temp_rec.PERCIVA,
           temp_rec.PERCALIQEXT,
           temp_rec.PERCALIQINT,
           temp_rec.PERCALIQEXTGUIA,
           temp_rec.REDBASEALIQEXT,
           temp_rec.PERCALIQSTRED,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000,
                                'Erro durante a cria��o da tabela: ' ||
                                SQLERRM);
    END;
  END LOOP;

  COMMIT;

END;
