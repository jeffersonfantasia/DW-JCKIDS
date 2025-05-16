CREATE OR REPLACE PROCEDURE PRC_SINC_FORNEC_ATUALIZA_TIPO AS

  --CODIGOS DE CLASSIFICACAO
  vCODCLASSIFICA_MERCADORIA  VARCHAR2(20) := '2.1.1.01.001';
  vCODCLASSIFICA_TRANSPORTE  VARCHAR2(20) := '2.1.1.03.001';
  vCODCLASSIFICA_IMOBILIZADO VARCHAR2(20) := '2.1.1.02.001';
  vCODCLASSIFICA_SERVICOS    VARCHAR2(20) := '2.1.1.04.001';
  vCODCLASSIFICA_OUTROS      VARCHAR2(20) := '2.1.1.05.001';

BEGIN
  FOR r IN (WITH TIPO_FORNECEDOR_MERC AS
               (SELECT E.CODFORNEC,
                      vCODCLASSIFICA_MERCADORIA CODCLASSIFICA
                 FROM PCNFENT E
                WHERE E.TIPODESCARGA IN ('1')
                  AND E.ESPECIE <> 'OE'
                GROUP BY E.CODFORNEC),
              
              TIPO_FORNECEDOR_TRANSP AS
               (SELECT E.CODFORNEC,
                      vCODCLASSIFICA_TRANSPORTE CODCLASSIFICA
                 FROM PCNFENT E
                 LEFT JOIN TIPO_FORNECEDOR_MERC M ON M.CODFORNEC = E.CODFORNEC
                WHERE E.ESPECIE = 'CT'
                  AND M.CODFORNEC IS NULL
                GROUP BY E.CODFORNEC),
              
              TIPO_FORNECEDOR_IMOB AS
               (SELECT E.CODFORNEC,
                      vCODCLASSIFICA_IMOBILIZADO CODCLASSIFICA
                 FROM PCNFENT E
                 LEFT JOIN PCCONTA C ON C.CODCONTA = E.CODCONT
                 LEFT JOIN TIPO_FORNECEDOR_MERC M ON M.CODFORNEC = E.CODFORNEC
                 LEFT JOIN TIPO_FORNECEDOR_TRANSP T ON T.CODFORNEC = E.CODFORNEC
                WHERE C.GRUPOCONTA IN (135, 145)
                  AND E.ESPECIE <> 'OE'
                  AND M.CODFORNEC IS NULL
                  AND T.CODFORNEC IS NULL
                GROUP BY E.CODFORNEC),
              
              TIPO_FORNECEDOR_SERV AS
               (SELECT E.CODFORNEC,
                      vCODCLASSIFICA_SERVICOS CODCLASSIFICA
                 FROM PCNFENT E
                 LEFT JOIN TIPO_FORNECEDOR_MERC M ON M.CODFORNEC = E.CODFORNEC
                 LEFT JOIN TIPO_FORNECEDOR_TRANSP T ON T.CODFORNEC = E.CODFORNEC
                 LEFT JOIN TIPO_FORNECEDOR_IMOB I ON I.CODFORNEC = E.CODFORNEC
                WHERE E.ESPECIE = 'NS'
                  AND M.CODFORNEC IS NULL
                  AND T.CODFORNEC IS NULL
                  AND I.CODFORNEC IS NULL
                GROUP BY E.CODFORNEC),
              
              TIPO_FORNECEDOR_OUTROS AS
               (SELECT E.CODFORNEC,
                      vCODCLASSIFICA_OUTROS CODCLASSIFICA
                 FROM PCNFENT E
                 LEFT JOIN TIPO_FORNECEDOR_MERC M ON M.CODFORNEC = E.CODFORNEC
                 LEFT JOIN TIPO_FORNECEDOR_TRANSP T ON T.CODFORNEC = E.CODFORNEC
                 LEFT JOIN TIPO_FORNECEDOR_IMOB I ON I.CODFORNEC = E.CODFORNEC
                 LEFT JOIN TIPO_FORNECEDOR_SERV S ON S.CODFORNEC = E.CODFORNEC
                WHERE E.TIPODESCARGA IN ('2')
                  AND M.CODFORNEC IS NULL
                  AND T.CODFORNEC IS NULL
                  AND I.CODFORNEC IS NULL
                  AND S.CODFORNEC IS NULL
                GROUP BY E.CODFORNEC),
              
              TIPO_FORNECEDOR_LANC AS
               (SELECT L.CODFORNEC,
                      (CASE
                        WHEN F.REVENDA = 'T' THEN
                         vCODCLASSIFICA_TRANSPORTE
                        ELSE
                         vCODCLASSIFICA_OUTROS
                      END) CODCLASSIFICA
                 FROM PCLANC L
                 LEFT JOIN PCFORNEC F ON F.CODFORNEC = L.CODFORNEC
                 LEFT JOIN TIPO_FORNECEDOR_MERC M ON M.CODFORNEC = L.CODFORNEC
                 LEFT JOIN TIPO_FORNECEDOR_TRANSP T ON T.CODFORNEC = L.CODFORNEC
                 LEFT JOIN TIPO_FORNECEDOR_IMOB I ON I.CODFORNEC = L.CODFORNEC
                 LEFT JOIN TIPO_FORNECEDOR_SERV S ON S.CODFORNEC = L.CODFORNEC
                 LEFT JOIN TIPO_FORNECEDOR_OUTROS O ON O.CODFORNEC = L.CODFORNEC
                WHERE L.TIPOPARCEIRO = 'F'
                  AND M.CODFORNEC IS NULL
                  AND T.CODFORNEC IS NULL
                  AND I.CODFORNEC IS NULL
                  AND S.CODFORNEC IS NULL
                  AND O.CODFORNEC IS NULL
                GROUP BY L.CODFORNEC,
                         F.REVENDA),
              
              TIPO_FORNECEDOR AS
               (SELECT *
                 FROM TIPO_FORNECEDOR_MERC
               UNION ALL
               SELECT *
                 FROM TIPO_FORNECEDOR_TRANSP
               UNION ALL
               SELECT *
                 FROM TIPO_FORNECEDOR_IMOB
               UNION ALL
               SELECT *
                 FROM TIPO_FORNECEDOR_SERV
               UNION ALL
               SELECT *
                 FROM TIPO_FORNECEDOR_OUTROS
               UNION ALL
               SELECT *
                 FROM TIPO_FORNECEDOR_LANC)
              
              SELECT F.*
                FROM TIPO_FORNECEDOR F
                JOIN BI_SINC_FORNECEDOR_CONTA S ON S.CODFORNEC = F.CODFORNEC
               WHERE NVL(S.CODCLASSIFICA, '0') <> NVL(F.CODCLASSIFICA, '0'))
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_FORNECEDOR_CONTA
         SET CODCLASSIFICA = r.CODCLASSIFICA,
             DT_UPDATE     = SYSDATE
       WHERE CODFORNEC = r.CODFORNEC;
    
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

END;
