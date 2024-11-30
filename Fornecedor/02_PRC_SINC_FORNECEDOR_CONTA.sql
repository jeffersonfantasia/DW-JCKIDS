CREATE OR REPLACE PROCEDURE PRC_SINC_FORNEC_INSERE_CONTA (
       pCODEMPRESA VARCHAR2;
			 pCODFORNEC NUMBER;
			 pCODCONTA NUMBER;
)
AS

BEGIN

FOR r IN (

    WITH FORNECEDORES AS
     (SELECT F.CODFORNEC,
             F.FORNECEDOR,
             COALESCE(F.CODFORNECPRINC, F.CODFORNEC) CODFORNECPRINC,
             (COALESCE(F.CODFORNECPRINC, F.CODFORNEC) || ' - ' ||
             (SELECT FF.FORNECEDOR
                 FROM PCFORNEC FF
                WHERE FF.CODFORNEC = COALESCE(F.CODFORNECPRINC, F.CODFORNEC))) FORNECPRINC,
             REGEXP_REPLACE(F.CGC,
                            '([0-9]{2})([0-9]{3})([0-9]{3})([0-9]{4})',
                            '\1.\2.\3/\4-') CNPJ,
             NVL(F.REVENDA, 'N') TIPO
        FROM PCFORNEC F),
    FORNECEDOR_VENDA AS
     (SELECT 0 CODFORNEC,
             'VENDAS FORNECEDOR' FORNECEDOR,
             0 CODFORNECPRINC,
             'VENDAS FORNECEDOR' FORNECPRINC,
             '99.999.999/9999-99' CNPJ,
             'N' TIPO
        FROM DUAL),
    TABELA_FORNECEDOR AS
     (SELECT * FROM FORNECEDORES UNION ALL SELECT * FROM FORNECEDOR_VENDA)
    
    SELECT F.*
      FROM TABELA_FORNECEDOR F
      LEFT JOIN BI_SINC_FORNECEDOR S ON S.CODFORNEC = F.CODFORNEC
     WHERE S.DT_UPDATE IS NULL
        OR S.FORNECEDOR <> F.FORNECEDOR
        OR S.CODFORNECPRINC <> F.CODFORNECPRINC
        OR S.FORNECPRINC <> F.FORNECPRINC
        OR S.CNPJ <> F.CNPJ
        OR NVL(S.TIPO, 'A') <> F.TIPO
)

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_FORNECEDOR
         SET FORNECEDOR     = r.FORNECEDOR,
             CODFORNECPRINC = r.CODFORNECPRINC,
             FORNECPRINC    = r.FORNECPRINC,
             CNPJ           = r.CNPJ,
             TIPO           = r.TIPO,
             DT_UPDATE      = SYSDATE
       WHERE CODFORNEC = r.CODFORNEC;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_FORNECEDOR
          (CODFORNEC,
           FORNECEDOR,
           CODFORNECPRINC,
           FORNECPRINC,
           CNPJ,
           TIPO,
           DT_UPDATE)
        VALUES
          (r.CODFORNEC,
           r.FORNECEDOR,
           r.CODFORNECPRINC,
           r.FORNECPRINC,
           r.CNPJ,
           r.TIPO,
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

END;
