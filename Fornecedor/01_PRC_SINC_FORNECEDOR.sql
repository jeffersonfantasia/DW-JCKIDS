CREATE OR REPLACE PROCEDURE PRC_SINC_FORNECEDOR AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCFORNEC
    (CODFORNEC, FORNECEDOR, CODFORNECPRINC, CNPJ, TIPO)
    WITH FORNECEDORES AS
     (SELECT F.CODFORNEC,
             F.FORNECEDOR,
             COALESCE(F.CODFORNECPRINC, F.CODFORNEC) CODFORNECPRINC,
             REGEXP_REPLACE(F.CGC,
                            '([0-9]{2})([0-9]{3})([0-9]{3})([0-9]{4})',
                            '\1.\2.\3/\4-') CGC,
             NVL(F.OBS2, 'N') TIPO
        FROM PCFORNEC F)
    
    SELECT F.*
      FROM FORNECEDORES F
      LEFT JOIN BI_SINC_FORNECEDOR S ON S.CODFORNEC = F.CODFORNEC
     WHERE S.DT_UPDATE IS NULL
        OR S.FORNECEDOR <> F.FORNECEDOR
        OR S.CODFORNECPRINC <> F.CODFORNECPRINC
        OR S.CNPJ <> F.CGC
        OR NVL(S.TIPO, 'S') <> F.TIPO;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condi��es mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCFORNEC)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_FORNECEDOR
         SET FORNECEDOR     = temp_rec.FORNECEDOR,
             CODFORNECPRINC = temp_rec.CODFORNECPRINC,
             CNPJ           = temp_rec.CNPJ,
             TIPO           = temp_rec.TIPO,
             DT_UPDATE      = SYSDATE
       WHERE CODFORNEC = temp_rec.CODFORNEC;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_FORNECEDOR
          (CODFORNEC,
           FORNECEDOR,
           CODFORNECPRINC,
           CNPJ,
           TIPO,
           DT_UPDATE)
        VALUES
          (temp_rec.CODFORNEC,
           temp_rec.FORNECEDOR,
           temp_rec.CODFORNECPRINC,
           temp_rec.CNPJ,
           temp_rec.TIPO,
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

  -- Exclui os registros da tabela tempor�ria TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_PCFORNEC';
END;
