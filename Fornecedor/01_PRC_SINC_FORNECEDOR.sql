CREATE OR REPLACE PROCEDURE PRC_SINC_FORNECEDOR AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCFORNEC
    (CODFORNEC, FORNECEDOR, CNPJ, TIPO)
    SELECT F.CODFORNEC, F.FORNECEDOR, F.CGC, F.OBS2
      FROM PCFORNEC F
      LEFT JOIN BI_SINC_FORNECEDOR S ON S.CODFORNEC = F.CODFORNEC
     WHERE S.DT_UPDATE IS NULL
        OR S.FORNECEDOR <> F.FORNECEDOR
        OR S.CNPJ <> F.CGC
        OR S.TIPO <> F.OBS2;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCFORNEC)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_FORNECEDOR
         SET FORNECEDOR = temp_rec.FORNECEDOR,
             CNPJ       = temp_rec.CNPJ,
             TIPO       = temp_rec.TIPO,
             DT_UPDATE  = SYSDATE
       WHERE CODFORNEC = temp_rec.CODFORNEC;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_FORNECEDOR
          (CODFORNEC, FORNECEDOR, CNPJ, TIPO, DT_UPDATE, DT_SINC)
        VALUES
          (temp_rec.CODFORNEC,
           temp_rec.FORNECEDOR,
           temp_rec.CNPJ,
           temp_rec.TIPO,
           SYSDATE,
           NULL);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000,
                                'Erro durante a criação da tabela: ' ||
                                SQLERRM);
    END;
  END LOOP;

  COMMIT;

  -- Exclui os registros da tabela temporária TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_PCFORNEC';
END;
