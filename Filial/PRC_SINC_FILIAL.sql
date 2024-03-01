CREATE OR REPLACE PROCEDURE PRC_SINC_FILIAL AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCFILIAL
    (CODFILIAL, EMPRESA, FILIAL)
    SELECT F.CODIGO, F.NOMEREMETENTE, F.FANTASIA
      FROM PCFILIAL F
      LEFT JOIN BI_SINC_FILIAL S ON S.CODFILIAL = F.CODIGO
     WHERE S.DT_UPDATE IS NULL
        OR S.EMPRESA <> F.NOMEREMETENTE
        OR S.FILIAL <> F.FANTASIA;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condi��es mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCFILIAL)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_FILIAL
         SET EMPRESA   = temp_rec.EMPRESA,
             FILIAL    = temp_rec.FILIAL,
             DT_UPDATE = SYSDATE
       WHERE CODFILIAL = temp_rec.CODFILIAL;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_FILIAL
          (CODFILIAL, EMPRESA, FILIAL, DT_UPDATE, DT_SINC)
        VALUES
          (temp_rec.CODFILIAL,
           temp_rec.EMPRESA,
           temp_rec.FILIAL,
           SYSDATE,
           NULL);
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
  EXECUTE IMMEDIATE 'DELETE TEMP_PCFILIAL';
END;
