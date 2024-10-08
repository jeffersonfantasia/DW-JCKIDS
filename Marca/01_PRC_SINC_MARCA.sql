CREATE OR REPLACE PROCEDURE PRC_SINC_MARCA AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP_PCMARCA
  FOR temp_rec IN (
    SELECT M.CODMARCA, M.MARCA, M.ATIVO
      FROM PCMARCA M
      LEFT JOIN BI_SINC_MARCA S ON S.CODMARCA = M.CODMARCA
     WHERE S.DT_UPDATE IS NULL
        OR M.MARCA <> S.MARCA
        OR M.ATIVO <> S.ATIVO
	)
        
  -- Atualiza ou insere os resultados na tabela BI_SINCMARCA conforme as condi��es mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_MARCA
         SET MARCA     = temp_rec.MARCA,
             ATIVO     = temp_rec.ATIVO,
             DT_UPDATE = SYSDATE
       WHERE CODMARCA = temp_rec.CODMARCA;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_MARCA
          (CODMARCA,
           MARCA,
           ATIVO,
           DT_UPDATE)
        VALUES
          (temp_rec.CODMARCA,
           temp_rec.MARCA,
           temp_rec.ATIVO,
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
