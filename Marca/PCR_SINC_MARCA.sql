CREATE OR REPLACE PROCEDURE PCR_SINC_MARCA AS
BEGIN
  -- Atualiza ou insere os resultados na tabela BI_SINCMARCA conforme as condi��es mencionadas
  EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_PCMARCA ON COMMIT PRESERVE ROWS AS
     SELECT M.CODMARCA, M.MARCA, M.ATIVO
       FROM PCMARCA M
       LEFT JOIN BI_SINCMARCA S ON S.CODMARCA = M.CODMARCA
      WHERE S.DT_UPDATE IS NULL
         OR M.MARCA <> S.MARCA
         OR M.ATIVO <> S.ATIVO';

  FOR temp_rec IN (SELECT * FROM TEMP_PCMARCA)
  
  LOOP
    BEGIN
      UPDATE BI_SINCMARCA
         SET MARCA     = temp_rec.MARCA,
             ATIVO     = temp_rec.ATIVO,
             DT_UPDATE = SYSDATE
       WHERE CODMARCA = temp_rec.CODMARCA;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINCMARCA
          (CODMARCA,
           MARCA,
           ATIVO,
           DT_UPDATE,
           DT_SINC,
           DTSINC_ERRO,
           MSG_ERRO)
        VALUES
          (temp_rec.CODMARCA,
           temp_rec.MARCA,
           temp_rec.ATIVO,
           SYSDATE,
           NULL,
           NULL,
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

  -- Exclui os registros da tabela tempor�ria TEMP_PCMARCA criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_PCMARCA';
END PCR_SINC_MARCA;
