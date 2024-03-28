CREATE OR REPLACE PROCEDURE PRC_SINC_COMPRADOR AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCEMPR
    (MATRICULA, COMPRADOR)
    SELECT E.MATRICULA, E.NOME_GUERRA
      FROM PCEMPR E
      LEFT JOIN BI_SINC_COMPRADOR S ON S.MATRICULA = E.MATRICULA
     WHERE E.CODSETOR = 2
       AND S.DT_UPDATE IS NULL
        OR S.COMPRADOR <> E.NOME_GUERRA;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCEMPR)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_COMPRADOR
         SET COMPRADOR     = temp_rec.COMPRADOR,
             DT_UPDATE      = SYSDATE
       WHERE MATRICULA = temp_rec.MATRICULA;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_COMPRADOR
          (MATRICULA,
           COMPRADOR,
           DT_UPDATE)
        VALUES
          (temp_rec.MATRICULA,
           temp_rec.COMPRADOR,
           SYSDATE);
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
  EXECUTE IMMEDIATE 'DELETE TEMP_PCEMPR';
END;
