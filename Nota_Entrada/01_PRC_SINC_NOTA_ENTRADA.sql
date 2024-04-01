CREATE OR REPLACE PROCEDURE PRC_SINC_NOTA_ENTRADA AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCNFENT
    (CODFILIAL,
     )
    WITH NOTAS_ENTRADAS AS
     (SELECT )
		 SELECT E.* FROM NOTAS_ENTRADAS
      LEFT JOIN BI_SINC_NOTA_ENTRADA S ON S.NUMTRANSENT = E.NUMTRANSENT
     WHERE S.DT_UPDATE IS NULL
        OR S.CODFILIAL <> P.CODFILIAL;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCNFENT)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_NOTA_ENTRADA
         SET CODFILIAL    = temp_rec.CODFILIAL,
             DATA         = temp_rec.DATA,
             DT_UPDATE    = SYSDATE
       WHERE NUMTRANSENT = temp_rec.NUMTRANSENT

    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_NOTA_ENTRADA
          (CODFILIAL,
           DATA,

           DT_UPDATE)
        VALUES
          (temp_rec.CODFILIAL,
           temp_rec.DATA,

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

  -- Exclui os registros da tabela temporária TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_PCNFENT';
END;
