CREATE OR REPLACE PROCEDURE PRC_SINC_LANC_PAGAR AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCLANC
    (CODFILIAL,
     DATA,
     )
    
      LEFT JOIN BI_SINC_LANC_PAGAR S ON S.NUMPED = P.NUMPED
     WHERE S.DT_UPDATE IS NULL
        OR S.CODFILIAL <> P.CODFILIAL
        OR S.DATA <> P.DATA
        OR S.CODFORNEC <> P.CODFORNEC
        OR S.CODCOMPRADOR <> P.CODCOMPRADOR
        OR S.TIPO <> P.TIPO
        OR S.PRECOCOMPRA <> P.PRECOCOMPRA
        OR S.QTPEDIDA <> P.QTPEDIDA
        OR S.QTENTREGUE <> P.QTENTREGUE;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCLANC)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_LANC_PAGAR
         SET CODFILIAL    = temp_rec.CODFILIAL,
             DATA         = temp_rec.DATA,
             CODFORNEC    = temp_rec.CODFORNEC,
             CODCOMPRADOR = temp_rec.CODCOMPRADOR,
             DT_UPDATE    = SYSDATE
       WHERE NUMPED = temp_rec.NUMPED;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_LANC_PAGAR
          (CODFILIAL,
           DATA,
           CODFORNEC,
           CODCOMPRADOR,
)
        VALUES
          (temp_rec.CODFILIAL,
           temp_rec.DATA,
           temp_rec.CODFORNEC,

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
  EXECUTE IMMEDIATE 'DELETE TEMP_PCLANC';
END;
