CREATE OR REPLACE PROCEDURE PRC_SINC_FILIAL AS
BEGIN

FOR temp_rec IN (
    WITH FILIAIS AS
     (SELECT F.CODIGO CODFILIAL,
             NVL(F.NOMEREMETENTE, 'JC BROTHERS') EMPRESA,
             NVL(F.FANTASIA, 'JC BROTHERS') FILIAL,
             TO_NUMBER(F.CODIGO) ORDEM
        FROM PCFILIAL F)
    SELECT F.*
      FROM FILIAIS F
      LEFT JOIN BI_SINC_FILIAL S ON S.CODFILIAL = F.CODFILIAL
     WHERE S.DT_UPDATE IS NULL
        OR NVL(S.EMPRESA, '0') <> F.EMPRESA
        OR NVL(S.FILIAL, '0') <> F.FILIAL
)

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionada
  
  LOOP
    BEGIN
      UPDATE BI_SINC_FILIAL
         SET EMPRESA   = temp_rec.EMPRESA,
             FILIAL    = temp_rec.FILIAL,
             ORDEM     = temp_rec.ORDEM,
             DT_UPDATE = SYSDATE
       WHERE CODFILIAL = temp_rec.CODFILIAL;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_FILIAL
          (CODFILIAL,
           EMPRESA,
           FILIAL,
           ORDEM,
           DT_UPDATE)
        VALUES
          (temp_rec.CODFILIAL,
           temp_rec.EMPRESA,
           temp_rec.FILIAL,
           temp_rec.ORDEM,
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
