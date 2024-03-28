CREATE OR REPLACE PROCEDURE PRC_SINC_VENDEDOR AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCUSUARI
    (CODUSUR,
     VENDEDOR,
     BLOQUEIO,
     CODSUPERVISOR,
     SUPERVISOR,
     CODGERENTE,
     GERENTE,
     CODAREA,
     AREACOMERCIAL)
    SELECT U.CODUSUR,
           COALESCE(U.USURDIRFV, U.NOME) VENDEDOR,
           U.BLOQUEIO,
           U.CODSUPERVISOR,
           S.NOME SUPERVISOR,
           S.CODGERENTE,
           G.NOMEGERENTE,
           C.CODAREA,
           C.AREACOMERCIAL
      FROM PCUSUARI U
      LEFT JOIN PCSUPERV S ON S.CODSUPERVISOR = U.CODSUPERVISOR
      LEFT JOIN PCGERENTE G ON G.CODGERENTE = S.CODGERENTE
      LEFT JOIN JFAREACOMERCIAL C ON C.CODAREA = G.CODGERENTESUPERIOR
      LEFT JOIN BI_SINC_VENDEDOR V ON V.CODUSUR = U.CODUSUR
     WHERE V.DT_UPDATE IS NULL
        OR V.VENDEDOR <> COALESCE(U.USURDIRFV, U.NOME)
        OR V.BLOQUEIO <> U.BLOQUEIO
        OR V.CODSUPERVISOR <> U.CODSUPERVISOR
        OR V.SUPERVISOR <> S.NOME
        OR V.CODGERENTE <> S.CODGERENTE
        OR V.GERENTE <> G.NOMEGERENTE
        OR V.CODAREA <> C.CODAREA
        OR V.AREACOMERCIAL <> C.AREACOMERCIAL;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCUSUARI)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_VENDEDOR
         SET VENDEDOR      = temp_rec.VENDEDOR,
             BLOQUEIO      = temp_rec.BLOQUEIO,
             CODSUPERVISOR = temp_rec.CODSUPERVISOR,
             SUPERVISOR    = temp_rec.SUPERVISOR,
             CODGERENTE    = temp_rec.CODGERENTE,
             GERENTE       = temp_rec.GERENTE,
             CODAREA       = temp_rec.CODAREA,
             AREACOMERCIAL = temp_rec.AREACOMERCIAL,
             DT_UPDATE     = SYSDATE
       WHERE CODUSUR = temp_rec.CODUSUR;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_VENDEDOR
          (CODUSUR,
           VENDEDOR,
           BLOQUEIO,
           CODSUPERVISOR,
           SUPERVISOR,
           CODGERENTE,
           GERENTE,
           CODAREA,
           AREACOMERCIAL,
           DT_UPDATE)
        VALUES
          (temp_rec.CODUSUR,
           temp_rec.VENDEDOR,
           temp_rec.BLOQUEIO,
           temp_rec.CODSUPERVISOR,
           temp_rec.SUPERVISOR,
           temp_rec.CODGERENTE,
           temp_rec.GERENTE,
           temp_rec.CODAREA,
           temp_rec.AREACOMERCIAL,
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
  EXECUTE IMMEDIATE 'DELETE TEMP_PCUSUARI';
END;
