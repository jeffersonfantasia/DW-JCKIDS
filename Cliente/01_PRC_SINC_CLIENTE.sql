CREATE OR REPLACE PROCEDURE PRC_SINC_CLIENTE AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCCLIENT
    (CODCLI,
     CLIENTE,
     CODCLIREDE,
     CLIENTEREDE,
     CNPJ,
     CEP,
     UF,
     CODUSUR,
     CODPRACA,
     PRACA,
     CODATIVIDADE,
     RAMOATIVIDADE,
     BLOQUEIODEFINITIVO,
     BLOQUEIOATUAL,
     LIMITECREDITO)
    WITH CLIENTES AS
     (SELECT C.CODCLI,
             C.CLIENTE,
             (CASE
               WHEN R.DESCRICAO IS NULL THEN
                ('C' || C.CODCLI)
               ELSE
                ('R' || C.CODREDE)
             END) AS CODCLIREDE,
             (CASE
               WHEN R.DESCRICAO IS NULL THEN
                ('C' || C.CODCLI || ' - ' || UPPER(C.CLIENTE))
               ELSE
                ('R' || C.CODREDE || ' - ' || UPPER(R.DESCRICAO))
             END) AS CLIENTEREDE,
             REGEXP_REPLACE(C.CGCENT,
                            '([0-9]{2})([0-9]{3})([0-9]{3})([0-9]{4})',
                            '\1.\2.\3/\4-') CNPJ,
             REPLACE(C.CEPENT, '-', '') AS CEP,
             C.ESTENT UF,
             C.CODUSUR1 CODUSUR,
             C.CODPRACA,
             P.PRACA,
             C.CODATV1 CODATIVIDADE,
             A.RAMO RAMOATIVIDADE,
             C.BLOQUEIODEFINITIVO,
             C.BLOQUEIO BLOQUEIOATUAL,
             C.LIMCRED LIMITECREDITO
        FROM PCCLIENT C
        LEFT JOIN PCREDECLIENTE R ON R.CODREDE = C.CODREDE
        LEFT JOIN PCPRACA P ON P.CODPRACA = C.CODPRACA
        LEFT JOIN PCATIVI A ON A.CODATIV = C.CODATV1)
    SELECT C.*
      FROM CLIENTES C
      LEFT JOIN BI_SINC_CLIENTE S ON S.CODCLI = C.CODCLI
     WHERE S.DT_UPDATE IS NULL
        OR S.CLIENTE <> C.CLIENTE
        OR S.CODCLIREDE <> C.CODCLIREDE
        OR S.CLIENTEREDE <> C.CLIENTEREDE
        OR S.CNPJ <> C.CNPJ
        OR S.CEP <> C.CEP
        OR S.UF <> C.UF
        OR S.CODUSUR <> C.CODUSUR
        OR S.CODPRACA <> C.CODPRACA
        OR S.PRACA <> C.PRACA
        OR S.CODATIVIDADE <> C.CODATIVIDADE
        OR S.RAMOATIVIDADE <> C.RAMOATIVIDADE
        OR S.BLOQUEIODEFINITIVO <> C.BLOQUEIODEFINITIVO
        OR S.BLOQUEIOATUAL <> C.BLOQUEIOATUAL
        OR S.LIMITECREDITO <> C.LIMITECREDITO;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCCLIENT)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_CLIENTE
         SET CLIENTE            = temp_rec.CLIENTE,
             CODCLIREDE         = temp_rec.CODCLIREDE,
             CLIENTEREDE        = temp_rec.CLIENTEREDE,
             CNPJ               = temp_rec.CNPJ,
             CEP                = temp_rec.CEP,
             UF                 = temp_rec.UF,
             CODUSUR            = temp_rec.CODUSUR,
             CODPRACA           = temp_rec.CODPRACA,
             PRACA              = temp_rec.PRACA,
             CODATIVIDADE       = temp_rec.CODATIVIDADE,
             RAMOATIVIDADE      = temp_rec.RAMOATIVIDADE,
             BLOQUEIODEFINITIVO = temp_rec.BLOQUEIODEFINITIVO,
             BLOQUEIOATUAL      = temp_rec.BLOQUEIOATUAL,
             LIMITECREDITO      = temp_rec.LIMITECREDITO,
             DT_UPDATE          = SYSDATE
       WHERE CODCLI = temp_rec.CODCLI;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_CLIENTE
          (CODCLI,
           CLIENTE,
           CODCLIREDE,
           CLIENTEREDE,
           CNPJ,
           CEP,
           UF,
           CODUSUR,
           CODPRACA,
           PRACA,
           CODATIVIDADE,
           RAMOATIVIDADE,
           BLOQUEIODEFINITIVO,
           BLOQUEIOATUAL,
           LIMITECREDITO,
           DT_UPDATE)
        VALUES
          (temp_rec.CODCLI,
           temp_rec.CLIENTE,
           temp_rec.CODCLIREDE,
           temp_rec.CLIENTEREDE,
           temp_rec.CNPJ,
           temp_rec.CEP,
           temp_rec.UF,
           temp_rec.CODUSUR,
           temp_rec.CODPRACA,
           temp_rec.PRACA,
           temp_rec.CODATIVIDADE,
           temp_rec.RAMOATIVIDADE,
           temp_rec.BLOQUEIODEFINITIVO,
           temp_rec.BLOQUEIOATUAL,
           temp_rec.LIMITECREDITO,
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
  EXECUTE IMMEDIATE 'DELETE TEMP_PCCLIENT';
END;
