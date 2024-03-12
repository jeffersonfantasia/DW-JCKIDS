CREATE OR REPLACE PROCEDURE PRC_SINC_CLIENTE AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCCLIENT
    (CODCLI,
     CLIENTE,
     CODREDE,
     REDE,
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
    SELECT C.CODCLI,
           C.CLIENTE,
           C.CODREDE,
           R.DESCRICAO REDE,
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
      LEFT JOIN PCATIVI A ON A.CODATIV = C.CODATV1
      LEFT JOIN BI_SINC_CLIENTE S ON S.CODCLI = C.CODCLI
     WHERE S.DT_UPDATE IS NULL
        OR S.CLIENTE <> C.CLIENTE
        OR S.CODREDE <> C.CODREDE
        OR S.REDE <> R.DESCRICAO
        OR S.CNPJ <> REGEXP_REPLACE(C.CGCENT,
                                    '([0-9]{2})([0-9]{3})([0-9]{3})([0-9]{4})',
                                    '\1.\2.\3/\4-')
        OR S.CEP <> REPLACE(C.CEPENT, '-', '')
        OR S.UF <> C.ESTENT
        OR S.CODUSUR <> C.CODUSUR1
        OR S.CODPRACA <> C.CODPRACA
        OR S.PRACA <> P.PRACA
        OR S.CODATIVIDADE <> C.CODATV1
        OR S.RAMOATIVIDADE <> A.RAMO
        OR S.BLOQUEIODEFINITIVO <> C.BLOQUEIODEFINITIVO
        OR S.BLOQUEIOATUAL <> C.BLOQUEIO
        OR S.LIMITECREDITO <> C.LIMCRED;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCCLIENT)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_CLIENTE
         SET CLIENTE            = temp_rec.CLIENTE,
             CODREDE            = temp_rec.CODREDE,
             REDE               = temp_rec.REDE,
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
           CODREDE,
           REDE,
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
           DT_UPDATE,
           DT_SINC)
        VALUES
          (temp_rec.CODCLI,
           temp_rec.CLIENTE,
           temp_rec.CODREDE,
           temp_rec.REDE,
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
  EXECUTE IMMEDIATE 'DELETE TEMP_PCCLIENT';
END;
