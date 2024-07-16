CREATE OR REPLACE PROCEDURE PRC_SINC_CLIENTE AS
BEGIN

  FOR temp_rec IN (
                   
                     WITH FILIAL AS
                      (SELECT F.CODIGO CODFILIALJCCLUB,
                              REPLACE(F.CEP, '-', '') CEP
                         FROM PCFILIAL F
                        WHERE INSTR(F.FANTASIA, 'LOJA') > 0),
                     CLIENTES AS
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
                                 ('R' || C.CODREDE || ' - ' ||
                                 UPPER(R.DESCRICAO))
                              END) AS CLIENTEREDE,
                              REGEXP_REPLACE(C.CGCENT,
                                             '([0-9]{2})([0-9]{3})([0-9]{3})([0-9]{4})',
                                             '\1.\2.\3/\4-') CNPJ,
                              REPLACE(C.CEPENT, '-', '') AS CEP,
                              C.ESTENT UF,
                              NVL(F.CODFILIALJCCLUB, '99') CODFILIALJCCLUB,
                              C.CODUSUR1 CODUSUR,
                              C.CODPRACA,
                              P.PRACA,
                              C.CODATV1 CODATIVIDADE,
                              A.RAMO RAMOATIVIDADE,
                              C.BLOQUEIODEFINITIVO,
                              C.BLOQUEIO BLOQUEIOATUAL,
                              C.LIMCRED LIMITECREDITO,
                              C.DTCADASTRO
                         FROM PCCLIENT C
                         LEFT JOIN PCREDECLIENTE R ON R.CODREDE = C.CODREDE
                         LEFT JOIN PCPRACA P ON P.CODPRACA = C.CODPRACA
                         LEFT JOIN PCATIVI A ON A.CODATIV = C.CODATV1
                         LEFT JOIN FILIAL F ON F.CEP =
                                               REPLACE(C.CEPENT, '-', ''))
                     
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
                         OR NVL(S.CODFILIALJCCLUB,'-') <> C.CODFILIALJCCLUB
                         OR S.CODUSUR <> C.CODUSUR
                         OR S.CODPRACA <> C.CODPRACA
                         OR S.PRACA <> C.PRACA
                         OR S.CODATIVIDADE <> C.CODATIVIDADE
                         OR S.RAMOATIVIDADE <> C.RAMOATIVIDADE
                         OR S.BLOQUEIODEFINITIVO <> C.BLOQUEIODEFINITIVO
                         OR S.BLOQUEIOATUAL <> C.BLOQUEIOATUAL
                         OR S.LIMITECREDITO <> C.LIMITECREDITO
                         OR S.DTCADASTRO <> C.DTCADASTRO)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_CLIENTE
         SET CLIENTE            = temp_rec.CLIENTE,
             CODCLIREDE         = temp_rec.CODCLIREDE,
             CLIENTEREDE        = temp_rec.CLIENTEREDE,
             CNPJ               = temp_rec.CNPJ,
             CEP                = temp_rec.CEP,
             UF                 = temp_rec.UF,
             CODFILIALJCCLUB    = temp_rec.CODFILIALJCCLUB,
             CODUSUR            = temp_rec.CODUSUR,
             CODPRACA           = temp_rec.CODPRACA,
             PRACA              = temp_rec.PRACA,
             CODATIVIDADE       = temp_rec.CODATIVIDADE,
             RAMOATIVIDADE      = temp_rec.RAMOATIVIDADE,
             BLOQUEIODEFINITIVO = temp_rec.BLOQUEIODEFINITIVO,
             BLOQUEIOATUAL      = temp_rec.BLOQUEIOATUAL,
             LIMITECREDITO      = temp_rec.LIMITECREDITO,
             DTCADASTRO         = temp_rec.DTCADASTRO,
             DT_UPDATE          = SYSDATE
       WHERE CODCLI = temp_rec.CODCLI;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_CLIENTE
          (CODCLI,
           CLIENTE,
           CODCLIREDE,
           CLIENTEREDE,
           CNPJ,
           CEP,
           UF,
           CODFILIALJCCLUB,
           CODUSUR,
           CODPRACA,
           PRACA,
           CODATIVIDADE,
           RAMOATIVIDADE,
           BLOQUEIODEFINITIVO,
           BLOQUEIOATUAL,
           LIMITECREDITO,
           DTCADASTRO,
           DT_UPDATE)
        VALUES
          (temp_rec.CODCLI,
           temp_rec.CLIENTE,
           temp_rec.CODCLIREDE,
           temp_rec.CLIENTEREDE,
           temp_rec.CNPJ,
           temp_rec.CEP,
           temp_rec.UF,
           temp_rec.CODFILIALJCCLUB,
           temp_rec.CODUSUR,
           temp_rec.CODPRACA,
           temp_rec.PRACA,
           temp_rec.CODATIVIDADE,
           temp_rec.RAMOATIVIDADE,
           temp_rec.BLOQUEIODEFINITIVO,
           temp_rec.BLOQUEIOATUAL,
           temp_rec.LIMITECREDITO,
           temp_rec.DTCADASTRO,
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

END;
