CREATE OR REPLACE PROCEDURE PRC_SINC_CLIENTE AS
BEGIN

  FOR r IN (
            
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
                          ('R' || C.CODREDE || ' - ' || UPPER(R.DESCRICAO))
                       END) AS CLIENTEREDE,
                       C.CGCENT CNPJ,
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
                  LEFT JOIN FILIAL F ON F.CEP = REPLACE(C.CEPENT, '-', '')),
              
              OUTROS_CLIENTES AS
               (SELECT 99 CODCLI,
                       'OUTROS CLIENTES' CLIENTE,
                       'C99' CODCLIREDE,
                       'Z - OUTROS CLIENTES' CLIENTEREDE,
                       NULL CNPJ,
                       NULL CEP,
                       'SP' UF,
                       '99' CODFILIALJCCLUB,
                       0 CODUSUR,
                       0 CODPRACA,
                       'OUTRAS PRACAS' PRACA,
                       0 CODATIVIDADE,
                       'OUTRA ATIVIDADE' RAMOATIVIDADE,
                       'N' BLOQUEIODEFINITIVO,
                       'N' BLOQUEIOATUAL,
                       0 LIMITECREDITO,
                       NULL DTCADASTRO
                  FROM DUAL),
              
              TODOS_CLIENTES AS
               (SELECT * FROM OUTROS_CLIENTES UNION ALL SELECT * FROM CLIENTES)
              
              SELECT C.*
                FROM TODOS_CLIENTES C
                LEFT JOIN BI_SINC_CLIENTE S ON S.CODCLI = C.CODCLI
               WHERE S.DT_UPDATE IS NULL
                  OR S.CLIENTE <> C.CLIENTE
                  OR S.CODCLIREDE <> C.CODCLIREDE
                  OR S.CLIENTEREDE <> C.CLIENTEREDE
                  OR S.CNPJ <> C.CNPJ
                  OR S.CEP <> C.CEP
                  OR S.UF <> C.UF
                  OR NVL(S.CODFILIALJCCLUB, '-') <> C.CODFILIALJCCLUB
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
         SET CLIENTE            = r.CLIENTE,
             CODCLIREDE         = r.CODCLIREDE,
             CLIENTEREDE        = r.CLIENTEREDE,
             CNPJ               = r.CNPJ,
             CEP                = r.CEP,
             UF                 = r.UF,
             CODFILIALJCCLUB    = r.CODFILIALJCCLUB,
             CODUSUR            = r.CODUSUR,
             CODPRACA           = r.CODPRACA,
             PRACA              = r.PRACA,
             CODATIVIDADE       = r.CODATIVIDADE,
             RAMOATIVIDADE      = r.RAMOATIVIDADE,
             BLOQUEIODEFINITIVO = r.BLOQUEIODEFINITIVO,
             BLOQUEIOATUAL      = r.BLOQUEIOATUAL,
             LIMITECREDITO      = r.LIMITECREDITO,
             DTCADASTRO         = r.DTCADASTRO,
             DT_UPDATE          = SYSDATE
       WHERE CODCLI = r.CODCLI;
    
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
          (r.CODCLI,
           r.CLIENTE,
           r.CODCLIREDE,
           r.CLIENTEREDE,
           r.CNPJ,
           r.CEP,
           r.UF,
           r.CODFILIALJCCLUB,
           r.CODUSUR,
           r.CODPRACA,
           r.PRACA,
           r.CODATIVIDADE,
           r.RAMOATIVIDADE,
           r.BLOQUEIODEFINITIVO,
           r.BLOQUEIOATUAL,
           r.LIMITECREDITO,
           r.DTCADASTRO,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a criação da tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

END;
