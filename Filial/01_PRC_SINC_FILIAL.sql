CREATE OR REPLACE PROCEDURE PRC_SINC_FILIAL AS
BEGIN

  FOR r IN (WITH FILIAIS AS
               (SELECT F.CODIGO CODFILIAL,
                      G.NOME_GRUPOFILIAL EMPRESA,
                      NVL(F.FANTASIA, 'JC BROTHERS') FILIAL,
                      REGEXP_REPLACE(F.CGC, '^([0-9]{2})([0-9]{3})([0-9]{3})([0-9]{4})([0-9]{2})$', '\1.\2.\3/\4-\5') CNPJ,
                      (G.NOME_GRUPOFILIAL || ' - CNPJ: ' ||
                      REGEXP_REPLACE(F.CGC, '^([0-9]{2})([0-9]{3})([0-9]{3})([0-9]{4})([0-9]{2})$', '\1.\2.\3/\4-\5')) FILIAL_CONTABIL,
                      TO_NUMBER(F.CODIGO) ORDEM,
                      F.CODGRUPOFILIAL CODEMPRESA,
                      NVL(F.CODFORNEC, 1) CODFORNEC,
                      NVL(F.CODCLI, 4) CODCLI,
                      (CASE
                        WHEN (INSTR(F.FANTASIA, 'LOJA') > 0 OR INSTR(F.FANTASIA, 'ECOMMERCE') > 0) THEN
                         'VAREJO'
                        WHEN INSTR(F.FANTASIA, 'DISTRIBUICAO') > 0 THEN
                         'DISTRIBUICAO'
                        WHEN INSTR(F.FANTASIA, 'ESTOQUE') > 0 THEN
                         'CORPORATIVO'
                        ELSE
                         'OUTROS'
                      END) TIPOFILIAL
                 FROM PCFILIAL F
                 LEFT JOIN PCGRUPOFILIAL G ON G.CODGRUPOFILIAL = F.CODGRUPOFILIAL)
              
              SELECT F.*
                FROM FILIAIS F
                LEFT JOIN BI_SINC_FILIAL S ON S.CODFILIAL = F.CODFILIAL
               WHERE S.DT_UPDATE IS NULL
                  OR NVL(S.EMPRESA, '0') <> F.EMPRESA
                  OR NVL(S.FILIAL, '0') <> F.FILIAL
                  OR NVL(S.CODEMPRESA, '0') <> F.CODEMPRESA
                  OR NVL(S.CODFORNEC, 0) <> F.CODFORNEC
                  OR NVL(S.CODCLI, 0) <> F.CODCLI
                  OR NVL(S.TIPOFILIAL, '0') <> F.TIPOFILIAL)
  
  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionada
  
  LOOP
    BEGIN
      UPDATE BI_SINC_FILIAL
         SET EMPRESA         = r.EMPRESA,
             FILIAL          = r.FILIAL,
             CNPJ            = r.CNPJ,
             FILIAL_CONTABIL = r.FILIAL_CONTABIL,
             ORDEM           = r.ORDEM,
             CODEMPRESA      = r.CODEMPRESA,
             CODFORNEC       = r.CODFORNEC,
             CODCLI          = r.CODCLI,
             TIPOFILIAL      = r.TIPOFILIAL,
             DT_UPDATE       = SYSDATE
       WHERE CODFILIAL = r.CODFILIAL;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_FILIAL
          (CODFILIAL,
           EMPRESA,
           FILIAL,
           CNPJ,
           FILIAL_CONTABIL,
           ORDEM,
           CODEMPRESA,
           CODFORNEC,
           CODCLI,
           TIPOFILIAL,
           DT_UPDATE)
        VALUES
          (r.CODFILIAL,
           r.EMPRESA,
           r.FILIAL,
           r.CNPJ,
           r.FILIAL_CONTABIL,
           r.ORDEM,
           r.CODEMPRESA,
           r.CODFORNEC,
           r.CODCLI,
           r.TIPOFILIAL,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000, 'Erro durante a insercao na tabela: ' || SQLERRM);
    END;
  END LOOP;

  COMMIT;

END;
