CREATE OR REPLACE PROCEDURE PRC_SINC_PAGAR_FORNECEDOR AS

  vDATA_PAG_INCREMENTAL DATE := TRUNC(SYSDATE) - 30;

BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PAGAR_FORNECEDOR
    (RECNUM,
     CODFILIAL,
     DTCOMPETENCIA,
     DTVENCIMENTO,
     DTPAGAMENTO,
     VALOR,
     VALORAPAGAR,
     CODFORNEC,
     NOTA,
     CODBANCO)
    WITH LANCAMENTOS AS
     (SELECT L.RECNUM,
             L.CODFILIAL,
             L.DTCOMPETENCIA,
             L.DTVENCIMENTO,
             L.DTPAGAMENTO,
             L.VALOR,
             L.VALORAPAGAR,
             L.CODFORNEC,
             (L.NUMNOTA || '-' || L.DUPLICATA) NOTA,
             L.CODBANCO
        FROM BI_SINC_LANC_PAGAR L
       WHERE L.TIPOPARCEIRO = 'F'
         AND L.CODCONTA = 100001)
    SELECT L.*
      FROM LANCAMENTOS L
      LEFT JOIN BI_SINC_PAGAR_FORNECEDOR S ON S.RECNUM = L.RECNUM
     WHERE (L.DTPAGAMENTO >= vDATA_PAG_INCREMENTAL OR L.DTPAGAMENTO IS NULL)
       AND (S.DT_UPDATE IS NULL OR
           NVL(S.DTVENCIMENTO, '01/01/1899') <> L.DTVENCIMENTO OR
           NVL(S.DTPAGAMENTO, '01/01/1899') <> L.DTPAGAMENTO OR
           S.VALOR <> L.VALOR OR S.VALORAPAGAR <> L.VALORAPAGAR OR
           S.NOTA <> L.NOTA);

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PAGAR_FORNECEDOR)

  LOOP
    BEGIN
      UPDATE BI_SINC_PAGAR_FORNECEDOR
         SET CODFILIAL     = temp_rec.CODFILIAL,
             DTCOMPETENCIA = temp_rec.DTCOMPETENCIA,
             DTVENCIMENTO  = temp_rec.DTVENCIMENTO,
             DTPAGAMENTO   = temp_rec.DTPAGAMENTO,
             VALOR         = temp_rec.VALOR,
             VALORAPAGAR   = temp_rec.VALORAPAGAR,
             CODFORNEC     = temp_rec.CODFORNEC,
             NOTA          = temp_rec.NOTA,
             CODBANCO      = temp_rec.CODBANCO,
             DT_UPDATE     = SYSDATE
       WHERE RECNUM = temp_rec.RECNUM;

      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_PAGAR_FORNECEDOR
          (RECNUM,
           CODFILIAL,
           DTCOMPETENCIA,
           DTVENCIMENTO,
           DTPAGAMENTO,
           VALOR,
           VALORAPAGAR,
           CODFORNEC,
           NOTA,
           CODBANCO,
           DT_UPDATE)
        VALUES
          (temp_rec.RECNUM,
           temp_rec.CODFILIAL,
           temp_rec.DTCOMPETENCIA,
           temp_rec.DTVENCIMENTO,
           temp_rec.DTPAGAMENTO,
           temp_rec.VALOR,
           temp_rec.VALORAPAGAR,
           temp_rec.CODFORNEC,
           temp_rec.NOTA,
           temp_rec.CODBANCO,
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
    -- Exclui os registros da tabela BI_SINC_PAGAR_FORNECEDOR que possuem DTPAGAMENTO NOT NULL
  EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_PAGAR_FORNECEDOR WHERE DTPAGAMENTO IS NOT NULL OR NVL(CODBANCO, 0) IN (17, 20, 35, 50, 52, 53, 54)';

  -- Exclui os registros da tabela temporária TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_PAGAR_FORNECEDOR';
END;
