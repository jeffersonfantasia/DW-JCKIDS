CREATE OR REPLACE PROCEDURE PRC_SINC_LANC_PAGAR AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCLANC
    (RECNUM,
     CODFILIAL,
     DTCOMPETENCIA,
     DTVENCIMENTO,
     DTPAGAMENTO,
     DTCONTABIL,
     TIPO,
     VALOR,
     VLJUROS,
     VLDESCONTO,
     CODBANCO,
     CODCONTA,
     CODFORNEC,
     TIPOPARCEIRO,
     NUMTRANS,
     NUMNOTA,
     DUPLICATA,
     HISTORICO,
     OBSERVACAO,
     RECNUMPRINC,
     CODROTINABAIXA)
    WITH LANCAMENTOS AS
     (SELECT L.RECNUM,
             L.CODFILIAL,
             L.DTCOMPETENCIA,
             L.DTVENC DTVENCIMENTO,
             L.DTPAGTO DTPAGAMENTO,
             NVL(M.DTCOMPENSACAO, L.DTPAGTO) DTCONTABIL,
             (CASE
               WHEN INSTR(L.HISTORICO2, 'RISCO') > 0 THEN
                'RISCO SACADO'
               WHEN L.CODCONTA = 100001 THEN
                'COMPRA MERCADORIA'
               WHEN L.TIPOLANC = 'C' THEN
                'CONFIRMADO'
               ELSE
                'PROVISIONADO'
             END) TIPO,
             (CASE
               WHEN L.DTPAGTO IS NULL THEN
                L.VALOR
               WHEN L.VPAGO < 0 THEN
                NVL(L.VPAGO, 0) * -1
               WHEN (NVL(L.VPAGOBORDERO, 0) > 0 AND
                    L.VPAGO > NVL(L.VPAGOBORDERO, 0)) THEN
                (L.VPAGOBORDERO - NVL(L.TXPERM, 0))
               ELSE
                L.VPAGO
             END) AS VALOR,
             NVL(L.TXPERM, 0) VLJUROS,
             NVL(L.DESCONTOFIN, 0) VLDESCONTO,
             M.CODBANCO,
             L.CODCONTA,
             L.CODFORNEC,
             L.TIPOPARCEIRO,
             L.NUMTRANS,
             L.NUMNOTA,
             NVL(L.DUPLIC, '1') DUPLICATA,
             L.HISTORICO,
             L.HISTORICO2 OBSERVACAO,
             L.RECNUMPRINC,
             L.CODROTINABAIXA
        FROM PCLANC L
        LEFT JOIN PCMOVCR M ON L.NUMTRANS = M.NUMTRANS
       WHERE NVL(L.INDICE, 0) NOT IN ('B')
         AND NVL(L.CODROTINABAIXA, 0) NOT IN (1207, 1502, 1503, 9806, 9876)
         AND NVL(M.CODBANCO, 0) NOT IN (17, 20, 35, 50, 52, 53, 54)
         AND M.DTESTORNO IS NULL
         AND NVL(L.CODCONTA, 0) NOT IN
             (37, 105, 100022, 100023, 100024, 100027, 101002))
    SELECT L.*
      FROM LANCAMENTOS L
      LEFT JOIN BI_SINC_LANC_PAGAR S ON S.RECNUM = L.RECNUM
     WHERE S.DT_UPDATE IS NULL
        OR S.DTCOMPETENCIA <> L.DTCOMPETENCIA
        OR S.DTVENCIMENTO <> L.DTVENCIMENTO
        OR S.DTPAGAMENTO <> L.DTPAGAMENTO
        OR S.DTCONTABIL <> L.DTCONTABIL
        OR S.TIPO <> L.TIPO
        OR S.VALOR <> L.VALOR
        OR S.VLJUROS <> L.VLJUROS
        OR S.VLDESCONTO <> L.VLDESCONTO
        OR S.CODBANCO <> L.CODBANCO
        OR S.CODCONTA <> L.CODCONTA
        OR S.TIPOPARCEIRO <> L.TIPOPARCEIRO
        OR S.NUMTRANS <> L.NUMTRANS
        OR S.NUMNOTA <> L.NUMNOTA
        OR S.HISTORICO <> L.HISTORICO
        OR S.OBSERVACAO <> L.OBSERVACAO
        OR S.RECNUMPRINC <> L.RECNUMPRINC
        OR S.CODROTINABAIXA <> L.CODROTINABAIXA;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCLANC)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_LANC_PAGAR
         SET CODFILIAL      = temp_rec.CODFILIAL,
             DTCOMPETENCIA  = temp_rec.DTCOMPETENCIA,
             DTVENCIMENTO   = temp_rec.DTVENCIMENTO,
             DTPAGAMENTO    = temp_rec.DTPAGAMENTO,
             DTCONTABIL     = temp_rec.DTCONTABIL,
             TIPO           = temp_rec.TIPO,
             VALOR          = temp_rec.VALOR,
             VLJUROS        = temp_rec.VLJUROS,
             VLDESCONTO     = temp_rec.VLDESCONTO,
             CODBANCO       = temp_rec.CODBANCO,
             CODCONTA       = temp_rec.CODCONTA,
             CODFORNEC      = temp_rec.CODFORNEC,
             TIPOPARCEIRO   = temp_rec.TIPOPARCEIRO,
             NUMTRANS       = temp_rec.NUMTRANS,
             NUMNOTA        = temp_rec.NUMNOTA,
             DUPLICATA      = temp_rec.DUPLICATA,
             HISTORICO      = temp_rec.HISTORICO,
             OBSERVACAO     = temp_rec.OBSERVACAO,
             RECNUMPRINC    = temp_rec.RECNUMPRINC,
             CODROTINABAIXA = temp_rec.CODROTINABAIXA,
             DT_UPDATE      = SYSDATE
       WHERE RECNUM = temp_rec.RECNUM;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_LANC_PAGAR
          (RECNUM,
           CODFILIAL,
           DTCOMPETENCIA,
           DTVENCIMENTO,
           DTPAGAMENTO,
           DTCONTABIL,
           TIPO,
           VALOR,
           VLJUROS,
           VLDESCONTO,
           CODBANCO,
           CODCONTA,
           CODFORNEC,
           TIPOPARCEIRO,
           NUMTRANS,
           NUMNOTA,
           DUPLICATA,
           HISTORICO,
           OBSERVACAO,
           RECNUMPRINC,
           CODROTINABAIXA,
           DT_UPDATE)
        VALUES
          (temp_rec.RECNUM,
           temp_rec.CODFILIAL,
           temp_rec.DTCOMPETENCIA,
           temp_rec.DTVENCIMENTO,
           temp_rec.DTPAGAMENTO,
           temp_rec.DTCONTABIL,
           temp_rec.TIPO,
           temp_rec.VALOR,
           temp_rec.VLJUROS,
           temp_rec.VLDESCONTO,
           temp_rec.CODBANCO,
           temp_rec.CODCONTA,
           temp_rec.CODFORNEC,
           temp_rec.TIPOPARCEIRO,
           temp_rec.NUMTRANS,
           temp_rec.NUMNOTA,
           temp_rec.DUPLICATA,
           temp_rec.HISTORICO,
           temp_rec.OBSERVACAO,
           temp_rec.RECNUMPRINC,
           temp_rec.CODROTINABAIXA,
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
