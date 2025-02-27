CREATE OR REPLACE PROCEDURE PRC_SINC_PEDIDO_COMPRA AS

   --vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 365;
   vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2020', 'DD/MM/YYYY'); 

BEGIN
FOR temp_rec IN (
    WITH PEDIDO_COMPRA AS
     (SELECT P.CODFILIAL,
             P.DTEMISSAO DATA,
             P.CODFORNEC,
             P.CODCOMPRADOR,
             DECODE(P.TIPODESCARGA,
                    '1',
                    'NORMAL',
                    '5',
                    'BONIFICADO',
                    'A',
                    'CONSIGNADO',
                    'TRANSFERENCIA') TIPO,
             I.NUMPED,
             I.NUMSEQ,
             I.CODPROD,
             ROUND((I.PLIQUIDO + NVL(I.VLIPI,0) + NVL(I.VLST,0)), 4) PRECOCOMPRA,
             I.QTPEDIDA,
             (CASE
               WHEN I.QTENTREGUE > I.QTPEDIDA THEN
                I.QTPEDIDA
               ELSE
                I.QTENTREGUE
             END) AS QTENTREGUE
        FROM PCITEM I
        JOIN PCPEDIDO P ON I.NUMPED = P.NUMPED
        JOIN BI_SINC_PRODUTO P ON P.CODPROD = I.CODPROD),
    
    RESULTADO AS
     (SELECT P.CODFILIAL,
             P.DATA,
             P.CODFORNEC,
             P.CODCOMPRADOR,
             P.TIPO,
             P.NUMPED,
             P.NUMSEQ,
             P.CODPROD,
             P.PRECOCOMPRA,
             P.QTPEDIDA,
             P.QTENTREGUE,
             (P.QTPEDIDA - P.QTENTREGUE) QTSALDO,
             ROUND((P.PRECOCOMPRA * P.QTPEDIDA), 4) VLPEDIDO,
             ROUND((P.PRECOCOMPRA * P.QTENTREGUE), 4) VLENTREGUE,
             ROUND((P.PRECOCOMPRA * (P.QTPEDIDA - P.QTENTREGUE)), 4) VLSALDO
        FROM PEDIDO_COMPRA P)
    
    SELECT P.*
      FROM RESULTADO P
      LEFT JOIN BI_SINC_PEDIDO_COMPRA S ON S.NUMPED = P.NUMPED
                                       AND S.CODPROD = P.CODPROD
                                       AND S.NUMSEQ = P.NUMSEQ
     WHERE 1 = 1 
       AND P.DATA >= vDATA_MOV_INCREMENTAL
       AND (S.DT_UPDATE IS NULL
        OR S.CODFILIAL <> P.CODFILIAL
        OR S.DATA <> P.DATA
        OR S.CODFORNEC <> P.CODFORNEC
        OR S.CODCOMPRADOR <> P.CODCOMPRADOR
        OR S.TIPO <> P.TIPO
        OR S.PRECOCOMPRA <> P.PRECOCOMPRA
        OR S.QTPEDIDA <> P.QTPEDIDA
        OR S.QTENTREGUE <> P.QTENTREGUE
        OR S.VLSALDO <> P.VLSALDO)
)

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condi��es mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_PEDIDO_COMPRA
         SET CODFILIAL    = temp_rec.CODFILIAL,
             DATA         = temp_rec.DATA,
             CODFORNEC    = temp_rec.CODFORNEC,
             CODCOMPRADOR = temp_rec.CODCOMPRADOR,
             TIPO         = temp_rec.TIPO,
             PRECOCOMPRA  = temp_rec.PRECOCOMPRA,
             QTPEDIDA     = temp_rec.QTPEDIDA,
             QTENTREGUE   = temp_rec.QTENTREGUE,
             QTSALDO      = temp_rec.QTSALDO,
             VLPEDIDO     = temp_rec.VLPEDIDO,
             VLENTREGUE   = temp_rec.VLENTREGUE,
             VLSALDO      = temp_rec.VLSALDO,
             DT_UPDATE    = SYSDATE
       WHERE NUMPED = temp_rec.NUMPED
         AND CODPROD = temp_rec.CODPROD
         AND NUMSEQ = temp_rec.NUMSEQ;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_PEDIDO_COMPRA
          (CODFILIAL,
           DATA,
           CODFORNEC,
           CODCOMPRADOR,
           TIPO,
           NUMPED,
           NUMSEQ,
           CODPROD,
           PRECOCOMPRA,
           QTPEDIDA,
           QTENTREGUE,
           QTSALDO,
           VLPEDIDO,
           VLENTREGUE,
           VLSALDO,
           DT_UPDATE)
        VALUES
          (temp_rec.CODFILIAL,
           temp_rec.DATA,
           temp_rec.CODFORNEC,
           temp_rec.CODCOMPRADOR,
           temp_rec.TIPO,
           temp_rec.NUMPED,
           temp_rec.NUMSEQ,
           temp_rec.CODPROD,
           temp_rec.PRECOCOMPRA,
           temp_rec.QTPEDIDA,
           temp_rec.QTENTREGUE,
           temp_rec.QTSALDO,
           temp_rec.VLPEDIDO,
           temp_rec.VLENTREGUE,
           temp_rec.VLSALDO,
           SYSDATE);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro encontrado: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20000,
                                'Erro durante a cria��o da tabela: ' ||
                                SQLERRM);
    END;
  END LOOP;


  --EXCLUIR GERISTROS QUE NAO PERTENCEM MAIS A PCITEM
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_PEDIDO_COMPRA
 WHERE (NUMPED, NUMSEQ, CODPROD) IN
       (SELECT S.NUMPED,
               S.NUMSEQ,
               S.CODPROD
          FROM BI_SINC_PEDIDO_COMPRA S
          LEFT JOIN PCITEM I ON S.NUMPED = I.NUMPED
                            AND S.NUMSEQ = I.NUMSEQ
                            AND S.CODPROD = I.CODPROD
         WHERE I.CODPROD IS NULL)';
  END;

COMMIT;

END;
