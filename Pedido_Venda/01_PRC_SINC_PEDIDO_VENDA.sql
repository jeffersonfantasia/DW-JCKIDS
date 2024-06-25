CREATE OR REPLACE PROCEDURE PRC_SINC_PEDIDO_VENDA AS

   vDATA_MOV_INCREMENTAL DATE := TRUNC(SYSDATE) - 90;
   --vDATA_MOV_INCREMENTAL DATE := TO_DATE('01/01/2014', 'DD/MM/YYYY'); 

BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PEDIDO_VENDA
    (CODFILIAL,
     CODFILIALRETIRA,
     DATA,
     DATALIMITE,
     NUMPED,
     TIPOVENDA,
     CODCLI,
     NUMSEQ,
     CODPROD,
     QT,
     PVENDA,
     VLPRODUTO,
     VLPEDIDO,
     CODUSUR,
     POSICAO,
     TIPOBLOQUEIO,
     MOTIVOBLOQUEIO,
     OBSPEDIDO,
     CODMOTIVOPENDENTE)
  
    WITH MAXCODIGOBLOQUEIO AS
     (SELECT MAX(CODIGO) CODIGO_MAX,
             NUMPED
        FROM PCBLOQUEIOSPEDIDO B
       GROUP BY NUMPED),
    
    MOTIVOBLOQUEIO AS
     (SELECT B.NUMPED,
             B.TIPO,
             B.MOTIVO
        FROM PCBLOQUEIOSPEDIDO B
       INNER JOIN MAXCODIGOBLOQUEIO M ON B.CODIGO = M.CODIGO_MAX
                                     AND B.NUMPED = M.NUMPED),
    
    PEDIDOS AS
     (SELECT C.CODFILIAL,
             NVL(I.CODFILIALRETIRA, C.CODFILIAL) CODFILIALRETIRA,
             I.DATA,
             P.DT_LIMITE DATALIMITE,
             I.NUMPED,
             C.CONDVENDA TIPOVENDA,
             I.CODCLI,
             I.NUMSEQ,
             I.CODPROD,
             I.QT,
             I.PVENDA,
             (I.QT * I.PVENDA) VLPRODUTO,
             (I.QT * (NVL(I.PVENDA, 0) + NVL(I.ST, 0))) VLPEDIDO,
             I.CODUSUR,
             I.POSICAO,
             M.TIPO TIPOBLOQUEIO,
             M.MOTIVO MOTIVOBLOQUEIO,
             TRIM(C.OBS) OBSPEDIDO,
             (CASE
               WHEN I.POSICAO = 'L' AND C.CODCOB = 'ANTE' THEN
                1 --'ANTECIPADO'
               WHEN I.POSICAO = 'L' THEN
                2 --'LIBERADO'
               WHEN I.POSICAO = 'M' THEN
                3 --'LOGÍSTICA'
               WHEN INSTR(TRIM(C.OBS), 'FATURAR') > 0 THEN
                4 --'DATA PROGRAMADA'
               WHEN I.POSICAO = 'P' THEN
                5 --'PENDENTE'
               WHEN (I.POSICAO = 'B' AND M.TIPO = 'F') THEN
                6 --'BLOQUEADO FINANCEIRO'
               WHEN (I.POSICAO = 'B' AND M.TIPO = 'C') THEN
                7 --'BLOQUEADO COMERCIAL'
               ELSE
                99
             END) CODMOTIVOPENDENTE
        FROM PCPEDI I
        JOIN PCPEDC C ON I.NUMPED = C.NUMPED
        LEFT JOIN MOTIVOBLOQUEIO M ON I.NUMPED = M.NUMPED
        LEFT JOIN JCPRIORIDADESEPARACAO P ON C.NUMPED = P.NUMPED)
    
    SELECT P.*
      FROM PEDIDOS P
      LEFT JOIN BI_SINC_PEDIDO_VENDA S ON S.NUMPED = P.NUMPED
                                      AND S.CODPROD = P.CODPROD
                                      AND S.NUMSEQ = P.NUMSEQ
     WHERE 1 = 1 
       AND P.DATA >= vDATA_MOV_INCREMENTAL
       AND (S.DT_UPDATE IS NULL
        OR S.CODFILIAL <> P.CODFILIAL
        OR S.CODFILIALRETIRA <> P.CODFILIALRETIRA
        OR NVL(S.DATA,'01/01/1889') <> P.DATA
        OR NVL(S.DATALIMITE,'01/01/1889') <> P.DATALIMITE
        OR S.TIPOVENDA <> P.TIPOVENDA
        OR S.CODCLI <> P.CODCLI
        OR S.CODPROD <> P.CODPROD
        OR S.QT <> P.QT
        OR ROUND(S.PVENDA,6) <> ROUND(P.PVENDA,6)
        OR ROUND(S.VLPEDIDO,6) <> ROUND(P.VLPEDIDO,6)
        OR S.CODUSUR <> P.CODUSUR
        OR S.POSICAO <> P.POSICAO
        OR NVL(S.TIPOBLOQUEIO,'-') <> P.TIPOBLOQUEIO
        OR NVL(S.MOTIVOBLOQUEIO,'-') <> P.MOTIVOBLOQUEIO
        OR NVL(S.OBSPEDIDO,'-') <> P.OBSPEDIDO);

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PEDIDO_VENDA)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_PEDIDO_VENDA
         SET CODFILIAL         = temp_rec.CODFILIAL,
             CODFILIALRETIRA   = temp_rec.CODFILIALRETIRA,
             DATA              = temp_rec.DATA,
             DATALIMITE        = temp_rec.DATALIMITE,
             TIPOVENDA         = temp_rec.TIPOVENDA,
             CODCLI            = temp_rec.CODCLI,
             QT                = temp_rec.QT,
             PVENDA            = temp_rec.PVENDA,
             VLPRODUTO         = temp_rec.VLPRODUTO,
             VLPEDIDO          = temp_rec.VLPEDIDO,
             CODUSUR           = temp_rec.CODUSUR,
             POSICAO           = temp_rec.POSICAO,
             TIPOBLOQUEIO      = temp_rec.TIPOBLOQUEIO,
             MOTIVOBLOQUEIO    = temp_rec.MOTIVOBLOQUEIO,
             OBSPEDIDO         = temp_rec.OBSPEDIDO,
             CODMOTIVOPENDENTE = temp_rec.CODMOTIVOPENDENTE
       WHERE NUMPED = temp_rec.NUMPED
         AND CODPROD = temp_rec.CODPROD
         AND NUMSEQ = temp_rec.NUMSEQ;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_PEDIDO_VENDA
          (CODFILIAL,
           CODFILIALRETIRA,
           DATA,
           DATALIMITE,
           NUMPED,
           TIPOVENDA,
           CODCLI,
           NUMSEQ,
           CODPROD,
           QT,
           PVENDA,
           VLPRODUTO,
           VLPEDIDO,
           CODUSUR,
           POSICAO,
           TIPOBLOQUEIO,
           MOTIVOBLOQUEIO,
           OBSPEDIDO,
           CODMOTIVOPENDENTE,
           DT_UPDATE)
        VALUES
          (temp_rec.CODFILIAL,
           temp_rec.CODFILIALRETIRA,
           temp_rec.DATA,
           temp_rec.DATALIMITE,
           temp_rec.NUMPED,
           temp_rec.TIPOVENDA,
           temp_rec.CODCLI,
           temp_rec.NUMSEQ,
           temp_rec.CODPROD,
           temp_rec.QT,
           temp_rec.PVENDA,
           temp_rec.VLPRODUTO,
           temp_rec.VLPEDIDO,
           temp_rec.CODUSUR,
           temp_rec.POSICAO,
           temp_rec.TIPOBLOQUEIO,
           temp_rec.MOTIVOBLOQUEIO,
           temp_rec.OBSPEDIDO,
           temp_rec.CODMOTIVOPENDENTE,
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

  --EXCLUIR GERISTROS QUE NAO PERTENCEM MAIS A PCPEDI
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM BI_SINC_PEDIDO_VENDA
 WHERE (NUMPED, NUMSEQ, CODPROD) IN
       (SELECT S.NUMPED,
               S.NUMSEQ,
               S.CODPROD
          FROM BI_SINC_PEDIDO_VENDA S
          LEFT JOIN PCPEDI I ON S.NUMPED = I.NUMPED
                            AND S.NUMSEQ = I.NUMSEQ
                            AND S.CODPROD = I.CODPROD
         WHERE I.CODPROD IS NULL)';
  END;
  -- Exclui os registros da tabela temporária TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_PEDIDO_VENDA';
END;
