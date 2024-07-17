CREATE OR REPLACE PROCEDURE PRC_SINC_VENDEDOR AS
BEGIN

FOR temp_rec IN (

    WITH VENDEDORES AS
     (SELECT U.CODUSUR,
             U.NOME NOMEORIGINAL,
             (CASE
               WHEN (U.BLOQUEIO = 'S' AND U.CODSUPERVISOR = 1) THEN
                'OUTROS CAPITAL'
               WHEN (U.BLOQUEIO = 'S' AND U.CODSUPERVISOR = 2) THEN
                'OUTROS INTERIOR'
               WHEN (U.BLOQUEIO = 'S' AND S.CODGERENTE = 3) THEN
                'OUTROS LOJA'
               WHEN (U.BLOQUEIO = 'S' AND S.CODGERENTE = 4) THEN
                'OUTROS MARKETPLACES'
               ELSE
                COALESCE(U.USURDIRFV, U.NOME)
             END) VENDEDOR,
             (CASE
               WHEN U.CODUSUR = S.COD_CADRCA THEN 'GERENTE'
               ELSE 'VENDEDOR'
             END) CARGO,
             NVL(U.CODFILIAL,'99') CODFILIAL,
             U.BLOQUEIO,
             U.CODSUPERVISOR,
             S.NOME SUPERVISOR,
             S.CODGERENTE,
             G.NOMEGERENTE GERENTE,
             C.CODAREA,
             C.AREACOMERCIAL
        FROM PCUSUARI U
        LEFT JOIN PCSUPERV S ON S.CODSUPERVISOR = U.CODSUPERVISOR
        LEFT JOIN PCGERENTE G ON G.CODGERENTE = S.CODGERENTE
        LEFT JOIN JFAREACOMERCIAL C ON C.CODAREA = G.CODGERENTESUPERIOR),
    VENDEDOR_COMPRADOR AS
     (SELECT 0 CODUSUR,
             'VENDEDOR COMPRADOR' NOMEORIGINAL,
             'VENDEDOR COMPRADOR' VENDEDOR,
             'COMPRADOR' CARGO,
             '99' CODFILIAL,
             'N' BLOQUEIO,
             0 CODSUPERVISOR,
             'COMPRAS' SUPERVISOR,
             0 CODGERENTE,
             'COMPRAS' GERENTE,
             0 CODAREA,
             'COMPRAS' AREACOMERCIAL
        FROM DUAL),
    TABELA_VENDEDOR AS
     (SELECT * FROM VENDEDORES UNION ALL SELECT * FROM VENDEDOR_COMPRADOR)

    SELECT V.*
      FROM TABELA_VENDEDOR V
      LEFT JOIN BI_SINC_VENDEDOR S ON S.CODUSUR = V.CODUSUR
     WHERE S.DT_UPDATE IS NULL
        OR S.VENDEDOR <> V.VENDEDOR
        OR S.CODFILIAL <> V.CODFILIAL
        OR NVL(S.CARGO,'-') <> V.CARGO
        OR S.BLOQUEIO <> V.BLOQUEIO
        OR S.CODSUPERVISOR <> V.CODSUPERVISOR
        OR S.SUPERVISOR <> V.SUPERVISOR
        OR S.CODGERENTE <> V.CODGERENTE
        OR S.GERENTE <> V.GERENTE
        OR S.CODAREA <> V.CODAREA
        OR S.AREACOMERCIAL <> V.AREACOMERCIAL
)

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_VENDEDOR
         SET NOMEORIGINAL  = temp_rec.NOMEORIGINAL,
             VENDEDOR      = temp_rec.VENDEDOR,
             CARGO         = temp_rec.CARGO,
             CODFILIAL     = temp_rec.CODFILIAL,
             BLOQUEIO      = temp_rec.BLOQUEIO,
             CODSUPERVISOR = temp_rec.CODSUPERVISOR,
             SUPERVISOR    = temp_rec.SUPERVISOR,
             CODGERENTE    = temp_rec.CODGERENTE,
             GERENTE       = temp_rec.GERENTE,
             CODAREA       = temp_rec.CODAREA,
             AREACOMERCIAL = temp_rec.AREACOMERCIAL,
             DT_UPDATE     = SYSDATE
       WHERE CODUSUR = temp_rec.CODUSUR;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_VENDEDOR
          (CODUSUR,
           NOMEORIGINAL,
           VENDEDOR,
           CARGO,
           CODFILIAL,
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
           temp_rec.NOMEORIGINAL,
           temp_rec.VENDEDOR,
           temp_rec.CARGO,
           temp_rec.CODFILIAL,
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
                                'Erro durante a insercao da tabela: ' ||
                                SQLERRM);
    END;
  END LOOP;

  COMMIT;

END;
