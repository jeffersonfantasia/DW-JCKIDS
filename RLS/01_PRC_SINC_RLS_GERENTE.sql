CREATE OR REPLACE PROCEDURE PRC_SINC_RLS_GERENTE AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_RLS_GERENTE
    (CODUSUARIO,
     EMAIL,
     AREA,
     CODSUPERVISOR)
  
    WITH EMAILS AS
     (SELECT 1 CODUSUARIO,
             'jefferson.corporativo@brokerdistribui.com.br' EMAIL,
             'DIRETORIA' AREA,
             NULL CODSUPERVISOR
        FROM DUAL
      UNION ALL
      SELECT 2,
             'joe.jc@brokerdistribui.com.br',
             'DIRETORIA',
             NULL
        FROM DUAL
      UNION ALL
      SELECT 3,
             'jonatas@brokerdistribui.com.br',
             'DIRETORIA',
             NULL
        FROM DUAL
      UNION ALL
      SELECT 4,
             'jennifer.corporativo@brokerdistribui.com.br',
             'DIRETORIA',
             NULL
        FROM DUAL
      UNION ALL
      SELECT 5,
             'c-buso@uol.com.br',
             'DIRETORIA',
             NULL
        FROM DUAL
      UNION ALL
      SELECT 6,
             'controller@brokerdistribui.com.br',
             'FINANCEIRO',
             NULL
        FROM DUAL
      UNION ALL
      SELECT 7,
             'camila.adm@brokerdistribui.com.br',
             'ADM',
             NULL
        FROM DUAL
      UNION ALL
      SELECT 8,
             'marcelo.vendas@brokerdistribui.com.br',
             'GERENTE',
             1
        FROM DUAL
      UNION ALL
      SELECT 9,
             'marcelo.vendas@brokerdistribui.com.br',
             'GERENTE',
             4
        FROM DUAL
      UNION ALL
      SELECT 10,
             'jcrosalem@terra.com.br',
             'GERENTE',
             2
        FROM DUAL
      UNION ALL
      SELECT 11,
             'lita.loja@lojasjckids.com.br',
             'GERENTE',
             5
        FROM DUAL
      UNION ALL
      SELECT 12,
             'gabriela.loja@lojasjckids.com.br',
             'GERENTE',
             13
        FROM DUAL
      UNION ALL
      SELECT 13,
             'juliana.loja@lojasjckids.com.br',
             'GERENTE',
             14
        FROM DUAL
      UNION ALL
      SELECT 14,
             'debora.loja@lojasjckids.com.br',
             'GERENTE',
             15
        FROM DUAL
      UNION ALL
      SELECT 15,
             'jefferson.fantasia@gmail.com',
             'GERENTE',
             15
        FROM DUAL)
    
    SELECT E.*
      FROM EMAILS E
      LEFT JOIN BI_SINC_RLS_GERENTE S ON E.CODUSUARIO = S.CODUSUARIO
     WHERE S.DT_UPDATE IS NULL
        OR S.EMAIL <> E.EMAIL
        OR S.AREA <> E.AREA
        OR S.CODSUPERVISOR <> E.CODSUPERVISOR;

  FOR temp_rec IN (SELECT * FROM TEMP_RLS_GERENTE)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_RLS_GERENTE
         SET EMAIL         = temp_rec.EMAIL,
             AREA          = temp_rec.AREA,
             CODSUPERVISOR = temp_rec.CODSUPERVISOR,
             DT_UPDATE     = SYSDATE
       WHERE CODUSUARIO = temp_rec.CODUSUARIO;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_RLS_GERENTE
          (CODUSUARIO,
           EMAIL,
           AREA,
           CODSUPERVISOR,
           DT_UPDATE)
        VALUES
          (temp_rec.CODUSUARIO,
           temp_rec.EMAIL,
           temp_rec.AREA,
           temp_rec.CODSUPERVISOR,
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
  -- Exclui os registros da tabela temporária TEMP criada;
  EXECUTE IMMEDIATE 'DELETE TEMP_RLS_GERENTE';
END;
