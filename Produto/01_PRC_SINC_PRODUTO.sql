CREATE OR REPLACE PROCEDURE PRC_SINC_PRODUTO AS
BEGIN
  -- Insere os resultados novos ou alterados na tabela TEMP
  INSERT INTO TEMP_PCPRODUT
    (CODPROD,
     PRODUTO,
     CODDEPTO,
     DEPARTAMENTO,
     CODSECAO,
     SECAO,
     CODCATEGORIA,
     CATEGORIA,
     CODLINHA,
     LINHA,
     CODFORNEC,
     CODMARCA,
     CODFAB,
     CODBARRAS,
     CODBARRASMASTER,
     PESO,
     LARGURA,
     ALTURA,
     COMPRIMENTO,
     VOLUME,
     QTCXMASTER,
     IMPORTADO,
     REVENDA,
     NCM,
     NCMEX,
     TIPOMERCADORIA,
     FORALINHA,
     CERTIFICACAO,
     DTEXCLUSAO)
    SELECT P.CODPROD,
           REPLACE (P.DESCRICAO, '"', '') DESCRICAO,
           P.CODEPTO,
           D.DESCRICAO,
           P.CODSEC,
           C.DESCRICAO,
           P.CODCATEGORIA,
           A.CATEGORIA,
           P.CODLINHAPROD,
           L.DESCRICAO,
           P.CODFORNEC,
           P.CODMARCA,
           P.CODFAB,
           P.CODAUXILIAR,
           P.CODAUXILIAR2,
           P.PESOLIQ,
           P.LARGURAM3,
           P.ALTURAM3,
           P.COMPRIMENTOM3,
           P.VOLUME,
           P.QTUNITCX,
           P.IMPORTADO,
           P.REVENDA,
           P.NBM,
           P.CODNCMEX,
           P.TIPOMERC,
           P.OBS2,
           P.SUBTITULOECOMMERCE,
           P.DTEXCLUSAO
      FROM PCPRODUT P
      LEFT JOIN PCDEPTO D ON D.CODEPTO = P.CODEPTO
      LEFT JOIN PCSECAO C ON C.CODSEC = P.CODSEC
      LEFT JOIN PCCATEGORIA A ON A.CODCATEGORIA = P.CODCATEGORIA
      LEFT JOIN PCLINHAPROD L ON L.CODLINHA = P.CODLINHAPROD
      LEFT JOIN BI_SINC_PRODUTO S ON S.CODPROD = P.CODPROD
     WHERE S.DT_UPDATE IS NULL
        OR S.PRODUTO <> P.DESCRICAO
        OR S.CODDEPTO <> P.CODEPTO
        OR S.DEPARTAMENTO <> D.DESCRICAO
        OR S.CODSECAO <> P.CODSEC
        OR S.SECAO <> C.DESCRICAO
        OR S.CODCATEGORIA <> P.CODCATEGORIA
        OR S.CATEGORIA <> A.CATEGORIA
        OR S.CODLINHA <> P.CODLINHAPROD
        OR S.LINHA <> L.DESCRICAO
        OR S.CODFORNEC <> P.CODFORNEC
        OR S.CODMARCA <> P.CODMARCA
        OR S.CODFAB <> P.CODFAB
        OR S.CODBARRAS <> P.CODAUXILIAR
        OR S.CODBARRASMASTER <> P.CODAUXILIAR2
        OR S.PESO <> P.PESOLIQ
        OR S.LARGURA <> P.LARGURAM3
        OR S.ALTURA <> P.ALTURAM3
        OR S.COMPRIMENTO <> P.COMPRIMENTOM3
        OR S.VOLUME <> P.VOLUME
        OR S.QTCXMASTER <> P.QTUNITCX
        OR S.IMPORTADO <> P.IMPORTADO
        OR S.REVENDA <> P.REVENDA
        OR S.NCM <> P.NBM
        OR S.NCMEX <> P.CODNCMEX
        OR S.TIPOMERCADORIA <> P.TIPOMERC
        OR S.FORALINHA <> P.OBS2
        OR S.CERTIFICACAO <> P.SUBTITULOECOMMERCE
        OR S.DTEXCLUSAO <> P.DTEXCLUSAO;

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  FOR temp_rec IN (SELECT * FROM TEMP_PCPRODUT)
  
  LOOP
    BEGIN
      UPDATE BI_SINC_PRODUTO
         SET PRODUTO         = temp_rec.PRODUTO,
             CODDEPTO        = temp_rec.CODDEPTO,
             DEPARTAMENTO    = temp_rec.DEPARTAMENTO,
             CODSECAO        = temp_rec.CODSECAO,
             SECAO           = temp_rec.SECAO,
             CODCATEGORIA    = temp_rec.CODCATEGORIA,
             CATEGORIA       = temp_rec.CATEGORIA,
             CODLINHA        = temp_rec.CODLINHA,
             LINHA           = temp_rec.LINHA,
             CODFORNEC       = temp_rec.CODFORNEC,
             CODMARCA        = temp_rec.CODMARCA,
             CODFAB          = temp_rec.CODFAB,
             CODBARRAS       = temp_rec.CODBARRAS,
             CODBARRASMASTER = temp_rec.CODBARRASMASTER,
             PESO            = temp_rec.PESO,
             LARGURA         = temp_rec.LARGURA,
             ALTURA          = temp_rec.ALTURA,
             COMPRIMENTO     = temp_rec.COMPRIMENTO,
             VOLUME          = temp_rec.VOLUME,
             QTCXMASTER      = temp_rec.QTCXMASTER,
             IMPORTADO       = temp_rec.IMPORTADO,
             REVENDA         = temp_rec.REVENDA,
             NCM             = temp_rec.NCM,
             NCMEX           = temp_rec.NCMEX,
             TIPOMERCADORIA  = temp_rec.TIPOMERCADORIA,
             FORALINHA       = temp_rec.FORALINHA,
             CERTIFICACAO    = temp_rec.CERTIFICACAO,
             DTEXCLUSAO      = temp_rec.DTEXCLUSAO,
             DT_UPDATE       = SYSDATE
       WHERE CODPROD = temp_rec.CODPROD;
    
      IF SQL%NOTFOUND
      THEN
        INSERT INTO BI_SINC_PRODUTO
          (CODPROD,
           PRODUTO,
           CODDEPTO,
           DEPARTAMENTO,
           CODSECAO,
           SECAO,
           CODCATEGORIA,
           CATEGORIA,
           CODLINHA,
           LINHA,
           CODFORNEC,
           CODMARCA,
           CODFAB,
           CODBARRAS,
           CODBARRASMASTER,
           PESO,
           LARGURA,
           ALTURA,
           COMPRIMENTO,
           VOLUME,
           QTCXMASTER,
           IMPORTADO,
           REVENDA,
           NCM,
           NCMEX,
           TIPOMERCADORIA,
           FORALINHA,
           CERTIFICACAO,
           DTEXCLUSAO,
           DT_UPDATE,
           DT_SINC)
        VALUES
          (temp_rec.CODPROD,
           temp_rec.PRODUTO,
           temp_rec.CODDEPTO,
           temp_rec.DEPARTAMENTO,
           temp_rec.CODSECAO,
           temp_rec.SECAO,
           temp_rec.CODCATEGORIA,
           temp_rec.CATEGORIA,
           temp_rec.CODLINHA,
           temp_rec.LINHA,
           temp_rec.CODFORNEC,
           temp_rec.CODMARCA,
           temp_rec.CODFAB,
           temp_rec.CODBARRAS,
           temp_rec.CODBARRASMASTER,
           temp_rec.PESO,
           temp_rec.LARGURA,
           temp_rec.ALTURA,
           temp_rec.COMPRIMENTO,
           temp_rec.VOLUME,
           temp_rec.QTCXMASTER,
           temp_rec.IMPORTADO,
           temp_rec.REVENDA,
           temp_rec.NCM,
           temp_rec.NCMEX,
           temp_rec.TIPOMERCADORIA,
           temp_rec.FORALINHA,
           temp_rec.CERTIFICACAO,
           temp_rec.DTEXCLUSAO,
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
  EXECUTE IMMEDIATE 'DELETE TEMP_PCPRODUT';
END;
