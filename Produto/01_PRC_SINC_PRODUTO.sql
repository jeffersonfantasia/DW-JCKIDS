CREATE OR REPLACE PROCEDURE PRC_SINC_PRODUTO AS
BEGIN
FOR temp_rec IN (

    WITH PRODUTOS AS
     (SELECT P.CODPROD,
             P.DESCRICAO PRODUTO,
             P.CODPRODMASTER,
             (SELECT P2.DESCRICAO
                FROM PCPRODUT P2
               WHERE P2.CODPROD = P.CODPRODMASTER) PRODUTOMASTER,
             P.CODEPTO CODDEPTO,
             D.DESCRICAO DEPARTAMENTO,
             P.CODSEC CODSECAO,
             C.DESCRICAO SECAO,
             P.CODCATEGORIA,
             A.CATEGORIA,
             DECODE(P.CODLINHAPROD, NULL, 0, P.CODLINHAPROD) CODLINHA,
             DECODE(L.DESCRICAO, NULL, 'SEM LINHA', L.DESCRICAO) LINHA,
             (CASE
               WHEN P.CODPROD = P.CODPRODMASTER THEN
                'N'
               ELSE
                'S'
             END) PRODUTOFILHO,
             P.CODFORNEC,
             P.CODMARCA,
             TRIM(UPPER(M.MARCA)) MARCA,
             TRIM(UPPER(NVL(M.TITULO,'NORMAL'))) TIPOCOMISSAO,
             P.CODFAB,
             P.CODAUXILIAR CODBARRAS,
             P.CODAUXILIAR2 CODBARRASMASTER,
             NVL(P.PESOLIQ, 0) PESO,
             NVL((P.LARGURAM3 * 100), 0) LARGURA,
             NVL((P.ALTURAM3 * 100), 0) ALTURA,
             NVL((P.COMPRIMENTOM3 * 100), 0) COMPRIMENTO,
             NVL(P.VOLUME, 0) VOLUME,
             P.QTUNITCX QTCXMASTER,
             P.IMPORTADO,
             P.REVENDA,
             P.NBM NCM,
             P.CODNCMEX NCMEX,
             DECODE(P.TIPOMERC,
                    'L',
                    'NORMAL',
                    'CB',
                    'CESTA BASICA',
                    'KT',
                    'KIT',
                    'DB',
                    'BRINDE',
                    'MC',
                    'MATERIAL CONSUMO',
                    NULL,
                    'NORMAL',
                    'NAO INFORMADO') TIPOMERCADORIA,
             DECODE(P.OBS2, 'FL', 'S', 'N') FORALINHA,
             NVL(P.SUBTITULOECOMMERCE, 'SEM CERTIFICACAO CADASTRADA') CERTIFICACAO
        FROM PCPRODUT P
        LEFT JOIN PCDEPTO D ON D.CODEPTO = P.CODEPTO
        LEFT JOIN PCSECAO C ON C.CODSEC = P.CODSEC
        LEFT JOIN PCCATEGORIA A ON A.CODCATEGORIA = P.CODCATEGORIA
        LEFT JOIN PCLINHAPROD L ON L.CODLINHA = P.CODLINHAPROD
        LEFT JOIN PCMARCA M ON M.CODMARCA = P.CODMARCA
       WHERE P.CODAUXILIAR IS NOT NULL
         AND P.CODEPTO IS NOT NULL
         AND P.CODSEC IS NOT NULL
         AND P.CODCATEGORIA IS NOT NULL
         AND P.CODFORNEC IS NOT NULL
         AND P.CODMARCA IS NOT NULL
         AND P.CODFAB IS NOT NULL)
    
    SELECT P.*
      FROM PRODUTOS P
      LEFT JOIN BI_SINC_PRODUTO S ON S.CODPROD = P.CODPROD
     WHERE S.DT_UPDATE IS NULL
        OR S.PRODUTO <> P.PRODUTO
        OR S.CODPRODMASTER <> P.CODPRODMASTER
        OR S.PRODUTOMASTER <> P.PRODUTOMASTER
        OR S.CODDEPTO <> P.CODDEPTO
        OR S.DEPARTAMENTO <> P.DEPARTAMENTO
        OR S.CODSECAO <> P.CODSECAO
        OR S.SECAO <> P.SECAO
        OR S.CODCATEGORIA <> P.CODCATEGORIA
        OR S.CATEGORIA <> P.CATEGORIA
        OR S.CODLINHA <> P.CODLINHA
        OR S.LINHA <> P.LINHA
        OR S.PRODUTOFILHO <> P.PRODUTOFILHO
        OR S.CODFORNEC <> P.CODFORNEC
        OR S.CODMARCA <> P.CODMARCA
        OR S.MARCA <> P.MARCA
        OR NVL(S.TIPOCOMISSAO,'_') <> P.TIPOCOMISSAO
        OR S.CODFAB <> P.CODFAB
        OR S.CODBARRAS <> P.CODBARRAS
        OR S.CODBARRASMASTER <> P.CODBARRASMASTER
        OR S.PESO <> P.PESO
        OR S.LARGURA <> P.LARGURA
        OR S.ALTURA <> P.ALTURA
        OR S.COMPRIMENTO <> P.COMPRIMENTO
        OR S.VOLUME <> P.VOLUME
        OR S.QTCXMASTER <> P.QTCXMASTER
        OR S.IMPORTADO <> P.IMPORTADO
        OR S.REVENDA <> P.REVENDA
        OR S.NCM <> P.NCM
        OR S.NCMEX <> P.NCMEX
        OR S.TIPOMERCADORIA <> P.TIPOMERCADORIA
        OR S.FORALINHA <> P.FORALINHA
        OR S.CERTIFICACAO <> P.CERTIFICACAO
)

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_PRODUTO
         SET PRODUTO         = temp_rec.PRODUTO,
             CODPRODMASTER   = CODPRODMASTER,
             PRODUTOMASTER   = temp_rec.PRODUTOMASTER,
             CODDEPTO        = temp_rec.CODDEPTO,
             DEPARTAMENTO    = temp_rec.DEPARTAMENTO,
             CODSECAO        = temp_rec.CODSECAO,
             SECAO           = temp_rec.SECAO,
             CODCATEGORIA    = temp_rec.CODCATEGORIA,
             CATEGORIA       = temp_rec.CATEGORIA,
             CODLINHA        = temp_rec.CODLINHA,
             LINHA           = temp_rec.LINHA,
             PRODUTOFILHO    = temp_rec.PRODUTOFILHO,
             CODFORNEC       = temp_rec.CODFORNEC,
             CODMARCA        = temp_rec.CODMARCA,
             MARCA           = temp_rec.MARCA,
             TIPOCOMISSAO    = temp_rec.TIPOCOMISSAO,
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
             DT_UPDATE       = SYSDATE
       WHERE CODPROD = temp_rec.CODPROD;
    
      IF SQL%NOTFOUND THEN
        INSERT INTO BI_SINC_PRODUTO
          (CODPROD,
           PRODUTO,
           CODPRODMASTER,
           PRODUTOMASTER,
           CODDEPTO,
           DEPARTAMENTO,
           CODSECAO,
           SECAO,
           CODCATEGORIA,
           CATEGORIA,
           CODLINHA,
           LINHA,
           PRODUTOFILHO,
           CODFORNEC,
           CODMARCA,
           MARCA,
           TIPOCOMISSAO,
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
           DT_UPDATE)
        VALUES
          (temp_rec.CODPROD,
           temp_rec.PRODUTO,
           temp_rec.CODPRODMASTER,
           temp_rec.PRODUTOMASTER,
           temp_rec.CODDEPTO,
           temp_rec.DEPARTAMENTO,
           temp_rec.CODSECAO,
           temp_rec.SECAO,
           temp_rec.CODCATEGORIA,
           temp_rec.CATEGORIA,
           temp_rec.CODLINHA,
           temp_rec.LINHA,
           temp_rec.PRODUTOFILHO,
           temp_rec.CODFORNEC,
           temp_rec.CODMARCA,
           temp_rec.MARCA,
           temp_rec.TIPOCOMISSAO,
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

END;
