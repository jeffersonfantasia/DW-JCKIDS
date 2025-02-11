CREATE OR REPLACE PROCEDURE PRC_SINC_PRODUTO AS
BEGIN
FOR r IN (

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
             P.ENVIARFORCAVENDAS ENVIAFV,
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
        OR NVL(S.QTCXMASTER,0) <> P.QTCXMASTER
        OR NVL(S.IMPORTADO,'0') <> P.IMPORTADO
        OR NVL(S.REVENDA,'0') <> P.REVENDA
        OR NVL(S.ENVIAFV,'0') <> NVL(P.ENVIAFV,'0')
        OR NVL(S.NCM,'0') <> P.NCM
        OR NVL(S.NCMEX,'0') <> P.NCMEX
        OR NVL(S.TIPOMERCADORIA,'0') <> P.TIPOMERCADORIA
        OR NVL(S.FORALINHA,'0') <> P.FORALINHA
        OR S.CERTIFICACAO <> P.CERTIFICACAO
)

  -- Atualiza ou insere os resultados na tabela BI_SINC conforme as condições mencionadas
  
  LOOP
    BEGIN
      UPDATE BI_SINC_PRODUTO
         SET PRODUTO         = r.PRODUTO,
             CODPRODMASTER   = r.CODPRODMASTER,
             PRODUTOMASTER   = r.PRODUTOMASTER,
             CODDEPTO        = r.CODDEPTO,
             DEPARTAMENTO    = r.DEPARTAMENTO,
             CODSECAO        = r.CODSECAO,
             SECAO           = r.SECAO,
             CODCATEGORIA    = r.CODCATEGORIA,
             CATEGORIA       = r.CATEGORIA,
             CODLINHA        = r.CODLINHA,
             LINHA           = r.LINHA,
             PRODUTOFILHO    = r.PRODUTOFILHO,
             CODFORNEC       = r.CODFORNEC,
             CODMARCA        = r.CODMARCA,
             MARCA           = r.MARCA,
             TIPOCOMISSAO    = r.TIPOCOMISSAO,
             CODFAB          = r.CODFAB,
             CODBARRAS       = r.CODBARRAS,
             CODBARRASMASTER = r.CODBARRASMASTER,
             PESO            = r.PESO,
             LARGURA         = r.LARGURA,
             ALTURA          = r.ALTURA,
             COMPRIMENTO     = r.COMPRIMENTO,
             VOLUME          = r.VOLUME,
             QTCXMASTER      = r.QTCXMASTER,
             IMPORTADO       = r.IMPORTADO,
             REVENDA         = r.REVENDA,
             ENVIAFV         = r.ENVIAFV,
             NCM             = r.NCM,
             NCMEX           = r.NCMEX,
             TIPOMERCADORIA  = r.TIPOMERCADORIA,
             FORALINHA       = r.FORALINHA,
             CERTIFICACAO    = r.CERTIFICACAO,
             DT_UPDATE       = SYSDATE
       WHERE CODPROD = r.CODPROD;
    
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
           ENVIAFV,
           NCM,
           NCMEX,
           TIPOMERCADORIA,
           FORALINHA,
           CERTIFICACAO,
           DT_UPDATE)
        VALUES
          (r.CODPROD,
           r.PRODUTO,
           r.CODPRODMASTER,
           r.PRODUTOMASTER,
           r.CODDEPTO,
           r.DEPARTAMENTO,
           r.CODSECAO,
           r.SECAO,
           r.CODCATEGORIA,
           r.CATEGORIA,
           r.CODLINHA,
           r.LINHA,
           r.PRODUTOFILHO,
           r.CODFORNEC,
           r.CODMARCA,
           r.MARCA,
           r.TIPOCOMISSAO,
           r.CODFAB,
           r.CODBARRAS,
           r.CODBARRASMASTER,
           r.PESO,
           r.LARGURA,
           r.ALTURA,
           r.COMPRIMENTO,
           r.VOLUME,
           r.QTCXMASTER,
           r.IMPORTADO,
           r.REVENDA,
           r.ENVIAFV,
           r.NCM,
           r.NCMEX,
           r.TIPOMERCADORIA,
           r.FORALINHA,
           r.CERTIFICACAO,
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
