CREATE OR ALTER PROCEDURE SP_CARREGA_PRODUTO
    @CODPROD INT,
    @PRODUTO NVARCHAR (40),
    @CODDEPTO INT,
    @DEPARTAMENTO NVARCHAR (40),
    @CODSECAO INT,
    @SECAO NVARCHAR (40),
    @CODCATEGORIA INT,
    @CATEGORIA NVARCHAR (40),
    @CODLINHA INT,
    @LINHA NVARCHAR (40),
    @CODFORNEC INT,
    @CODMARCA INT,
    @CODFABRICA NVARCHAR (30),
    @CODBARRAS NVARCHAR (30),
    @CODBARRASMASTER NVARCHAR (30),
    @PESO NUMERIC (12, 6),
    @LARGURA NUMERIC (10, 6),
    @ALTURA NUMERIC (10, 6),
    @COMPRIMENTO NUMERIC (10, 6),
    @VOLUME NUMERIC (20, 8),
    @QTCXMASTER NUMERIC (8, 2),
    @IMPORTADO CHAR (1),
    @REVENDA CHAR (1),
    @NCM NVARCHAR (15),
    @NCMEX NVARCHAR (20),
    @TIPOMERCADORIA NVARCHAR (15),
    @FORALINHA CHAR (1),
    @CERTIFICACAO NVARCHAR (200),
    @DTEXCLUSAO NVARCHAR(30)
AS
BEGIN
    DECLARE @DTUPDATE DATETIME = DATEADD(HOUR, -3, GETUTCDATE())
    
    --CONVERTER DE METROS PARA CENTIMETROS
    SET @LARGURA = @LARGURA * 100
    SET @ALTURA = @ALTURA * 100
    SET @COMPRIMENTO = @COMPRIMENTO * 100

    --TRATANDO ESPACOS VAZIOS NAS STRING
    SET @PRODUTO = TRIM(@PRODUTO)
    SET @CODBARRAS= TRIM(@CODBARRAS)
    
    
    --TRATANDO OS RETORNOS NULOS
    SET @DEPARTAMENTO = CASE WHEN @DEPARTAMENTO = 'NULL' THEN NULL ELSE TRIM(@DEPARTAMENTO) END
    SET @SECAO = CASE WHEN @SECAO = 'NULL' THEN NULL ELSE TRIM(@SECAO) END
    SET @CATEGORIA = CASE WHEN @CATEGORIA = 'NULL' THEN NULL ELSE TRIM(@CATEGORIA) END
    SET @LINHA = CASE WHEN @LINHA = 'NULL' THEN NULL ELSE TRIM(@LINHA) END
    SET @CODFABRICA = CASE WHEN @CODFABRICA = 'NULL' THEN NULL ELSE TRIM(@CODFABRICA) END
    SET @CODBARRASMASTER = CASE WHEN @CODBARRASMASTER = 'NULL' THEN NULL ELSE TRIM(@CODBARRASMASTER) END
    SET @IMPORTADO = CASE WHEN @IMPORTADO = 'NULL' THEN NULL ELSE TRIM(@IMPORTADO) END
    SET @NCM = CASE WHEN @NCM = 'NULL' THEN NULL ELSE TRIM(@NCM) END
    SET @NCMEX = CASE WHEN @NCMEX = 'NULL' THEN NULL ELSE TRIM(@NCMEX) END
    SET @CERTIFICACAO = CASE WHEN @CERTIFICACAO = 'NULL' THEN NULL ELSE TRIM(@CERTIFICACAO) END
    
    --NOMEANDO OS TIPOS DE MERCADORIA
    SET @TIPOMERCADORIA = 
        CASE @TIPOMERCADORIA 
            WHEN 'L' THEN 'NORMAL'
            WHEN 'CB' THEN 'CESTA BASICA'
            WHEN 'KT' THEN 'KIT'
            WHEN 'DB' THEN 'BRINDE'
            WHEN 'MC' THEN 'MATERIAL CONSUMO'
            WHEN 'NULL' THEN 'NORMAL'
        ELSE 'NAO INFORMADO'
        END
    
    --ALTERANDO O FORA DE LINHA
    SET @FORALINHA = 
        CASE 
            WHEN @FORALINHA = 'FL' THEN 'S'
            ELSE 'N'
        END

    --TRANSFORMANDO A DATA RECEBIDA DO BANCO ORACLE
    SET @DTEXCLUSAO = LEFT(@DTEXCLUSAO, 23)
    SET @DTEXCLUSAO = CONVERT(DATETIME, @DTEXCLUSAO, 127)

    IF EXISTS (SELECT 1 FROM PRODUTO WHERE CODPROD = @CODPROD)
    BEGIN
        UPDATE PRODUTO
        SET PRODUTO = @PRODUTO,
            CODDEPTO = @CODDEPTO,
            DEPARTAMENTO = @DEPARTAMENTO,
            CODSECAO = @CODSECAO,
            SECAO = @SECAO,
            CODCATEGORIA = @CODCATEGORIA,
            CATEGORIA = @CATEGORIA,
            CODLINHA = @CODLINHA,
            LINHA = @LINHA,
            CODFORNEC = @CODFORNEC,
            CODMARCA = @CODMARCA,
            CODFABRICA = @CODFABRICA,
            CODBARRAS = @CODBARRAS,
            CODBARRASMASTER = @CODBARRASMASTER,
            PESO = @PESO,
            LARGURA = @LARGURA,
            ALTURA = @ALTURA,
            COMPRIMENTO = @COMPRIMENTO,
            VOLUME = @VOLUME,
            QTCXMASTER = @QTCXMASTER,
            IMPORTADO = @IMPORTADO,
            REVENDA = @REVENDA,
            NCM = @NCM,
            NCMEX = @NCMEX,
            TIPOMERCADORIA = @TIPOMERCADORIA,
            FORALINHA = @FORALINHA,
            CERTIFICACAO = @CERTIFICACAO,
            DTEXCLUSAO = @DTEXCLUSAO,
            DTUPDATE = @DTUPDATE
        WHERE CODPROD = @CODPROD
    END
    ELSE 
    BEGIN
        INSERT INTO PRODUTO (
                    CODPROD,
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
                    CODFABRICA,
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
                    DTUPDATE)
            VALUES (
                    @CODPROD,
                    @PRODUTO,
                    @CODDEPTO,
                    @DEPARTAMENTO,
                    @CODSECAO,
                    @SECAO,
                    @CODCATEGORIA,
                    @CATEGORIA,
                    @CODLINHA,
                    @LINHA,
                    @CODFORNEC,
                    @CODMARCA,
                    @CODFABRICA,
                    @CODBARRAS,
                    @CODBARRASMASTER,
                    @PESO,
                    @LARGURA,
                    @ALTURA,
                    @COMPRIMENTO,
                    @VOLUME,
                    @QTCXMASTER,
                    @IMPORTADO,
                    @REVENDA,
                    @NCM,
                    @NCMEX,
                    @TIPOMERCADORIA,
                    @FORALINHA,
                    @CERTIFICACAO,
                    @DTEXCLUSAO,
                    @DTUPDATE)
    END
END