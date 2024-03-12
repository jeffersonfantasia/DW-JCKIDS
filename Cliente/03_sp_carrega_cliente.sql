CREATE OR ALTER PROCEDURE SP_CARREGA_CLIENTE
    @CODCLIENTE         INT,
    @CLIENTE            NVARCHAR (60),
    @CODREDE            INT,
    @REDE               NVARCHAR (60),
    @CNPJ               NVARCHAR (18),
    @CEP                NVARCHAR (9),
    @UF                 NVARCHAR (2),
    @CODVENDEDOR        INT,
    @CODPRACA           INT,
    @PRACA              NVARCHAR (25),
    @CODATIVIDADE       INT,
    @RAMOATIVIDADE      NVARCHAR (40),
    @BLOQUEIODEFINITIVO CHAR (1),
    @BLOQUEIOATUAL      CHAR (1),
    @LIMITECREDITO      NUMERIC (12, 2)
AS
BEGIN
    DECLARE @DTUPDATE DATETIME = DATEADD(HOUR, -3, GETUTCDATE())
    
    --TRATANDO OS RETORNOS NULOS
    SET @CLIENTE = CASE WHEN @CLIENTE = 'NULL' THEN NULL ELSE TRIM(UPPER(@CLIENTE)) END
    SET @REDE = CASE WHEN @REDE = 'NULL' THEN NULL ELSE TRIM(UPPER(@REDE)) END
    SET @PRACA = CASE WHEN @PRACA = 'NULL' THEN NULL ELSE TRIM(@PRACA) END
    SET @RAMOATIVIDADE = CASE WHEN @RAMOATIVIDADE = 'NULL' THEN NULL ELSE TRIM(@RAMOATIVIDADE) END
    
    --CRIANDO O CODCLIENTEREDE E CLIENTEREDE
    DECLARE @CODCLIENTEREDE NVARCHAR(10) = 
        CASE
            WHEN @REDE IS NULL THEN ('C' + CONVERT(NVARCHAR(4), @CODCLIENTE))
            ELSE ('R' + CONVERT(NVARCHAR(4), @CODREDE))
        END
    
    DECLARE @CLIENTEREDE NVARCHAR(70) = 
        CASE
            WHEN @REDE IS NULL THEN ( @CODCLIENTEREDE + ' - ' + @CLIENTE )
            ELSE ( @CODCLIENTEREDE + ' - ' + @REDE )
        END

    IF EXISTS (SELECT 1 FROM CLIENTE WHERE CODCLIENTE = @CODCLIENTE)
    BEGIN
        UPDATE CLIENTE
            SET CLIENTE = @CLIENTE,
                CODCLIENTEREDE = @CODCLIENTEREDE,
                CLIENTEREDE = @CLIENTEREDE,
                CNPJ = @CNPJ,
                CEP = @CEP,
                UF = @UF,
                CODVENDEDOR = @CODVENDEDOR,
                CODPRACA = @CODPRACA,
                PRACA = @PRACA,
                CODATIVIDADE = @CODATIVIDADE,
                RAMOATIVIDADE = @RAMOATIVIDADE,
                BLOQUEIODEFINITIVO = @BLOQUEIODEFINITIVO,
                BLOQUEIOATUAL = @BLOQUEIOATUAL,
                LIMITECREDITO = @LIMITECREDITO,
                DTUPDATE = @DTUPDATE
          WHERE CODCLIENTE = @CODCLIENTE
    END
    ELSE 
    BEGIN
        INSERT INTO CLIENTE (
            CODCLIENTE,
            CODCLIENTEREDE,
            CLIENTEREDE,
            CNPJ,
            CEP,
            UF,
            CODVENDEDOR,
            CODPRACA,
            PRACA,
            CODATIVIDADE,
            RAMOATIVIDADE,
            BLOQUEIODEFINITIVO,
            BLOQUEIOATUAL,
            LIMITECREDITO,
            DTUPDATE)
    VALUES (
            @CODCLIENTE,
            @CODCLIENTEREDE,
            @CLIENTEREDE,
            @CNPJ,
            @CEP,
            @UF,
            @CODVENDEDOR,
            @CODPRACA,
            @PRACA,
            @CODATIVIDADE,
            @RAMOATIVIDADE,
            @BLOQUEIODEFINITIVO,
            @BLOQUEIOATUAL,
            @LIMITECREDITO,
            @DTUPDATE)                          
    END
END