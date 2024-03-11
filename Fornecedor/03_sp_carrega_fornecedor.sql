CREATE OR ALTER PROCEDURE SP_CARREGA_FORNECEDOR 
    @CODFORNEC INT,
    @FORNECEDOR NVARCHAR,
    @CNPJ NVARCHAR,
    @TIPO NVARCHAR
AS
BEGIN
    DECLARE @DTUPDATE DATETIME = DATEADD(hour, -3, GETUTCDATE())
    
    
    IF EXISTS (SELECT 1 FROM FORNECEDOR WHERE CODFORNEC = @CODFORNEC)
    BEGIN
        UPDATE FORNECEDOR 
        SET FORNECEDOR = @FORNECEDOR, CNPJ = @CNPJ, TIPO = @TIPO, DTUPDATE = @DTUPDATE 
        WHERE CODFORNEC = @CODFORNEC
    END
    ELSE
    BEGIN
        INSERT INTO FORNECEDOR (CODFORNEC, FORNECEDOR, CNPJ, TIPO, DTUPDATE)
        VALUES (@CODFORNEC, @FORNECEDOR, @CNPJ, @TIPO, @DTUPDATE)
    END
END