CREATE OR ALTER PROCEDURE SP_RETORNA_PRODUTO
    @CODPROD INT
AS
BEGIN    
    IF EXISTS (SELECT 1 FROM PRODUTO WHERE CODPROD = @CODPROD)
	SELECT FORMAT(DTUPDATE, 'yyyy-MM-dd HH:mm:ss') DTUPDATE FROM PRODUTO WHERE CODPROD = @CODPROD
END