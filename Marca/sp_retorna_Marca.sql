CREATE OR ALTER PROCEDURE SP_RETORNA_MARCA
    @CODMARCA SMALLINT
AS
BEGIN    

    IF EXISTS (SELECT 1 FROM MARCA WHERE CODMARCA = @CODMARCA)
	SELECT FORMAT(DTUPDATE, 'yyyy-MM-dd HH:mm:ss') DTUPDATE FROM MARCA WHERE CODMARCA = @CODMARCA
END