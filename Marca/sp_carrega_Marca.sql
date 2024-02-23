CREATE OR ALTER PROCEDURE SP_CARREGA_MARCA
    @CODMARCA SMALLINT,
    @MARCA   NVARCHAR (40),
    @ATIVO    CHAR (1)
AS
BEGIN    
    DECLARE 
        @DTATUALIZACAO DATETIME = DATEADD(hour, -3, GETUTCDATE())

    ----faz o merge das informações
        IF EXISTS (SELECT 1 FROM MARCA WHERE CODMARCA = @CODMARCA)
			BEGIN
			UPDATE MARCA SET MARCA = @MARCA, ATIVO = @ATIVO, DTATUALIZACAO = @DTATUALIZACAO WHERE CODMARCA = @CODMARCA
		END
    	ELSE
			BEGIN
			INSERT INTO MARCA (CODMARCA, MARCA, ATIVO, DTATUALIZACAO)
			VALUES (@CODMARCA, @MARCA, @ATIVO, @DTATUALIZACAO)
	END
END