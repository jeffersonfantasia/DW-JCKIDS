CREATE OR ALTER PROCEDURE SP_CARREGA_FILIAL
	@CODFILIAL NCHAR(2),
	@EMPRESA NVARCHAR(150),
	@FILIAL NVARCHAR(25)
AS
BEGIN
	----Verifica se a tabela existe
	IF NOT EXISTS (SELECT 1	FROM INFORMATION_SCHEMA.TABLES	WHERE TABLE_NAME = 'FILIAL')
    BEGIN
		CREATE TABLE [dbo].[Filial] (
    [Cod.Filial] NCHAR (2)      NOT NULL,
    [Empresa]    NVARCHAR (150) NULL,
    [Filial]     NVARCHAR (50)  NULL,
    [Ordem]      SMALLINT       NOT NULL,
    CONSTRAINT [PK_Filial] PRIMARY KEY CLUSTERED ([Cod.Filial] ASC)
);

CREATE NONCLUSTERED INDEX [Index_Filial_2]
    ON [dbo].[Filial]([Empresa] ASC);


CREATE NONCLUSTERED INDEX [Index_Filial_1]
    ON [dbo].[Filial]([Cod.Filial] ASC);


CREATE NONCLUSTERED INDEX [Index_Filial_5]
    ON [dbo].[Filial]([Cod.Filial] ASC, [Empresa] ASC, [Filial] ASC);


CREATE NONCLUSTERED INDEX [Index_Filial_4]
    ON [dbo].[Filial]([Ordem] ASC);


CREATE NONCLUSTERED INDEX [Index_Filial_3]
    ON [dbo].[Filial]([Filial] ASC);
	END
    
	----Faz a transformação das informações
	BEGIN
       DECLARE @ORDEM SMALLINT
       SET @ORDEM = CONVERT(SMALLINT, @CODFILIAL)
	   SET @EMPRESA  = CASE WHEN @EMPRESA IS NULL THEN 'JC BROTHERS' ELSE @EMPRESA END
       SET @FILIAL  =  CASE WHEN @FILIAL IS NULL THEN  'JC BROTHERS' ELSE @FILIAL END
        
    ----faz o merge das informações
        IF EXISTS (SELECT 1 FROM FILIAL WHERE [Cod.Filial] = @CODFILIAL)
			BEGIN
			UPDATE FILIAL SET EMPRESA = @EMPRESA, FILIAL = @FILIAL WHERE [Cod.Filial] = @CODFILIAL
		END
    	ELSE
			BEGIN
			INSERT INTO FILIAL ([Cod.Filial], Empresa, Filial, Ordem)
			VALUES (@CODFILIAL, @EMPRESA, @FILIAL, @ORDEM)
		END
	END
END