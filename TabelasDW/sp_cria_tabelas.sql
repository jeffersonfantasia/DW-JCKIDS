CREATE OR ALTER PROCEDURE SP_CRIA_TABELAS_DW AS
BEGIN
    ----TABELA FILIAL
    IF NOT EXISTS (SELECT 1	FROM INFORMATION_SCHEMA.TABLES	WHERE TABLE_NAME = 'FILIAL')
    BEGIN
        CREATE TABLE [dbo].[Filial] (
            [CodFilial]  CHAR (3)      NOT NULL,
            [Empresa]    NVARCHAR (150) NULL,
            [Filial]     NVARCHAR (50)  NULL,
            [Ordem]      SMALLINT       NOT NULL,
            CONSTRAINT [PK_Filial] PRIMARY KEY CLUSTERED ([CodFilial] ASC)
        );

        CREATE NONCLUSTERED INDEX [Index_Filial_2]
            ON [dbo].[Filial]([Empresa] ASC);


        CREATE NONCLUSTERED INDEX [Index_Filial_1]
            ON [dbo].[Filial]([CodFilial] ASC);


        CREATE NONCLUSTERED INDEX [Index_Filial_5]
            ON [dbo].[Filial]([CodFilial] ASC, [Empresa] ASC, [Filial] ASC);


        CREATE NONCLUSTERED INDEX [Index_Filial_4]
            ON [dbo].[Filial]([Ordem] ASC);


        CREATE NONCLUSTERED INDEX [Index_Filial_3]
            ON [dbo].[Filial]([Filial] ASC)
    END

    ----TABELA MARCA
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MARCA')
    BEGIN
        CREATE TABLE [dbo].[Marca] (
            [CodMarca] SMALLINT      NOT NULL,
            [Marca]    NVARCHAR (40) NOT NULL,
            [Ativo]    CHAR (1)      NOT NULL,
            [DtAtualizacao] DATETIME      NULL,
            CONSTRAINT [PK_Marca] PRIMARY KEY CLUSTERED ([CodMarca] ASC, [Marca] ASC, [Ativo] ASC)
        );

        CREATE UNIQUE NONCLUSTERED INDEX [Index_Marca_1]
            ON [dbo].[Marca]([CodMarca] ASC);

        CREATE NONCLUSTERED INDEX [Index_Marca_2]
            ON [dbo].[Marca]([Marca] ASC);

        CREATE NONCLUSTERED INDEX [Index_Marca_3]
            ON [dbo].[Marca]([Ativo] ASC);

        CREATE UNIQUE NONCLUSTERED INDEX [Index_Marca_4]
            ON [dbo].[Marca]([CodMarca] ASC, [Ativo] ASC);

        CREATE NONCLUSTERED INDEX [Index_Marca_5]
            ON [dbo].[Marca]([Marca] ASC, [Ativo] ASC);
        
        CREATE NONCLUSTERED INDEX [Index_Marca_6]
            ON [dbo].[Marca]([DtAtualizacao] ASC);
    END
END