CREATE OR ALTER PROCEDURE SP_CRIA_TABELAS_DW AS
BEGIN
    ----TABELA FILIAL
    IF NOT EXISTS (SELECT 1	FROM INFORMATION_SCHEMA.TABLES	WHERE TABLE_NAME = 'FILIAL')
    BEGIN
        CREATE TABLE [dbo].[Filial] (
            [CodFilial]  CHAR (3)       NOT NULL,
            [Empresa]    NVARCHAR (150) NULL,
            [Filial]     NVARCHAR (50)  NULL,
            [Ordem]      SMALLINT       NOT NULL,
            [DtUpdate]   DATETIME       NULL,
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

        CREATE NONCLUSTERED INDEX [Index_Filial_6]
            ON [dbo].[Filial]([DtUpdate] ASC);
    END

    ----TABELA MARCA
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MARCA')
    BEGIN
        CREATE TABLE [dbo].[Marca] (
            [CodMarca] SMALLINT      NOT NULL,
            [Marca]    NVARCHAR (40) NOT NULL,
            [Ativo]    CHAR (1)      NOT NULL,
            [DtUpdate] DATETIME      NULL,
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
            ON [dbo].[Marca]([DtUpdate] ASC);
    END

    ----TABELA PRODUTO
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PRODUTO')
    BEGIN
        CREATE TABLE [dbo].[Produto] (
        [CodProd]         INT             NOT NULL,
        [Produto]         NVARCHAR (40)   NOT NULL,
        [CodDepto]        INT             NULL,
        [Departamento]    NVARCHAR (40)   NULL,
        [CodSecao]        INT             NULL,
        [Secao]           NVARCHAR (40)   NULL,
        [CodCategoria]    INT             NULL,
        [Categoria]       NVARCHAR (40)   NOT NULL,
        [CodLinha]        INT             NULL,
        [Linha]           NVARCHAR (40)   NULL,
        [CodFornec]       INT             NULL,
        [CodMarca]        INT             NULL,
        [CodFabrica]      NVARCHAR (30)   NULL,
        [CodBarras]       NVARCHAR (15)   NOT NULL,
        [CodBarrasMaster] NVARCHAR (15)   NULL,
        [Peso]            NUMERIC (12, 6) NULL,
        [Largura]         NUMERIC (10, 6) NULL,
        [Altura]          NUMERIC (10, 6) NULL,
        [Comprimento]     NUMERIC (10, 6) NULL,
        [Volume]          NUMERIC (20, 8) NULL,
        [QtCxMaster]      NUMERIC (8, 2)  NULL,
        [Importado]       CHAR (1)        NULL,
        [Revenda]         CHAR (1)        NULL,
        [NCM]             NVARCHAR (15)   NULL,
        [NCMEX]           NVARCHAR (20)   NULL,
        [TipoMercadoria]  NVARCHAR (30)   NULL,
        [ForaLinha]       CHAR (1)        NULL,
        [Certificacao]    NVARCHAR (200)  NULL,
        [DtExclusao]      DATETIME            NULL,
        [DtUpdate]        DATETIME        NULL,
        CONSTRAINT [PK_Produto] PRIMARY KEY CLUSTERED ([CodProd] ASC)
    );

    CREATE NONCLUSTERED INDEX [Index_Produto_5]
        ON [dbo].[Produto]([CodCategoria] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_18]
        ON [dbo].[Produto]([Produto] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_19]
        ON [dbo].[Produto]([Secao] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_17]
        ON [dbo].[Produto]([NCMEX] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_8]
        ON [dbo].[Produto]([CodFornec] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_7]
        ON [dbo].[Produto]([CodFabrica] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_4]
        ON [dbo].[Produto]([CodBarrasMaster] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_6]
        ON [dbo].[Produto]([CodDepto] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_12]
        ON [dbo].[Produto]([DtUpdate] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_2]
        ON [dbo].[Produto]([Categoria] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_20]
        ON [dbo].[Produto]([TipoMercadoria] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_10]
        ON [dbo].[Produto]([Departamento] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_13]
        ON [dbo].[Produto]([ForaLinha] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_11]
        ON [dbo].[Produto]([CodSecao] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_3]
        ON [dbo].[Produto]([CodBarras] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_1]
        ON [dbo].[Produto]([CodProd] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_9]
        ON [dbo].[Produto]([CodLinha] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_15]
        ON [dbo].[Produto]([Linha] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_16]
        ON [dbo].[Produto]([NCM] ASC);

    CREATE NONCLUSTERED INDEX [Index_Produto_14]
        ON [dbo].[Produto]([Importado] ASC);

    END

    ----TABELA FORNECEDOR
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'FORNECEDOR')
    BEGIN
        CREATE TABLE [dbo].[Fornecedor] (
        [CodFornec]  INT           NOT NULL,
        [Fornecedor] NVARCHAR (60) NOT NULL,
        [CodFornecPrinc]  INT           NULL,
        [CNPJ]       NVARCHAR (18) NOT NULL,
        [Tipo]       NVARCHAR (35) NULL,
        [DtUpdate]   DATETIME      NULL,
        CONSTRAINT [PK_Fornecedor] PRIMARY KEY CLUSTERED ([CodFornec] ASC)
    );

    CREATE NONCLUSTERED INDEX [Index_Fornecedor_1]
        ON [dbo].[Fornecedor]([CodFornec] ASC);

    CREATE NONCLUSTERED INDEX [Index_Fornecedor_2]
        ON [dbo].[Fornecedor]([Fornecedor] ASC);

    CREATE NONCLUSTERED INDEX [Index_Fornecedor_3]
        ON [dbo].[Fornecedor]([CNPJ] ASC);

    CREATE NONCLUSTERED INDEX [Index_Fornecedor_4]
        ON [dbo].[Fornecedor]([Tipo] ASC);

    CREATE NONCLUSTERED INDEX [Index_Fornecedor_5]
        ON [dbo].[Fornecedor]([DtUpdate] ASC);
    
    CREATE NONCLUSTERED INDEX [Index_Fornecedor_6]
        ON [dbo].[Fornecedor]([CodFornecPrinc] ASC);
    
    END

    ----TABELA VENDEDOR
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'VENDEDOR')
    BEGIN
        CREATE TABLE [dbo].[Vendedor] (
        [CodVendedor]   SMALLINT      NOT NULL,
        [Vendedor]      NVARCHAR (40) NOT NULL,
        [CodSupervisor] SMALLINT      NULL,
        [Supervisor]    NVARCHAR (40) NULL,
        [CodGerente]    SMALLINT      NULL,
        [Gerente]       NVARCHAR (40) NULL,
        [CodArea]       SMALLINT      NULL,
        [AreaComercial] NVARCHAR (40) NULL,
        [DtUpdate]      DATETIME      NULL,
        CONSTRAINT [PK_Vendedor] PRIMARY KEY CLUSTERED ([CodVendedor] ASC)
    );

    CREATE NONCLUSTERED INDEX [Index_Vendedor_1]
        ON [dbo].[Vendedor]([CodVendedor] ASC);

    CREATE NONCLUSTERED INDEX [Index_Vendedor_2]
        ON [dbo].[Vendedor]([AreaComercial] ASC);

    CREATE NONCLUSTERED INDEX [Index_Vendedor_3]
        ON [dbo].[Vendedor]([CodArea] ASC);

    CREATE NONCLUSTERED INDEX [Index_Vendedor_4]
        ON [dbo].[Vendedor]([CodGerente] ASC);

    CREATE NONCLUSTERED INDEX [Index_Vendedor_5]
        ON [dbo].[Vendedor]([CodSupervisor] ASC);

    CREATE NONCLUSTERED INDEX [Index_Vendedor_6]
        ON [dbo].[Vendedor]([DtUpdate] ASC);

    CREATE NONCLUSTERED INDEX [Index_Vendedor_7]
        ON [dbo].[Vendedor]([Gerente] ASC);

    CREATE NONCLUSTERED INDEX [Index_Vendedor_8]
        ON [dbo].[Vendedor]([Supervisor] ASC);

    CREATE NONCLUSTERED INDEX [Index_Vendedor_9]
        ON [dbo].[Vendedor]([Vendedor] ASC);

    END

    ----TABELA COMPRADOR
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'COMPRADOR')
    BEGIN
        CREATE TABLE [dbo].[Comprador] (
        [CodComprador] SMALLINT      NOT NULL,
        [Comprador]    NVARCHAR (40) NOT NULL,
        [DtUpdate]     DATETIME      NULL,
        CONSTRAINT [PK_Comprador] PRIMARY KEY CLUSTERED ([CodComprador] ASC)
    );

    CREATE NONCLUSTERED INDEX [Index_Comprador_1]
        ON [dbo].[Comprador]([CodComprador] ASC);

    CREATE NONCLUSTERED INDEX [Index_Comprador_2]
        ON [dbo].[Comprador]([Comprador] ASC);

    CREATE NONCLUSTERED INDEX [Index_Comprador_3]
        ON [dbo].[Comprador]([DtUpdate] ASC);

    END

    ----TABELA CLIENTE
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'CLIENTE')
    BEGIN
        CREATE TABLE [dbo].[Cliente] (
            [CodCliente]         INT             NOT NULL,
            [Cliente]            NVARCHAR (60)   NOT NULL,
            [CodClienteRede]     NVARCHAR (10)   NULL,
            [ClienteRede]        NVARCHAR (70)   NULL,
            [CNPJ]               NVARCHAR (18)   NULL,
            [CEP]                NVARCHAR (9)    NULL,
            [UF]                 NVARCHAR (2)    NULL,
            [CodVendedor]        INT             NULL,
            [CodPraca]           INT             NULL,
            [Praca]              NVARCHAR (25)   NULL,
            [CodAtividade]       INT             NULL,
            [RamoAtividade]      NVARCHAR (40)   NULL,
            [BloqueioDefinitivo] CHAR (1)        NULL,
            [BloqueioAtual]      CHAR (1)        NULL,
            [LimiteCredito]      NUMERIC (12, 2) NULL,
            [DtUpdate]           DATETIME        NULL,
            CONSTRAINT [PK_Cliente] PRIMARY KEY CLUSTERED ([CodCliente] ASC)
        );

        CREATE NONCLUSTERED INDEX [Index_Cliente_1]
            ON [dbo].[Cliente]([CodCliente] ASC);

        CREATE NONCLUSTERED INDEX [Index_Cliente_2]
            ON [dbo].[Cliente]([CodClienteRede] ASC);
            
        CREATE NONCLUSTERED INDEX [Index_Cliente_3]
            ON [dbo].[Cliente]([CodPraca] ASC);

        CREATE NONCLUSTERED INDEX [Index_Cliente_4]
            ON [dbo].[Cliente]([CodAtividade] ASC);

        CREATE NONCLUSTERED INDEX [Index_Cliente_5]
            ON [dbo].[Cliente]([Cliente] ASC);

        CREATE NONCLUSTERED INDEX [Index_Cliente_6]
            ON [dbo].[Cliente]([ClienteRede] ASC);

        CREATE NONCLUSTERED INDEX [Index_Cliente_7]
            ON [dbo].[Cliente]([Praca] ASC);

        CREATE NONCLUSTERED INDEX [Index_Cliente_8]
            ON [dbo].[Cliente]([RamoAtividade] ASC);

        CREATE NONCLUSTERED INDEX [Index_Cliente_9]
            ON [dbo].[Cliente]([CodVendedor] ASC);

        CREATE NONCLUSTERED INDEX [Index_Cliente_10]
            ON [dbo].[Cliente]([UF] ASC);

    END

    ----TABELA ESTOQUE
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ESTOQUE')
    BEGIN
        CREATE TABLE [dbo].[Estoque] (
            [CodFilial]       INT             NOT NULL,
            [CodProd]         INT             NOT NULL,
            [QtContabil]      NUMERIC (22, 8) NULL,
            [QtGerencial]     NUMERIC (22, 8) NULL,
            [QtBloqueada]     NUMERIC (20, 6) NULL,
            [QtReservada]     NUMERIC (22, 6) NULL,
            [QtAvariada]      NUMERIC (20, 6) NULL,
            [QtDisponivel]    NUMERIC (22, 8) NULL,
            [QtFrenteLoja]    NUMERIC (22, 6) NULL,
            [QtDeposito]      NUMERIC (22, 6) NULL,
            [ValorUltEntrada] NUMERIC (18, 6) NULL,
            [CustoReposicao]  NUMERIC (18, 6) NULL,
            [CustoFinanceiro] NUMERIC (18, 6) NULL,
            [CustoContabil]   NUMERIC (18, 6) NULL,
            [CodBloqueio]     SMALLINT        NULL,
            [MotivoBloqueio]  NVARCHAR (30)   NULL,
            [DtUpdate]        DATETIME        NULL,
            CONSTRAINT [PK_Estoque] PRIMARY KEY CLUSTERED ([CodFilial] ASC, [CodProd] ASC)
        );

        CREATE NONCLUSTERED INDEX [Index_Estoque_1]
            ON [dbo].[Estoque]([CodFilial] ASC);

        CREATE NONCLUSTERED INDEX [Index_Estoque_2]
            ON [dbo].[Estoque]([CodProd] ASC);

        CREATE NONCLUSTERED INDEX [Index_Estoque_3]
            ON [dbo].[Estoque]([CodBloqueio] ASC);

        CREATE NONCLUSTERED INDEX [Index_Estoque_4]
            ON [dbo].[Estoque]([MotivoBloqueio] ASC);

        CREATE NONCLUSTERED INDEX [Index_Estoque_5]
            ON [dbo].[Estoque]([CodFilial] ASC, [CodProd] ASC);

    END

END