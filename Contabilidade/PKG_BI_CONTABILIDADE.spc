CREATE OR REPLACE PACKAGE PKG_BI_CONTABILIDADE IS

  -- Author  : JEFFERSON
  -- Created : 29/11/2024 23:26:32
  -- Purpose : Usado para gerar o BI contábil

/*  -----------------------CENTROS DE CUSTO
   vCC_SPMARKET         VARCHAR(3) := '1.1';
   vCC_PARQUE           VARCHAR(3) := '1.2';
   vCC_JUNDIAI          VARCHAR(3) := '1.3';
   vCC_TRIMAIS          VARCHAR(3) := '1.4';
   vCC_CAMPINAS         VARCHAR(3) := '1.5';
   vCC_DISTRIBUICAO_SP  VARCHAR(3) := '2.1';
	 vCC_DISTRIBUICAO_ES  VARCHAR(3) := '2.2';
	 vCC_ECOMMERCE_SP     VARCHAR(3) := '3.1';
	 vCC_CORPORATIVO_SP   VARCHAR(3) := '4.1';*/
	
	-----------------------FUNÇÕES
	FUNCTION FN_MOV_PROD_BASE RETURN T_MOV_PROD_BASE_TABLE PIPELINED;

END PKG_BI_CONTABILIDADE;
/
