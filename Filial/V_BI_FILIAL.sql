CREATE OR REPLACE VIEW V_BI_FILIAL AS
SELECT CODIGO CODFILIAL, 
       NOMEREMETENTE EMPRESA, 
       FANTASIA FILIAL
  FROM PCFILIAL
