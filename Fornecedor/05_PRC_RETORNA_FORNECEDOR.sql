CREATE OR REPLACE PROCEDURE PRC_RETORNA_FORNECEDOR
(
  pCODFORNEC      IN PCFORNEC.CODFORNEC%TYPE,
  pDTUPDATE       IN TIMESTAMP
) AS
BEGIN
  UPDATE BI_SINC_FORNECEDOR
     SET DT_SINC = pDTUPDATE
   WHERE CODFORNEC = pCODFORNEC;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    DBMS_OUTPUT.PUT_LINE('Nenhum registro encontrado para a marca ' ||
                         TO_CHAR(pCODFORNEC));
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Erro inesperado:' || SQLERRM);
  
END;