CREATE OR REPLACE PROCEDURE PRC_RETORNA_CLIENTE
(
  pCODCLI   IN PCCLIENT.CODCLI%TYPE,
  pDTUPDATE IN TIMESTAMP
) AS
BEGIN
  UPDATE BI_SINC_CLIENTE SET DT_SINC = pDTUPDATE WHERE CODCLI = pCODCLI;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    DBMS_OUTPUT.PUT_LINE('Nenhum registro encontrado para a marca ' ||
                         TO_CHAR(pCODCLI));
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Erro inesperado:' || SQLERRM);
  
END;