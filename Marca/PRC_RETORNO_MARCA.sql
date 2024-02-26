CREATE OR REPLACE PROCEDURE PRC_RETORNO_MARCA
(
  pCODMARCA      IN PCMARCA.CODMARCA%TYPE,
  pDTATUALIZACAO IN TIMESTAMP DEFAULT SYSDATE
) AS
BEGIN
  UPDATE BI_SINCMARCA
     SET DT_SINC = pDTATUALIZACAO
   WHERE CODMARCA = PCODMARCA;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    DBMS_OUTPUT.PUT_LINE('Nenhum registro encontrado para a marca ' ||
                         TO_CHAR(PCODMARCA));
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Erro inesperado:' || SQLERRM);

END;
