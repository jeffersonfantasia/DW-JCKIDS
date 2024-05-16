CREATE OR REPLACE FUNCTION FN_BI_DIA_UTIL_FINANCEIRO(p_data DATE) RETURN DATE IS
  v_proximo_dia_util DATE;
BEGIN
  SELECT MIN(DATA)
    INTO v_proximo_dia_util
    FROM PCDIASUTEIS
   WHERE DATA >= p_data
     AND DIAFINANCEIRO = 'S'
		 AND CODFILIAL = '1';

  RETURN v_proximo_dia_util;
	
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    -- Caso não encontre um próximo dia útil, retorna NULL
    RETURN NULL;
END;
