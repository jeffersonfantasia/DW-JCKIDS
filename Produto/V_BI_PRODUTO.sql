CREATE OR REPLACE VIEW V_BI_PRODUTO AS
SELECT *
  FROM BI_SINC_PRODUTO S
 WHERE S.DT_SINC <= S.DT_UPDATE
    OR DT_SINC IS NULL;
