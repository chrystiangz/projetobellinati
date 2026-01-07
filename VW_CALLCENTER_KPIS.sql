CREATE OR REPLACE VIEW `projeto-bellinati.SILVER.VW_CALLCENTER_KPIS` AS
WITH base AS (
  SELECT
    DATE(CallStartDt) AS data,
    EXTRACT(HOUR FROM CallStartDt) AS hora,
    ResourceGroupDesc,
    Disposition_Desc,
    CAST(ATENDIDA AS INT64) AS atendida_i,

    CASE
      WHEN RING_TIME_SEC IS NULL THEN NULL
      WHEN SAFE_CAST(RING_TIME_SEC AS FLOAT64) < 0 THEN NULL
      ELSE SAFE_CAST(RING_TIME_SEC AS FLOAT64)
    END AS ring_time_sec_f,

    CASE
      WHEN TALK_TIME_SEC IS NULL THEN NULL
      WHEN SAFE_CAST(TALK_TIME_SEC AS FLOAT64) < 0 THEN NULL
      ELSE SAFE_CAST(TALK_TIME_SEC AS FLOAT64)
    END AS talk_time_sec_f,

    CASE
      WHEN CALL_DURATION_SEC IS NULL THEN NULL
      WHEN SAFE_CAST(CALL_DURATION_SEC AS FLOAT64) < 0 THEN NULL
      ELSE SAFE_CAST(CALL_DURATION_SEC AS FLOAT64)
    END AS call_duration_sec_f,

    CASE
      WHEN CAST(ATENDIDA AS INT64) = 1
       AND RING_TIME_SEC IS NOT NULL
       AND SAFE_CAST(RING_TIME_SEC AS FLOAT64) >= 0
       AND SAFE_CAST(RING_TIME_SEC AS FLOAT64) <= 15
      THEN 1 ELSE 0
    END AS sla_15_ok,

    CASE
      WHEN CAST(ATENDIDA AS INT64) = 1
       AND RING_TIME_SEC IS NOT NULL
       AND SAFE_CAST(RING_TIME_SEC AS FLOAT64) >= 0
       AND SAFE_CAST(RING_TIME_SEC AS FLOAT64) <= 30
      THEN 1 ELSE 0
    END AS sla_30_ok
  FROM `projeto-bellinati.BRONZE.BASE_CALLCENTER`
)

SELECT
  data,
  hora,
  ResourceGroupDesc,
  Disposition_Desc,

  COUNT(*) AS total_chamadas,
  SUM(atendida_i) AS atendidas,
  COUNT(*) - SUM(atendida_i) AS nao_atendidas,

  SAFE_DIVIDE(SUM(atendida_i), COUNT(*)) AS taxa_atendimento,

  -- Totais de SLA
  SUM(sla_30_ok) AS sla_30_total,
  SUM(sla_15_ok) AS sla_15_total,

  -- Taxas de SLA 
  SAFE_DIVIDE(SUM(sla_15_ok), NULLIF(SUM(atendida_i), 0)) AS sla_15_rate,
  SAFE_DIVIDE(SUM(sla_30_ok), NULLIF(SUM(atendida_i), 0)) AS sla_30_rate,

  -- Tempos 
  SUM(ring_time_sec_f) AS ring_total,
  COUNTIF(ring_time_sec_f IS NOT NULL) AS qtd_ring,

  SUM(talk_time_sec_f) AS talk_total,
  COUNTIF(talk_time_sec_f IS NOT NULL) AS qtd_talk,

  AVG(ring_time_sec_f) AS ring_medio,
  AVG(talk_time_sec_f) AS talk_medio,
  AVG(call_duration_sec_f) AS duracao_media

FROM base
GROUP BY 1,2,3,4;
