#!/usr/bin/env bash
set -euo pipefail

# --- Estilo (opcional) ---
RED=$(tput setaf 1); GREEN=$(tput setaf 2); YELLOW=$(tput setaf 3); BOLD=$(tput bold); RESET=$(tput sgr0)

echo "${YELLOW}${BOLD}Iniciando execução...${RESET}"

# --- Configs comuns do bq ---
BQ_FLAGS=(--use_legacy_sql=false --quiet)
# Opcional: limite de bytes para evitar surpresas (ajuste conforme necessário)
# BQ_FLAGS+=(--maximum_bytes_billed=10000000000)  # 10 GB

# Parâmetros de datas (ajuste aqui se mudar o período)
TRAIN_START="20160801"
TRAIN_END_EXCL="20170701"   # semiaberto: treina até 2017-06-30
TEST_START="20170701"
TEST_END_EXCL="20170802"    # semiaberto: testa até 2017-08-01

bq query "${BQ_FLAGS[@]}" <<SQL
-- Cria o dataset se não existir (ajuste a localização se precisar)
CREATE SCHEMA IF NOT EXISTS \`bqml_lab\`;

-- Treino: use LIMIT se quiser priorizar velocidade em detrimento de qualidade
CREATE OR REPLACE MODEL \`bqml_lab.sample_model\`
OPTIONS (model_type = 'logistic_reg') AS
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.operatingSystem, '')    AS os,
  device.isMobile                       AS is_mobile,
  IFNULL(geoNetwork.country, '')        AS country,
  IFNULL(totals.pageviews, 0)           AS pageviews
FROM \`bigquery-public-data.google_analytics_sample.ga_sessions_*\`
WHERE _TABLE_SUFFIX >= '${TRAIN_START}'
  AND _TABLE_SUFFIX <  '${TRAIN_END_EXCL}';
  -- Se quiser treinar mais rápido, descomente a linha abaixo:
  -- LIMIT 100000;

-- Prepara dados de teste/score uma única vez e reaproveita
CREATE TEMP TABLE test_base AS
SELECT
  IFNULL(device.operatingSystem, '') AS os,
  device.isMobile                    AS is_mobile,
  IFNULL(geoNetwork.country, '')     AS country,
  IFNULL(totals.pageviews, 0)        AS pageviews,
  IFNULL(CAST(fullVisitorId AS STRING), '') AS fullVisitorId,  -- pode ser NULL nos dados
  IF(totals.transactions IS NULL, 0, 1) AS label
FROM \`bigquery-public-data.google_analytics_sample.ga_sessions_*\`
WHERE _TABLE_SUFFIX >= '${TEST_START}'
  AND _TABLE_SUFFIX <  '${TEST_END_EXCL}';

-- Avaliação (usa a temp table)
SELECT * FROM ML.EVALUATE(MODEL \`bqml_lab.sample_model\`,
  (SELECT label, os, is_mobile, country, pageviews FROM test_base)
);

-- Predição agregada por país (reaproveita a mesma base)
SELECT
  country,
  SUM(predicted_label) AS total_predicted_purchases
FROM ML.PREDICT(MODEL \`bqml_lab.sample_model\`,
  (SELECT os, is_mobile, pageviews, country FROM test_base)
)
GROUP BY country
ORDER BY total_predicted_purchases DESC
LIMIT 10;

-- Predição agregada por visitante (reaproveita a mesma base)
SELECT
  fullVisitorId,
  SUM(predicted_label) AS total_predicted_purchases
FROM ML.PREDICT(MODEL \`bqml_lab.sample_model\`,
  (SELECT os, is_mobile, pageviews, country, fullVisitorId FROM test_base)
)
GROUP BY fullVisitorId
ORDER BY total_predicted_purchases DESC
LIMIT 10;
SQL

echo "${GREEN}${BOLD}Concluído com sucesso!${RESET}"
