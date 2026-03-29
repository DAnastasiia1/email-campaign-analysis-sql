WITH account_information AS (
-- 1. Метрики по акаунтах
SELECT
  s.date,
  sp.country,
  a.send_interval,
  a.is_verified,
  a.is_unsubscribed,
  COUNT(DISTINCT a.id) AS account_cnt
FROM `DA.account` a
JOIN `DA.account_session` acs
  ON a.id = acs.account_id
JOIN `DA.session` s
  ON acs.ga_session_id = s.ga_session_id
JOIN `DA.session_params` sp
  ON s.ga_session_id = sp.ga_session_id
GROUP BY 1,2,3,4,5
),
emails_information AS (
-- 2. Метрики по емейлах
SELECT
  DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
  sp.country,
  a.send_interval,
  a.is_verified,
  a.is_unsubscribed,
  COUNT(DISTINCT es.id_message) AS sent_msg,
  COUNT(DISTINCT eo.id_message) AS open_msg,
  COUNT(DISTINCT ev.id_message) AS visit_msg
FROM `DA.email_sent` es
JOIN `DA.account_session` acs
  ON es.id_account = acs.account_id
JOIN `DA.account` a
  ON acs.account_id = a.id
JOIN `DA.session` s
  ON acs.ga_session_id = s.ga_session_id
JOIN `DA.session_params` sp
  ON s.ga_session_id = sp.ga_session_id
LEFT JOIN `DA.email_open` eo
  ON es.id_message = eo.id_message
LEFT JOIN `DA.email_visit` ev
  ON es.id_message = ev.id_message
GROUP BY 1,2,3,4,5
),
account_email_inf AS (
-- 3. Об'єднуємо акаунти та емейли
SELECT
  date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed,
  account_cnt,
  0 AS sent_msg,
  0 AS open_msg,
  0 AS visit_msg
FROM account_information
UNION ALL
SELECT
  date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed,
  0 AS account_cnt,
  sent_msg,
  open_msg,
  visit_msg
FROM emails_information
),
agg AS (
-- 3.2 Агрегація після UNION ALL
SELECT
  date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed,
  SUM(account_cnt) AS account_cnt,
  SUM(sent_msg) AS sent_msg,
  SUM(open_msg) AS open_msg,
  SUM(visit_msg) AS visit_msg
FROM account_email_inf
GROUP BY 1,2,3,4,5
),
final AS (
-- 4. Тотали по країнах
SELECT
  *,
  SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
  SUM(sent_msg)   OVER (PARTITION BY country) AS total_country_sent_cnt
FROM agg
),
ranked AS (
-- 5. Ранки
SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
sent_msg,
open_msg,
visit_msg,
total_country_account_cnt,
total_country_sent_cnt,
DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC)  AS rank_total_country_sent_cnt
FROM final)


SELECT *
FROM ranked
WHERE rank_total_country_account_cnt <=10 or rank_total_country_sent_cnt <=10
