-- Сквозная проверка витрины cdm.user_category_counters.
-- Запрос пересчитывает количество CLOSED-заказов по категориям напрямую из DDS
-- и сравнивает результат с данными в CDM для указанного пользователя.
-- Одна категория учитывается не более одного раза в рамках одного заказа,
-- даже если в заказе несколько продуктов этой категории.

-- Ожидаемый результат: 0 строк.
-- Если строки вернулись, значит значения в DDS и CDM отличаются.

WITH params AS (
    SELECT
        'cd16bcdc-77f9-549f-9114-a7c1566d76df'::uuid AS user_id
),
latest_status AS (
    SELECT DISTINCT ON (h_order_pk)
        h_order_pk,
        status
    FROM dds.s_order_status
    ORDER BY h_order_pk, load_dt DESC
),
dds_counts AS (
    SELECT
        hc.category_name,
        COUNT(DISTINCT lou.h_order_pk) AS order_cnt
    FROM dds.l_order_user lou
    JOIN latest_status ls
        ON ls.h_order_pk = lou.h_order_pk
    JOIN dds.l_order_product lop
        ON lop.h_order_pk = lou.h_order_pk
    JOIN dds.l_product_category lpc
        ON lpc.h_product_pk = lop.h_product_pk
    JOIN dds.h_category hc
        ON hc.h_category_pk = lpc.h_category_pk
    CROSS JOIN params p
    WHERE lou.h_user_pk = p.user_id
      AND ls.status = 'CLOSED'
    GROUP BY hc.category_name
),
cdm_counts AS (
    SELECT
        ucc.category_name,
        ucc.order_cnt
    FROM cdm.user_category_counters ucc
    CROSS JOIN params p
    WHERE ucc.user_id = p.user_id
)
SELECT
    COALESCE(d.category_name, c.category_name) AS category_name,
    d.order_cnt AS dds_order_cnt,
    c.order_cnt AS cdm_order_cnt
FROM dds_counts d
FULL JOIN cdm_counts c
    ON c.category_name = d.category_name
WHERE d.order_cnt IS DISTINCT FROM c.order_cnt
ORDER BY category_name;