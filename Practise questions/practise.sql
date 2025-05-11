-- 1/ Write a SQL query to identify users whose average basket value has increased for three consecutive months.
with user_monthly_basket as
(
    select
        user_id,
        TO_CHAR(transaction_date, 'YYYY-MM') AS month_trans_date,
        sum(basket_value) AS monthly_basket_value
    FROM
        transactions
),
basket_lag AS
(
    select
         user_id,
         month_trans_date,
         monthly_basket_value,
         LAG(monthly_basket_value, 1) OVER (PARTITION BY user_id ORDER BY month_trans_date) AS prev_monthly_basket_value
    FROM    
        user_monthly_basket
),
increase_flags AS
(
    select 
        *,
        CASE
            WHEN monthly_basket_value > prev_monthly_basket_value THEN 1 ELSE 0
        END AS is_increased
    FROM
        basket_lag
),
streaks AS
(
    SELECT 
        *,
        SUM(CASE WHEN is_increased=0 THEN 1 ELSE 0 END)
        OVER (PARTITION BY user_id ORDER BY month_trans_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS streak_id
    FROM
        increase_flags
),
consecutive_streak AS
(
    SELECT 
            user_id,
            COUNT(*) AS consecutive_streaks
    FROM 
        streaks
    GROUP BY 
            user_id, streak_id

)
select COUNT(DISTINCT user_id) from consecutive_streak
WHERE 
    consecutive_streaks >= 3;

-- Calculate the 7-day rolling average of delivery time for each restaurant, ordered by date.
select
     d1.restaurant_id,
     d1.delivery_date,
     d1.delivery_time_minutes,
     (
        SELECT 
              AVG(d2.delivery_time_minutes)
        FROM
            deliveries d2
        WHERE 
                d1.restaurant_id = d2.restaurant_id
            AND d2.delivery_date BETWEEN d1.delivery_date - INTERVAL '6 days' AND d1.delivery_date       
     ) AS rolling_average_7_days
FROM
    deliveries d1
ORDER BY    
    d1.restaurant_id, d1.delivery_date;

--  Find delivery partners whose cancellation rate exceeded 10% in any 2 consecutive weeks.

-- Calculate total trips, cancelled trips in a week
WITH partner_cancellation AS
(SELECT
    partner_id,
    DATE_TRUNC('week', transaction_date) AS week_start_date,
    SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) * 100/COUNT(*) AS cancellation_rate
FROM 
    deliveries
GROUP BY 
        partner_id, DATE_TRUNC('week', transaction_date)
ORDER BY
        partner_id, week_start_date),
lagged_cancellation AS
(
    SELECT 
         *,
         LAG(cancellation_rate, 1) OVER (PARTITION BY partner_id ORDER BY week_start_date) AS lagged_cancellation
    FROM
        partner_cancellation
),
cancellation_exceeded AS
(
    SELECT 
        *,
        CASE
        WHEN cancellation_rate > 10 and lagged_cancellation > 10 THEN 1 ELSE 0 END AS consecutive_weeks_cancelled
    FROM
        lagged_cancellation
)
SELECT 
    distinct partner_id
FROM
    cancellation_exceeded
WHERE 
    consecutive_weeks_cancelled = 1;

-- Write a query to calculate the conversion rate by city: users who opened the app vs. those who placed an order.

SELECT 
    city,
    COUNT(DISTINCT CASE WHEN event_type='order_placed' THEN user_id ELSE 0 END) * 100.0/COUNT(DISTINCT CASE WHEN event_type='app_opened' THEN user_id ELSE 0 END) AS conversion_rate
FROM
    user_activity
GROUP BY 
    city;

-- 5/ Detect anomalies in daily orders using the IQR method in SQL.




