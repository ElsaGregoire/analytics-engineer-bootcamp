SELECT
    format_date('%F', d)  as id,
    d                     as full_date,
    extract(YEAR FROM d)  as year,
    extract(WEEK FROM d)  as year_week,
    extract(DAY FROM d)   as year_day,
    extract(YEAR from d)  as fiscal_year,
    format_date('%Q', d)  as fiscal_qtr,
    extract(MONTH FROM d) as MONTH,
    format_date('%B', d)  as month_name,
    format_date('%w', d)  as week_day,
    format_date('%A', d)  as day_name,
    CASE
        WHEN format_date('%A', d) IN ('Sunday', 'Saturday') THEN 0
        ELSE 1
    END AS day_is_weekday
FROM (
    SELECT *
    FROM unnest(generate_date_array('2014-01-01', '2050-01-01', interval 1 day)) AS d
)
