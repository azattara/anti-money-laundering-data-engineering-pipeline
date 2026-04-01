-- macros/rolling_window.sql
-- Reusable rolling-window aggregation macro for AML feature engineering.
--
-- Usage inside a model (BigQuery SQL):
--   {{ rolling_window('sum', 'amount', 'customer_id', 'feature_date', 7) }}
--
-- Arguments:
--   agg_fn       : SQL aggregate function name  (e.g. 'sum', 'avg', 'max', 'count')
--   column_name  : column to aggregate          (e.g. 'amount'; use '*' for count)
--   partition_col: entity column                (e.g. 'customer_id')
--   date_col     : daily date column            (e.g. 'feature_date')
--   days         : lookback window in days      (e.g. 7, 30, 90)
--
-- The macro emits a BigQuery analytic expression using RANGE BETWEEN so that
-- it correctly handles gaps in the daily time series.

{% macro rolling_window(agg_fn, column_name, partition_col, date_col, days) %}
{{ agg_fn }}({{ column_name }}) over (
    partition by {{ partition_col }}
    order by unix_date({{ date_col }})
    range between {{ days - 1 }} preceding and current row
)
{%- endmacro %}
