CREATE EXTENSION col_order;

SELECT * FROM compute_col_order(ARRAY['bigint'::regtype, 'bigint', 'bigint', 'bigint', 'bigint', 'bigint', 'bigint', 'bigint']);
SELECT * FROM compute_col_order(ARRAY['bigint'::regtype, 'text', 'timestamptz', 'jsonb', 'timestamp', 'date']);
