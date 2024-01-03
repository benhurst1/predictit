CREATE DATABASE raw

CREATE OR REPLACE WAREHOUSE predictit

-- Link to AWS S3
CREATE OR REPLACE storage integration s3_int
    type = external_stage
    storage_provider = 'S3'
    storage_aws_role_arn = 'arn:aws:iam:::role'
    enabled = true
    storage_allowed_locations = ('s3://bucket/folder/')

-- Create Stage
CREATE OR REPLACE STAGE raw_predictit
    storage_integration = s3_int
    FILE_FORMAT = (type = json)
    URL = 's3://bucket/folder/'

-- Create raw data table
CREATE OR REPLACE TABLE tbl_raw_predictit
(
file_name varchar(100),
raw_value variant
)

-- Create task
CREATE TASK task_insert_raw_predictit
WAREHOUSE = PREDICTIT
SCHEDULE = 'USING CRON 5 0 * * * Europe/London'
TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD'
AS
COPY INTO tbl_raw_predictit (file_name, raw_value)
FROM (SELECT metadata$filename, t.$1 from @raw_predictit T)


ALTER TASK public.task_insert_raw_predictit RESUME

-- Create parsed data table
CREATE
	OR replace TABLE stg_predictit_markets (
	id INT
	,predictit_name VARCHAR(200)
	,predictit_short_name VARCHAR(100)
	,predicit_url VARCHAR(500)
	);

-- Create task to parse data
CREATE
	OR REPLACE TASK task_insert_stg_predictit_market WAREHOUSE = PREDICTIT TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24' AFTER task_insert_raw_predictit
    AS
INSERT INTO stg_predictit_markets
WITH raw_predictit AS (
		SELECT DISTINCT cast(parse_json(markets_json.value) :id AS INT) AS id
			,replace(parse_json(markets_json.value) :name, '"', '') AS predictit_name
			,replace(parse_json(markets_json.value) :shortName, '"', '') AS predictit_short_name
			,replace(parse_json(markets_json.value) :url, '"', '') AS predictit_url
		FROM raw.PUBLIC.tbl_raw_predictit
			,lateral flatten(parse_json(raw_value) :markets) markets_json
		)
SELECT raw_predictit.*
FROM raw_predictit
LEFT JOIN stg_predictit_markets stg_predictit ON raw_predictit.id = stg_predictit.id
WHERE stg_predictit.id IS NULL
ORDER BY 1;