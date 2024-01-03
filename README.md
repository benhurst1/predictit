## Predictit markets ETL

This project was about pulling data from an API, storing the raw json data in an S3 bucket and then using snowflake to turn that json into accessible tables.

I started off using docker-compose to create my environment for airflow to gather the data daily.
In it, I set some environment variables for AWS, such as access keys and bucket id.

I then set up a DAG to access those variables, create a connection using boto3, request the data from predictit, and then use put_object to dump the json file into the S3 bucket.

Once this was set up, I used snowflake to create a database, link it to the S3 bucket, and create a task to pull the data from the bucket daily (5 minutes after pulling the API from predictit).

Learnings:

- New pipeline technologies such as snowflake and setting up environment variables when creating airflow in docker.
- Fundamentals of datawarhousing.
