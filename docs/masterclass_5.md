1) Go through the sql queries, incremental_load.sql
2) For glue, 
	1) Go to iam 
	2) Choose aws service and usecase glue
	3) Attach AWSGlueServiceRole
	4) Role name - AWSGlueServiceRole-IcebergLab
	5) After creating the role, open the same role and Permissions → Add permissions → Create inline policy → JSON, paste the
	below
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3IcebergLabAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::iceberg-data-lake-958165011713",
        "arn:aws:s3:::iceberg-data-lake-958165011713/*"
      ]
    },
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetTableVersion",
        "glue:GetTableVersions",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition",
        "glue:GetPartition",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchLogsAccess",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
	Name this policy as IcebergLabS3GlueAccess
	6) Use this role in glue script role
	7) Paste the script.
	8) Name the script orders_source_to_replica_incremental
	9) Add a job parameter, key --datalake-formats, value iceberg. 
	10) Set the worker count to 2 and Hit save.
	11) Run 
	
Nature of the script
No new source snapshot
→ no target merge
→ no target snapshot

New source snapshot but no row-level changes
→ no target merge
→ no target snapshot
→ only control table updated

New source snapshot with real changes
→ target merge runs
→ target updated_at changes only for incremental records
→ control table updated