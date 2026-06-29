1) Create these 2 folders
	s3://iceberg-data-lake-958165011713/glue-iceberg-warehouse/
	s3://iceberg-data-lake-958165011713/glue-temp/
	
ADD this into the iam role (created earlier) permissions 
{
  "Sid": "AllowPassSelfToGlue",
  "Effect": "Allow",
  "Action": "iam:PassRole",
  "Resource": "arn:aws:iam::958165011713:role/AWSGlueServiceRole-IcebergLab",
  "Condition": {
	"StringEquals": {
	  "iam:PassedToService": "glue.amazonaws.com"
	}
  }
}

2) Open glue studio notebook, set the iam role previously created. Start notebook, name it spark_introduction
3) refer spark_introduction.ipynb in the repo, try to run one by one cell rather than all.