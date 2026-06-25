1) Create s3 bucket with everything default, name: iceberg-data-lake-<aws account id>
2) Create these folders under the bucket:
	raw
	iceberg
	athena-results
3) Run 
4) Upload the created csv file under raw directory
5) Go to glue catalog, create a database, name it practice. Keep the first option in database type. Rest all default
6) Create an athena workgroup, name it practice, set the engine to Athena SQL, set upgrade query engine to manual.
7) Open athena query editor and run this in the order
	i) table_creation.sql
	ii) iceberg_table_creation.sql
	iii) Play with inspect_metadata.sql
	iv) Do updates refer, update_table.sql