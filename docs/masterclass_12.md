AWS AURORA INFRA CREATION
1) Open AWS CONSOLE and go to aurora and rds.
2) Create database with full configuration
3) Set Engine to Aurora (PostgreSQL Compatible)
4) Set template to Dev/Test
5) Choose Provisioned as cluster scalibility type and set it to Burstable classes
    choose db.t3.medium
6) Set engine version to 17.7
7) Name the db cluster identifier as practice
8) Under credentials settings
    1) Choose self managed
    2) Master username: postgres
    3) Master password: <come up with a secured password>
9) Set cluster storage configuration to Aurora Standard
10) For availability and durability set it to Dont create an replica
11) Set network type to IPv4
12) Set vpc and db subnet to default
13) Set public access to yes
14) Create a new vpc security group and name it as iceberg_aurora_sg
15) Under additional configuration, in initial database name set it to practice
16) For maintenance, set auto minor version upgrade to off.
17) Create Database

Modifying iceberg_aurora_sg
1) Go to security groups, find the one we created
2) Set an inbound rule, go to edit inbound rules and add a new rule
3) Set type to all custom tcp and source as anywhere-ipv4, it should display 0.0.0.0/0 and keep
port range 0-65535
4) Hit save rules

Create a glue connection
1) JDBC, set url: jdbc:postgresql://<your endpoint>/ordersdb 
2) Set the correct sg iceberg_aurora_sg
3) Create vpc endpoint, name it to s3-gateway-endpoint-for-glue as name
    Select aws services
    use service: com.amazonaws.us-east-1.s3 (type gateway)
    vpc: default available
    Add the route table
    Create endpoint
4) Create another vpc endpoint, name it to Glue Interface Endpoint
    Select aws services
    In services search, com.amazonaws.us-east-1.glue and select
    Use the vpc from the dropdown
    Enable private dns name
    dns record type ipv4
    select subnet, us-east-1a
    add the sg, iceberg_aurora_sg
    Set policy to Full access


Connecting it via PgAdmin4
1) Open pgadmin, register server, name it as enqurious_tredence_iceberg
2) Copy the endpoint of the writer instance and add it in connection details.

NECESSARY TABLES REQUIRED FOR BOTH SIDES
---------------------------------------------------------------------------------------------------
1) Create the necessary tables in aurora, refer aurora_table_creation.sql run it in aurora
2) Create new iceberg table, use athena for this ddl, refer bidirectional.sql
---------------------------------------------------------------------------------------------------

ETL PIPELINE 1 - ICEBERG TO AURORA INCREMENTAL
---------------------------------------------------------------------------------------------------
1) Create a new glue etl script, name it iceberg_to_aurora_incremental
2) Glue Job Parameters Needed

Add these job parameters:

Key	Value
--AURORA_HOST	your Aurora writer endpoint
--AURORA_PORT	5432
--AURORA_DB	practice
--AURORA_USER	postgres
--AURORA_PASSWORD	your password
--ICEBERG_TABLE	glue_catalog.practice.orders_iceberg_bidir
--PIPELINE_NAME	iceberg_to_aurora_orders
--datalake-formats iceberg
3) Upload wheel file in s3 and set additional python modules to that s3 uri (upload the wheel file under assets)
4) Change the worker count to 2 from 10(default)
5) Refer aurora_table_creation.sql, run the full query.
6) Run the script and see the changes.

ETL PIPELINE 2 - ICEBERG TO AURORA INCREMENTAL
---------------------------------------------------------------------------------------------------
1) Create a new glue etl script, name it aurora_to_iceberg_reverse_sync
2) Glue Job Parameters Needed

Add these job parameters:

Key	Value
--AURORA_HOST	your Aurora writer endpoint
--AURORA_PORT	5432
--AURORA_DB	practice
--AURORA_USER	postgres
--AURORA_PASSWORD	your password
--ICEBERG_TABLE	glue_catalog.practice.orders_iceberg_bidir
--PIPELINE_NAME	iceberg_to_aurora_orders
--datalake-formats iceberg
3) Upload wheel file in s3 and set additional python modules to that s3 uri (upload the wheel file under assets)
4) Change the worker count to 2 from 10(default)
5) Run some update queries in aurora to make changes, refer bidirectional.sql line 107