########Inspect manifest files
SELECT *
FROM practice."orders_iceberg_partitionby_month$manifests";
"""Each record here corresponds to a manifest file,
which contains metadata about the data files in the Iceberg table. 
The manifest file lists the data files that are part of a specific 
snapshot and includes information such as file paths, partition values,
and statistics about the data in those files."""

########Inspect Data files########
SELECT *
FROM practice."orders_iceberg_partitionby_month$files";

########Inspect Snapshots########
SELECT *
FROM practice."orders_iceberg_partitionby_month$snapshots";