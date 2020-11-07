
CREATE SCHEMA snebolsin_schema 
GO
CREATE USER ss WITH PASSWORD = 's!nEbOlSi_n', DEFAULT_SCHEMA = snebolsin_schema 
GO

---------------------################--------------------------

CREATE DATABASE SCOPED CREDENTIAL ssstoragecred3
WITH
  IDENTITY = 'snbigdataschool',
  SECRET = 'hBLSDYuwDx+M3GGznz3Xyn3/6x9ulJ6zEG4Iru53DLZ4r/+BmS3RhkVvrB0/yFjQELsYukBFIk7e4D1OKUVdOw==' ;

CREATE EXTERNAL DATA SOURCE ssstorageblob4
WITH
  ( LOCATION = 'wasbs://nebolsinsergey@snbigdataschool.blob.core.windows.net/' ,
    CREDENTIAL = ssstoragecred3,
    TYPE = HADOOP
  ) ;

  CREATE EXTERNAL FILE FORMAT ssformat3
WITH (
    FORMAT_TYPE = DELIMITEDTEXT  ,
    FORMAT_OPTIONS (FIELD_TERMINATOR =',',
	FIRST_ROW = 2, 
    USE_TYPE_DEFAULT = True)
);


CREATE EXTERNAL TABLE snebolsin_schema.yellow_tripdata_2020_01_ext4 (
		VendorID INT NOT NULL,
		tpep_pickup_datetime DATETIME,
		tpep_dropoff_datetime DATETIME,
		passenger_count INT NOT NULL,
		trip_distance FLOAT,
		RatecodeID INT NOT NULL,
		store_and_fwd_flag CHAR,
		PULocationID INT NOT NULL,
		DOLocationID INT NOT NULL,
		payment_type INT NOT NULL,
		fare_amount FLOAT,
		extra FLOAT,
		mta_tax FLOAT,
		tip_amount FLOAT,
		tolls_amount FLOAT,
		improvement_surcharge FLOAT,
		total_amount FLOAT,
		congestion_surcharge FLOAT
		)

    WITH (
        LOCATION = '/yellow_tripdata_2020-01.csv' ,
        DATA_SOURCE = ssstorageblob4,
        FILE_FORMAT = ssformat3
    );

  --select top 10 * from snebolsin_schema.yellow_tripdata_2020_01_ext4

  ---------------------################--------------------------

  CREATE TABLE snebolsin_schema.fact_tripdata
WITH
(
    DISTRIBUTION = HASH(tpep_pickup_datetime),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM snebolsin_schema.yellow_tripdata_2020_01_ext4
OPTION (LABEL = 'CTAS : Load snebolsin_schema.fact_tripdata')
;

  ---------------------################--------------------------

CREATE TABLE snebolsin_schema.Vendor 
		(
		VendorID INT NOT NULL,
		Name VARCHAR(50)
		)
WITH
(
    DISTRIBUTION = HASH(VendorID),
    CLUSTERED COLUMNSTORE INDEX
);



INSERT INTO snebolsin_schema.Vendor
SELECT 1, 'Creative Mobile Technologies'
UNION ALL
SELECT 2, 'VeriFone Inc.' ;


  ---------------------################--------------------------

CREATE TABLE snebolsin_schema.RateCode 
		(
		ID INT NOT NULL,
		Name VARCHAR(50)
		)
WITH
(
    DISTRIBUTION = HASH(ID),
    CLUSTERED COLUMNSTORE INDEX
);

INSERT INTO snebolsin_schema.RateCode
SELECT 1, 'Standard rate'
UNION ALL
SELECT 2, 'JFK' 
UNION ALL
SELECT 3, 'Newark' 
UNION ALL
SELECT 4, 'Nassau or Westchester' 
UNION ALL
SELECT 5, 'Negotiated fare'
UNION ALL
SELECT 6, 'Group ride';

  ---------------------################--------------------------

CREATE TABLE snebolsin_schema.PaymentType 
		(
		ID INT NOT NULL,
		Name VARCHAR(50)
		)
WITH
(
    DISTRIBUTION = HASH(ID),
    CLUSTERED COLUMNSTORE INDEX
);

INSERT INTO snebolsin_schema.PaymentType
SELECT 1, 'Credit card'
UNION ALL
SELECT 2, 'Cash'
UNION ALL
SELECT 3, 'No charge' 
UNION ALL
SELECT 4, 'Dispute' 
UNION ALL
SELECT 5, 'Unknown' 
UNION ALL
SELECT 6, 'Voided trip' ;

