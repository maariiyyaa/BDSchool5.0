create login mbondar
with password 'qwer#1234'
---------
create schema bondar_schema;
go
create user masha for login mbondar 
with default_schema = bondar_schema;
go
exec sp_addrolemember N'db_owner', N'masha';
go
alter user masha with default_schema = bondar_schema;
go

create database  scoped credential AccessInvoice1
with
	identity = 'shared_signature',
	secret = 'Access Key';

go

create external data source AzureSource
with
 (
	location = 'abfss://container@storage.dfs.core.windows.net',
	credential = AccessInvoice1,
	type = HADOOP);

go
create external file format format_csv
with (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (field_terminator =',',  first_row=2, use_type_default = True)
);
go

-- Create a new external table bondar_schema.my_extable
CREATE EXTERNAL TABLE bondar_schema.my_extable (
   VendorID tinyint,
   tpep_pickup_datetime datetime not null,
   tpep_dropoff_datetime datetime not null,
   passenger_count int,
   trip_distance float,
   RatecodeID tinyint,
   store_and_fwd_flag char(10),
   PULocationID int not null,
   DOLocationID int not null,
   payment_type tinyint,
   fare_amount float,
   extra float,
   mta_tax float,
   tip_amount float,
   tolls_amount float,
   improvement_surcharge float,
   total_amount float,
   congestion_surcharge float
)
WITH (
        LOCATION='/dir/yellow_tripdata_2020-01.csv',
        DATA_SOURCE = AzureSource,
        FILE_FORMAT = format_csv
    );
go

-- looking for the beat column for destribution
--	select count (*)
--	from bondar_schema.my_extable
--	group by tpep_pickup_datetime


-- Create a new table bondar_schema.fact_tripdata
create table bondar_schema.fact_tripdata
with
(
	clustered columnstore index,
	distribution = hash(tpep_pickup_datetime)
)
as
	select * 
	from bondar_schema.my_extable;

go

-- Create a new table bondar_schema.Vendor
create table bondar_schema.Vendor
(
	ID INT IDENTITY(0,1) NOT NULL,
	[Name] VARCHAR(50) NULL
)
with
(
	clustered columnstore index,
	distribution = hash([Name])
);
go
-- Insert into table bondar_schema.Vendor VendorID description from bondar_schema.my_extable
insert into bondar_schema.Vendor
	select 
	case
		when VendorID = 1 then 'Creative Mobile Technologies, LLC'
		when VendorID = 2 then 'VeriFone Inc'
		end 'Name'
	from bondar_schema.my_extable;
go

-- Create a new table bondar_schema.RateCode
create table bondar_schema.RateCode
(
	ID INT IDENTITY(0,1) NOT NULL,
	[Name] VARCHAR(50) NULL
)
with
(
	clustered columnstore index,
	distribution = hash([Name])
);
go

-- Insert into table bondar_schema.RateCode RateCodeID description from bondar_schema.my_extable
insert into bondar_schema.RateCode
	select 
	case
		when VendorID = 1 then 'Standart rate'
		when VendorID = 2 then 'JFK'
		when VendorID = 3 then 'Newark'
		when VendorID = 4 then 'Nassau or Westchester'
		when VendorID = 5 then 'Negotiated fare'
		when VendorID = 6 then 'Group ride'

		end 'Name'
	from bondar_schema.my_extable;

go

-- Create a new table bondar_schema.Payment_type
create table bondar_schema.Payment_type
(
	ID INT IDENTITY(0,1) NOT NULL,
	[Name] VARCHAR(50) NULL
)
with
(
	clustered columnstore index,
	distribution = hash([Name])
);
go

-- Insert into table bondar_schema.Payment_type Payment_type description from bondar_schema.my_extable
insert into bondar_schema.Payment_type
	select 
	case
		when VendorID = 1 then 'Credit card'
		when VendorID = 2 then 'Cash'
		when VendorID = 3 then 'No charge'
		when VendorID = 4 then 'Dispute'
		when VendorID = 5 then 'Unknown'
		when VendorID = 6 then 'Voided trip'
		end 'Name'
	from bondar_schema.my_extable;

