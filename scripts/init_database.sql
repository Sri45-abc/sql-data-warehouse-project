/*
=======================================================================
Create Database and Schemas
=======================================================================
Scripts Purpose:
  This script creates a new databse named 'Datawarehouse' after checking if it already exists.
  If the databse exists, if is dropped and recreated. Additionally,  the scripts sets up three schemas
  within the database: 'Bronze', 'Silver' and 'Gold'.

WARNING:
  Running this script will drop the entire "Datawarehouse" database if it exists.
  All data in the database will be permenently deleted. Proceed with caution
  and ensure you have proper backups before running this script.
*/
Use master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 from sys.databases WHERE name='DataWarehouse')
BEGIN 
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
Go

USE DataWarehouse;
Go

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
