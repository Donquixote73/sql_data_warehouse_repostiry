/*
===========================
Create Database and Schemas
===========================
Script Purpose;
  This script creates a new database named 'DataWarehouse' after checking if already exists.
  If the databas exists, It is dropped and recreated. Additionally, The script sets up three scemas
  within the database:'bronze', 'silver', and 'gold'. 
*/
--If the Database named 'DataWarehouse' exists, delete or rename the old database 
--Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse
USE DataWarehouse
--Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold
