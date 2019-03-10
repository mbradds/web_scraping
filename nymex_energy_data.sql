USE master
GO
-- Create the new database if it does not exist already
IF NOT EXISTS (
    SELECT [name]
        FROM sys.databases
        WHERE [name] = N'energy_data'
)
CREATE DATABASE energy_data
GO

use master
GO
ALTER DATABASE [energy_data] SET AUTO_CLOSE OFF;
GO

IF OBJECT_ID('[dbo].test', 'U') IS NOT NULL
DROP TABLE [dbo].test
GO
-- Create the table in the specified schema
CREATE TABLE [dbo].[test]
(
    [Id] INT NOT NULL PRIMARY KEY, -- Primary Key column
    [ColumnName2] NVARCHAR(50) NOT NULL,
    [ColumnName3] NVARCHAR(50) NOT NULL
    -- Specify more columns here
);
GO