USE CustomerDB;
GO

-- Drop existing objects
IF OBJECT_ID('dbo.Customer_Current', 'U') IS NOT NULL DROP TABLE dbo.Customer_Current;
IF OBJECT_ID('dbo.Customer_History', 'U') IS NOT NULL DROP TABLE dbo.Customer_History;
IF OBJECT_ID('dbo.Customer_Update', 'U') IS NOT NULL DROP TABLE dbo.Customer_Update;
IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'PS_CustomerHistory')
    DROP PARTITION SCHEME PS_CustomerHistory;
IF EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'PF_CustomerHistory')
    DROP PARTITION FUNCTION PF_CustomerHistory;
GO

-- Create partition function and scheme
CREATE PARTITION FUNCTION PF_CustomerHistory (datetime)
AS RANGE RIGHT FOR VALUES ('2023-01-01', '2024-01-01', '2025-01-01');
GO

CREATE PARTITION SCHEME PS_CustomerHistory
AS PARTITION PF_CustomerHistory ALL TO ([PRIMARY]);
GO

-- Create tables
CREATE TABLE dbo.Customer_Current (
    CustomerID INT NOT NULL PRIMARY KEY,
    CustomerName NVARCHAR(100),
    Address NVARCHAR(255),
    LastUpdated DATETIME NOT NULL DEFAULT GETDATE()
);

CREATE TABLE dbo.Customer_History (
    HistoryID INT IDENTITY(1,1) NOT NULL,
    CustomerID INT NOT NULL,
    CustomerName NVARCHAR(100),
    Address NVARCHAR(255),
    ValidFrom DATETIME NOT NULL,
    ValidTo DATETIME NULL,
    CONSTRAINT PK_CustomerHistory PRIMARY KEY NONCLUSTERED (HistoryID, ValidFrom)
) ON PS_CustomerHistory(ValidFrom);

CREATE TABLE dbo.Customer_Update (
    CustomerID INT NOT NULL PRIMARY KEY,
    CustomerName NVARCHAR(100),
    Address NVARCHAR(255)
);
GO

-- Create indexes
CREATE NONCLUSTERED INDEX IX_CustomerCurrent_LastUpdated
ON dbo.Customer_Current (LastUpdated)
INCLUDE (CustomerName, Address);

CREATE CLUSTERED COLUMNSTORE INDEX CCI_CustomerHistory 
ON dbo.Customer_History;

CREATE NONCLUSTERED INDEX IX_CustomerHistory_CustomerID
ON dbo.Customer_History (CustomerID)
INCLUDE (CustomerName, Address, ValidFrom, ValidTo);

CREATE NONCLUSTERED INDEX IX_CustomerHistory_Active
ON dbo.Customer_History (CustomerID, ValidFrom)
WHERE ValidTo IS NULL;
GO

-- Create foreign key
ALTER TABLE dbo.Customer_History  
ADD CONSTRAINT FK_CustomerHistory_CustomerID 
FOREIGN KEY (CustomerID) REFERENCES dbo.Customer_Current(CustomerID);
GO

-- Create statistics
CREATE STATISTICS STAT_CustomerHistory_ValidDates
ON dbo.Customer_History(ValidFrom, ValidTo);
GO

-- Create cleanup procedure
CREATE PROCEDURE dbo.CleanupHistoricalData
    @RetentionPeriod INT = 730  -- Default 2 years
AS
BEGIN
    SET NOCOUNT ON;
    DELETE FROM dbo.Customer_History
    WHERE ValidTo IS NOT NULL 
    AND DATEDIFF(DAY, ValidTo, GETDATE()) > @RetentionPeriod;
END;
GO