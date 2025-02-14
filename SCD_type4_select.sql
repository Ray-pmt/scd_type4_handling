select * from [dbo].[Customer_Current];
select * from [dbo].[Customer_History];
select * from [dbo].[Customer_Update];

-- First remove the foreign key constraint
ALTER TABLE [dbo].[Customer_History]
DROP CONSTRAINT [FK_CustomerHistory_CustomerID];

-- Then drop the table
DROP TABLE [dbo].[Customer_Current];

DROP TABLE [dbo].[Customer_History];

DROP TABLE [dbo].[Customer_Update];

DROP PROCEDURE IF EXISTS dbo.CleanupHistoricalData;
GO