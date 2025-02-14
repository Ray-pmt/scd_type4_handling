-- Archive changed records before updating
INSERT INTO dbo.Customer_History (CustomerID, CustomerName, Address, ValidFrom, ValidTo)
SELECT 
    cc.CustomerID,
    cc.CustomerName,
    cc.Address,
    cc.LastUpdated AS ValidFrom,
    GETDATE() AS ValidTo
FROM dbo.Customer_Current cc
JOIN dbo.Customer_Update cu 
    ON cc.CustomerID = cu.CustomerID
WHERE 
    ISNULL(cc.CustomerName, '') <> ISNULL(cu.CustomerName, '')
    OR ISNULL(cc.Address, '') <> ISNULL(cu.Address, '');
GO

-- Update the main table with the new values
UPDATE cc
SET 
    cc.CustomerName = cu.CustomerName,
    cc.Address = cu.Address,
    cc.LastUpdated = GETDATE()
FROM dbo.Customer_Current cc
JOIN dbo.Customer_Update cu 
    ON cc.CustomerID = cu.CustomerID
WHERE 
    ISNULL(cc.CustomerName, '') <> ISNULL(cu.CustomerName, '')
    OR ISNULL(cc.Address, '') <> ISNULL(cu.Address, '');
GO
