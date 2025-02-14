-- Insert initial data into Customer_Current
INSERT INTO dbo.Customer_Current (CustomerID, CustomerName, Address)
VALUES (1, 'John Doe', '123 Main St'),
       (2, 'Jane Smith', '456 Oak Ave'),
       (3, 'Alice Johnson', '789 Pine Rd');
GO

-- Insert updates into Customer_Update (simulating changes)
INSERT INTO dbo.Customer_Update (CustomerID, CustomerName, Address)
VALUES (1, 'John A. Doe', '123 Main St'), -- Name updated
       (3, 'Alice Johnson', '987 Maple Rd'); -- Address updated
GO
