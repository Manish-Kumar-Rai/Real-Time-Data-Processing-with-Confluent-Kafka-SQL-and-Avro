-- Create the database if it doesn't exist
IF DB_ID('GrowDataSkill') IS NULL
	CREATE DATABASE GrowDataSkill;
GO

-- Switch to the database
USE GrowDataSkill;
GO

-- Drop the table if it exists
IF OBJECT_ID('product', 'U') IS NOT NULL
    DROP TABLE product;
GO

-- Create the product table
CREATE TABLE product (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price MONEY CHECK(price >=0),
    last_updated DATETIME DEFAULT GETDATE()
);
GO

-- Optional: Add an index on last_updated for faster incremental fetches
CREATE INDEX idx_last_updated ON product(last_updated);

-- Optional: view the table
SELECT * FROM product;
