/***************************************************************
Author: Stevan Rakonjac
Project: eCommerce Data Normalization Case Study in SQL Server
Dataset: https://www.kaggle.com/datasets/carrie1/ecommerce-data
****************************************************************/




CREATE DATABASE eCommerceData;
GO

USE eCommerceData;
GO


/*************************************************************************
Import a CSV file containing data about eCommerce (downloaded from Kaggle:
https://www.kaggle.com/datasets/carrie1/ecommerce-data) 
**************************************************************************/

SELECT *
FROM eCommerce;


-- CLEANING AND NORMALIZING THE DATA


/************************************************************************
There are some NULLs in the CustomerID column. I will replace these NULLS 
with NON-NULL values based on the Country column. These new CustomerID 
values will be negative in order to distinguish them from the CustomerIDs
in the original data, which are all positive.
*************************************************************************/

SELECT CustomerID, Country
FROM eCommerce
GROUP BY Country, CustomerID
HAVING CustomerID IS NULL;

UPDATE eCommerce
SET CustomerID=
	CASE 
	WHEN Country='Israel' THEN -1
	WHEN Country='Portugal' THEN -2
	WHEN Country='EIRE' THEN -3
	WHEN Country='Hong Kong' THEN -4
	WHEN Country='Bahrain' THEN -5
	WHEN Country='United Kingdom' THEN -6
	WHEN Country='Switzerland' THEN -7
	WHEN Country='France' THEN -8
	WHEN Country='Unspecified' THEN -9
	ELSE -99
	END
WHERE CustomerID IS NULL;



/************************************************************************
Country seem to depend separately on CustomerID and on InvoiceNo. We can 
make it depend only on CustomerID by creating a separate Customers table
containing columns CustomerID and Country. If Country really depends on 
CustomerID then the new Customers table can have one row per CustomerID.
So first lets check whether there is only one Country for each CustomerID
*************************************************************************/


SELECT CustomerID, COUNT(DISTINCT Country) as CountryCount
FROM eCommerce
GROUP BY CustomerID
HAVING COUNT(DISTINCT Country)>1
ORDER BY CountryCount DESC;


/**************************************************************************
Only 8 CustomerIDs have two Countries associate with them, all other 
CustomerIDs have only one Country associated with them. Lost of information
about the second country associated with these 8 cases seem negligible, so
I will choose one Country per CustomerID to put in the new Customers table.
In this way we get a cleaner and a more normalized data at a small cost. 
I will select that Country which corresponds to the latest InvoiceDate for
each Customer ID. In that way, the 8 ambiguous case will be handled.
***************************************************************************/


CREATE TABLE Customers
(
CustomerID SMALLINT PRIMARY KEY,
Country NVARCHAR(50)
);
GO

WITH LatestCountry AS
(
	SELECT CustomerID, Country, InvoiceDate,
	ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY InvoiceDate DESC) 
	AS RN
	FROM eCommerce
)
INSERT INTO Customers(CustomerID, Country)
SELECT CustomerID, Country
FROM LatestCountry
WHERE RN=1;

SELECT *
FROM Customers;



/**************************************************************************
Next I will create a separate Products table. Product Description should 
depend on StockCode. But in the original eCommerce table StockCodes often
have more  than one Descriptions associated with them. This can be checked
with the following query.
***************************************************************************/

SELECT StockCode, COUNT(DISTINCT Description) as DescriptionCount
FROM eCommerce
GROUP BY StockCode
HAVING COUNT(DISTINCT Description)>1
ORDER BY DescriptionCount DESC;


/***********************************************************************
Upon manual inspection of the StockCodes that were obtained in the above 
query, it seems that different Descriptions for the same StockCode are 
mainly due to  some rows havin NULL, or values like "?" or "wrongly 
marked 23343" in the  Description column. For Example:
************************************************************************/

SELECT *
FROM eCommerce
WHERE StockCode='20713';

SELECT *
FROM eCommerce
WHERE StockCode='21421';

SELECT *
FROM eCommerce
WHERE StockCode='48185';


/***************************************************************************
I will handle the issue in the following way. First I will create a Products
table with columns ProductID, StockCode, and Description. Then, for every 
StockCode from the original table, I will choose its most common description
and insert insert it in the new Products  table. In this way, we should end 
up with a Products table in which each StockCode has only one description, 
the most common one, thus cleaning  the data from occasional NULLs and 
errors in Description. In order to choose the  most common description 
I will use two CTEs.
****************************************************************************/

CREATE TABLE Products
(
	ProductID SMALLINT IDENTITY(1,1) PRIMARY KEY,
	StockCode NVARCHAR(50),
	ProductDescription NVARCHAR(50)
);
GO

WITH DescriptionCount
AS
(
	SELECT StockCode, Description, COUNT(*) AS DescripCount
	FROM eCommerce
	GROUP BY StockCode, Description
),
	MostCommonDescription
AS
(
	SELECT StockCode, Description, DescripCount,
	ROW_NUMBER() OVER (PARTITION BY StockCode ORDER BY DescripCount DESC) AS RN
	FROM DescriptionCount
)
INSERT INTO Products (StockCode, ProductDescription)
SELECT StockCode, Description
FROM MostCommonDescription
WHERE RN=1;


SELECT * FROM Products;


/******************************************************************
Next I will create a separate Invoice table, containing one row per
InvoiceNo. It  will contain columns: InvoiceNo (PK), InvoiceDate,
and CustomerID (FK). I will then check whether there are more than 
one InvoiceDates and CustomerIDs per InvoiceNo.
*******************************************************************/

CREATE TABLE Invoices 
(
	InvoiceNo NVARCHAR(50) PRIMARY KEY,
	InvoiceDate DATETIME2,
	CustomerID SMALLINT FOREIGN KEY REFERENCES Customers(CustomerID)
);
GO

SELECT 
	InvoiceNo, 
	COUNT(DISTINCT CustomerID) as CustomerCount,
	COUNT(DISTINCT InvoiceDate) as DateCount
FROM eCommerce
GROUP BY InvoiceNo
HAVING COUNT(DISTINCT CustomerID)>1 OR COUNT(DISTINCT InvoiceDate)>1;


/************************************************************************
There are some cases of different InvoiceDates attached to single 
InvoiceNo. Upon inspection it appears that in all such cases InvoiceDates
differ only by a single minute. It seem safe, thus, to neglect these 
differences and treat InvoiceDate as strictly depending on InvoiceNo,
chosing MIN(InvoiceDate) as a unique InvoiceDate when there are multiple
values. Also, since there is one CustomerID per InvoiceNo in all cases,
GROUP BY InvoiceNo, CustomerID can be used to get exactly one row  per 
InvoiceNo.
*************************************************************************/


INSERT INTO Invoices (InvoiceNo, InvoiceDate, CustomerID)
SELECT 
	InvoiceNo,
	MIN(InvoiceDate),
	CustomerID
FROM eCommerce
GROUP BY InvoiceNo, CustomerID;


SELECT * FROM Invoices;


/******************************************************************
Finally, I will create an InvoiceLines table, which stores invoice
information which differ across different rows within the same 
InvoiceNo. Since non of the columns  from the original eCommerce
table uniquely identifies line-level details about invoices, a
surrogate primary key column called InvoiceLineID will be added.
Besides this, the table will contain InvoiceNo (FK), ProductID (FK),
Quantity, and UnitPrice.
********************************************************************/

CREATE TABLE InvoiceLines
(
	InvoiceLineID INT IDENTITY(1,1) PRIMARY KEY,
	InvoiceNo NVARCHAR(50) FOREIGN KEY REFERENCES Invoices(InvoiceNo),
	ProductID SMALLINT FOREIGN KEY REFERENCES Products(ProductID),
	Quantity INT,
	UnitPrice FLOAT
);
GO

INSERT INTO InvoiceLines (InvoiceNo, ProductID, Quantity, UnitPrice)
SELECT
	e.InvoiceNo,
	P.ProductID,
	e.Quantity,
	e.UnitPrice 
FROM eCommerce e
INNER JOIN Products P on e.StockCode=P.StockCode;

SELECT * FROM InvoiceLines;


/*****************************************************************
The SELECT statement that retrieves (almost) all the data from the 
original table
******************************************************************/

SELECT
	IL.InvoiceNo,
	P.StockCode,
	P.ProductDescription,
	IL.Quantity,
	I.InvoiceDate,
	C.CustomerID,
	C.Country
FROM InvoiceLines IL
INNER JOIN Products P ON P.ProductID=IL.ProductID
INNER JOIN Invoices I ON I.InvoiceNo=IL.InvoiceNo
INNER JOIN Customers C ON C.CustomerID=I.CustomerID;
