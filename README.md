# MultiSpecialtyHospital_ETL_Project
ETL process for splitting customer data based on country
#Create Table for Staging Data

CREATE TABLE Staging_Customers (
    Customer_Name VARCHAR(255) NOT NULL,
    Customer_ID VARCHAR(18) NOT NULL,
    Open_Date DATE NOT NULL,
    Last_Consulted_Date DATE,
    Vaccination_Id CHAR(5),
    Dr_Name VARCHAR(255),
    State CHAR(5),
    Country CHAR(5),
    DOB DATE,
    Is_Active CHAR(1)
);

#Create Tables for Each Country
#Create tables for each country dynamically based on country codes.

CREATE TABLE Table_India (
    Customer_Name VARCHAR(255) NOT NULL,
    Customer_ID VARCHAR(18) NOT NULL,
    Open_Date DATE NOT NULL,
    Last_Consulted_Date DATE,
    Vaccination_Id CHAR(5),
    Dr_Name VARCHAR(255),
    State CHAR(5),
    Country CHAR(5),
    DOB DATE,
    Is_Active CHAR(1),
    Age INT, Days_Since_Last_Consulted INT ); 

#Extract Data
#to extract customer data from the source file.

import pandas as pd
import pyodbc
data = pd.read_csv('customer_data_file.csv', delimiter='|') 
# Extract relevant data 
staging_data = data[data['Record_Type'] == 'D']
# Connect to the database 
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=your_server;DATABASE=your_db;UID=your_username;PWD=your_password') 
# Load data into staging table 
for index, row in staging_data.iterrows(): cursor = conn.cursor() cursor.execute(""" INSERT INTO Staging_Customers (Customer_Name, Customer_ID, Open_Date, Last_Consulted_Date, Vaccination_Id, Dr_Name, State, Country, DOB, Is_Active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """, row['Customer_Name'], row['Customer_ID'], row['Open_Date'], row['Last_Consulted_Date'], row['Vaccination_Id'], row['Dr_Name'], row['State'], row['Country'], row['DOB'], row['Is_Active']) conn.commit()

#Create additional derived columns (age and days since last consulted).
ALTER TABLE Staging_Customers ADD Age INT;
ALTER TABLE Staging_Customers ADD Days_Since_Last_Consulted INT;

UPDATE Staging_Customers
SET Age = DATEDIFF(YEAR, DOB, GETDATE());

UPDATE Staging_Customers
SET Days_Since_Last_Consulted = DATEDIFF(DAY, Last_Consulted_Date, GETDATE())
WHERE Last_Consulted_Date IS NOT NULL;

#Move the data from the staging table to the corresponding country tables.

INSERT INTO Table_India (Customer_Name, Customer_ID, Open_Date, Last_Consulted_Date, Vaccination_Id, Dr_Name, State, Country, DOB, Is_Active, Age, Days_Since_Last_Consulted)
SELECT Customer_Name, Customer_ID, Open_Date, Last_Consulted_Date, Vaccination_Id, Dr_Name, State, Country, DOB, Is_Active, Age, Days_Since_Last_Consulted
FROM Staging_Customers
WHERE Country = 'IND';

#Implement necessary data validations to ensure data quality.
Example: Validate Customer ID is not null
ALTER TABLE Staging_Customers
ADD CONSTRAINT chk_Customer_ID CHECK (Customer_ID IS NOT NULL);
