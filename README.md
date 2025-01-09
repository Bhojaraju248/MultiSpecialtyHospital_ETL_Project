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
