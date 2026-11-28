use master
go

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'HealthcareETL')
BEGIN
    ALTER DATABASE HealthcareETL SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE HealthcareETL;
END;
GO

CREATE DATABASE HealthcareETL;

go

use HealthcareETL

go

if OBJECT_ID('patient_info', 'U') is not null
	drop table patient_info;

go

create table patient_info(
	event_id			int primary key,
	patient_id			int,
	patient_name		nvarchar(100),
	age					int,
	gender				nvarchar(10),
	appointment_date	date,
	booking_date		date,
	doctor_name			nvarchar(100),
	department			nvarchar(50),
	currency			nvarchar(10),
	billing_amount		decimal(10, 2),
	follow_up_required	bit
);
