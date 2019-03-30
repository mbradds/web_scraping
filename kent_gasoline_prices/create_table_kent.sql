use EnergyData
go


DROP TABLE kent;
CREATE TABLE kent (
	[Region]	varchar(100),
	[Date]	datetime,
	[Price]	float,
	[Year]	int,
	[Product]	varchar(100),
	[Report]	varchar(50),
	[Frequency]	varchar(50),
	[url] varchar(MAX)
);
GO
