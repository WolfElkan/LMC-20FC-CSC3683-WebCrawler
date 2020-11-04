DROP DATABASE Crawler;
CREATE DATABASE Crawler;
USE Crawler;

create table Crawl (
	CID INT primary key auto_increment,
    starttime DATETIME default NOW(),
    rootWID INT references Webpage(WID)
);

create table Webpage (
	WID INT primary key auto_increment,
	url VARCHAR(2048),
    newCID INT REFERENCES Crawl(CID)
    -- protocol VARCHAR(10),
    -- subdomain VARCHAR(16),
    -- domain VARCHAR(64),
    -- tld VARCHAR(10),
    -- pathway VARCHAR(256),
    -- query VARCHAR(256),
    -- ext VARCHAR(8),
    -- fragment VARCHAR(16)
);

create table Observation (
	OID INT primary key auto_increment,
    WID INT references Webpage(WID),
    CID INT references Crawl(CID),
    http INT,
    html TEXT
);

create table Link (
	lID INT PRIMARY KEY auto_increment,
    fromID INT references Webpage(wID),
    toID INT references Webpage(wID)
);
    