DROP DATABASE Crawler;
CREATE DATABASE Crawler;
USE Crawler;

create table Crawl (
	CID INT primary key auto_increment,
    starttime DATETIME(6) default NOW(6),
    endtime DATETIME(6),
    nLevels INT,
    rootWID INT references Webpage(WID),
    created_at TIMESTAMP(6) default NOW(6),
    updated_at TIMESTAMP(6) default NOW(6) ON UPDATE NOW(6),
    access BOOLEAN
);

create table Webpage (
	WID INT primary key auto_increment,
	url VARCHAR(2048),
    newCID INT REFERENCES Crawl(CID),
    mined BOOLEAN default FALSE,
    -- protocol VARCHAR(10),
    -- subdomain VARCHAR(16),
    -- domain VARCHAR(64),
    -- tld VARCHAR(10),
    -- pathway VARCHAR(256),
    -- query VARCHAR(256),
    -- ext VARCHAR(8),
    -- fragment VARCHAR(16)
    created_at TIMESTAMP(6) default NOW(6),
    updated_at TIMESTAMP(6) default NOW(6) ON UPDATE NOW(6),
    access BOOLEAN
);

create table Observation (
	OID INT primary key auto_increment,
    WID INT references Webpage(WID),
    CID INT references Crawl(CID),
    http INT,
    html LONGTEXT,
    created_at TIMESTAMP(6) default NOW(6),
    updated_at TIMESTAMP(6) default NOW(6) ON UPDATE NOW(6),
    access BOOLEAN
);

create table Link (
	lID INT PRIMARY KEY auto_increment,
    fromWID INT references Webpage(WID),
    toWID INT references Webpage(WID),
    created_at TIMESTAMP(6) default NOW(6),
    updated_at TIMESTAMP(6) default NOW(6) ON UPDATE NOW(6),
    access BOOLEAN
);

create table Stem (
	stem VARCHAR(25) PRIMARY KEY,
    created_at TIMESTAMP(6) default NOW(6),
    updated_at TIMESTAMP(6) default NOW(6) ON UPDATE NOW(6),
    access BOOLEAN
);

create table Word (
	RID INT primary key auto_increment,
	stem VARCHAR(25) references Stem(stem),
    OID INT references Observation(OID),
    pos INT,
    created_at TIMESTAMP(6) default NOW(6),
    updated_at TIMESTAMP(6) default NOW(6) ON UPDATE NOW(6),
    access BOOLEAN
)
    