DROP DATABASE Crawler;
CREATE DATABASE Crawler;
USE Crawler;

create table Crawl (
	cID INT primary key auto_increment,
    StartTime DATETIME default NOW()
);

create table Webpage (
	wID INT primary key auto_increment,
	url VARCHAR(2048),
    protocol VARCHAR(10),
    subdomain VARCHAR(16),
    domain VARCHAR(64),
    tld VARCHAR(10),
    pathway VARCHAR(256),
    query VARCHAR(256),
    ext VARCHAR(8),
    fragment VARCHAR(16)
);

create table Observation (
	oID INT primary key auto_increment,
    WebpageID INT references Webpage(wID),
    CrawlID INT references Crawl(cID),
    html TEXT
);