DROP DATABASE Crawler;
CREATE DATABASE Crawler;
USE Crawler;

create table Crawl (
	CrawlID INT primary key auto_increment,
    StartTime DATETIME default NOW()
);

create table Webpage (
	WebpageID INT primary key auto_increment,
	url VARCHAR(2048),
    protocol VARCHAR(10),
    subdomain VARCHAR(10),
    domain VARCHAR(64),
    tld VARCHAR(10)
);

create table Observation (
	ObservationID INT primary key auto_increment,
    WebpageID INT references Webpage(WebpageID),
    CrawlID INT references Crawl(CrawlID),
    html TEXT
);