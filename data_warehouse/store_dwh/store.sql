BEGIN;

CREATE TABLE IF NOT EXISTS public."DimStore"
(
    storeid INT NOT NULL CONSTRAINT storeid_pk PRIMARY KEY,
    country VARCHAR(50),
    city VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS public."DimDate"
(
    dateid INT NOT NULL CONSTRAINT dateid_pk PRIMARY KEY,
    date TIMESTAMP,
    day INT,
    month INT,
    monthname varchar(12),
    year INT
);

CREATE TABLE IF NOT EXISTS public."FactSales"
(
    id INT NOT NULL CONSTRAINT salesid_pk PRIMARY KEY,
    storeid INT NOT NULL,
    dateid INT NOT NULL,
    totalsales FLOAT,
    FOREIGN KEY (storeid) REFERENCES public."DimStore" (storeid),
    FOREIGN KEY (dateid) REFERENCES public."DimDate" (dateid)
);

END;