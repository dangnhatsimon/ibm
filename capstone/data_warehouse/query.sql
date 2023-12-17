SELECT co.country, ca.category, SUM(sa.amount) AS "Total sales"
FROM dimcountry AS co
INNER JOIN factsales AS sa
ON co.countryid = sa.countryid
INNER JOIN dimcategory AS ca
ON ca.categoryid = sa.categoryid
GROUP BY
	GROUPING SETS (
		(co.country, ca.category),
		(co.country),
		(ca.category)
	);



SELECT co.country, da.year, SUM(sa.amount) AS "Total sales"
FROM dimcountry AS co
INNER JOIN factsales AS sa
ON co.countryid = sa.countryid
INNER JOIN dimdate AS da
ON da.dateid = sa.dateid
GROUP BY
	ROLLUP (co.country, da.year);


CREATE MATERIALIZED VIEW IF NOT EXISTS total_sales_per_country AS
SELECT 
	co.country,
	SUM(sa.amount) AS "total_sales"
FROM dimcountry AS co
INNER JOIN factsales AS sa
ON co.countryid = sa.countryid
GROUP BY
	CUBE(co.country);
	
REFRESH MATERIALIZED VIEW total_sales_per_country;