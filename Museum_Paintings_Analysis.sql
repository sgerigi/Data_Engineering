use museum;
-- Read the data
select top 10 * from [dbo].[artist];
select top 10 * from [dbo].[canvas_size];
select top 10 * from [dbo].[image_link];
select top 10 * from [dbo].[museum];
select top 10 * from [dbo].[museum_hours];
select top 10 * from [dbo].[product_size];
select top 10 * from [dbo].[subject];
select top 10 * from [dbo].[work];

-- Fetch all the paintings which are not displayed on any museums?
select
	name as paintings
from
	work
where
	museum_id is null;

--Are there museums without any paintings?
select 
	*
from
	museum
where
	museum_id not in (select distinct museum_id from work);

--select * from museum m
--	where not exists (select 1 from work w
--					 where w.museum_id=m.museum_id)

--How many paintings have an asking price of more than their regular price?
select
	count(work_id) as no_of_paintings
from 
	product_size
where
	sale_price > regular_price;

--Identify the paintings whose asking price is less than 50% of its regular price
select * from product_size where sale_price < (regular_price * 0.5);

--Which canva size costs the most?
select size_id, label --only size display
from canvas_size
where size_id = (select size_id from product_size where sale_price = (select MAX(sale_price) from product_size))

select	
	cs.label as canva_size, 
	ps.sale_price as price
from 
	canvas_size as cs	-- display size with cost
	join 
	(select *
			,DENSE_RANK() over (order by sale_price desc) as rnk
		from product_size
		) as ps
	on cs.size_id = ps.size_id
where rnk = 1;



select cs.label as canva, ps.sale_price
	from (select *
		  , rank() over(order by sale_price desc) as rnk 
		  from product_size) ps
	join canvas_size cs on cs.size_id=ps.size_id
	where ps.rnk=1;	

-- Delete duplicate records from work, product_size, subject and image_link tables
-- work
with cte
as
(
select *,
	row_number() over (partition by work_id order by work_id) as rn
from
	work
) 
delete from cte where rn > 1;

--product size
with cte
as
(
select *,
	row_number() over (partition by work_id, size_id order by work_id) as rn
from
	product_size
) 
delete from cte where rn > 1;

-- subject
with cte
as
(
select *,
	row_number() over (partition by work_id, subject order by work_id) as rn
from
	subject
) 
delete from cte where rn > 1;

--Image
with cte
as
(
select *,
	row_number() over (partition by work_id order by work_id) as rn
from
	image_link
) 
delete from cte where rn > 1

select count(*) from image_link;


-- Identify the museums with invalid city information in the given dataset
select name as museum_name, city from [dbo].[museum] where ISNUMERIC(city) = 0; -- only numeric will eliminate

select name as museum_name, city from [dbo].[museum] where city like '%[0-9]%' -- numeric and in between numbers in string as well.


--Museum_Hours table has 1 invalid entry. Identify it and remove it.
with cte 
as
	(
	select	
		*,
		row_number() over (partition by museum_id, day order by museum_id) rn	
	from [dbo].[museum_hours]
	)
delete from cte where rn = 2;


--Fetch the top 10 most famous painting subject
select * from [dbo].[subject]

select subject from
(
select b.subject, count(1) as no_of_paintings, rank() over (order by count(1) desc) as rnk
from [dbo].[work] a join [dbo].[subject] b
on a.work_id=b.work_id 
group by b.subject
) k
where rnk <=10;


select subject,
	COUNT(*) as cnt
from subject
group by subject
having count(*) > 1000

--Identify the museums which are open on both Sunday and Monday. Display museum name, city.

select distinct a.museum_id, a.name, a.city, b.day
from [dbo].[museum] a join [dbo].[museum_hours] b
on	a.museum_id = b.museum_id
where upper(b.day) = 'MONDAY'
 and a.museum_id in (select distinct museum_id from museum_hours where UPPER(day) = 'SUNDAY')


 select distinct m.museum_id,m.name as museum_name, m.city, m.state,m.country, mh.day
	from museum_hours mh 
	join museum m on m.museum_id=mh.museum_id
	where day='Sunday'
	and exists (select museum_id from museum_hours mh2
				where mh2.museum_id=mh.museum_id 
			    and mh2.day='Monday');

--How many museums are open every single day?

select count(*) as no_of_museums
from
(
select museum_id, COUNT(*) as days
from museum_hours
group by museum_id
having count(*) = 7
) as a;

select count(*) as musuems_all_day_open
from
(
select museum_id,
	   ROW_NUMBER() over (partition by museum_id order by day) as rn
from museum_hours
) k
where rn = 7
;

--Which are the top 5 most popular museum? (Popularity is defined based on most
--no of paintings in a museum)

select name
from museum
where museum_id in (
		select top 5 museum_id
		from [dbo].[work] 
		where museum_id is not null 
		group by museum_id 
		order by count(*) desc
		);

select name
from museum
where museum_id in
(
select distinct museum_id
from
(
	select museum_id,
			count(1) as cnt,
			DENSE_RANK() over (order by count(*) desc) as rnk
	from work
	where museum_id is not null
	group by museum_id
) k
where rnk <=5
) ;

-- Who are the top 5 most popular artist? (Popularity is defined based on most no of
--paintings done by an artist)

select full_name, no_of_paints
from artist a join (
				select 
					artist_id
					,count(*) as no_of_paints
					,rank() over (order by count(*) desc) as rnk
				from work
				group by artist_id
				) w
on a.artist_id = w.artist_id
where w.rnk <=5;

--Display the 3 least popular canva sizes


-- Which museum is open for the longest during a day. Dispay museum name, state and hours 
-- open and which day?

WITH museum_hrs
AS
(
SELECT 
     [museum_id],
	 [day],
     CAST(SUBSTRING([open], 1, 5) + ':00' + RIGHT([open], 2) AS TIME) open_time,
	 CAST(SUBSTRING([close], 1, 5) + ':00' + RIGHT([close], 2) AS TIME) AS close_time
FROM 
    [dbo].[museum_hours]
)
SELECT 
	a.[name], 
	a.[state], 
	datediff(MINUTE, b.open_time, b.close_time)/60.0 as hr_open,
	b.[day],
	open_time,
	close_time
FROM
	[dbo].[museum] as a
	join
	(SELECT *, 
		RANK() over (order by datediff(MINUTE, open_time, close_time) desc) as rnk
	FROM museum_hrs) b
ON	a.museum_id = b.museum_id
WHERE
	rnk = 1;

--Which museum has the most no of most popular painting style?


SELECT name,
	style,
	no_of_paintings 
FROM [dbo].[museum] a JOIN
	(SELECT museum_id,
		style,
		count(style) as no_of_paintings,
		rank() over (order by count(style) desc) as rnk
	FROM [work] 
	WHERE museum_id IS NOT NULL
	GROUP BY museum_id,style) as b
	ON a.museum_id = b.museum_id
WHERE style = (
		SELECT TOP 1 style
		FROM [dbo].[work] 
		GROUP BY  style
		ORDER BY COUNT(style) desc
		)
and rnk = 1;

with pop_style as 
			(select style
			,rank() over(order by count(1) desc) as rnk
			from work
			group by style),
		cte as
			(select w.museum_id,m.name as museum_name,ps.style, count(1) as no_of_paintings
			,rank() over(order by count(1) desc) as rnk
			from work w
			join museum m on m.museum_id=w.museum_id
			join pop_style ps on ps.style = w.style
			where w.museum_id is not null
			and ps.rnk=1
			group by w.museum_id, m.name,ps.style)
	select museum_name,style,no_of_paintings
	from cte 
	where rnk=1;

-- Identify the artists whose paintings are displayed in multiple countries
WITH multi_country
AS
(
SELECT
	a.[museum_id]
	,[country]
	,[artist_id]
	,DENSE_RANK() over (PARTITION BY [artist_id] ORDER BY [country]) as rnk
FROM
	[dbo].[museum] as a
	JOIN [dbo].[work] as b
ON	a.museum_id = b.museum_id
)
SELECT
	
	[full_name],
	[no_of_paintings]
FROM
	[dbo].[artist] as a
	JOIN
	(SELECT distinct artist_id, MAX(rnk) OVER (PARTITION BY artist_id ORDER BY rnk desc) as no_of_paintings FROM multi_country) as b
ON	a.artist_id = b.artist_id
WHERE a.artist_id IN (SELECT artist_id FROM multi_country WHERE rnk > 1 )
ORDER BY [no_of_paintings] DESC;

with cte as
		(select distinct a.full_name as artist
		--, w.name as painting, m.name as museum
		, m.country
		from work w
		join artist a on a.artist_id=w.artist_id
		join museum m on m.museum_id=w.museum_id)
	select artist,count(1) as no_of_countries
	from cte
	group by artist
	having count(1)>1
	order by 2 desc;

/*
Display the country and the city with most no of museums. 
Output 2 seperate columns to mention the city and country. 
If there are multiple value, seperate them with comma.
*/
SELECT country,
	cities,
	no_of_museums
FROM
(
	SELECT  
		a.country,
		STRING_AGG(b.city, ', ') as cities,
		no_of_museums,
		RANK() OVER (ORDER BY COUNT(*) desc) rnk
	FROM 
		(SELECT country, count(*) as no_of_museums FROM  [dbo].[museum] WHERE city not like '%[0-9]%' GROUP BY country) a, 
		(SELECT distinct country, city FROM museum WHERE city not like '%[0-9]%') b
	WHERE
		a.country = b.country
	GROUP BY
		a.country,
		no_of_museums
)country_cities
WHERE rnk =1;


/*
19) Identify the artist and the museum where the most expensive and least expensive painting is placed. 
Display the artist name, sale_price, painting name, museum name, museum city and canvas label
*/

WITH cost_paintings
AS
(
SELECT *,
	CASE
			WHEN max_price = sale_price THEN 'EXPENSIVE'
			WHEN min_price = sale_price THEN 'INEXPENSIVE'
			ELSE 'NA'
		END as COST_FLAG
FROM
(
	SELECT *,
		max(sale_price) over(order by sale_price desc) as max_price,
		min(sale_price) over(order by sale_price asc) as min_price	
	FROM [dbo].[product_size] 
	) k
WHERE max_price = sale_price or min_price = sale_price
)
SELECT --artist name, sale_price, painting name, museum name, museum city and canvas label
	e.full_name,
	b.sale_price,
	a.name as painting_name,
	c.name as museum_name,
	c.city as museum_city,
	d.label as canvas_label,
	b.COST_FLAG
FROM [dbo].[work] a JOIN cost_paintings b
ON	a.work_id = b.work_id
JOIN [dbo].[museum] c
ON a.museum_id = c.museum_id
join [dbo].[canvas_size] d
ON d.size_id = b.size_id
JOIN [dbo].[artist] e
ON a.artist_id = e.artist_id;

with cte as 
		(select *
		, rank() over(order by sale_price desc) as rnk
		, rank() over(order by sale_price ) as rnk_asc
		from product_size )
	select w.name as painting
	, cte.sale_price
	, a.full_name as artist
	, m.name as museum, m.city
	, cz.label as canvas
	from cte
	join work w on w.work_id=cte.work_id
	join museum m on m.museum_id=w.museum_id
	join artist a on a.artist_id=w.artist_id
	join canvas_size cz on cz.size_id = cte.size_id
	where rnk=1 or rnk_asc=1;

-- Which country has the 5th highest no of paintings?

SELECT
	country,
	no_of_paintings
FROM
(
	SELECT 
		country,
		count(*) as no_of_paintings,
		rank() over (order by count(*) desc) rnk
	FROM	[dbo].[work] a
		JOIN	[dbo].[museum] b
	ON a.museum_id = b.museum_id
	WHERE a.museum_id is not null
	GROUP BY country
) k
WHERE
	rnk = 5;

with cte as 
		(select m.country, count(1) as no_of_Paintings
		, rank() over(order by count(1) desc) as rnk
		from work w
		join museum m on m.museum_id=w.museum_id
		group by m.country)
	select country, no_of_Paintings
	from cte 
	where rnk=5;

--Which are the 3 most popular and 3 least popular painting styles?
	with cte as 
		(select style, count(1) as cnt
		, rank() over(order by count(1) desc) rnk
		, count(1) over() as no_of_records
		from work
		where style is not null
		group by style)
	select style
	, case when rnk <=3 then 'Most Popular' else 'Least Popular' end as remarks 
	from cte
	where rnk <=3
	or rnk > no_of_records - 3;

