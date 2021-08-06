select area.name, newtable.amount from area join
 (SELECT area, count(*) as amount from artist where begin_date_year < 1850 group by area) as newtable
on  area.id == newtable.area
order by newtable.amount desc;
