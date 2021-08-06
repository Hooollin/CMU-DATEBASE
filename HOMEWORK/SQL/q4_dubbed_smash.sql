select artist.name, newtable.amount from artist join
(select artist, count(artist) as amount from artist_alias group by artist) as newtable
on artist.id == newtable.artist
join area on artist.area == area.id
where artist.begin_date_year > 1950 and artist.area in (select area.id from area where area.name == "United Kingdom")
order by newtable.amount desc limit 10;
