select (date_year - date_year % 10) || "s" as decades, count(*) as amount from release_info where release_info.release in(
select release.id from release 
    join 
    release_status 
    on release.status == release_status.id where release_status.name == "Official"
) and date_year is not NULL and date_year >= 1900
group by decades order by decades desc;

