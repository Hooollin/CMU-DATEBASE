with past_year_release as 
    (select (date_year || "." || (
        case
            when date_month < 10 then "0"
            else ""
        end
    ) || date_month) as year_month, count(*) as amount
    from 
        release 
    join 
        release_info 
        on release.id == release_info.release
    where 
        (date_year == 2019 and date_month >= 7) 
        or 
        (date_year == 2020 and date_month <= 7)
group by date_year, date_month)
select year_month,
round(amount * 100.0 / (select sum(amount) from past_year_release), 2) as `percentage`
from past_year_release;