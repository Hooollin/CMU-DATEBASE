select work.name, work.type, type_name
from work, 
    (select type, type_name, MAX(LENGTH(name)) as max_len 
        from (select name, type, type_name from work 
                join (
                    select name as type_name, id as wtid from work_type) on type == wtid) 
        group by type order by type) as t 
where work.type == t.type and LENGTH(work.name) >= t.max_len 
order by work.type asc, work.name asc;

select newtable.name, work_type.name from 
    (select work.name, work.type from work 
    join (select type, max(length(name)) as max_name_length from work group by type) as t 
on work.type = t.type and length(work.name) == t.max_name_length) as newtable 
join work_type on work_type.id == newtable.type order by work_type.name asc, newtable.name asc;