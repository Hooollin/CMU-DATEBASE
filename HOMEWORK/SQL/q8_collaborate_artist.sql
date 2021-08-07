select 
    count(*) as total_number
from 
    artist_credit
where
    `name` like "%Ariana Grande%";