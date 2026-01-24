
    
    

select
    id as unique_field,
    count(*) as n_records

from "iceberg"."silver_gold"."gold_documents"
where id is not null
group by id
having count(*) > 1


