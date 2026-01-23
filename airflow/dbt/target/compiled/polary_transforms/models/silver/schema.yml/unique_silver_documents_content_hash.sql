
    
    

select
    content_hash as unique_field,
    count(*) as n_records

from "iceberg"."silver_silver"."silver_documents"
where content_hash is not null
group by content_hash
having count(*) > 1


