select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    content_hash as unique_field,
    count(*) as n_records

from "iceberg"."silver_silver"."silver_documents"
where content_hash is not null
group by content_hash
having count(*) > 1



      
    ) dbt_internal_test