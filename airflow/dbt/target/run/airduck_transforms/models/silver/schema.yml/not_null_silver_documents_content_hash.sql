select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select content_hash
from "iceberg"."silver_silver"."silver_documents"
where content_hash is null



      
    ) dbt_internal_test