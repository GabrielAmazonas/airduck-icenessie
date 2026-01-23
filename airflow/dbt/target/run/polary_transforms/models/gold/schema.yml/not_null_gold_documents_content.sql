select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select content
from "iceberg"."silver_gold"."gold_documents"
where content is null



      
    ) dbt_internal_test