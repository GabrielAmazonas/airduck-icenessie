select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select content
from "iceberg"."bronze"."raw_documents"
where content is null



      
    ) dbt_internal_test