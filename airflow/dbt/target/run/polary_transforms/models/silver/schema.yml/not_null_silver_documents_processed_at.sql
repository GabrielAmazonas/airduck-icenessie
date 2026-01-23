select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select processed_at
from "iceberg"."silver_silver"."silver_documents"
where processed_at is null



      
    ) dbt_internal_test