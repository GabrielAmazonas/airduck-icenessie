select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ready_at
from "iceberg"."silver_gold"."gold_documents"
where ready_at is null



      
    ) dbt_internal_test