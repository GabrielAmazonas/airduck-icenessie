select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_chunks
from "iceberg"."silver_gold"."gold_documents"
where total_chunks is null



      
    ) dbt_internal_test