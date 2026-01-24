select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select chunk_index
from "iceberg"."silver_gold"."gold_documents"
where chunk_index is null



      
    ) dbt_internal_test