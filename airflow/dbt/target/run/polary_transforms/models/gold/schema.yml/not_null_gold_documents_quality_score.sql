select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select quality_score
from "iceberg"."silver_gold"."gold_documents"
where quality_score is null



      
    ) dbt_internal_test