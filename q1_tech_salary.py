from pyspark.sql import SparkSession, functions as F
spark=SparkSession.builder.appName("A2-Q1").getOrCreate()
industries=spark.read.parquet("data/tables/industries"); companies=spark.read.parquet("data/tables/companies"); locations=spark.read.parquet("data/tables/locations"); job_postings=spark.read.parquet("data/tables/job_postings")
industries.createOrReplaceTempView("industries"); job_postings.createOrReplaceTempView("job_postings")
df=spark.sql("""
SELECT i.naics_2022_6_name AS industry_name,
       i.lot_specialized_occupation_name AS specialized_occupation,
       CASE 
         WHEN j.salary IS NOT NULL AND j.salary>0 THEN DOUBLE(j.salary)
         WHEN j.salary_from IS NOT NULL AND j.salary_to IS NOT NULL AND j.salary_from>0 AND j.salary_to>0 THEN (DOUBLE(j.salary_from)+DOUBLE(j.salary_to))/2.0
       END AS salary_val
FROM job_postings j
JOIN industries i ON j.industry_id=i.industry_id
WHERE i.naics_2022_6 = "518210"
  AND (
       (j.salary IS NOT NULL AND j.salary>0) OR
       (j.salary_from IS NOT NULL AND j.salary_to IS NOT NULL AND j.salary_from>0 AND j.salary_to>0)
  )
""")
agg=df.groupBy("industry_name","specialized_occupation").agg(F.expr("percentile_approx(salary_val,0.5,100) as median_salary"))
res=agg.orderBy(F.desc("median_salary"))
res.write.mode("overwrite").option("header",True).csv("results/q1_median_salary_tech_csv")
import pandas as pd
pdf=res.toPandas()
from pathlib import Path
Path("results").mkdir(exist_ok=True)
pdf.to_csv("results/q1_median_salary_tech.csv",index=False)
import plotly.express as px
fig=px.bar(pdf,x="specialized_occupation",y="median_salary",color="industry_name",title="Median Salary by Specialized Occupation – Tech (NAICS 518210)")
fig.update_layout(xaxis_title="Specialized Occupation",yaxis_title="Median Salary")
fig.write_html("results/q1_median_salary_tech.html")
print("Saved: results/q1_median_salary_tech.csv and .html")
