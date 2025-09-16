from pyspark.sql import SparkSession, functions as F
spark=SparkSession.builder.appName("A2-Q4").getOrCreate()
locations=spark.read.parquet("data/tables/locations"); job_postings=spark.read.parquet("data/tables/job_postings")
locations.createOrReplaceTempView("locations"); job_postings.createOrReplaceTempView("job_postings")
msa_map={"14460":"Boston","47900":"Washington DC","35620":"New York","41860":"San Francisco","42660":"Seattle","31080":"Los Angeles","19100":"Dallas","26420":"Houston","12420":"Austin","34980":"Nashville","28140":"Kansas City","19740":"Denver"}
msa_list=list(msa_map.keys())
q=spark.sql("""
SELECT l.msa,
       CASE WHEN j.salary IS NOT NULL AND j.salary>0 THEN DOUBLE(j.salary)
            WHEN j.salary_from IS NOT NULL AND j.salary_to IS NOT NULL AND j.salary_from>0 AND j.salary_to>0 THEN (DOUBLE(j.salary_from)+DOUBLE(j.salary_to))/2.0
       END AS salary_val
FROM job_postings j
JOIN locations l ON j.location_id=l.location_id
WHERE ((j.salary IS NOT NULL AND j.salary>0) OR (j.salary_from IS NOT NULL AND j.salary_to IS NOT NULL AND j.salary_from>0 AND j.salary_to>0))
  AND l.msa IS NOT NULL
""")
q=q.where(F.col("msa").isin(msa_list))
agg=q.groupBy("msa").agg(F.round(F.avg("salary_val"),2).alias("avg_salary"),F.count("*").alias("job_count"))
from pyspark.sql import types as T
u=F.udf(lambda x: msa_map.get(x,x), T.StringType())
res=agg.withColumn("metro", u(F.col("msa"))).select("metro","avg_salary","job_count").orderBy(F.desc("avg_salary"))
from pathlib import Path
Path("results").mkdir(exist_ok=True)
pdf=res.toPandas(); pdf.to_csv("results/q4_salary_by_metro.csv", index=False)
import matplotlib.pyplot as plt
plt.figure(); plt.bar(pdf["metro"], pdf["avg_salary"]); plt.xticks(rotation=30, ha="right"); plt.ylabel("Average Salary"); plt.title("Average Salary Across Major US Metros"); plt.tight_layout(); plt.savefig("results/q4_salary_by_metro.png", dpi=200)
print("Saved: results/q4_salary_by_metro.csv and .png")
