from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark=SparkSession.builder.appName("A2-Q2").getOrCreate()
companies=spark.read.parquet("data/tables/companies"); locations=spark.read.parquet("data/tables/locations"); job_postings=spark.read.parquet("data/tables/job_postings")
job_postings.createOrReplaceTempView("job_postings"); companies.createOrReplaceTempView("companies"); locations.createOrReplaceTempView("locations")
res=spark.sql("""
SELECT c.company_name,
       COUNT(*) AS remote_jobs
FROM job_postings j
JOIN companies c ON j.company_id=c.company_id
JOIN locations l ON j.location_id=l.location_id
WHERE j.remote_type_name=Remote AND l.state_name=California
GROUP BY c.company_name
ORDER BY remote_jobs DESC
LIMIT 5
""")
pdf=res.toPandas()
from pathlib import Path
Path("results").mkdir(exist_ok=True)
pdf.to_csv("results/q2_top5_remote_companies_ca.csv",index=False)
import matplotlib.pyplot as plt
import seaborn as sns
plt.figure()
sns.barplot(data=pdf,x="company_name",y="remote_jobs")
plt.xticks(rotation=30,ha="right")
plt.title("Top 5 Companies by Remote Job Postings in California")
plt.tight_layout()
plt.savefig("results/q2_top5_remote_companies_ca.png",dpi=200)
print("Saved: results/q2_top5_remote_companies_ca.csv and .png")
