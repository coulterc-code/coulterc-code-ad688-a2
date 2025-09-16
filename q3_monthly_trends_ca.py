from pyspark.sql import SparkSession, functions as F
spark=SparkSession.builder.appName("A2-Q3").getOrCreate()
locations=spark.read.parquet("data/tables/locations"); job_postings=spark.read.parquet("data/tables/job_postings")
locations.createOrReplaceTempView("locations"); job_postings.createOrReplaceTempView("job_postings")
q=spark.sql("""
SELECT year(to_date(j.posted, "yyyy-MM-dd")) AS yr,
       month(to_date(j.posted, "yyyy-MM-dd")) AS mo,
       count(*) AS job_count
FROM job_postings j
JOIN locations l ON j.location_id=l.location_id
WHERE l.state_name=California AND to_date(j.posted, "yyyy-MM-dd") IS NOT NULL
GROUP BY yr, mo
ORDER BY yr, mo
""")
from pathlib import Path
Path("results").mkdir(exist_ok=True)
pdf=q.toPandas()
pdf.to_csv("results/q3_monthly_trends_ca.csv", index=False)
import matplotlib.pyplot as plt
plt.figure()
for y,grp in pdf.groupby("yr"):
    plt.plot(grp["mo"], grp["job_count"], marker="o", label=str(y))
plt.xlabel("Month"); plt.ylabel("Job Postings"); plt.title("Monthly Job Posting Trends – California"); plt.legend()
plt.tight_layout(); plt.savefig("results/q3_monthly_trends_ca.png", dpi=200)
print("Saved: results/q3_monthly_trends_ca.csv and .png")
