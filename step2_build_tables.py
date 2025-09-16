from pyspark.sql import SparkSession, functions as F
spark=SparkSession.builder.appName("A2-BuildTables").getOrCreate()
df=spark.read.option("header","true").option("inferSchema","true").option("multiLine","true").option("escape","\"").csv("data/lightcast_job_postings.csv")
ind_cols=["naics_2022_6","naics_2022_6_name","soc_5","soc_5_name","lot_specialized_occupation_name","lot_occupation_group"]
comp_cols=["company_id","company","company_name","company_raw","company_is_staffing"]
loc_cols=["location","city_name","state_name","county_name","msa","msa_name"]
industries=df.select([F.col(c) for c in ind_cols]).dropDuplicates()
industries=industries.withColumn("industry_id",F.sha2(F.concat_ws("||",*ind_cols),256))
companies=df.select("company","company_name","company_raw","company_is_staffing").dropDuplicates()
companies=companies.withColumn("company_id",F.sha2(F.concat_ws("||","company_name","company_raw"),256))
locations=df.select([F.col(c) for c in loc_cols]).dropDuplicates()
locations=locations.withColumn("location_id",F.sha2(F.concat_ws("||",*loc_cols),256))
df_ids=df.withColumn("industry_id",F.sha2(F.concat_ws("||",*ind_cols),256))\
    .withColumn("company_id",F.sha2(F.concat_ws("||","company_name","company_raw"),256))\
    .withColumn("location_id",F.sha2(F.concat_ws("||",*loc_cols),256))\
    .withColumn("id",F.sha2(F.concat_ws("||",F.coalesce(F.col("title_clean"),F.lit("")),F.coalesce(F.col("company_name"),F.lit("")),F.coalesce(F.col("posted"),F.lit("")),F.coalesce(F.col("location"),F.lit(""))),256))
job_cols=["id","title_clean","company_id","industry_id","employment_type_name","remote_type_name","body","min_years_experience","max_years_experience","salary","salary_from","salary_to","location_id","posted","expired","duration"]
job_postings=df_ids.select([F.col(c) for c in job_cols])
industries.createOrReplaceTempView("industries")
companies.createOrReplaceTempView("companies")
locations.createOrReplaceTempView("locations")
job_postings.createOrReplaceTempView("job_postings")
F.coalesce(F.lit(1),F.lit(1))
spark.sql("select count(*) as n from industries").show()
spark.sql("select count(*) as n from companies").show()
spark.sql("select count(*) as n from locations").show()
spark.sql("select count(*) as n from job_postings").show()
industries.write.mode("overwrite").parquet("data/tables/industries")
companies.write.mode("overwrite").parquet("data/tables/companies")
locations.write.mode("overwrite").parquet("data/tables/locations")
job_postings.write.mode("overwrite").parquet("data/tables/job_postings")
print("Tables materialized under data/tables/*")
