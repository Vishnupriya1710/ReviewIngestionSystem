## Scaling discussion

1. **Distributed Data Processing:**
   Use Apache Spark's cluster mode to run the spark application on a cluster of machines and leverage the parallel processing power of multiple nodes.

2. **Data Partitioning:**
   Partition the large datasets into smaller chunks and process in parallel across different nodes. Spark automatically handles partitioning when working with large datasets.

3. **Cluster Management:**
   We can deploy the Spark application on a cluster manager like YARN or Kubernetes to manage resources and distribute workloads across the cluster efficiently.

4. **Load Balancing:**
   Implement load balancing using cluster manager, to ensure that no single node is overwhelmed with too much data processing. 

5. **Scalable Storage:**
   Use scalable storage solutions like Amazon S3 or HDFS to store input and output data which can handle large volumes of data and support parallel read/write operations.

6. **Error Handling:**
   Implement robust error handling and retry mechanisms to manage node failures and ensure data processing continuity. Can leverage Spark Framework to achieve this.

7. **Resource Monitoring:**
   Monitor cluster performance and resource utilization Spark's built-in monitoring tools like Spark Web UI, Spark History Server etc. This helps in identifying bottlenecks and scaling resources as needed.

## Future Implementation Scope

1.  **Natural Language Processing:**
    Implement NLP techniques like lemmatization, which reduces words to their base or root form, called a "lemma". This will help in reducing the size of `inappropriate_words.txt` by having a single word `frack` instead of `frack` and `fracking`. Lemmatization would take care of both these words and consider them both as `frack` and detect them as inappropriate.

2.  **Include the Schema in the Code Base:**
    We can include the schema based Dataframe creation by loading the `schemas/review.json` and the final output `schemas/aggregation.json`. Since the mandatory CLI parameters did not have these paths, I have defined a method `load_schema_from_json` but haven't invoked it. It can be considered in the future scope and the dataframes can be created likewise. 

3.  **Load aggregated data to database:**
    The aggregated metrics can be loaded into Hive, Impala or any other database for further analysis.

4.  **Use different format for output files:**
    Parquet files can be used as an alternative for large-scale data processing due to their high compression, reducing storage costs and enhancing read/write efficiency. These are also widely supported by various data processing tools like Apache Spark, Hive, and Impala, facilitating easier integration and interoperability in big data environments.
