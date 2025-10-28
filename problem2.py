from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, min, max, to_timestamp, input_file_name, count, try_to_timestamp, lit
import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Initialize Spark
# Configure the master according to your environment (local or cluster)
# spark = SparkSession.builder.appName("ClusterUsageAnalysis").getOrCreate()

def run_spark_analysis(input_path):
    spark = SparkSession.builder.appName("ClusterUsageAnalysis").getOrCreate()
    print(f"Running Spark analysis on {input_path}")
    logs_df = spark.read.text(input_path)

    # Extract application_id and cluster_id
    df = logs_df.withColumn('file_path', input_file_name())
    df = df.withColumn('application_id',
        regexp_extract('file_path', r'application_(\d+_\d+)', 0))
    df = df.withColumn('cluster_id',
        regexp_extract('file_path', r'application_(\d+)_\d+', 1))
    
    # Extract timestamps and convert to time type
    df = df.withColumn('timestamp_str',
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1))
    df = df.filter(col('timestamp_str').isNotNull())  # Filter out lines without timestamps
    df = df.withColumn('start_time',
        try_to_timestamp('timestamp_str', lit('yy/MM/dd HH:mm:ss')))

    # 过滤掉无法解析的时间戳（即为 NULL 的行）
    app_timeline = df.filter(col('start_time').isNotNull())

    # Compute start and end times for each application
    app_timeline = app_timeline.groupBy('cluster_id', 'application_id').agg(
        min('start_time').alias('start_time'),
        max('start_time').alias('end_time')
    ).orderBy('cluster_id', 'application_id')
    app_timeline = app_timeline.withColumn('app_number', regexp_extract('application_id', r'.*_(\d+)$', 1))
    app_timeline.show(truncate=False)

    # Save timeline data
    app_timeline.coalesce(1).write.mode('overwrite').option('header', 'true').csv("data/output/problem2_timeline.csv")
    print("Application timeline saved to data/output/problem2_timeline.csv")

    # Aggregate cluster statistics
    cluster_summary = app_timeline.groupBy('cluster_id').agg(
        count('application_id').alias('num_applications'),
        min('start_time').alias('cluster_first_app'),
        max('end_time').alias('cluster_last_app')
    ).orderBy(col('num_applications').desc())
    cluster_summary.show(truncate=False)

    # Save cluster summary data
    cluster_summary.coalesce(1).write.mode('overwrite').option('header', 'true').csv("data/output/problem2_cluster_summary.csv")
    print("Cluster summary saved to data/output/problem2_cluster_summary.csv")

    # Overall statistics
    total_unique_clusters = cluster_summary.count()
    total_applications = app_timeline.count()
    avg_applications_per_cluster = round(total_applications / total_unique_clusters, 2) if total_unique_clusters > 0 else 0

    stats_content = [
        f"Total unique clusters: {total_unique_clusters}",
        f"Total applications: {total_applications}",
        f"Average applications per cluster: {avg_applications_per_cluster}",
        "\nMost heavily used clusters:"
    ]

    for row in cluster_summary.collect():
        stats_content.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")

    with open("data/output/problem2_stats.txt", "w") as f:
        for line in stats_content:
            f.write(line + "\n")
    print("\nOverall summary statistics saved to data/output/problem2_stats.txt")

    # Convert Spark DataFrames to Pandas DataFrames for visualization
    app_timeline_pd = app_timeline.toPandas()
    cluster_summary_pd = cluster_summary.toPandas()

    spark.stop() # 移动 spark.stop() 到这里

    return app_timeline_pd, cluster_summary_pd

def generate_visualizations(app_timeline_pd, cluster_summary_pd):
    print("Generating visualizations...")

    # 1. Bar chart: number of applications per cluster
    plt.figure(figsize=(12, 7))
    sns.barplot(x='cluster_id', y='num_applications', data=cluster_summary_pd, palette='viridis')
    plt.title('Number of Applications per Cluster')
    plt.xlabel('Cluster ID')
    plt.ylabel('Number of Applications')
    plt.xticks(rotation=45, ha='right')
    for index, row in cluster_summary_pd.iterrows():
        plt.text(index, row['num_applications'] + 0.5, round(row['num_applications'], 0), color='black', ha="center")
    plt.tight_layout()
    plt.savefig('data/output/problem2_bar_chart.png')
    plt.close()
    print("Bar chart saved to data/output/problem2_bar_chart.png")

    # 2. Density plot: job duration distribution for the largest cluster
    largest_cluster_id = cluster_summary_pd.loc[cluster_summary_pd['num_applications'].idxmax()]['cluster_id']
    largest_cluster_data = app_timeline_pd[app_timeline_pd['cluster_id'] == largest_cluster_id].copy()

    # Compute job duration in seconds
    largest_cluster_data['duration_sec'] = (largest_cluster_data['end_time'] - largest_cluster_data['start_time']).dt.total_seconds()
    largest_cluster_data = largest_cluster_data[largest_cluster_data['duration_sec'] > 0]  # Filter out zero or negative durations

    if not largest_cluster_data.empty:
        plt.figure(figsize=(12, 7))
        # Use np.log1p (log(1+x)) for logarithmic scaling to avoid log(0)
        sns.histplot(largest_cluster_data['duration_sec'], kde=True, bins=50, log_scale=True, color='skyblue')
        plt.title(f'Job Duration Distribution for Cluster {largest_cluster_id} (n={len(largest_cluster_data)})')
        plt.xlabel('Job Duration (seconds, log scale)')
        plt.ylabel('Density')
        plt.tight_layout()
        plt.savefig('data/output/problem2_density_plot.png')
        plt.close()
        print("Density plot saved to data/output/problem2_density_plot.png")
    else:
        print(f"No valid duration data for largest cluster {largest_cluster_id} to generate density plot.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Cluster Usage Analysis")
    parser.add_argument("master", nargs='?', default="local[*]", help="Spark master URL (e.g., spark://MASTER_PRIVATE_IP:7077)")
    parser.add_argument("--net-id", required=True, help="Your Net ID for S3 bucket access")
    parser.add_argument("--skip-spark", action="store_true", help="Skip Spark processing and regenerate visualizations from existing CSVs")

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs("data/output", exist_ok=True)

    if args.skip_spark:
        print("Skipping Spark processing, loading data from existing CSVs.")
        try:
            app_timeline_pd = pd.read_csv("data/output/problem2_timeline.csv", parse_dates=['start_time', 'end_time'])
            cluster_summary_pd = pd.read_csv("data/output/problem2_cluster_summary.csv")
            generate_visualizations(app_timeline_pd, cluster_summary_pd)
        except FileNotFoundError:
            print("Error: CSV files not found. Please run Spark analysis first or ensure files exist.")
    else:
        # For cluster execution, use the provided master and S3 path
        if "local" not in args.master:
            spark_session = SparkSession.builder.appName("ClusterUsageAnalysis") \
                        .master(args.master) \
                        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
                        .getOrCreate()
            s3_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/*.log"
            app_timeline_pd, cluster_summary_pd = run_spark_analysis(s3_path)
            generate_visualizations(app_timeline_pd, cluster_summary_pd)
            spark_session.stop()
        else:  # Local run
            spark_session = SparkSession.builder.appName("ClusterUsageAnalysis").master("local[*]").getOrCreate()
            app_timeline_pd, cluster_summary_pd = run_spark_analysis("data/sample/application_*/*.log")
            generate_visualizations(app_timeline_pd, cluster_summary_pd)
            spark_session.stop()
