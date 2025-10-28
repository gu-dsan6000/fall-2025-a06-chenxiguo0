from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, lit, rand, round
from pyspark.sql.window import Window
import argparse
import os

# Initialize Spark
# Configure the master according to execution environment
# For local testing: spark = SparkSession.builder.appName("LogLevelAnalysis").master("local[*]").getOrCreate()
# For cluster execution: pass the master URL through a command-line argument
# spark = SparkSession.builder.appName("LogLevelAnalysis").getOrCreate()

def run_problem1_analysis(input_path):
    spark = SparkSession.builder.appName("LogLevelAnalysis").getOrCreate()

    # Load log files
    logs_df = spark.read.text(input_path)

    # Parse log entries
    parsed_logs = logs_df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
        col('value').alias('message')
    ).filter(col('log_level').isNotNull())  # Filter out rows that cannot extract a valid log level

    # 1. Count log levels
    log_level_counts = parsed_logs.groupBy('log_level').count().orderBy('log_level')
    log_level_counts.coalesce(1).write.mode('overwrite').option('header', 'true').csv("data/output/problem1_counts.csv")
    print("Log level counts saved to data/output/problem1_counts.csv")

    # 2. Randomly sample 10 log entries
    sample_logs = parsed_logs.orderBy(rand()).limit(10).select('message', 'log_level')
    sample_logs.coalesce(1).write.mode('overwrite').option('header', 'true').csv("data/output/problem1_sample.csv")
    print("Sample log entries saved to data/output/problem1_sample.csv")

    # 3. Generate summary statistics
    total_log_lines = logs_df.count()
    total_lines_with_log_levels = parsed_logs.count()
    unique_log_levels = parsed_logs.select('log_level').distinct().count()

    summary_content = [
        f"Total log lines processed: {total_log_lines}",
        f"Total lines with log levels: {total_lines_with_log_levels}",
        f"Unique log levels found: {unique_log_levels}",
        "\nLog level distribution:"
    ]

    # Calculate percentage distribution for each log level
    log_level_distribution = log_level_counts.withColumn(
        "percentage", round((col("count") / total_lines_with_log_levels) * 100, 2)
    ).orderBy(col("count").desc())

    for row in log_level_distribution.collect():
        summary_content.append(f"  {row['log_level'].ljust(6)}: {str(row['count']).rjust(9)} ({str(row['percentage']).rjust(5)}%)")

    # Write summary to file
    with open("data/output/problem1_summary.txt", "w") as f:
        for line in summary_content:
            f.write(line + "\n")

    print("\nSummary statistics saved to data/output/problem1_summary.txt")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Log Level Analysis")
    parser.add_argument("master", nargs='?', default="local[*]", help="Spark master URL (e.g., spark://MASTER_PRIVATE_IP:7077)")
    parser.add_argument("--net-id", required=True, help="Your Net ID for S3 bucket access")

    args = parser.parse_args()

    os.makedirs("data/output", exist_ok=True)

    if "local" not in args.master:
        spark = SparkSession.builder.appName("LogLevelAnalysis") \
                    .master(args.master) \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
                    .getOrCreate()
        input_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/*.log"
        run_problem1_analysis(input_path)
    else:
        spark = SparkSession.builder.appName("LogLevelAnalysis").master("local[*]").getOrCreate()
        run_problem1_analysis("data/sample/application_*/*.log")
