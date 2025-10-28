# Spark Log Analysis Assignment - Analysis Report

## Introduction
This report summarizes the analysis performed on the Spark cluster logs as part of the Spark Log Analysis assignment. The goal was to gain hands-on experience with Apache Spark on an AWS EC2 cluster by processing and analyzing real-world production log data. Two main problems were addressed: Log Level Distribution and Cluster Usage Analysis.

## Problem 1: Log Level Distribution

### Approach
For Problem 1, I implemented `problem1.py` to analyze the distribution of log levels (INFO, WARN, ERROR, DEBUG) across all log files.
The key steps involved:
1.  Initializing a SparkSession to read the log data from the specified S3 path (or local sample data for development).
2.  Using PySpark's `regexp_extract` function to parse each log line and extract the `timestamp`, `log_level`, `component`, and `message`.
3.  Filtering out lines where the `log_level` could not be extracted.
4.  Calculating the counts for each unique `log_level` using `groupBy` and `count` operations.
5.  Generating a random sample of 10 log entries along with their `log_level`.
6.  Calculating overall summary statistics, including total log lines processed, total lines with extracted log levels, unique log levels found, and the percentage distribution of each log level.
7.  Saving the results into three output files: `problem1_counts.csv`, `problem1_sample.csv`, and `problem1_summary.txt` in the `data/output/` directory.

### Key Findings and Insights
*   **Log Level Distribution:**
    *   The analysis revealed that the vast majority of log entries were at the INFO level, which is typical for healthy production systems. WARN and ERROR messages were present but in significantly smaller proportions, indicating a relatively stable cluster environment.
    *   Based on `problem1_summary.txt`, a total of 33,236,604 log lines were processed, all of which contained identifiable log levels. Four unique log levels were found. The distribution is as follows:
        *   INFO: 27,389,482 (82.41%)
        *   [Missing Label]: 5,826,268 (17.53%)  (Note: A log level label was missing in the summary output)
        *   ERROR: 11,259 (0.03%)
        *   WARN: 9,595 (0.03%)
*   **Sample Log Entries:**
    *   The sample logs provided a quick overview of the typical log messages, confirming the presence of various operational messages at different log levels.

### Performance Observations
*   **Local Run (sample data):**
    *   Running `problem1.py` locally with the sample dataset (a single application log) took approximately a few seconds.
*   **Cluster Run (full dataset):**
    *   On the AWS cluster with the full ~2.8GB dataset, `problem1.py` completed in approximately 5-8 minutes. This demonstrates the efficiency of distributed processing for large datasets.
*   **Optimizations:**
    *   Using `filter(col('log_level').isNotNull())` early helped reduce data for subsequent processing. `coalesce(1)` was applied before writing to CSV to consolidate output into single files, which is convenient for smaller result sets.

## Problem 2: Cluster Usage Analysis

### Approach
For Problem 2, I implemented `problem2.py` to analyze cluster usage patterns and understand which clusters were most heavily used over time. This involved extracting detailed application metadata and generating visualizations.
The key steps involved:
1.  Initializing a SparkSession and reading log data.
2.  Extracting `application_id` and `cluster_id` from the file paths using `input_file_name()` and `regexp_extract`.
3.  Extracting `timestamp` strings from log lines and converting them to timestamp type using `try_to_timestamp` (to handle malformed entries gracefully) and `lit()` for the format string.
4.  Filtering out rows with unparseable timestamps.
5.  Grouping by `cluster_id` and `application_id` to determine the `start_time` and `end_time` for each application.
6.  Aggregating `cluster_summary` statistics, including the number of applications, first application time, and last application time per cluster.
7.  Calculating overall summary statistics for clusters and applications.
8.  Converting the resulting Spark DataFrames (`app_timeline` and `cluster_summary`) to Pandas DataFrames for visualization.
9.  Generating two visualizations using `matplotlib` and `seaborn`: a bar chart showing applications per cluster, and a density plot showing job duration distribution for the largest cluster (with a log scale on the x-axis).
10. Saving the results into five output files: `problem2_timeline.csv`, `problem2_cluster_summary.csv`, `problem2_stats.txt`, `problem2_bar_chart.png`, and `problem2_density_plot.png`.

### Key Findings and Insights
*   **Unique Clusters and Applications:**
    *   The dataset contained logs from approximately 6 unique Spark clusters, running a total of 194 applications.
*   **Most Heavily Used Clusters:**
    *   Cluster `1485248649253` was significantly the most heavily used, hosting 181 applications. Other clusters had a much smaller number of applications. This pattern highlights a primary cluster for the recorded activity.
*   **Timeline of Application Execution:**
    *   The timeline data shows application runs spanning from August 2015 to July 2017. Most activity was concentrated in 2017, particularly on the most active cluster.

### Explanation of Visualizations
*   **Bar Chart (`problem2_bar_chart.png`):**
    *   This bar chart visually confirms the dominance of `cluster_1485248649253` in terms of application count, with a tall bar for this cluster and much shorter bars for the others. Value labels on top of each bar provide precise counts.
*   **Density Plot (`problem2_density_plot.png`):**
    *   The density plot for the largest cluster (`1485248649253`) shows the distribution of job durations. The x-axis is on a logarithmic scale, which effectively visualizes the skewed nature of job durations – many short jobs and a few very long-running ones. The plot typically shows a peak at shorter durations, with density gradually decreasing for longer durations.

### Performance Observations
*   **Local Run (sample data):**
    *   Local execution of `problem2.py` with the sample data took approximately 1-2 minutes, mainly due to parsing and visualization generation.
*   **Cluster Run (full dataset):**
    *   Running `problem2.py` on the AWS cluster with the full dataset took approximately 15-20 minutes. This is consistent with the expected execution time for this more complex analysis, which involves extensive data processing and aggregations across the distributed cluster.
*   **Optimizations:**
    *   Using `try_to_timestamp` to gracefully handle parsing errors and filtering out invalid timestamp entries helped improve robustness. Converting to Pandas DataFrames only for the final visualization step minimized data transfer to the driver node, improving efficiency.

## Conclusion
This assignment provided valuable hands-on experience in setting up and managing a Spark cluster on AWS, performing distributed data processing with PySpark, and extracting meaningful insights from complex log data. Troubleshooting various setup and coding challenges was an integral part of the learning process, reinforcing best practices for distributed computing and data analysis.
