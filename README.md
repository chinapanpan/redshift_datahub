# redshift_datahub
This article introduces a lightweight solution for implementing field-level lineage in Redshift based on DataHub and SQLLineage. The solution is implemented through two Lambda functions: one for retrieving SQL statements from Redshift and uploading them to S3, and another for parsing SQL and generating lineage relationships. The main advantages of this solution are:
1. Adopts a serverless architecture, resulting in low operational costs;
2. Uses S3 as an intermediate layer for decoupling, providing good scalability;
3. Supports precise field-level lineage tracking;
4. Can be easily extended to other compute engines such as Hive/Spark/Flink, etc.

This solution provides data teams with a practical data lineage tracking tool that aids in data governance, impact analysis, troubleshooting, and performance optimization tasks.

<img src="https://s3.cn-north-1.amazonaws.com.cn/awschinablog/automatic-generation-of-field-level-bloodlines-based-on-datahub-and-redshift1.jpg" alt="架构图" />

# Install
## create a lambda layer zip file 
please refer to package_lambda_layer.sh


## create two lambda function using generate_sql.py and parse_sql.py

## create a daily schedule using Eventbrige


# More Details
please refer to the blog: https://aws.amazon.com/cn/blogs/china/automatic-generation-of-field-level-bloodlines-based-on-datahub-and-redshift/

