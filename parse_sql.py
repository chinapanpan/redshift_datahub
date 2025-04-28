import json
import boto3
import os
from sqllineage.runner import LineageRunner
from sqllineage.config import SQLLineageConfig

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)


# 从环境变量获取DataHub服务器地址
datahub_server = os.environ['DATAHUB_SERVER_URL']
engine_type = os.environ['ENGINE_TYPE']
#默认的数据库名字
default_database=os.environ['DEFAULT_DATABASE']
#默认的schema名字
default_schema=os.environ['DEFAULT_SCHEMA']

# 库名设置
def datasetUrn(tableName):
    # 检查tableName是否包含数据库名和schema名
    parts = tableName.split('.')
    if len(parts) == 1:  # 只有表名
        if default_schema:
            tableName = f"{default_schema}.{tableName}"
        if default_database:
            tableName = f"{default_database}.{tableName}"
    elif len(parts) == 2:  # 包含schema和表名，但没有数据库名
        if default_database:
            tableName = f"{default_database}.{tableName}"
    # 如果已经包含了数据库名和schema名，则不做处理
    return builder.make_dataset_urn(engine_type, tableName)  # platform = redshift

# 表、列级信息设置
def fieldUrn(tableName, fieldName):
    return builder.make_schema_field_urn(datasetUrn(tableName), fieldName)

def process_sql_from_s3(bucket, key):
    """
    从S3读取SQL文件并处理
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        sql = response['Body'].read().decode('utf-8')

        with SQLLineageConfig(DEFAULT_SCHEMA=default_schema):
            # 获取sql血缘
            result = LineageRunner(sql,dialect=engine_type)

            # 获取sql中的下游表名
            targetTableName = result.target_tables[0].__str__()
            print(result)
            print('===============')
            # 打印列级血缘结果
            result.print_column_lineage()
            print('===============')

            return result
    except Exception as e:
        print(f"读取S3文件错误: {str(e)}")
        raise e

def generateDatahubLineage(lineage):
    # 创建一个map数据结构来存储血缘关系的结果
    lineage_map = {}

    # 遍历列级血缘
    for columnTuples in lineage():
        # 逐个字段遍历
        for i in range(len(columnTuples) - 1):
            # 上游list
            upStreamStrList = []

            # 下游list 类似:datahub.public.dws_sales_summary.customer_name <- datahub.public.dim_customer.customer_name <- datahub.public.raw_sales_data.customer_id
            downStreamStrList = []
            if i + 1 < len(columnTuples):
                upStreamColumn = columnTuples[i]
                downStreamColumn = columnTuples[i + 1]

                upStreamFieldName = upStreamColumn.raw_name.__str__()
                upStreamTableName = upStreamColumn.__str__().replace('.' + upStreamFieldName, '').__str__()

                downStreamFieldName = downStreamColumn.raw_name.__str__()
                downStreamTableName = downStreamColumn.__str__().replace('.' + downStreamFieldName, '').__str__()

                print(f"{upStreamTableName}.{upStreamFieldName}-->{downStreamTableName}.{downStreamFieldName}")

                upStreamStrList.append(fieldUrn(upStreamTableName, upStreamFieldName))
                downStreamStrList.append(fieldUrn(downStreamTableName, downStreamFieldName))
                print(f"{upStreamTableName}.{upStreamFieldName}-->{downStreamTableName}.{downStreamFieldName}")
                fineGrainedLineage = FineGrainedLineage(upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                                                        upstreams=upStreamStrList,
                                                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                                                        downstreams=downStreamStrList)

                # 将血缘关系添加到map中
                if downStreamTableName not in lineage_map and downStreamTableName != '':
                    lineage_map[downStreamTableName] = {}
                if upStreamTableName not in lineage_map[downStreamTableName]:
                    lineage_map[downStreamTableName][upStreamTableName] = []

                lineage_map[downStreamTableName][upStreamTableName].append(fineGrainedLineage)

    # 打印血缘关系map
    print(lineage_map)

    return lineage_map

def lambda_handler(event, context):
    try:
        print(event)
        # 从事件中获取S3桶和文件信息
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        # 处理SQL文件
        result = process_sql_from_s3(bucket, key)

        # 获取列级血缘
        lineage = result.get_column_lineage
        lineage_map = generateDatahubLineage(lineage)

        for downStreamTableName, upStreamDict in lineage_map.items():
            upStreamsList = []
            total_fineGrainedLineageList = []
            for upStreamTableName, fineGrainedLineageList in upStreamDict.items():
                fineGrainedLineageList = lineage_map[downStreamTableName][upStreamTableName]
                total_fineGrainedLineageList.extend(fineGrainedLineageList)

                print(f"下游表名: {downStreamTableName}, 上游表名: {upStreamTableName}, 细粒度血缘: {fineGrainedLineageList}")
                upstream = Upstream(
                    dataset=datasetUrn(upStreamTableName), type=DatasetLineageType.TRANSFORMED
                )
                upStreamsList.append(upstream)

            fieldLineages = UpstreamLineage(
                upstreams=upStreamsList, fineGrainedLineages=total_fineGrainedLineageList
            )

            lineageMcp = MetadataChangeProposalWrapper(
                entityUrn=datasetUrn(downStreamTableName),  # 下游表名
                aspect=fieldLineages
            )

            blank_fieldLineages = UpstreamLineage(
                upstreams=[], fineGrainedLineages=total_fineGrainedLineageList
            )
            blank_lineageMcp = MetadataChangeProposalWrapper(
                entityUrn=datasetUrn(downStreamTableName),  # 下游表名
                aspect=blank_fieldLineages
            )

            # 调用datahub REST API
            emitter = DatahubRestEmitter(datahub_server)

            # clear the existing linage for the downstreamTable
            emitter.emit_mcp(blank_lineageMcp)

            # set the latest lineage
            emitter.emit_mcp(lineageMcp)

        return {
            'statusCode': 200,
            'body': json.dumps('SQL血缘分析完成')
        }

    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'处理失败: {str(e)}')
        }
