import psycopg2
import boto3
import logging
from sqllineage.runner import LineageRunner
from typing import Optional, List, Dict, Any
import os

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 从环境变量获取Redshift连接配置
REDSHIFT_CONFIG = {
    "host": os.environ['REDSHIFT_HOST'],
    "port": int(os.environ['REDSHIFT_PORT']),
    "database": os.environ['REDSHIFT_DATABASE'],
    "user": os.environ['REDSHIFT_USER'],
    "password": os.environ['REDSHIFT_PASSWORD']
}

project_name = os.environ['PROJECT_NAME']
bucket_name = os.environ['S3_BUCKET']

def get_table_ddl(conn, table_name: str) -> Optional[str]:
    """获取Redshift表的DDL"""
    try:
        # 创建新的连接
        with psycopg2.connect(**REDSHIFT_CONFIG) as new_conn:
            with new_conn.cursor() as cur:
                db_name = REDSHIFT_CONFIG['database']
                if '.' in table_name and len(table_name.split('.')) == 2 :
                    query = f'SHOW TABLE {table_name};'
                else:
                    query = f'SHOW TABLE {db_name}.{table_name};'
                # 将默认schema '<default>'替换为'public'
                query = query.replace('<default>', 'public')

                logger.info(f"执行查询: {query}")

                cur.execute(query)
                ddl = cur.fetchone()[0]  # 修复了变量赋值的空格问题
                # 替换DISTSTYLE AUTO为分号
                ddl = ddl.replace("DISTSTYLE AUTO;", ";")
                return ddl
    except Exception as e:
        logger.error(f"获取表 {table_name} DDL失败: {str(e)}")
        return None

def get_today_insert_queries(conn) -> List[str]:
    """获取今天执行的所有INSERT语句"""
    try:
        with conn.cursor() as cur:
            query = """
            SELECT query_text 
            FROM SYS_QUERY_HISTORY 
            WHERE query_type = 'INSERT' 
            AND start_time >= TRUNC(SYSDATE)
            AND status = 'success'  -- 只获取执行成功的语句
            ORDER BY end_time DESC;
            """
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"获取INSERT语句失败: {str(e)}")
        return []

def save_queries_to_s3(insert_queries: List[str]) -> None:
    """将INSERT语句保存到本地文件并上传到S3"""
    try:
        processed_tables = set()  # 用于记录已处理的目标表
        ddl_tables = set()  # 用于记录已获取过DDL的表
        # 为DDL查询创建独立连接
        with psycopg2.connect(**REDSHIFT_CONFIG) as ddl_conn:
            # Lambda中使用/tmp目录进行临时文件存储
            local_path = f'/tmp/sql_queries_{project_name}.sql'

            with open(local_path, 'w', encoding='utf-8') as f:
                for query in insert_queries:
                    try:
                        # 将\n转换为实际换行符
                        query = query.replace('\\n', '\n')
                        query = f"{query};"  # 加上分号结尾

                        # 分析SQL依赖的表
                        result = LineageRunner(query, dialect="redshift")
                        tables = result.source_tables
                        logger.info(f"SQL涉及的表: {tables}")

                        # 获取目标表
                        target_tables = result.target_tables
                        logger.info(f"目标表: {target_tables}")

                        should_skip = False

                        table_str = target_tables[0].__str__()
                        if table_str in processed_tables:
                            logger.info(f"目标表 {table_str} 已处理过,跳过该SQL")
                            should_skip = True
                        processed_tables.add(table_str)

                        if should_skip:
                            continue

                        # 获取每张表的DDL
                        for table in tables:
                            # 将table对象转换为字符串
                            table_name = table.__str__()
                            # 检查表是否已经获取过DDL
                            if table_name in ddl_tables:
                                logger.info(f"表 {table_name} 的DDL已获取过,跳过")
                                continue
                            ddl_tables.add(table_name)
                            ddl = get_table_ddl(ddl_conn, table_name)

                            if ddl:  # 修复了缩进问题
                                f.write(f"\n-- DDL for table {table_name}:\n{ddl}\n")
                            else:
                                f.write(f"\n-- Failed to get DDL for table {table_name}\n")
                            f.write("-" * 80 + "\n")

                        f.write(f"{query}\n{'-' * 80}\n")
                    except Exception as e:
                        logger.error(f"处理SQL查询时出错: {str(e)}")
                        logger.error(f"出错的SQL: {query[:200]}...")
                        # 继续处理下一个查询
                        continue

            # 上传到S3
            s3_key = f'sql-scripts/sql_queries_{project_name}.sql'
            s3_client = boto3.client('s3')
            s3_client.upload_file(local_path, bucket_name, s3_key)

            logger.info(f"查询已保存到本地文件: {local_path}")
            logger.info(f"文件已上传到S3: s3://{bucket_name}/{s3_key}")

    except Exception as e:
        logger.error(f"保存查询失败: {str(e)}")
        raise

def lambda_handler(event, context):
    """Lambda处理函数"""
    try:
        # 查询INSERT语句使用独立连接
        with psycopg2.connect(**REDSHIFT_CONFIG) as conn:
            insert_queries = get_today_insert_queries(conn)
            if not insert_queries:
                logger.warning("未找到今天的INSERT语句")
                return {
                    'statusCode': 200,
                    'body': '未找到今天的INSERT语句'
                }

        # 保存查询时使用新的连接
        save_queries_to_s3(insert_queries)

        return {
            'statusCode': 200,
            'body': 'SQL查询已成功保存到S3'
        }

    except Exception as e:
        logger.error(f"处理SQL失败: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'处理失败: {str(e)}'
        }
