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
batch_size = int(os.environ.get('BATCH_SIZE', 1000))
processed_tables = set()  # 用于记录已处理的目标表

def get_table_ddl(conn, table_name: str) -> Optional[str]:
    """获取Redshift表的DDL"""
    try:
        with conn.cursor() as cur:
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

# 针对部分sql被截断的场景，获取完整的SQL语句
def get_full_sql(conn, query_id: int) -> Optional[str]:
    try:
        with conn.cursor() as cur:
            query = """
            SELECT LISTAGG(text, '') WITHIN GROUP (ORDER BY sequence) AS full_query
            FROM SYS_QUERY_TEXT
            WHERE query_id = %s
            """
            cur.execute(query, (query_id,))
            result = cur.fetchone()
            
            if result:
                return result[0]  # 返回完整的SQL文本
            return None
    except Exception as e:
        logger.error(f"获取完整SQL失败: {str(e)}")
        return None
def count_today_insert_queries(conn) -> int:
    """获取今天执行的所有INSERT语句，返回包含多个字段的字典列表"""
    try:
        with conn.cursor() as cur:
            query = """
            SELECT count(1)
            FROM SYS_QUERY_HISTORY 
            WHERE query_type = 'INSERT' 
            AND start_time >= TRUNC(SYSDATE)
            AND status = 'success'  -- 只获取执行成功的语句
            ;
            """
            cur.execute(query)
            results = cur.fetchall()
            
            # 直接返回查询结果的第一行第一列（count值）
            if results and len(results) > 0:
                return results[0][0]
            return 0
    except Exception as e:
        logger.error(f"获取INSERT语句失败: {str(e)}")
        return None
def get_today_insert_queries(conn, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """获取今天执行的所有INSERT语句，返回包含多个字段的字典列表"""
    try:
        with conn.cursor() as cur:
            query = """
            SELECT query_id, transaction_id, query_text 
            FROM SYS_QUERY_HISTORY 
            WHERE query_type = 'INSERT' 
            AND start_time >= TRUNC(SYSDATE)
            AND status = 'success'  -- 只获取执行成功的语句
            ORDER BY end_time DESC
            LIMIT %s OFFSET %s;
            """
            cur.execute(query, (limit, offset))
            results = cur.fetchall()
            
            # 将结果转换为字典列表
            return [
                {
                    "query_id": row[0],
                    "transaction_id": row[1],
                    "query_text": row[2]
                }
                for row in results
            ]
    except Exception as e:
        logger.error(f"获取INSERT语句失败: {str(e)}")
        return []


def save_queries_to_s3(insert_queries: List[Dict[str, Any]], batch_num: int = 1) -> None:
    """将INSERT语句保存到本地文件并上传到S3"""
    try:
        global processed_tables
        ddl_tables = set()  # 用于记录已获取过DDL的表
        # 为DDL查询创建独立连接
        with psycopg2.connect(**REDSHIFT_CONFIG) as ddl_conn:
            # Lambda中使用/tmp目录进行临时文件存储
            local_path = f'/tmp/sql_queries_{project_name}_batch{batch_num}.sql'

            with open(local_path, 'w', encoding='utf-8') as f:
                for query_info in insert_queries:
                    try:
                        query = query_info["query_text"]
                        # 检查SQL长度是否超过3990字符(超过 4000 会被截断)，如果超过则获取完整SQL
                        if len(query) > 3990:
                            logger.info(f"SQL长度超过3990字符，尝试获取完整SQL，query_id: {query_info['query_id']}")
                            full_sql = get_full_sql(ddl_conn, query_info['query_id'])
                            if full_sql:
                                query = full_sql
                                logger.info("成功获取完整SQL")
                            else:
                                logger.warning(f"无法获取完整SQL，使用原始截断SQL，query_id: {query_info['query_id']}")
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

                        if target_tables:
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
            from datetime import datetime
            current_date = datetime.now().strftime('%Y-%m-%d')
            s3_key = f'sql-scripts/sql_queries_{project_name}_{current_date}_batch{batch_num}.sql'
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
        # 每次执行时重置全局变量
        global processed_tables
        processed_tables = set()
        
        # 查询INSERT语句使用独立连接
        with psycopg2.connect(**REDSHIFT_CONFIG) as conn:
            # 先获取今日插入查询的总数
            total_queries = count_today_insert_queries(conn)
            if total_queries == 0:
                logger.warning("未找到今天的INSERT语句")
                return {
                    'statusCode': 200,
                    'body': '未找到今天的INSERT语句'
                }
            
            logger.info(f"今日共有 {total_queries} 条INSERT语句")
            
            # 按照100条一个批次处理
            global batch_size
            batch_count = (total_queries + batch_size - 1) // batch_size  # 向上取整计算批次数
            
            for batch_num in range(1, batch_count + 1):
                offset = (batch_num - 1) * batch_size
                logger.info(f"处理第 {batch_num}/{batch_count} 批次，偏移量: {offset}")
                
                # 获取当前批次的查询
                batch_queries = get_today_insert_queries(conn, batch_size, offset)
                if not batch_queries:
                    logger.warning(f"第 {batch_num} 批次未获取到查询")
                    continue
                
                # 保存当前批次的查询
                save_queries_to_s3(batch_queries, batch_num)
                logger.info(f"第 {batch_num} 批次处理完成")

        return {
            'statusCode': 200,
            'body': f'SQL查询已成功保存到S3，共处理 {batch_count} 个批次'
        }

    except Exception as e:
        logger.error(f"生成SQL失败: {str(e)}")
        raise
