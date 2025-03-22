import pymongo
from pymongo import MongoClient
from pymongo.operations import UpdateOne
from tqdm import tqdm
import logging
import time

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MongoDBMigration:
    """MongoDB数据迁移工具 (同步版本)"""

    def __init__(self, source_uri, target_uri,
                 db_name="danbooru", collection_name="pics",
                 batch_size=2000):
        """初始化迁移工具"""
        self.source_uri = source_uri
        self.target_uri = target_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.batch_size = batch_size

        self.source_client = None
        self.source_db = None
        self.source_collection = None

        self.target_client = None
        self.target_db = None
        self.target_collection = None

    def connect(self):
        """连接源和目标数据库"""
        try:
            # 连接源数据库
            self.source_client = MongoClient(
                self.source_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=30000,
                maxPoolSize=20,
                waitQueueTimeoutMS=10000,
                retryWrites=True
            )
            self.source_db = self.source_client[self.db_name]
            self.source_collection = self.source_db[self.collection_name]
            logger.info("源数据库连接成功")

            # 连接目标数据库
            self.target_client = MongoClient(
                self.target_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=30000,
                maxPoolSize=20,
                waitQueueTimeoutMS=10000,
                retryWrites=True
            )
            self.target_db = self.target_client[self.db_name]
            self.target_collection = self.target_db[self.collection_name]
            logger.info("目标数据库连接成功")

            # 检查连接
            self.source_db.command("ping")
            self.target_db.command("ping")

            # 创建索引
            self.target_collection.create_index([("_id", pymongo.ASCENDING)])
            logger.info("目标数据库索引创建成功")

            return True
        except Exception as e:
            logger.error(f"数据库连接错误: {e}")
            return False

    def close(self):
        """关闭数据库连接"""
        if self.source_client:
            self.source_client.close()
        if self.target_client:
            self.target_client.close()
        logger.info("数据库连接已关闭")

    def get_count(self):
        """获取源数据库中的记录数量"""
        try:
            count = self.source_collection.count_documents({})
            logger.info(f"源数据库中共有 {count} 条记录")
            return count
        except Exception as e:
            logger.error(f"获取记录数量时出错: {e}")
            return 0

    def migrate(self):
        """执行顺序数据迁移"""
        # 连接数据库
        if not self.connect():
            logger.error("数据库连接失败，无法进行迁移")
            return

        try:
            # 获取总记录数
            total_count = self.get_count()
            if total_count == 0:
                logger.warning("源数据库中没有数据，无需迁移")
                return

            # 创建进度条
            migrated_count = 0
            pbar = tqdm(total=total_count, desc="数据迁移进度")

            # 直接对整个集合排序
            cursor = self.source_collection.find().sort('_id',1)

            # 批量处理
            batch_docs = []
            batch_count = 0

            # 处理每个文档
            for doc in cursor:
                batch_docs.append(doc)
                batch_count += 1

                # 当累积到batch_size大小时，执行批量写入
                if batch_count >= self.batch_size:
                    self._process_batch(batch_docs)
                    migrated_count += batch_count
                    pbar.update(batch_count)
                    logger.info(f"已迁移 {migrated_count}/{total_count} 条记录")

                    # 清空批次
                    batch_docs = []
                    batch_count = 0

                    # 添加短暂延迟
                    time.sleep(0.1)

            # 处理最后一批剩余文档
            if batch_docs:
                self._process_batch(batch_docs)
                migrated_count += batch_count
                pbar.update(batch_count)
                logger.info(f"已迁移 {migrated_count}/{total_count} 条记录")

            # 关闭进度条
            pbar.close()

            logger.info(f"数据迁移完成，共迁移 {migrated_count} 条记录")

            # 检查是否所有数据都已迁移
            target_count = self.target_collection.count_documents({})
            logger.info(f"目标数据库中共有 {target_count} 条记录")
            if target_count >= total_count:
                logger.info("所有数据已成功迁移")
            else:
                logger.warning(f"目标数据库中记录数 ({target_count}) 小于源数据库 ({total_count})，可能有部分数据未迁移成功")

        except Exception as e:
            logger.error(f"迁移过程中出错: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.close()

    def _process_batch(self, documents):
        """处理一批文档"""
        try:
            if not documents:
                return 0

            # 准备批量操作
            bulk_operations = []
            for doc in documents:
                bulk_operations.append(
                    UpdateOne(
                        {"_id": doc["_id"]},
                        {"$set": doc},
                        upsert=True
                    )
                )

            # 执行批量写入
            result = self.target_collection.bulk_write(bulk_operations, ordered=True)  # 使用ordered=True确保按顺序处理
            return result.modified_count + result.upserted_count

        except Exception as e:
            logger.error(f"处理批次数据时出错: {e}")
            return 0

def main():
    # 源数据库和目标数据库的连接信息
    source_uri = "mongodb://shiertier:20000418Nuo.@localhost:27000/"
    target_uri = "mongodb://8.153.97.53:27815/"

    migration = MongoDBMigration(
        source_uri=source_uri,
        target_uri=target_uri,
        db_name="danbooru",
        collection_name="pics",
        batch_size=2000  # 每2000条记录批量处理一次
    )

    migration.migrate()

if __name__ == "__main__":
    main()