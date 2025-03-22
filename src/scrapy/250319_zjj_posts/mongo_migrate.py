import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.operations import UpdateOne
from tqdm import tqdm
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MongoDBMigration:
    """MongoDB数据迁移工具"""

    def __init__(self, source_uri, target_uri,
                 db_name="danbooru", collection_name="pics",
                 batch_size=100):
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

    async def connect(self):
        """连接源和目标数据库"""
        try:
            # 连接源数据库
            self.source_client = AsyncIOMotorClient(
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
            self.target_client = AsyncIOMotorClient(
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
            await self.source_db.command("ping")
            await self.target_db.command("ping")

            # 创建索引
            await self.target_collection.create_index([("_id", 1)])
            logger.info("目标数据库索引创建成功")

            return True
        except Exception as e:
            logger.error(f"数据库连接错误: {e}")
            return False

    async def close(self):
        """关闭数据库连接"""
        if self.source_client:
            self.source_client.close()
        if self.target_client:
            self.target_client.close()
        logger.info("数据库连接已关闭")

    async def get_count(self):
        """获取源数据库中的记录数量"""
        try:
            count = await self.source_collection.count_documents({})
            logger.info(f"源数据库中共有 {count} 条记录")
            return count
        except Exception as e:
            logger.error(f"获取记录数量时出错: {e}")
            return 0

    async def migrate_batch(self, skip, limit):
        """迁移一批数据"""
        try:
            # 从源数据库读取一批数据，并按_id排序
            cursor = self.source_collection.find({}).sort([("_id", 1)]).skip(skip).limit(limit)
            documents = []
            async for doc in cursor:
                documents.append(doc)

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
            result = await self.target_collection.bulk_write(bulk_operations, ordered=True)  # 使用ordered=True确保按顺序处理
            return result.modified_count + result.upserted_count

        except Exception as e:
            logger.error(f"迁移批次数据时出错: {e}")
            return 0

    async def migrate(self):
        """执行顺序数据迁移"""
        # 连接数据库
        if not await self.connect():
            logger.error("数据库连接失败，无法进行迁移")
            return

        try:
            # 获取总记录数
            total_count = await self.get_count()
            if total_count == 0:
                logger.warning("源数据库中没有数据，无需迁移")
                return

            # 计算批次数量
            batches = (total_count + self.batch_size - 1) // self.batch_size
            logger.info(f"将分 {batches} 批进行顺序迁移")

            # 创建进度条
            migrated_count = 0
            pbar = tqdm(total=total_count, desc="数据迁移进度")

            # 按顺序处理批次
            for i in range(batches):
                skip = i * self.batch_size
                batch_result = await self.migrate_batch(skip, self.batch_size)
                migrated_count += batch_result
                pbar.update(min(self.batch_size, total_count - skip))
                logger.info(f"完成批次 {i+1}/{batches}，已迁移 {migrated_count} 条记录")

            # 关闭进度条
            pbar.close()

            logger.info(f"数据迁移完成，共迁移 {migrated_count} 条记录")

            # 检查是否所有数据都已迁移
            target_count = await self.target_collection.count_documents({})
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
            await self.close()

async def main_async():
    # 源数据库和目标数据库的连接信息
    source_uri = "mongodb://shiertier:20000418Nuo.@localhost:27000/"
    target_uri = "mongodb://8.153.97.53:27815/"

    migration = MongoDBMigration(
        source_uri=source_uri,
        target_uri=target_uri,
        db_name="danbooru",
        collection_name="pics",
        batch_size=500  # 每批处理500条记录，按顺序迁移
    )

    await migration.migrate()

def is_running_in_ipython():
    """检查是否在IPython/Jupyter环境中运行"""
    try:
        return get_ipython().__class__.__name__ != 'TerminalInteractiveShell'
    except NameError:
        return False

def main():
    """自适应运行主异步函数"""
    try:
        # 检查是否在IPython/Jupyter环境中运行
        if is_running_in_ipython():
            # 在Jupyter中获取当前事件循环
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果事件循环已在运行，使用nest_asyncio允许嵌套事件循环
                try:
                    import nest_asyncio
                    nest_asyncio.apply()
                    logger.info("应用nest_asyncio以支持嵌套事件循环")
                except ImportError:
                    logger.warning("推荐安装nest_asyncio以支持在Jupyter中运行: pip install nest_asyncio")

            # 使用当前事件循环运行
            asyncio.get_event_loop().run_until_complete(main_async())
        else:
            # 在普通Python环境中，使用asyncio.run()
            asyncio.run(main_async())
    except RuntimeError as e:
        if "already running" in str(e) or "cannot be called from a running event loop" in str(e):
            # 处理已有事件循环的情况
            logger.warning("检测到运行中的事件循环，尝试使用现有循环")
            try:
                # 获取当前事件循环
                loop = asyncio.get_event_loop()
                if not loop.is_closed():
                    # 使用当前事件循环运行
                    loop.run_until_complete(main_async())
                else:
                    # 如果循环已关闭，创建新循环
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    new_loop.run_until_complete(main_async())
            except Exception as inner_e:
                logger.error(f"运行异步主函数时出错: {inner_e}")
                raise
        else:
            # 其他RuntimeError
            logger.error(f"运行主函数时出错: {e}")
            raise

if __name__ == "__main__":
    main()