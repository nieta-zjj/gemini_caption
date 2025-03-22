from pymongo import MongoClient
from pymongo.operations import UpdateOne
import json
import logging
from tqdm import tqdm
import os

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InputCharacterStats:
    def __init__(self,
                 client_url: str = 'mongodb://8.153.97.53:27815/',
                 db_name: str = 'danbooru',
                 collection_name: str = 'character_stats',
                 file_path: str = None):
        """
        初始化角色统计数据导入器

        Args:
            client_url: MongoDB连接URL
            db_name: 数据库名称
            collection_name: 集合名称
            file_path: 角色统计JSON文件路径
        """
        self.client_url = client_url
        self.db_name = db_name
        self.collection_name = collection_name

        # 如果未提供文件路径，则使用默认路径
        if file_path is None:
            # 使用相对于当前目录的路径
            self.file_path = os.path.join(os.path.dirname(__file__),
                                         'character_stats_currate_stage2_20250315(1).json')
        else:
            self.file_path = file_path

        self.client = None
        self.db = None
        self.collection = None
        self.character_stats = {}

    def connect(self):
        """连接到MongoDB数据库"""
        try:
            self.client = MongoClient(self.client_url)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            # 验证连接
            self.client.admin.command('ping')
            logger.info(f"成功连接到MongoDB: {self.db_name}.{self.collection_name}")
            return True
        except Exception as e:
            logger.error(f"连接MongoDB失败: {e}")
            return False

    def load_character_stats(self):
        """从JSON文件加载角色统计数据"""
        try:
            logger.info(f"从文件加载角色统计数据: {self.file_path}")
            with open(self.file_path, 'r', encoding='utf-8') as f:
                self.character_stats = json.load(f)
            logger.info(f"成功加载数据，共 {len(self.character_stats)} 个角色条目")
            return True
        except Exception as e:
            logger.error(f"加载角色统计数据失败: {e}")
            return False

    def import_to_db(self, batch_size=1000):
        """
        将角色统计数据导入到MongoDB

        Args:
            batch_size: 每批处理的记录数量

        Returns:
            tuple: (成功处理的记录数, 总记录数)
        """
        if not self.character_stats:
            logger.warning("没有数据可导入，请先调用load_character_stats()")
            return 0, 0

        # if self.collection is None:
        #     if not self.connect():
        #         logger.error("数据库连接失败，无法导入数据")
        #         return 0, 0

        # 创建索引以加快查询
        try:
            self.collection.create_index("_id")
            logger.info("创建或确认索引成功")
        except Exception as e:
            logger.warning(f"创建索引时出现警告: {e}")

        # 开始批量导入
        total_items = len(self.character_stats)
        processed_items = 0
        batch_operations = []

        try:
            # 使用tqdm显示进度
            with tqdm(total=total_items, desc="导入角色统计数据") as pbar:
                for character_name, stats in self.character_stats.items():
                    # 确保stats是字典
                    if not isinstance(stats, dict):
                        logger.warning(f"跳过非字典数据: {character_name}")
                        continue

                    # 准备文档
                    document = stats.copy()
                    document["_id"] = character_name  # 使用角色名作为_id

                    # 添加到批处理操作
                    batch_operations.append(
                        UpdateOne(
                            {"_id": character_name},
                            {"$set": document},
                            upsert=True
                        )
                    )

                    # 如果达到批处理大小，执行批量写入
                    if len(batch_operations) >= batch_size:
                        result = self.collection.bulk_write(batch_operations)
                        processed_items += len(batch_operations)
                        batch_operations = []
                        pbar.update(processed_items - pbar.n)

                # 处理剩余的操作
                if batch_operations:
                    result = self.collection.bulk_write(batch_operations)
                    processed_items += len(batch_operations)
                    pbar.update(processed_items - pbar.n)

            logger.info(f"导入完成: {processed_items}/{total_items} 条记录已处理")
            return processed_items, total_items

        except Exception as e:
            logger.error(f"导入过程中出错: {e}")
            return processed_items, total_items

    def close(self):
        """关闭数据库连接"""
        if self.client:
            self.client.close()
            logger.info("MongoDB连接已关闭")

    def run(self):
        """执行完整的导入流程"""
        try:
            if self.connect() and self.load_character_stats():
                processed, total = self.import_to_db()
                success_rate = (processed / total * 100) if total > 0 else 0
                logger.info(f"导入完成, 成功率: {success_rate:.2f}%")
                return True
            return False
        finally:
            self.close()


# 示例用法
if __name__ == "__main__":
    importer = InputCharacterStats()
    importer.run()
