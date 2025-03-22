import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import logging
import os


# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DanbooruTags:
    """分析Danbooru标签关系并创建层次结构的类"""

    def __init__(self, mongodb_uri=None,
                 db_name="danbooru",
                 collection="tags"):
        try:
            # 如果未提供连接URL，则使用环境变量
            if mongodb_uri is None:
                mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")

            self.mongodb_uri = mongodb_uri
            self.client = AsyncIOMotorClient(mongodb_uri)
            self.db = self.client[db_name]
            self.collection = self.db[collection]
            logger.debug("MongoDB连接初始化成功")
        except Exception as e:
            logger.error(f"MongoDB连接初始化失败: {e}")
            raise

    async def judge_root_tag(self, tag_name):
        """
        判断一个标签是否为根标签（没有父标签的标签）

        Args:
            tag_name: 标签名称

        Returns:
            bool: 如果是根标签则为True，否则为False
        """
        try:
            # 查询标签数据
            parents_tag_data = await self.collection.find_one({"name": tag_name}, {"parents": 1, "_id": 1}) # 仅需要parents字段

            if not parents_tag_data:
                logger.warning(f"标签不存在: {tag_name}")
                return False

            # 判断是否有父标签
            return len(parents_tag_data.get("parents", [])) == 0
        except Exception as e:
            logger.error(f"判断根标签时出错: {e}")
            return False

    async def get_children_tags(self, tag_name):
        """
        获取一个标签的直接子标签名称列表

        Args:
            tag_name: 标签名称

        Returns:
            list: 子标签名称列表
        """
        try:
            # 查询标签数据
            children_tag_data = await self.collection.find_one({"name": tag_name}, {"children": 1, "_id": 1}) # 仅需要children字段

            if not children_tag_data:
                logger.warning(f"标签不存在: {tag_name}")
                return []

            # 直接返回子标签列表，因为children字段已经是字符串列表
            return children_tag_data.get("children", [])
        except Exception as e:
            logger.error(f"获取子标签时出错: {e}")
            return []

    async def get_parent_tags(self, tag_name):
        """
        获取一个标签的直接父标签名称列表
        """
        try:
            # 查询标签数据
            parent_tag_data = await self.collection.find_one({"name": tag_name}, {"parents": 1, "_id": 1}) # 仅需要parents字段

            if not parent_tag_data:
                logger.warning(f"标签不存在: {tag_name}")
                return []

            # 返回父标签名称列表
            return [parent.get("name") for parent in parent_tag_data.get("parents", []) if parent.get("name")]
        except Exception as e:
            logger.error(f"获取父标签时出错: {e}")
            return []


    async def get_related_tags(self, tag_name):
        """
        获取一个标签的直接相关标签列表

        Args:
            tag_name: 标签名称

        Returns:
            list: 相关标签列表
        """
        try:
            # 查询标签数据
            related_tag_data = await self.collection.find_one({"name": tag_name}, {"related": 1, "_id": 1}) # 仅需要related字段

            if not related_tag_data:
                logger.warning(f"标签不存在: {tag_name}")
                return []

            # 直接返回相关标签列表
            return related_tag_data.get("related", [])
        except Exception as e:
            logger.error(f"获取相关标签时出错: {e}")
            return []