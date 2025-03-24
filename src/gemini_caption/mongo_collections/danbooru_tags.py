'''
Danbooru标签操作模块

提供Danbooru标签层次结构的查询和处理功能，支持获取标签的父子关系和相关标签。
用于构建标签树和分析标签间的关联。
'''

import asyncio
import logging
import os
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DanbooruTags:
    """分析Danbooru标签关系并创建层次结构的类"""

    def __init__(self, mongodb_uri=None,
                 db_name="danbooru",
                 collection="tags"):
        try:
            # 如果未提供URI，使用环境变量
            if mongodb_uri is None:
                mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")

            self.mongodb_uri = mongodb_uri
            self.db_name = db_name
            self.collection_name = collection
            self._client = None
            self._db = None
            logger.debug("MongoDB连接初始化准备就绪")
        except Exception as e:
            logger.error(f"MongoDB连接初始化失败: {e}")
            raise

    async def initialize(self):
        """异步初始化MongoDB连接"""
        if self._client is None:
            try:
                # 设置客户端选项，启用压缩以减少网络流量
                client_options = {
                    'compressors': 'zlib',
                    'zlibCompressionLevel': 9  # 最高压缩级别
                }

                self._client = AsyncIOMotorClient(self.mongodb_uri, **client_options)
                self._db = self._client[self.db_name]
                logger.debug("MongoDB连接初始化成功")
            except Exception as e:
                logger.error(f"创建MongoDB客户端失败: {str(e)}")
                raise
        return self

    async def close(self):
        """关闭MongoDB连接"""
        if self._client:
            self._client.close()
            self._client = None
            logger.info(f"已关闭MongoDB客户端连接: {self.db_name}")

    async def get_collection(self) -> AsyncIOMotorCollection:
        """获取集合实例"""
        # 确保已初始化
        if self._client is None:
            await self.initialize()
        return self._db[self.collection_name]

    async def judge_root_tag(self, tag_name):
        """
        判断一个标签是否为根标签（没有父标签的标签）

        Args:
            tag_name: 标签名称

        Returns:
            bool: 如果是根标签则为True，否则为False
        """
        try:
            # 确保已初始化
            if self._client is None:
                await self.initialize()

            # 获取集合
            collection = await self.get_collection()

            # 查询标签数据
            parents_tag_data = await collection.find_one({"name": tag_name}, {"parents": 1, "_id": 1}) # 仅需要parents字段

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
            # 确保已初始化
            if self._client is None:
                await self.initialize()

            # 获取集合
            collection = await self.get_collection()

            # 查询标签数据
            children_tag_data = await collection.find_one({"name": tag_name}, {"children": 1, "_id": 1}) # 仅需要children字段

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
            # 确保已初始化
            if self._client is None:
                await self.initialize()

            # 获取集合
            collection = await self.get_collection()

            # 查询标签数据
            parent_tag_data = await collection.find_one({"name": tag_name}, {"parents": 1, "_id": 1}) # 仅需要parents字段

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
            # 确保已初始化
            if self._client is None:
                await self.initialize()

            # 获取集合
            collection = await self.get_collection()

            # 查询标签数据
            related_tag_data = await collection.find_one({"name": tag_name}, {"related": 1, "_id": 1}) # 仅需要related字段

            if not related_tag_data:
                logger.warning(f"标签不存在: {tag_name}")
                return []

            # 直接返回相关标签列表
            return related_tag_data.get("related", [])
        except Exception as e:
            logger.error(f"获取相关标签时出错: {e}")
            return []