from motor.motor_asyncio import AsyncIOMotorClient
from typing import List, Optional, Any, Dict, Union, Tuple
import logging
import os

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DanbooruPics:
    def __init__(self, client_url: str = None,
                 db_name: str = 'danbooru',
                 collection_name: str = 'captions',
                 use_danbooru_meta: bool = True):
        # 如果未提供连接URL，则使用环境变量
        if client_url is None:
            client_url = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")

        self.client = AsyncIOMotorClient(client_url)
        self.db = self.client[db_name]
        self.collection_name = collection_name
        self.collection = self.db[collection_name]
        self.character_stats = self.client['danbooru']["character_stats"]  # 添加character_stats集合
        # 设置默认ID为"_id"，因为示例数据使用_id作为主键
        self.default_id = "_id"

    async def async_init(self):
        """异步初始化方法"""
        logger.debug(f"成功初始化 DanbooruPics，默认ID字段: {self.default_id}")
        return self

    async def get_pic_by_id(self, id: int) -> dict:
        """
        根据ID获取图片完整信息

        Args:
            id: 图片ID（整数）

        Returns:
            包含图片信息的文档或None
        """
        try:
            # 使用整数ID查询
            doc = await self.collection.find_one({self.default_id: id})

            if doc:
                logger.debug(f"找到ID为 {id} 的文档")
            else:
                logger.warning(f"未找到ID为 {id} 的文档")

            return doc
        except Exception as e:
            logger.error(f"查询ID为 {id} 的图片时出错: {e}")
            return None

    async def _get_pic_field_by_id(self, id: int, field_name: str) -> List[str]:
        """
        通用方法，根据ID获取指定字段的标签

        Args:
            id: 图片ID（整数）
            field_name: 字段名称 (对应danbooru_meta中的键名)

        Returns:
            指定标签类型的列表
        """
        try:
            # 获取完整文档
            doc = await self.get_pic_by_id(id)
            if not doc:
                logger.warning(f"获取字段 {field_name} 失败: 未找到ID为 {id} 的文档")
                return []

            # 从固定的danbooru_meta结构中获取标签字段
            # 将field_name映射到danbooru_meta中的实际字段名
            field_mapping = {
                'character_tags': 'character',
                'artist_tags': 'artist',
                'copyright_tags': 'series',
                'general_tags': 'general'
            }

            # 获取对应的字段名
            meta_field = field_mapping.get(field_name, field_name)

            # 从danbooru_meta中获取对应的标签
            danbooru_meta = doc.get('danbooru_meta', {})
            if not danbooru_meta:
                logger.warning(f"ID为 {id} 的文档缺少danbooru_meta字段")
                return []

            # 获取对应的标签列表
            tags = danbooru_meta.get(meta_field, [])
            if tags:
                logger.debug(f"从danbooru_meta.{meta_field}获取到{len(tags)}个标签")
            else:
                logger.warning(f"danbooru_meta中未找到{meta_field}标签")

            return tags
        except Exception as e:
            logger.error(f"获取字段 {field_name} 时出错: {e}")
            return []

    async def get_pic_character_by_id(self, id: int) -> List[str]:
        """获取角色标签"""
        return await self._get_pic_field_by_id(id, 'character_tags')

    async def get_pic_artist_by_id(self, id: int) -> List[str]:
        """获取艺术家标签"""
        return await self._get_pic_field_by_id(id, 'artist_tags')

    async def get_pic_series_by_id(self, id: int) -> List[str]:
        """获取系列标签"""
        return await self._get_pic_field_by_id(id, 'copyright_tags')

    async def get_pic_general_by_id(self, id: int) -> List[str]:
        """获取通用标签"""
        return await self._get_pic_field_by_id(id, 'general_tags')

    async def extract_character_stats(self, character: str) -> Tuple[List[str], Dict[str, Any]]:
        """
        提取角色的统计数据

        Args:
            character: 角色名称

        Returns:
            包含角色属性和系列信息的元组(属性列表, 系列字典)
        """
        data = await self.character_stats.find_one({"_id": character})
        if data:
            attribute = data.get('attribute', [])
            series_dict = data.get('series', {})
            return attribute, series_dict
        else:
            return [], {}  # 返回空列表和空字典，保持返回类型一致