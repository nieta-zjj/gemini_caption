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
        # 暂时设置默认ID为"id"，实际值可以在使用前通过async_init方法设置
        self.default_id = "_id"
        self.use_danbooru_meta = use_danbooru_meta

    async def async_init(self):
        """异步初始化方法，用于获取动态确定的默认ID字段"""
        self.default_id = await self._judge_default_id()
        logger.info(f"成功初始化 DanbooruPics，默认ID字段: {self.default_id}")
        return self

    async def _judge_default_id(self):
        """异步方法，用于判断ID字段"""
        try:
            # 获取一个项，判断ID字段
            example_item = await self.collection.find_one()
            if example_item:
                for key in example_item.keys():
                    if isinstance(example_item[key], int) or (isinstance(example_item[key], str) and example_item[key].isdigit()):
                        return key
            # 如果没有找到合适的字段，返回默认值
            return "id"
        except Exception as e:
            logger.error(f"判断默认ID字段时出错: {e}")
            return "id"

    async def get_pic_by_id(self, id: int) -> dict:
        """
        根据ID获取图片完整信息

        Args:
            id: 图片ID（整数）

        Returns:
            包含图片信息的文档或None
        """
        try:
            # 查询并记录结果
            doc = await self.collection.find_one({self.default_id: id})

            if doc:
                logger.debug(f"找到ID为 {id} 的文档，字段: {list(doc.keys())}")
            else:
                # 如果没找到，尝试使用其他ID字段
                logger.warning(f"使用 {self.default_id}={id} 未找到文档，尝试其他ID字段")
                if self.default_id != "_id":
                    doc = await self.collection.find_one({"_id": id})
                    if doc:
                        logger.debug(f"使用 _id={id} 找到文档")
                    else:
                        logger.warning(f"使用 _id={id} 仍未找到文档")

            return doc
        except Exception as e:
            logger.error(f"查询ID为 {id} 的图片时出错: {e}")
            return None

    async def _get_pic_field_by_id(self, id: int, field_name: str) -> List[str]:
        """
        通用方法，根据ID获取指定字段的值

        Args:
            id: 图片ID（整数）
            field_name: 字段名称

        Returns:
            指定字段的值列表
        """
        try:
            # 首先获取完整文档
            doc = await self.get_pic_by_id(id)
            if not doc:
                logger.warning(f"获取字段 {field_name} 失败: 未找到ID为 {id} 的文档")
                return []

            logger.debug(f"找到ID为 {id} 的文档，尝试获取字段 {field_name}")

            # 根据use_danbooru_meta确定如何获取字段
            if self.use_danbooru_meta:
                # 先尝试从danbooru_meta中获取
                danbooru_meta = doc.get('danbooru_meta', {})
                if danbooru_meta:
                    field_value = danbooru_meta.get(field_name, [])
                    if field_value:
                        logger.debug(f"从danbooru_meta.{field_name}找到值: {field_value[:5] if len(field_value) > 5 else field_value}")
                        return field_value

                # 如果danbooru_meta中没有，尝试直接获取
                if field_name in doc:
                    field_value = doc.get(field_name, [])
                    logger.debug(f"从文档根级别找到字段 {field_name}: {field_value[:5] if len(field_value) > 5 else field_value}")
                    return field_value

                # 尝试使用collection_name前缀的字段路径
                nested_field = f"{self.collection_name}.{field_name}"
                if nested_field in doc:
                    field_value = doc.get(nested_field, [])
                    logger.debug(f"找到嵌套字段 {nested_field}")
                    return field_value

                # 尝试从其他可能的嵌套字段中查找
                for parent_field in ['tags', 'meta', 'info', 'data']:
                    if parent_field in doc and isinstance(doc[parent_field], dict):
                        field_value = doc[parent_field].get(field_name, [])
                        if field_value:
                            logger.debug(f"从{parent_field}.{field_name}找到值")
                            return field_value

                logger.warning(f"在多个可能位置未找到字段 {field_name}")
                return []
            else:
                # 直接从文档获取
                field_value = doc.get(field_name, [])
                if field_value:
                    logger.debug(f"直接获取到字段 {field_name}: {field_value[:5] if len(field_value) > 5 else field_value}")
                else:
                    logger.warning(f"未找到字段 {field_name}")
                return field_value

        except Exception as e:
            logger.error(f"获取字段 {field_name} 时出错: {e}")
            return []

    async def get_pic_character_by_id(self, id: int) -> List[str]:
        return await self._get_pic_field_by_id(id, 'character_tags')

    async def get_pic_artist_by_id(self, id: int) -> List[str]:
        return await self._get_pic_field_by_id(id, 'artist_tags')

    async def get_pic_series_by_id(self, id: int) -> List[str]:
        return await self._get_pic_field_by_id(id, 'copyright_tags')

    async def get_pic_general_by_id(self, id: int) -> List[str]:
        return await self._get_pic_field_by_id(id, 'general_tags')

    async def get_pic_url_by_id(self, id: int) -> str:
        """
        根据ID获取图片URL

        Args:
            id: 图片ID（整数）

        Returns:
            图片URL字符串，如果未找到则返回空字符串
        """
        doc = await self.get_pic_by_id(id)
        if not doc:
            logger.warning(f"未找到ID为 {id} 的图片，无法生成URL")
            return ""

        # 记录文档中的所有键，帮助调试
        keys = list(doc.keys())
        logger.debug(f"文档包含字段: {keys}")

        # 尝试获取md5和file_ext字段
        file_md5 = None
        ext = None

        # 1. 直接从文档根级别查找
        file_md5 = doc.get('md5', '')
        ext = doc.get('file_ext', '')

        # 2. 如果没找到，尝试其他可能的字段名
        if not file_md5:
            for md5_field in ['MD5', 'hash', 'file_hash']:
                if md5_field in doc:
                    file_md5 = doc.get(md5_field, '')
                    logger.debug(f"从字段 {md5_field} 找到md5: {file_md5}")
                    break

        if not ext:
            for ext_field in ['extension', 'ext', 'format']:
                if ext_field in doc:
                    ext = doc.get(ext_field, '')
                    logger.debug(f"从字段 {ext_field} 找到扩展名: {ext}")
                    break

        # 3. 尝试嵌套字段，例如media_asset、file等
        if not file_md5 or not ext:
            for nested_field in ['file', 'media', 'image', 'picture', 'media_asset']:
                if nested_field in doc and isinstance(doc[nested_field], dict):
                    nested_doc = doc[nested_field]
                    logger.debug(f"找到嵌套字段 {nested_field}: {list(nested_doc.keys())}")

                    if not file_md5:
                        for md5_key in ['md5', 'hash', 'MD5', 'file_hash']:
                            if md5_key in nested_doc:
                                file_md5 = nested_doc[md5_key]
                                logger.debug(f"从嵌套字段 {nested_field}.{md5_key} 找到md5: {file_md5}")
                                break

                    if not ext:
                        for ext_key in ['file_ext', 'extension', 'ext', 'format']:
                            if ext_key in nested_doc:
                                ext = nested_doc[ext_key]
                                logger.debug(f"从嵌套字段 {nested_field}.{ext_key} 找到扩展名: {ext}")
                                break

                    if file_md5 and ext:
                        break

        # 如果仍然找不到信息，记录警告并返回空字符串
        if not file_md5 or not ext:
            logger.warning(f"ID为 {id} 的图片缺少必要字段，md5={file_md5}, ext={ext}")
            logger.warning(f"文档内容: {doc}")
            return ""

        return f"https://danbooru.donmai.us/original/{file_md5[:2]}/{file_md5[2:4]}/{file_md5}.{ext}"

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

