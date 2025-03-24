'''
处理gemini_captions_danbooru数据库操作的模块
'''

import os
import json
import time
import asyncio
from typing import Dict, Any, Optional, Union, List, Tuple, Set
from gemini_caption.utils.logger_utils import log_info, log_debug, log_warning, log_error
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

class DanbooruGeminiCaptions:
    """
    处理gemini_captions_danbooru数据库操作的类，专注于描述结果的存储和检索
    """

    def __init__(self, mongodb_uri: Optional[str] = None, db_name: str = "gemini_captions_danbooru"):
        """
        初始化DanbooruGeminiCaptions类

        Args:
            mongodb_uri: MongoDB连接URI，如果为None则使用环境变量
            db_name: 数据库名称
        """
        # 如果未提供URI，使用环境变量
        if mongodb_uri is None:
            mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")

        self.mongodb_uri = mongodb_uri
        self.db_name = db_name
        self._client = None
        self._db = None
        self._initialized = False  # 添加初始化标志

    async def initialize(self):
        """异步初始化MongoDB连接"""
        if self._initialized:  # 检查是否已经初始化
            return self

        if self._client is None:
            try:
                # 设置客户端选项，启用压缩以减少网络流量
                client_options = {
                    'compressors': 'zlib',
                    'zlibCompressionLevel': 9  # 最高压缩级别
                }

                self._client = AsyncIOMotorClient(self.mongodb_uri, **client_options)
                self._db = self._client[self.db_name]
                self._initialized = True  # 设置初始化标志
                log_info(f"成功创建MongoDB客户端连接: {self.db_name}")
            except Exception as e:
                log_error(f"创建MongoDB客户端失败: {str(e)}")
                raise
        return self

    async def close(self):
        """关闭MongoDB连接"""
        if self._client:
            self._client.close()
            self._client = None
            log_info(f"已关闭MongoDB客户端连接: {self.db_name}")

    async def get_collection(self, collection_name: str) -> AsyncIOMotorCollection:
        """获取指定的集合实例"""
        # 确保已初始化
        if self._client is None:
            await self.initialize()
        return self._db[collection_name]

    def _get_collection_name(self, id_value: Union[str, int]) -> str:
        """
        根据ID获取集合名称，每100000个ID为一个集合

        Args:
            id_value: ID值

        Returns:
            集合名称
        """
        # 确保ID是整数
        if isinstance(id_value, str):
            try:
                id_value = int(id_value)
            except ValueError:
                log_warning(f"无法将ID转换为整数: {id_value}")
                return "default"

        # 计算集合名称
        return str(id_value // 100000)

    async def get_existing_ids(self, start: int, end: int, collection_prefix: str = "") -> Set[int]:
        """
        获取指定范围内存在的ID集合，仅获取ID字段

        Args:
            start: 起始ID
            end: 结束ID
            collection_prefix: 集合名称前缀，默认为空

        Returns:
            存在的ID集合
        """
        try:
            # 确保已初始化
            if self._client is None:
                await self.initialize()

            log_info(f"正在获取ID范围 {start}-{end} 内的现有ID")
            result = set()

            # 计算涉及的集合范围
            start_collection = start // 100000
            end_collection = (end - 1) // 100000 + 1

            # 查询每个集合
            for coll_key in range(start_collection, end_collection):
                collection_name = f"{collection_prefix}{coll_key}"
                collection = self._db[collection_name]

                # 计算当前集合中的ID范围
                coll_start = max(start, coll_key * 100000)
                coll_end = min(end, (coll_key + 1) * 100000)

                # 查询该范围内的所有ID
                query = {"_id": {"$gte": coll_start, "$lt": coll_end}}
                projection = {"_id": 1}

                cursor = collection.find(query, projection).batch_size(10000)

                # 收集ID
                async for doc in cursor:
                    result.add(doc["_id"])

            log_info(f"共找到 {len(result)} 个ID")
            return result

        except Exception as e:
            log_error(f"获取现有ID列表时出错: {str(e)}")
            return set()

    async def get_processed_ids(self, start_id: int, end_id: int) -> Set[int]:
        """
        获取指定范围内已成功处理的ID集合

        Args:
            start_id: 起始ID
            end_id: 结束ID

        Returns:
            已处理ID集合
        """
        processed_ids = set()
        try:
            log_info(f"正在获取已处理ID列表: {start_id}-{end_id}")

            # 按集合分组处理，避免每个ID单独查询
            collection_range = range(start_id // 100000, (end_id // 100000) + 1)

            for coll_key in collection_range:
                collection_name = str(coll_key)
                collection = await self.get_collection(collection_name)

                # 计算当前集合中的ID范围
                coll_start = max(start_id, coll_key * 100000)
                coll_end = min(end_id, (coll_key + 1) * 100000)

                # 查询该范围内已成功处理的ID，prompt存在或success=true或status_code in in: [200, 403, 404, 999, 998]
                cursor = collection.find(
                    {
                        "_id": {"$gte": coll_start, "$lt": coll_end},
                        "$or": [
                            {"prompt": {"$exists": True}},
                            {"success": True},
                            {"status_code": {"$in": [200, 403, 404, 999, 998]}}
                        ]
                    },
                    {"_id": 1}
                )

                # 收集ID
                async for doc in cursor:
                    processed_ids.add(doc["_id"])

            log_info(f"共找到 {len(processed_ids)} 个已处理ID")
            return processed_ids

        except Exception as e:
            log_error(f"获取已处理ID列表时出错: {str(e)}")
            return processed_ids

    async def check_existing_result(self, dan_id: Union[str, int]) -> Optional[Dict[str, Any]]:
        """
        检查是否存在已处理的结果

        Args:
            dan_id: Danbooru图片ID

        Returns:
            如果存在，返回结果字典；否则返回None
        """
        try:
            # 确保已初始化
            if self._client is None:
                await self.initialize()

            # 确保ID是整数
            if isinstance(dan_id, str):
                dan_id = int(dan_id)

            # 获取对应的集合
            collection_name = self._get_collection_name(dan_id)
            collection = self._db[collection_name]

            # 查询结果
            result = await collection.find_one({"_id": dan_id})

            if result and result.get("success", False):
                log_debug(f"找到已存在的处理结果: ID {dan_id}")
                return result

            return None
        except Exception as e:
            log_error(f"检查已有结果时出错: {str(e)}")
            return None

    async def save_caption_result(self, dan_id: Union[str, int], result: Dict[str, Any]) -> bool:
        """
        保存描述结果到数据库

        Args:
            dan_id: Danbooru图片ID
            result: 结果数据

        Returns:
            是否成功保存
        """
        try:
            # 确保已初始化
            if self._client is None:
                await self.initialize()

            # 确保ID是整数
            if isinstance(dan_id, str):
                dan_id = int(dan_id)

            # 获取对应的集合
            collection_name = self._get_collection_name(dan_id)
            collection = self._db[collection_name]

            # 添加时间戳
            if "created_at" not in result:
                result["created_at"] = time.time()

            # 确保_id字段存在
            result["_id"] = dan_id

            # 保存到MongoDB
            await collection.update_one(
                {"_id": dan_id},
                {"$set": result},
                upsert=True
            )

            log_debug(f"结果已保存到MongoDB，ID: {dan_id}，集合: {collection_name}")
            return True
        except Exception as e:
            log_error(f"保存到MongoDB失败: {str(e)}")
            return False

    async def save_result_to_file(self, dan_id: Union[str, int], result: Dict[str, Any], output_dir: str) -> bool:
        """
        保存结果到本地文件

        Args:
            dan_id: Danbooru图片ID
            result: 结果数据
            output_dir: 输出目录路径

        Returns:
            是否成功保存
        """
        try:
            # 确保目录存在
            os.makedirs(output_dir, exist_ok=True)

            # 构建输出文件路径
            result_path = os.path.join(output_dir, f"{dan_id}_caption.json")

            # 保存为JSON文件
            with open(result_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            log_debug(f"结果已保存到文件: {result_path}")
            return True
        except Exception as e:
            log_error(f"保存结果到文件失败: {str(e)}")
            return False