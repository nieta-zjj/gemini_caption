'''
finished


'''

from typing import List, Optional, Any, Dict, Union, Tuple, Set
import logging
import os
import json
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from gemini_caption.mongo_collections.danbooru_pics_model import DanbooruPicDoc
from gemini_caption.mongo_collections.danbooru_gemini_captions import DanbooruGeminiCaptions

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DanbooruPics:
    def __init__(self, client_url: str = None,
                 db_name: str = 'danbooru',
                 collection_name: str = 'captions',
                 pics_collection_name: str = 'pics',
                 compression_enabled: bool = True):
        """
        初始化DanbooruPics类

        Args:
            client_url: MongoDB连接URL，如果为None则使用环境变量
            db_name: 数据库名称
            collection_name: 集合名称(用于标签查询)
            pics_collection_name: 图片集合名称(用于URL查询)
            compression_enabled: 是否启用MongoDB网络压缩
        """
        # 如果未提供URI，使用环境变量
        if client_url is None:
            client_url = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")

        self.client_url = client_url
        self.gemini_captions = DanbooruGeminiCaptions(mongodb_uri=client_url)

        self.db_name = db_name
        self.collection_name = collection_name
        self.pics_collection_name = pics_collection_name
        self.character_stats_collection = "character_stats"
        self.compression_enabled = compression_enabled

        # 文件类型到MIME类型的映射
        self.mime_type_map = {
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "webp": "image/webp",
        }

        self._client = None
        self._db = None
        self.pics_db_cache = {}


    async def initialize(self):
        """异步初始化MongoDB连接"""
        if self._client is None:
            try:
                # 设置客户端选项，启用压缩以减少网络流量
                client_options = {}
                if self.compression_enabled:
                    client_options = {
                        'compressors': 'zlib',
                        'zlibCompressionLevel': 9  # 最高压缩级别
                    }

                self._client = AsyncIOMotorClient(self.client_url, **client_options)
                self._db = self._client[self.db_name]
                await self.gemini_captions.initialize()
                logger.debug(f"MongoDB连接初始化成功: {self.db_name}")
            except Exception as e:
                logger.error(f"创建MongoDB客户端失败: {str(e)}")
                raise
        return self

    def close(self):
        """关闭MongoDB连接"""
        if self._client:
            self._client.close()
            self._client = None
            logger.info(f"已关闭MongoDB客户端连接: {self.db_name}")


    async def get_pic_data_by_id(self, id: int, use_cache: bool = True) -> DanbooruPicDoc:
        """
        从数据库获取图片文档，返回完整的DanbooruPicDoc对象

        Args:
            id: 图片ID
            use_cache: 是否使用缓存

        Returns:
            DanbooruPicDoc对象，如果文档不存在则返回包含默认值的对象(status=404)
        """
        # 确保已初始化
        if self._client is None:
            await self.initialize()

        # 从缓存获取或查询数据库
        if not id or not self.pics_db_cache.get(id) or not use_cache:
            try:
                pics_collection = self._db[self.pics_collection_name]
                doc_dict = await pics_collection.find_one({"_id": id})
                if doc_dict:
                    # 转换为DanbooruPicDoc对象
                    doc = DanbooruPicDoc(**doc_dict)
                    self.pics_db_cache[id] = doc
                else:
                    # 未找到文档时，使用from_id方法创建默认对象
                    logger.debug(f"未找到ID为 {id} 的图片")
                    return DanbooruPicDoc.from_id(id)
            except Exception as e:
                logger.error(f"获取ID为 {id} 的图片时出错: {e}")
                return DanbooruPicDoc.from_id(id)
        else:
            doc = self.pics_db_cache[id]

        return doc

    async def get_url_by_id(self, post_id: int) -> Tuple[Optional[str], int]:
        """
        获取单个ID的URL和状态

        Args:
            post_id: Danbooru图片ID

        Returns:
            (url, status_code)元组
        """
        try:
            doc = await self.get_pic_data_by_id(post_id)
            return doc.url, doc.status

        except Exception as e:
            logger.error(f"获取ID为 {post_id} 的URL时出错: {e}")
            return None, 500


    async def _build_cache(self, start: int, end: int) -> Dict[int, DanbooruPicDoc]:
        """
        构建ID-文档缓存

        Args:
            start: 起始ID
            end: 结束ID

        Returns:
            ID到DanbooruPicDoc对象的缓存字典
        """
        try:
            # 确保已初始化
            if self._client is None:
                await self.initialize()

            # 查询条件：_id >= start 且 _id < end
            query = {"_id": {"$gte": start, "$lt": end}}

            # 获取集合
            pics_collection = self._db[self.pics_collection_name]

            # 执行查询
            # 优化：使用批量游标处理大量数据
            cursor = pics_collection.find(query).batch_size(1000)

            async for doc_dict in cursor:
                post_id = doc_dict["_id"]
                doc = DanbooruPicDoc(**doc_dict)
                # 更新实例缓存
                self.pics_db_cache[post_id] = doc

        except Exception as e:
            logger.error(f"构建缓存时出错: {e}")


    async def get_existing_ids(self, start: int, end: int, collection: str = "captions", gemini_db: str = "gemini_captions_danbooru") -> Set[int]:
        """
        获取指定范围内已处理过的ID集合，通过调用danbooru_gemini_captions中的方法

        Args:
            start: 起始ID
            end: 结束ID
            collection: 要查询的集合名称前缀，默认为"captions"
            gemini_db: 要查询的数据库名称

        Returns:
            已处理的ID集合
        """
        # 调用danbooru_gemini_captions中的get_existing_ids方法获取已处理的ID
        existing_ids = await self.gemini_captions.get_existing_ids(
            start=start,
            end=end,
            collection_prefix=collection
        )

        logger.info(f"从gemini_captions_danbooru获取到已处理的ID: {len(existing_ids)}个")
        return existing_ids


    async def check_url_by_id_batch(self, post_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """
        批量获取多个ID的URL和状态

        Args:
            post_ids: Danbooru图片ID列表

        Returns:
            {id: {"url": url, "status": status_code}} 格式的字典
        """
        try:
            results = {}

            # 并发查询所有ID，利用asyncio提高性能
            tasks = [self.get_pic_data_by_id(post_id) for post_id in post_ids]
            docs = await asyncio.gather(*tasks)

            # 构建结果字典
            for i, doc in enumerate(docs):
                post_id = post_ids[i]
                results[post_id] = {"url": doc.url, "status": doc.status}

            return results
        except Exception as e:
            logger.error(f"批量获取URL时出错: {e}")
            return {post_id: {"url": None, "status": 500} for post_id in post_ids}

    async def check_urls_by_key(self, key: int, output_file: Optional[str] = None, batch_size: int = 10000) -> Dict[int, Dict[str, Any]]:
        """
        检查指定key范围内的所有ID的URL状态

        Args:
            key: 区间键值，用于计算ID范围 (start = key*100000, end = (key+1)*100000)
            output_file: 结果保存的文件路径，如果为None则不保存
            batch_size: 处理批次大小，用于分批处理大量ID

        Returns:
            包含所有ID状态的字典 {id: {"url": url, "status": status_code}}
        """
        # 计算ID范围
        start = key * 100000
        end = (key + 1) * 100000

        logger.info(f"检查ID范围: {start} - {end}")

        # 清理缓存中范围外的数据，避免内存泄漏
        self.pics_db_cache = {k: v for k, v in self.pics_db_cache.items() if start <= k < end}
        logger.info(f"清理缓存，保留 {len(self.pics_db_cache)} 条记录")

        # 从数据库获取数据并构建缓存
        await self._build_cache(start, end)
        cache_size = len(self.pics_db_cache)
        logger.info(f"从数据库获取到 {cache_size} 条记录")

        # 构建结果字典，分批处理以减少内存使用
        result = {}

        # 计算总批次数
        total_ids = end - start
        total_batches = (total_ids + batch_size - 1) // batch_size

        for batch_index in range(total_batches):
            batch_start = start + batch_index * batch_size
            batch_end = min(batch_start + batch_size, end)
            batch_result = {}

            logger.info(f"处理批次 {batch_index+1}/{total_batches}: ID {batch_start}-{batch_end-1}")

            for post_id in range(batch_start, batch_end):
                # 从缓存中获取或创建默认文档
                if post_id in self.pics_db_cache:
                    doc = self.pics_db_cache[post_id]
                    batch_result[post_id] = {"url": doc.url, "status": doc.status}
                else:
                    # 不创建新对象，直接记录不存在的状态，提高性能
                    batch_result[post_id] = {"url": None, "status": 404}

            # 更新总结果字典
            result.update(batch_result)
            logger.info(f"批次 {batch_index+1} 处理完成，当前已处理 {len(result)} 个ID")

            # 显示结果统计
            status_counts = {200: 0, 404: 0, 405: 0, 500: 0}
            for item in result.values():
                status = item.get("status")
                if status in status_counts:
                    status_counts[status] += 1

            logger.info(f"结果统计: 总计 {len(result)} 个ID")
            logger.info(f"  状态码 200 (ID和URL都存在): {status_counts[200]} 个")
            logger.info(f"  状态码 404 (ID不存在): {status_counts[404]} 个")
            logger.info(f"  状态码 405 (ID存在但URL不存在): {status_counts[405]} 个")
            if status_counts[500] > 0:
                logger.info(f"  状态码 500 (处理错误): {status_counts[500]} 个")

        # 所有数据处理完成后，如果指定了输出文件，一次性保存所有结果
        if output_file:
            await self.save_results_to_file(result, output_file)
            logger.info(f"所有结果已保存到文件: {output_file}")

        return result

    async def save_results_to_file(self, results: Dict[int, Dict[str, Any]], output_file: str) -> bool:
        """
        将结果保存到文件

        Args:
            results: 结果字典
            output_file: 输出文件路径

        Returns:
            是否成功保存
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)

            # 使用asyncio.to_thread将同步文件操作转换为异步操作
            def write_to_file():
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)

            await asyncio.to_thread(write_to_file)
            logger.info(f"结果已保存到文件: {output_file}")
            return True
        except Exception as e:
            logger.error(f"保存结果到文件失败: {str(e)}")
            return False

    async def update_results_file(self, output_file: str, new_results: Dict[int, Dict[str, Any]]) -> bool:
        """
        将新的结果更新到已有的结果文件中

        Args:
            output_file: 结果文件路径
            new_results: 新的结果字典

        Returns:
            是否成功更新
        """
        try:
            # 读取现有结果
            existing_results = {}

            def read_and_update():
                nonlocal existing_results
                try:
                    with open(output_file, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        if content:
                            existing_results = json.loads(content)
                except (FileNotFoundError, json.JSONDecodeError):
                    # 如果文件不存在或内容为空，使用空字典
                    existing_results = {}

                # 更新结果
                existing_results.update(new_results)

                # 写回文件
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(existing_results, f, ensure_ascii=False, indent=2)

            # 使用asyncio.to_thread将同步文件操作转换为异步操作
            await asyncio.to_thread(read_and_update)
            return True
        except Exception as e:
            logger.error(f"更新结果文件失败: {str(e)}")
            return False

    async def extract_character_stats(self, character: str) -> Tuple[List[str], Dict[str, Any]]:
        """
        提取角色的统计数据

        Args:
            character: 角色名称

        Returns:
            包含角色属性和系列信息的元组(属性列表, 系列字典)
        """
        try:
            # 确保已初始化
            if self._client is None:
                await self.initialize()

            character_stats_collection = self._db[self.character_stats_collection]
            data = await character_stats_collection.find_one({"_id": character})
            if data:
                attribute = data.get('attribute', [])
                series_dict = data.get('series', {})
                return attribute, series_dict
            else:
                return [], {}  # 返回空列表和空字典，保持返回类型一致
        except Exception as e:
            logger.error(f"提取角色统计数据时出错: {e}")
            return [], {}