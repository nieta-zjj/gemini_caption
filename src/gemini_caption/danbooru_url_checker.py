import os
import logging
import json
from pymongo import MongoClient
from typing import Dict, Any, Tuple, List, Optional, Set

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DanbooruUrlChecker:
    """Danbooru URL检查器，用于获取指定范围内的图片URL和状态"""

    def __init__(self, mongodb_uri: Optional[str] = None, compression_enabled: bool = True):
        """
        初始化URL检查器

        Args:
            mongodb_uri: MongoDB连接URI，如果为None则使用环境变量
            compression_enabled: 是否启用MongoDB网络压缩
        """
        # 获取MongoDB连接URI
        if mongodb_uri is None:
            mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")

        self.mongodb_uri = mongodb_uri
        self.mongo_client = None
        self.db = None
        self.compression_enabled = compression_enabled

        # 文件类型到MIME类型的映射
        self.mime_type_map = {
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "webp": "image/webp",
            "gif": "image/gif"
        }

    def connect(self):
        """连接到MongoDB数据库，使用压缩选项减少网络开销"""
        try:
            # 添加压缩选项以减少网络流量
            client_options = {}
            if self.compression_enabled:
                client_options = {
                    'compressors': 'zlib',
                    'zlibCompressionLevel': 9  # 最高压缩级别
                }

            self.mongo_client = MongoClient(self.mongodb_uri, **client_options)
            self.db = self.mongo_client["danbooru"]
            logger.info("成功连接到MongoDB数据库")
            return True
        except Exception as e:
            logger.error(f"连接MongoDB失败: {str(e)}")
            return False

    def close(self):
        """关闭MongoDB连接"""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("已关闭MongoDB连接")

    def build_url(self, md5: str, file_ext: str) -> str:
        """
        根据md5和文件扩展名构建URL

        Args:
            md5: 图片的md5哈希值
            file_ext: 文件扩展名

        Returns:
            构建的URL
        """
        # 构建Danbooru URL (根据md5前两位字符分组)
        return f"https://cdn.donmai.us/original/{md5[0:2]}/{md5[2:4]}/{md5}.{file_ext}"

    def check_urls_by_key(self, key: int, output_file: Optional[str] = None, batch_size: int = 10000) -> Dict[int, Dict[str, Any]]:
        """
        检查指定key范围内的所有ID的URL状态

        Args:
            key: 区间键值，用于计算ID范围 (start = key*100000, end = (key+1)*100000)
            output_file: 结果保存的文件路径，如果为None则不保存
            batch_size: 处理批次大小，用于分批处理大量ID

        Returns:
            包含所有ID状态的字典 {id: {"url": url, "status": status_code}}
        """
        if not self.mongo_client:
            if not self.connect():
                return {}

        # 计算ID范围
        start = key * 100000
        end = (key + 1) * 100000

        logger.info(f"检查ID范围: {start} - {end}")

        # 从数据库获取数据并构建缓存
        # 优化：仅获取必要字段 _id, md5, file_ext
        cache = self._build_cache(start, end)
        cache_size = len(cache)
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
                if post_id in cache:
                    url = cache[post_id].get("url")
                    if url:
                        # ID存在且URL也存在
                        batch_result[post_id] = {"url": url, "status": 200}
                    else:
                        # ID存在但URL不存在
                        batch_result[post_id] = {"url": None, "status": 405}
                else:
                    # ID不存在
                    batch_result[post_id] = {"url": None, "status": 404}

            # 更新总结果字典
            result.update(batch_result)
            logger.info(f"批次 {batch_index+1} 处理完成，当前已处理 {len(result)} 个ID")

        # 显示结果统计
        status_counts = {200: 0, 404: 0, 405: 0}
        for item in result.values():
            status = item.get("status")
            if status in status_counts:
                status_counts[status] += 1

        logger.info(f"结果统计: 总计 {len(result)} 个ID")
        logger.info(f"  状态码 200 (ID和URL都存在): {status_counts[200]} 个")
        logger.info(f"  状态码 404 (ID不存在): {status_counts[404]} 个")
        logger.info(f"  状态码 405 (ID存在但URL不存在): {status_counts[405]} 个")

        # 所有数据处理完成后，如果指定了输出文件，一次性保存所有结果
        if output_file:
            self.save_results_to_file(result, output_file)
            logger.info(f"所有结果已保存到文件: {output_file}")

        return result

    def save_results_to_file(self, results: Dict[int, Dict[str, Any]], output_file: str) -> bool:
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

            # 保存为JSON文件
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)

            logger.info(f"结果已保存到文件: {output_file}")
            return True
        except Exception as e:
            logger.error(f"保存结果到文件失败: {str(e)}")
            return False

    def update_results_file(self, output_file: str, new_results: Dict[int, Dict[str, Any]]) -> bool:
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

            return True
        except Exception as e:
            logger.error(f"更新结果文件失败: {str(e)}")
            return False

    def _build_cache(self, start: int, end: int) -> Dict[int, Dict[str, Any]]:
        """
        构建ID-URL缓存

        Args:
            start: 起始ID
            end: 结束ID

        Returns:
            ID到URL的缓存字典
        """
        cache = {}
        collection = self.db["pics"]

        # 查询条件：_id >= start 且 _id < end
        query = {"_id": {"$gte": start, "$lt": end}}

        # 优化：只获取必要的字段，减少网络传输
        # 仅请求需要用到的三个字段：_id, md5, file_ext
        projection = {"_id": 1, "md5": 1, "file_ext": 1}

        # 执行查询
        # 优化：使用批量游标处理大量数据
        cursor = collection.find(query, projection).batch_size(1000)

        # 构建缓存
        for doc in cursor:
            post_id = doc["_id"]
            md5 = doc.get("md5")
            file_ext = doc.get("file_ext")

            cache_entry = {"id": post_id}

            if md5 and file_ext:
                url = self.build_url(md5, file_ext)
                cache_entry["url"] = url
            else:
                cache_entry["url"] = None

            cache[post_id] = cache_entry

        return cache

    def get_existing_ids(self, start: int, end: int) -> Set[int]:
        """
        获取指定范围内存在的ID集合，仅获取ID字段

        Args:
            start: 起始ID
            end: 结束ID

        Returns:
            存在的ID集合
        """
        if not self.mongo_client:
            if not self.connect():
                return set()

        collection = self.db["pics"]

        # 查询条件：_id >= start 且 _id < end
        query = {"_id": {"$gte": start, "$lt": end}}

        # 优化：只获取ID字段，不获取其他任何字段
        projection = {"_id": 1}

        # 执行查询
        cursor = collection.find(query, projection).batch_size(10000)

        # 返回ID集合
        return {doc["_id"] for doc in cursor}

    def get_url_by_id(self, post_id: int) -> Tuple[str, int]:
        """
        获取单个ID的URL和状态

        Args:
            post_id: Danbooru图片ID

        Returns:
            (url, status_code)元组
        """
        if not self.mongo_client:
            if not self.connect():
                return None, 500

        collection = self.db["pics"]

        # 优化：只获取必要的字段，减少网络传输
        doc = collection.find_one({"_id": post_id}, {"_id": 1, "md5": 1, "file_ext": 1})

        if not doc:
            return None, 404

        md5 = doc.get("md5")
        file_ext = doc.get("file_ext")

        if not md5 or not file_ext:
            return None, 405

        url = self.build_url(md5, file_ext)
        return url, 200

    def check_url_by_id_batch(self, post_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """
        批量获取多个ID的URL和状态

        Args:
            post_ids: Danbooru图片ID列表

        Returns:
            {id: {"url": url, "status": status_code}} 格式的字典
        """
        if not self.mongo_client:
            if not self.connect():
                return {post_id: {"url": None, "status": 500} for post_id in post_ids}

        collection = self.db["pics"]

        # 使用$in操作符批量查询
        query = {"_id": {"$in": post_ids}}

        # 优化：只获取必要字段
        projection = {"_id": 1, "md5": 1, "file_ext": 1}

        # 执行查询
        cursor = collection.find(query, projection)

        # 创建ID到文档的映射
        found_docs = {doc["_id"]: doc for doc in cursor}

        # 构建结果
        results = {}
        for post_id in post_ids:
            if post_id in found_docs:
                doc = found_docs[post_id]
                md5 = doc.get("md5")
                file_ext = doc.get("file_ext")

                if md5 and file_ext:
                    url = self.build_url(md5, file_ext)
                    results[post_id] = {"url": url, "status": 200}
                else:
                    results[post_id] = {"url": None, "status": 405}
            else:
                results[post_id] = {"url": None, "status": 404}

        return results

# 便于导入的函数
def check_urls_by_key(key: int, mongodb_uri: Optional[str] = None, output_file: Optional[str] = None, batch_size: int = 10000) -> Dict[int, Dict[str, Any]]:
    """
    检查指定key范围内的所有ID的URL状态

    Args:
        key: 区间键值，用于计算ID范围 (start = key*100000, end = (key+1)*100000)
        mongodb_uri: MongoDB连接URI，如果为None则使用环境变量
        output_file: 结果保存的文件路径，如果为None则不保存
        batch_size: 处理批次大小，用于分批处理大量ID

    Returns:
        包含所有ID状态的字典 {id: {"url": url, "status": status_code}}
    """
    checker = DanbooruUrlChecker(mongodb_uri)
    try:
        return checker.check_urls_by_key(key, output_file, batch_size)
    finally:
        checker.close()

def get_url_by_id(post_id: int, mongodb_uri: Optional[str] = None) -> Tuple[str, int]:
    """
    获取单个ID的URL和状态

    Args:
        post_id: Danbooru图片ID
        mongodb_uri: MongoDB连接URI，如果为None则使用环境变量

    Returns:
        (url, status_code)元组
    """
    checker = DanbooruUrlChecker(mongodb_uri)
    try:
        return checker.get_url_by_id(post_id)
    finally:
        checker.close()

def check_url_by_id_batch(post_ids: List[int], mongodb_uri: Optional[str] = None) -> Dict[int, Dict[str, Any]]:
    """
    批量获取多个ID的URL和状态

    Args:
        post_ids: Danbooru图片ID列表
        mongodb_uri: MongoDB连接URI，如果为None则使用环境变量

    Returns:
        {id: {"url": url, "status": status_code}} 格式的字典
    """
    checker = DanbooruUrlChecker(mongodb_uri)
    try:
        return checker.check_url_by_id_batch(post_ids)
    finally:
        checker.close()

def save_results(results: Dict[int, Dict[str, Any]], output_file: str) -> bool:
    """
    将结果保存到文件（独立函数）

    Args:
        results: 结果字典
        output_file: 输出文件路径

    Returns:
        是否成功保存
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)

        # 保存为JSON文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        logger.info(f"结果已保存到文件: {output_file}")
        return True
    except Exception as e:
        logger.error(f"保存结果到文件失败: {str(e)}")
        return False

if __name__ == "__main__":
    # 从命令行获取参数
    mongo_uri = "mongodb://8.153.97.53:27815/"
    key = 0
    output_file = f"results_{key}.json"

    results = check_urls_by_key(key, mongo_uri, output_file)
    print(f"已完成检查，共处理 {len(results)} 个ID")
