import json
from datetime import datetime
import os
os.environ['START_ID'] = '0'
os.environ['END_ID'] = '8400000'

import asyncio
import httpx
import time
from tqdm import tqdm
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.operations import UpdateOne
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_post(post_data):
    """
    将原始的帖子数据转换为目标格式

    Args:
        post_data (dict): 原始帖子数据

    Returns:
        dict: 转换后的帖子数据
    """
    # 转换日期时间格式
    created_at = None
    if post_data.get("created_at"):
        try:
            dt = datetime.fromisoformat(post_data["created_at"].replace("Z", "+00:00"))
            created_at = dt.strftime("%Y%m%d%H%M%S")
        except Exception:
            created_at = None

    # 拆分标签字符串为列表
    general_tags = post_data.get("tag_string_general", "").split() if post_data.get("tag_string_general") else None
    character_tags = post_data.get("tag_string_character", "").split() if post_data.get("tag_string_character") else None
    copyright_tags = post_data.get("tag_string_copyright", "").split() if post_data.get("tag_string_copyright") else None
    artist_tags = post_data.get("tag_string_artist", "").split() if post_data.get("tag_string_artist") else None
    meta_tags = post_data.get("tag_string_meta", "").split() if post_data.get("tag_string_meta") else None

    # 构建转换后的数据
    transformed_data = {
        "_id": post_data.get("id"),
        "created_at": created_at,
        "score": post_data.get("score", 0),
        "source": post_data.get("source"),
        "md5": post_data.get("md5"),
        "rating": post_data.get("rating"),
        "width": post_data.get("image_width"),
        "height": post_data.get("image_height"),
        "fav_count": post_data.get("fav_count", 0),
        "file_ext": post_data.get("file_ext"),
        "parent_id": post_data.get("parent_id"),
        "has_children": post_data.get("has_children"),
        "file_size": post_data.get("file_size"),
        "pixiv_id": post_data.get("pixiv_id"),
        "general_tags": general_tags,
        "character_tags": character_tags,
        "copyright_tags": copyright_tags,
        "artist_tags": artist_tags,
        "meta_tags": meta_tags,
        "status": 0
    }

    return transformed_data

class DanbooruPostsFetcher:
    """异步获取Danbooru所有帖子并存储到MongoDB的类"""

    def __init__(self, mongodb_uri="mongodb://shiertier:20000418Nuo.@localhost:27000/",
                 db_name="danbooru", collection_name="pics",
                 concurrency=10):
        """初始化获取器"""
        self.base_url = "https://danbooru.donmai.us/posts.json"
        self.page_size = 200  # 每页200条帖子
        self.concurrency = concurrency  # 并发请求数

        # 请求头 - 简化为基本必需的头信息
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'Accept': 'application/json'
        }

        # 连接MongoDB (使用异步客户端)
        self.mongodb_uri = mongodb_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None

        # 初始化MongoDB连接
        self._init_mongodb_connection()

    def _init_mongodb_connection(self):
        """初始化MongoDB连接函数，便于重连"""
        try:
            # 如果之前有连接，先关闭
            if self.client:
                self.client.close()

            # 创建新连接
            self.client = AsyncIOMotorClient(
                self.mongodb_uri,
                serverSelectionTimeoutMS=5000,  # 服务器选择超时
                connectTimeoutMS=10000,         # 连接超时
                socketTimeoutMS=30000,          # 套接字操作超时
                maxPoolSize=20,                 # 最大连接池大小
                waitQueueTimeoutMS=10000,       # 连接池队列等待超时
                retryWrites=True                # 开启写入重试
            )
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            logger.info("MongoDB异步连接已初始化")
        except Exception as e:
            logger.error(f"MongoDB连接初始化失败: {e}")
            raise

    async def _ensure_connection(self):
        """确保MongoDB连接有效，如果无效则重新连接"""
        try:
            # 尝试执行简单命令测试连接
            await self.db.command("ping")
        except Exception as e:
            logger.warning(f"MongoDB连接检测失败，尝试重新连接: {e}")
            self._init_mongodb_connection()
            # 再次测试连接
            try:
                await self.db.command("ping")
                logger.info("MongoDB重新连接成功")
            except Exception as e:
                logger.error(f"MongoDB重新连接失败: {e}")
                raise

    async def initialize_db(self):
        """初始化数据库，创建索引"""
        try:
            # 使用异步方式创建索引
            await self.collection.create_index([("_id", 1)])
            logger.info("MongoDB索引创建成功")
        except Exception as e:
            logger.error(f"MongoDB索引创建失败: {e}")
            raise

    async def get_latest_post_id(self, client):
        """获取Danbooru最新帖子ID"""
        params = {'limit': 1}

        try:
            logger.info(f"正在请求最新帖子ID: {self.base_url}?limit=1")
            response = await client.get(self.base_url, params=params, headers=self.headers)

            logger.info(f"API响应状态码: {response.status_code}")
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 30))
                logger.warning(f"请求频率受限，等待 {retry_after} 秒")
                await asyncio.sleep(retry_after)
                return await self.get_latest_post_id(client)

            response.raise_for_status()
            data = response.json()

            if data and len(data) > 0:
                latest_id = data[0]['id']
                logger.info(f"获取到最新帖子ID: {latest_id}")
                return latest_id
            else:
                logger.warning("API返回空数据")
                return 0
        except Exception as e:
            logger.error(f"获取最新帖子ID时出错: {e}")
            return 0

    async def get_db_max_id(self):
        """获取数据库中最大的帖子ID (异步版本)"""
        try:
            # 确保连接有效
            await self._ensure_connection()

            # 使用异步方式查询
            result = await self.collection.find_one(sort=[("_id", -1)])
            if result:
                logger.info(f"数据库中最大帖子ID: {result['_id']}")
                return result['_id']
            else:
                logger.info("数据库为空")
                return 0
        except Exception as e:
            logger.error(f"获取数据库最大ID时出错: {e}")
            return 0

    async def save_batch_to_mongodb(self, posts):
        """批量保存帖子到MongoDB (异步版本)"""
        if not posts:
            return

        # 确保连接有效
        await self._ensure_connection()

        bulk_operations = []
        for post in posts:
            try:
                # 确保post["id"]存在
                if "id" not in post:
                    logger.warning(f"跳过没有id的帖子: {post}")
                    continue

                # 使用transform_post函数转换数据格式
                transformed_post = transform_post(post)

                # 创建UpdateOne操作
                bulk_operations.append(
                    UpdateOne(
                        {"_id": transformed_post["_id"]},
                        {"$set": transformed_post},
                        upsert=True
                    )
                )
            except Exception as e:
                logger.error(f"转换帖子时出错: {e}, 帖子: {post}")

        if bulk_operations:
            try:
                # 执行批量操作
                result = await self.collection.bulk_write(bulk_operations, ordered=False)
                logger.debug(f"批量更新成功，更新: {result.modified_count}，插入: {result.upserted_count} 条记录")
                return result.modified_count + result.upserted_count
            except Exception as e:
                logger.error(f"批量更新帖子时出错: {e}")
                return 0
        return 0

    async def get_sample_posts(self, limit=5):
        """获取样本帖子以显示 (按分数排序)"""
        try:
            # 确保连接有效
            await self._ensure_connection()

            sample_posts = []
            async for post in self.collection.find().sort("score", -1).limit(limit):
                sample_posts.append(post)
            return sample_posts
        except Exception as e:
            logger.error(f"获取样本帖子时出错: {e}")
            return []

    async def close(self):
        """关闭MongoDB连接"""
        if hasattr(self, 'client') and self.client:
            self.client.close()
            logger.info("MongoDB连接已关闭")

    async def fetch_posts(self, start_id=None, end_id=None):
        """
        通用方法：根据ID范围获取帖子

        参数:
            start_id: 起始ID。如果为None，将从数据库中最新ID开始增量更新
            end_id: 结束ID。如果为None，将获取最新帖子ID

        处理逻辑:
            - 不传参数: 增量更新 (数据库最新ID 到 Danbooru最新ID)
            - 只传end_id: 全量更新 (0 到 end_id)
            - 传start_id和end_id: 指定范围更新 (start_id 到 end_id)
        """
        logger.info("开始异步获取Danbooru帖子...")

        # 确保数据库已初始化
        await self.initialize_db()

        # 配置httpx客户端
        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=30)
        ) as client:
            # 确定获取范围
            if start_id is None and end_id is None:
                # 增量更新 - 从数据库最新ID到Danbooru最新ID
                db_max_id = await self.get_db_max_id()
                latest_id = await self.get_latest_post_id(client)

                start_id = db_max_id
                end_id = latest_id

                logger.info(f"执行增量更新: {start_id} -> {end_id}")

            elif start_id is None and end_id is not None:
                # 全量更新 - 从0到指定end_id
                start_id = 0
                logger.info(f"执行全量更新: 0 -> {end_id}")

            else:
                # 指定范围更新
                logger.info(f"执行指定范围更新: {start_id} -> {end_id}")

            # 如果end_id未指定，获取当前最新ID
            if end_id is None:
                end_id = await self.get_latest_post_id(client)
                logger.info(f"获取Danbooru最新ID: {end_id}")

            # 检查是否需要更新
            if start_id >= end_id:
                logger.info(f"无需更新: 起始ID({start_id}) >= 结束ID({end_id})")
                return 0

            # 创建分块
            pages = []  # 存储页面标识符
            current_id = end_id
            while current_id > start_id:
                pages.append(current_id)
                current_id -= self.page_size
                if current_id < start_id:
                    break

            total_pages = len(pages)
            logger.info(f"将获取 {total_pages} 个页面，从ID {end_id} 到 {start_id}")

            # 使用一个进度条显示当前进度
            completed_pages = 0
            total_posts_fetched = 0
            pbar = tqdm(total=total_pages, desc=f"进度 (0/{total_pages})")

            # 限制并发数量
            semaphore = asyncio.Semaphore(self.concurrency)
            # 使用锁防止进度条更新冲突
            progress_lock = asyncio.Lock()

            async def process_page(page_index, page_id):
                nonlocal completed_pages, total_posts_fetched

                async with semaphore:
                    page_num = page_index + 1
                    logger.debug(f"开始处理页面 {page_num}/{total_pages} (ID: {page_id})")

                    # 获取帖子批次
                    try:
                        # 构建请求参数 - 使用分页格式 a{id} 按ID获取
                        params = {
                            'limit': self.page_size,
                            'page': f'a{page_id}'  # 使用a分页获取小于等于指定ID的帖子
                        }

                        # 添加短暂延迟防止频率限制
                        await asyncio.sleep(0.5 + (time.time() % 1))

                        response = await client.get(self.base_url, params=params, headers=self.headers)

                        # 处理频率限制
                        if response.status_code == 429:
                            retry_after = int(response.headers.get('Retry-After', 30))
                            logger.warning(f"请求频率受限，等待 {retry_after} 秒")
                            await asyncio.sleep(retry_after)
                            # 重试当前页面
                            return await process_page(page_index, page_id)

                        response.raise_for_status()
                        data = response.json()

                        # 筛选ID大于起始ID的帖子
                        filtered_data = [
                            post for post in data
                            if post.get('id', 0) >= start_id
                        ]

                        batch_count = len(filtered_data)
                        logger.debug(f"页面 {page_num} 获取到 {batch_count} 条记录")

                        if batch_count > 0:
                            # 保存到数据库
                            saved_count = await self.save_batch_to_mongodb(filtered_data)

                            # 使用锁安全更新进度
                            async with progress_lock:
                                completed_pages += 1
                                total_posts_fetched += saved_count
                                pbar.update(1)
                                pbar.set_description(f"进度 ({completed_pages}/{total_pages}) 获取: {total_posts_fetched}")

                            await asyncio.sleep(1)  # 添加延迟避免请求过快
                            return saved_count
                        else:
                            # 如果该页面没有数据，仍然更新进度
                            async with progress_lock:
                                completed_pages += 1
                                pbar.update(1)
                                pbar.set_description(f"进度 ({completed_pages}/{total_pages}) 获取: {total_posts_fetched}")
                            return 0

                    except Exception as e:
                        logger.error(f"处理页面 {page_num} 时出错: {e}")
                        # 出错时也更新进度
                        async with progress_lock:
                            completed_pages += 1
                            pbar.update(1)
                            pbar.set_description(f"进度 ({completed_pages}/{total_pages}) 获取: {total_posts_fetched}")
                        return 0

            # 处理所有页面
            logger.info(f"开始处理 {len(pages)} 个页面")

            # 创建所有页面的任务
            tasks = [
                process_page(i, page_id)
                for i, page_id in enumerate(pages)
            ]

            # 执行所有任务
            results = await asyncio.gather(*tasks)
            total_fetched = sum(results)

            # 关闭进度条
            pbar.close()

            logger.info(f"总共获取并保存了 {total_fetched} 个帖子")

            return total_fetched

async def main_async():
    # 创建获取器实例
    fetcher = None
    try:
        fetcher = DanbooruPostsFetcher(
            mongodb_uri="mongodb://shiertier:20000418Nuo.@localhost:27000/",
            db_name="danbooru",
            collection_name="pics",
            concurrency=5  # 降低并发数以减少被封风险
        )

        print("\n选择操作模式:")
        print("1. 增量更新帖子 (只获取新增帖子)")
        print("2. 全量更新帖子 (从头开始)")
        print("3. 获取指定区间的帖子")
        choice = input("请输入选项 (1/2/3，默认1): ").strip() or "1"

        if choice == "1":
            # 增量更新 - 不传参数
            print("\n开始增量更新Danbooru帖子...")
            total_fetched = await fetcher.fetch_posts()

        elif choice == "2":
            # 全量更新
            print("\n开始全量更新Danbooru帖子...")
            total_fetched = await fetcher.fetch_posts(start_id=0)

        elif choice == "3":
            # 获取指定区间
            start_id = input("请输入起始ID: ").strip()
            end_id = input("请输入结束ID: ").strip()

            try:
                start_id = int(start_id) if start_id else None
                end_id = int(end_id) if end_id else None

                print(f"\n开始获取Danbooru帖子区间 ({start_id} 到 {end_id})...")
                total_fetched = await fetcher.fetch_posts(start_id, end_id)
            except ValueError:
                print("ID必须是整数")
                return

        if total_fetched > 0:
            print(f"成功获取并保存 {total_fetched} 个帖子")
        else:
            print("没有新帖子需要更新")

        # 显示样例帖子
        print("\n高分帖子样例:")
        for post in await fetcher.get_sample_posts(5):
            print(f"ID: {post.get('_id')}, 分数: {post.get('score')}, 分类: {post.get('rating')}")

        # 获取并显示总帖子数
        try:
            post_count = await fetcher.collection.count_documents({})
            print(f"\n数据库中总帖子数: {post_count}")
        except Exception as e:
            print(f"获取帖子总数时出错: {e}")

    except Exception as e:
        logger.error(f"主异步函数执行错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if fetcher:
            await fetcher.close()

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