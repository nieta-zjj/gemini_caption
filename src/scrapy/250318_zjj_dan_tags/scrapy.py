import os
os.environ['START_ID'] = '0'
os.environ['END_ID'] = '1000000'

import asyncio
import httpx
import time
from tqdm import tqdm
from motor.motor_asyncio import AsyncIOMotorClient
import logging
import os

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DanbooruTagsFetcher:
    """异步获取Danbooru所有标签并存储到MongoDB的类"""

    def __init__(self, mongodb_uri="mongodb://8.153.97.53:3000/",
                 db_name="danbooru", collection_name="tags",
                 concurrency=10):
        """初始化获取器"""
        self.base_url = "https://danbooru.donmai.us/tags.json"
        self.chunk_size = 1000
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
            await self.collection.create_index([("name", 1)])
            logger.info("MongoDB索引创建成功")
        except Exception as e:
            logger.error(f"MongoDB索引创建失败: {e}")
            raise

    async def get_latest_tag_id(self, client):
        """获取Danbooru最新标签ID"""
        params = {'limit': 1}

        try:
            logger.info(f"正在请求最新标签ID: {self.base_url}?limit=1")
            response = await client.get(self.base_url, params=params, headers=self.headers)

            logger.info(f"API响应状态码: {response.status_code}")
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 30))
                logger.warning(f"请求频率受限，等待 {retry_after} 秒")
                await asyncio.sleep(retry_after)
                return await self.get_latest_tag_id(client)

            response.raise_for_status()
            data = response.json()

            if data and len(data) > 0:
                latest_id = data[0]['id']
                logger.info(f"获取到最新标签ID: {latest_id}")
                return latest_id
            else:
                logger.warning("API返回空数据")
                return 0
        except Exception as e:
            logger.error(f"获取最新标签ID时出错: {e}")
            return 0

    async def get_db_max_id(self):
        """获取数据库中最大的标签ID (异步版本)"""
        try:
            # 确保连接有效
            await self._ensure_connection()

            # 使用异步方式查询
            result = await self.collection.find_one(sort=[("_id", -1)])
            if result:
                logger.info(f"数据库中最大标签ID: {result['_id']}")
                return result['_id']
            else:
                logger.info("数据库为空")
                return 0
        except Exception as e:
            logger.error(f"获取数据库最大ID时出错: {e}")
            return 0

    async def save_batch_to_mongodb(self, tags):
        """批量保存标签到MongoDB (异步版本)"""
        if not tags:
            return

        # 确保连接有效
        await self._ensure_connection()

        operations = []
        for tag in tags:
            try:
                # 确保tag["id"]存在
                if "id" not in tag:
                    logger.warning(f"跳过没有id的标签: {tag}")
                    continue

                # 使用原始id作为MongoDB的_id
                tag_id = tag["id"]

                # 创建一个新字典，将id值赋给_id，并移除原始id字段以避免冗余
                tag_doc = tag.copy()
                tag_doc["_id"] = tag_id
                # 移除原始id字段，因为已经用作_id
                if "id" in tag_doc:
                    del tag_doc["id"]

                # 将文档添加到操作列表
                operations.append(tag_doc)
            except Exception as e:
                logger.error(f"准备MongoDB操作时出错: {e}, 标签: {tag}")

        if operations:
            try:
                # 使用异步方式执行批量插入/更新
                result = await self.collection.insert_many(
                    operations,
                    ordered=False  # 非顺序执行，出错时继续执行其他操作
                )
                logger.debug(f"批量保存成功，插入: {len(result.inserted_ids)} 条记录")
            except Exception as e:
                if "E11000 duplicate key error" in str(e):
                    # 忽略重复键错误，这可能是标签已经存在
                    # 对于已存在的标签，使用更新操作
                    try:
                        bulk_operations = []
                        for tag_doc in operations:
                            bulk_operations.append({
                                "replaceOne": {
                                    "filter": {"_id": tag_doc["_id"]},
                                    "replacement": tag_doc,
                                    "upsert": True
                                }
                            })
                        if bulk_operations:
                            result = await self.collection.bulk_write(bulk_operations)
                            logger.debug(f"批量更新成功，更新/插入: {result.upserted_count + result.modified_count} 条记录")
                    except Exception as update_error:
                        logger.error(f"批量更新标签时出错: {update_error}")
                else:
                    logger.error(f"批量保存标签时出错: {e}")

    async def get_sample_tags(self, limit=5):
        """获取样本标签以显示 (按帖子数量排序)"""
        try:
            # 确保连接有效
            await self._ensure_connection()

            sample_tags = []
            async for tag in self.collection.find().sort("post_count", -1).limit(limit):
                sample_tags.append(tag)
            return sample_tags
        except Exception as e:
            logger.error(f"获取样本标签时出错: {e}")
            return []

    async def close(self):
        """关闭MongoDB连接"""
        if hasattr(self, 'client') and self.client:
            self.client.close()
            logger.info("MongoDB连接已关闭")

    async def fetch_tags(self, start_id=None, end_id=None):
        """
        通用方法：根据ID范围获取标签

        参数:
            start_id: 起始ID。如果为None，将从数据库中最新ID开始增量更新
            end_id: 结束ID。如果为None，将获取最新标签ID

        处理逻辑:
            - 不传参数: 增量更新 (数据库最新ID 到 Danbooru最新ID)
            - 只传end_id: 全量更新 (0 到 end_id)
            - 传start_id和end_id: 指定范围更新 (start_id 到 end_id)
        """
        logger.info("开始异步获取Danbooru标签...")

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
                latest_id = await self.get_latest_tag_id(client)

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
                end_id = await self.get_latest_tag_id(client)
                logger.info(f"获取Danbooru最新ID: {end_id}")

            # 检查是否需要更新
            if start_id >= end_id:
                logger.info(f"无需更新: 起始ID({start_id}) >= 结束ID({end_id})")
                return 0

            # 创建分块
            chunks = []
            for i in range(start_id, end_id, self.chunk_size):
                chunk_start = i
                chunk_end = min(i + self.chunk_size, end_id)
                chunks.append((chunk_start, chunk_end))

            total_chunks = len(chunks)
            logger.info(f"将获取 {total_chunks} 个区块，从ID {start_id} 到 {end_id}")

            # 使用一个进度条显示当前进度
            completed_chunks = 0
            total_tags_fetched = 0
            pbar = tqdm(total=total_chunks, desc=f"进度 (0/{total_chunks})")

            # 限制并发数量
            semaphore = asyncio.Semaphore(self.concurrency)
            # 使用锁防止进度条更新冲突
            progress_lock = asyncio.Lock()

            async def process_chunk(chunk_index, chunk_start, chunk_end):
                nonlocal completed_chunks, total_tags_fetched

                async with semaphore:
                    chunk_id = chunk_index + 1
                    logger.debug(f"开始处理区块 {chunk_id}/{total_chunks} ({chunk_start}-{chunk_end})")

                    # 获取标签批次
                    try:
                        # 构建请求参数 - 使用分页格式 b{id} 按ID获取
                        params = {
                            'limit': self.chunk_size,
                            'page': f'b{chunk_end}'  # 使用b分页获取小于等于指定ID的标签
                        }

                        # 添加短暂延迟防止频率限制
                        await asyncio.sleep(0.5 + (time.time() % 1))

                        response = await client.get(self.base_url, params=params, headers=self.headers)

                        # 处理频率限制
                        if response.status_code == 429:
                            retry_after = int(response.headers.get('Retry-After', 30))
                            logger.warning(f"请求频率受限，等待 {retry_after} 秒")
                            await asyncio.sleep(retry_after)
                            # 重试当前区块
                            return await process_chunk(chunk_index, chunk_start, chunk_end)

                        response.raise_for_status()
                        data = response.json()

                        # 筛选ID在范围内的标签
                        filtered_data = [
                            tag for tag in data
                            if tag.get('id', 0) >= chunk_start and tag.get('id', 0) <= chunk_end
                        ]

                        batch_count = len(filtered_data)
                        logger.debug(f"区块 {chunk_id} 获取到 {batch_count} 条记录")

                        if batch_count > 0:
                            # 保存到数据库
                            await self.save_batch_to_mongodb(filtered_data)

                            # 使用锁安全更新进度
                            async with progress_lock:
                                completed_chunks += 1
                                total_tags_fetched += batch_count
                                pbar.update(1)
                                pbar.set_description(f"进度 ({completed_chunks}/{total_chunks}) 获取: {total_tags_fetched}")

                            await asyncio.sleep(1)  # 添加延迟避免请求过快
                            return batch_count
                        else:
                            # 如果该区块没有数据，仍然更新进度
                            async with progress_lock:
                                completed_chunks += 1
                                pbar.update(1)
                                pbar.set_description(f"进度 ({completed_chunks}/{total_chunks}) 获取: {total_tags_fetched}")
                            return 0

                    except Exception as e:
                        logger.error(f"处理区块 {chunk_id} 时出错: {e}")
                        # 出错时也更新进度
                        async with progress_lock:
                            completed_chunks += 1
                            pbar.update(1)
                            pbar.set_description(f"进度 ({completed_chunks}/{total_chunks}) 获取: {total_tags_fetched}")
                        return 0

            # 先尝试处理少量区块以测试连接
            test_chunks = chunks[:min(5, len(chunks))]
            logger.info(f"先处理 {len(test_chunks)} 个区块进行测试")

            # 创建测试任务
            test_tasks = [
                process_chunk(i, start, end)
                for i, (start, end) in enumerate(test_chunks)
            ]

            # 执行测试任务
            test_results = await asyncio.gather(*test_tasks)
            test_fetched = sum(test_results)
            logger.info(f"测试完成，获取了 {test_fetched} 个标签")

            # 如果测试成功，处理剩余区块
            remaining_chunks = chunks[min(5, len(chunks)):]
            remaining_fetched = 0

            if test_fetched > 0 or len(chunks) <= 5:
                if remaining_chunks:
                    logger.info(f"开始处理剩余 {len(remaining_chunks)} 个区块")

                    # 创建剩余任务
                    remaining_tasks = [
                        process_chunk(i + len(test_chunks), start, end)
                        for i, (start, end) in enumerate(remaining_chunks)
                    ]

                    # 执行剩余任务
                    remaining_results = await asyncio.gather(*remaining_tasks)
                    remaining_fetched = sum(remaining_results)

            # 关闭进度条
            pbar.close()

            # 计算总获取数量
            total_fetched = test_fetched + remaining_fetched
            logger.info(f"总共获取并保存了 {total_fetched} 个标签")

            return total_fetched

async def main_async():
    # 创建获取器实例
    fetcher = None
    try:
        fetcher = DanbooruTagsFetcher(
            mongodb_uri="mongodb://8.153.97.53:3000/",
            db_name="danbooru",
            collection_name="tags",
            concurrency=5  # 降低并发数以减少被封风险
        )

        # 检查环境变量
        env_start_id = os.environ.get('START_ID')
        env_end_id = os.environ.get('END_ID')

        # 如果环境变量存在，则直接使用它们
        if env_start_id is not None or env_end_id is not None:
            try:
                start_id = int(env_start_id) if env_start_id else None
                end_id = int(env_end_id) if env_end_id else None

                # 根据环境变量组合确定操作模式
                if start_id is None and end_id is None:
                    # 不应该发生，因为至少一个变量已设置
                    print("\n环境变量设置不正确，开始增量更新...")
                    total_fetched = await fetcher.fetch_tags()
                elif start_id == 0 and end_id is None:
                    # 全量更新
                    print("\n通过环境变量触发全量更新...")
                    total_fetched = await fetcher.fetch_tags(start_id=0)
                elif start_id is None and end_id is not None:
                    # 从数据库最后一条到指定end_id
                    print(f"\n通过环境变量触发增量更新到指定ID {end_id}...")
                    total_fetched = await fetcher.fetch_tags(end_id=end_id)
                else:
                    # 指定区间更新
                    print(f"\n通过环境变量触发指定区间更新 ({start_id} 到 {end_id})...")
                    total_fetched = await fetcher.fetch_tags(start_id, end_id)
            except ValueError:
                print("环境变量ID必须是整数，回退到交互模式")
                # 回退到交互模式
                env_start_id = env_end_id = None

        # 如果没有环境变量，或者环境变量解析失败，则使用交互模式
        if env_start_id is None and env_end_id is None:
            print("\n选择操作模式:")
            print("1. 增量更新标签 (只获取新增标签)")
            print("2. 全量更新标签 (从头开始)")
            print("3. 获取指定区间的标签")
            choice = input("请输入选项 (1/2/3，默认1): ").strip() or "1"

            if choice == "1":
                # 增量更新 - 不传参数
                print("\n开始增量更新Danbooru标签...")
                total_fetched = await fetcher.fetch_tags()

            elif choice == "2":
                # 全量更新
                print("\n开始全量更新Danbooru标签...")
                total_fetched = await fetcher.fetch_tags(start_id=0)

            elif choice == "3":
                # 获取指定区间
                start_id = input("请输入起始ID: ").strip()
                end_id = input("请输入结束ID: ").strip()

                try:
                    start_id = int(start_id) if start_id else None
                    end_id = int(end_id) if end_id else None

                    print(f"\n开始获取Danbooru标签区间 ({start_id} 到 {end_id})...")
                    total_fetched = await fetcher.fetch_tags(start_id, end_id)
                except ValueError:
                    print("ID必须是整数")
                    return

        if total_fetched > 0:
            print(f"成功获取并保存 {total_fetched} 个标签")
        else:
            print("没有新标签需要更新")

        # 显示样例标签
        print("\n热门标签样例:")
        for tag in await fetcher.get_sample_tags(5):
            print(f"名称: {tag.get('name')}, 类别: {tag.get('category')}, 数量: {tag.get('post_count')}")

        # 获取并显示总标签数
        try:
            tag_count = await fetcher.collection.count_documents({})
            print(f"\n数据库中总标签数: {tag_count}")
        except Exception as e:
            print(f"获取标签总数时出错: {e}")

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