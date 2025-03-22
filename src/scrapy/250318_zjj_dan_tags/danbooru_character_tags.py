import asyncio
import httpx
import time
import math
from tqdm import tqdm
from motor.motor_asyncio import AsyncIOMotorClient
import logging
import sys
import json

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DanbooruTagsFetcher:
    """异步获取Danbooru所有标签并存储到MongoDB的类"""

    def __init__(self, mongodb_uri="mongodb://8.153.97.53:3000/",
                 db_name="danbooru", collection_name="tags",
                 chunk_size=1000, concurrency=10):
        """初始化获取器"""
        self.base_url = "https://danbooru.donmai.us/tags.json"
        self.chunk_size = chunk_size
        self.concurrency = concurrency

        # 请求头 - 简化为基本必需的头信息
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'Accept': 'application/json'
        }

        # 连接MongoDB (使用异步客户端)
        try:
            self.client = AsyncIOMotorClient(mongodb_uri)
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            logger.info("MongoDB异步连接已初始化")
        except Exception as e:
            logger.error(f"MongoDB连接初始化失败: {e}")
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

    async def fetch_tags_by_range(self, client, start_id, end_id):
        """按ID范围获取标签"""
        params = {
            'limit': 200,
            'page': f'b{end_id}'
        }

        try:
            logger.debug(f"请求标签范围 {start_id}-{end_id}: {self.base_url}?limit=200&page=b{end_id}")
            await asyncio.sleep(0.5 + (time.time() % 1))  # 添加短暂延迟

            response = await client.get(self.base_url, params=params, headers=self.headers)

            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 30))
                logger.warning(f"请求频率受限，等待 {retry_after} 秒")
                await asyncio.sleep(retry_after)
                return await self.fetch_tags_by_range(client, start_id, end_id)

            response.raise_for_status()
            data = response.json()
            logger.debug(f"获取到 {len(data)} 条记录")

            # 直接返回原始数据，不进行过滤
            return data
        except Exception as e:
            logger.error(f"获取ID范围 {start_id}-{end_id} 的标签时出错: {e}")
            await asyncio.sleep(5)
            return await self.fetch_tags_by_range(client, start_id, end_id)

    async def process_chunk(self, client, start_id, end_id):
        """处理一个标签ID区块"""
        tags = await self.fetch_tags_by_range(client, start_id, end_id)
        if tags:
            logger.debug(f"保存 {len(tags)} 个标签到MongoDB (ID范围: {start_id}-{end_id})")
            await self.save_batch_to_mongodb(tags)

        await asyncio.sleep(1 + (time.time() % 2))  # 增加随机延迟
        return len(tags)

    async def fetch_all_tags(self):
        """异步获取所有标签"""
        logger.info("开始异步获取所有Danbooru标签...")

        # 确保数据库已初始化
        await self.initialize_db()

        # 配置httpx客户端
        async with httpx.AsyncClient(
            timeout=60.0,
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=30)
        ) as client:
            # 获取Danbooru最新标签ID
            latest_id = await self.get_latest_tag_id(client)
            logger.info(f"Danbooru最新标签ID: {latest_id}")

            # 获取数据库中最大的标签ID
            db_max_id = await self.get_db_max_id()
            logger.info(f"数据库中最大标签ID: {db_max_id}")

            # 如果数据库是空的或者ID比最新的小，则需要更新
            if db_max_id < latest_id:
                # 计算需要获取的范围
                start_id = db_max_id
                end_id = latest_id

                # 创建分块
                chunks = []
                for i in range(start_id, end_id, self.chunk_size):
                    chunk_start = i
                    chunk_end = min(i + self.chunk_size, end_id)
                    chunks.append((chunk_start, chunk_end))

                total_chunks = len(chunks)
                logger.info(f"将获取 {total_chunks} 个区块，从ID {start_id} 到 {end_id}")

                # 改进进度显示 - 使用一个进度条，只用数字显示当前区块
                completed_chunks = 0
                total_tags_fetched = 0
                pbar = tqdm(total=total_chunks, desc=f"进度 (0/{total_chunks})")

                # 限制并发数量
                semaphore = asyncio.Semaphore(self.concurrency)
                # 使用锁防止进度条更新冲突
                progress_lock = asyncio.Lock()

                async def process_with_semaphore(chunk_index, chunk_start, chunk_end):
                    nonlocal completed_chunks, total_tags_fetched

                    async with semaphore:
                        chunk_id = chunk_index + 1
                        logger.debug(f"开始处理区块 {chunk_id}/{total_chunks} ({chunk_start}-{chunk_end})")

                        # 处理区块
                        tags_count = await self.process_chunk(client, chunk_start, chunk_end)

                        # 使用锁安全更新进度
                        async with progress_lock:
                            completed_chunks += 1
                            total_tags_fetched += tags_count
                            pbar.update(1)
                            pbar.set_description(f"进度 ({completed_chunks}/{total_chunks})")

                        return tags_count

                # 创建任务
                tasks = [
                    process_with_semaphore(i, chunk_start, chunk_end)
                    for i, (chunk_start, chunk_end) in enumerate(chunks)
                ]

                # 执行任务
                results = await asyncio.gather(*tasks)

                # 关闭进度条
                pbar.close()

                # 获取总数
                total_fetched = sum(results)
                logger.info(f"总共获取并保存了 {total_fetched} 个标签")
                return total_fetched
            else:
                logger.info("数据库已是最新，无需更新")
                return 0

    async def save_batch_to_mongodb(self, tags):
        """批量保存标签到MongoDB (异步版本)"""
        if not tags:
            return

        operations = []
        for tag in tags:
            try:
                # 确保tag["id"]存在
                if "id" not in tag:
                    logger.warning(f"跳过没有id的标签: {tag}")
                    continue

                # 使用原始id作为MongoDB的_id
                tag_id = tag["id"]

                # 创建一个新字典，将id值赋给_id，保留原始id字段
                tag_doc = tag.copy()
                tag_doc["_id"] = tag_id

                # 将文档添加到操作列表
                operations.append(tag_doc)
            except Exception as e:
                logger.error(f"准备MongoDB操作时出错: {e}, 标签: {tag}")

        if operations:
            try:
                # 异步批量插入
                await self.collection.bulk_write([
                    {
                        'updateOne': {
                            'filter': {'_id': doc['_id']},
                            'update': {'$set': doc},
                            'upsert': True
                        }
                    }
                    for doc in operations
                ])
                logger.debug(f"MongoDB操作: 批量处理 {len(operations)} 条记录")
            except Exception as e:
                logger.error(f"MongoDB批量写入错误: {e}")
                # 尝试一个一个处理
                success_count = 0
                for tag in tags:
                    try:
                        if "id" not in tag:
                            continue

                        tag_id = tag["id"]
                        tag_doc = tag.copy()
                        tag_doc["_id"] = tag_id

                        # 异步单条更新
                        await self.collection.update_one(
                            {"_id": tag_id},
                            {"$set": tag_doc},
                            upsert=True
                        )
                        success_count += 1
                    except Exception as inner_e:
                        logger.error(f"单条更新错误 (ID: {tag.get('id')}): {inner_e}")

                logger.info(f"单条更新完成，成功处理 {success_count}/{len(tags)} 条记录")

    async def get_sample_tags(self, limit=5):
        """获取样例标签 (异步版本)"""
        try:
            # 使用异步方式获取最常用的标签作为样例
            cursor = self.collection.find().sort("post_count", -1).limit(limit)
            samples = await cursor.to_list(length=limit)
            return samples
        except Exception as e:
            logger.error(f"获取样例标签时出错: {e}")
            return []

    async def close(self):
        """关闭MongoDB连接 (异步版本)"""
        if hasattr(self, 'client') and self.client:
            self.client.close()
            logger.info("已关闭MongoDB连接")

    async def fetch_all_tags_from_scratch(self, start_offset=0):
        """
        从头开始并发获取所有标签，使用a{offset}分页参数，从0直接获取到最大ID

        参数:
            start_offset: 起始偏移量，默认为0
        """
        logger.info(f"开始从offset={start_offset}并发重新获取所有Danbooru标签...")

        # 确保数据库已初始化
        try:
            await self.initialize_db()
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            return 0

        # 配置httpx客户端
        async with httpx.AsyncClient(
            timeout=30.0,  # 降低超时时间更快检测到问题
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=30)
        ) as client:
            # 先获取最新标签ID，以确定需要获取的范围
            try:
                logger.info("正在获取最新标签ID...")
                latest_id = await self.get_latest_tag_id(client)
                logger.info(f"Danbooru最新标签ID: {latest_id}")

                if latest_id == 0:
                    logger.error("无法获取最新标签ID，退出")
                    return 0
            except Exception as e:
                logger.error(f"获取最新标签ID时出错: {e}")
                return 0

            try:
                # 创建分块
                batch_size = 1000  # Danbooru API最大允许的limit值
                chunks = []

                # 从start_offset开始，直到最新标签的offset
                # 这里我们粗略地估计offset最大值为最大ID
                max_offset = max(latest_id - batch_size, 0)  # 确保不会出现负数

                for offset in range(start_offset, max_offset + batch_size, batch_size):
                    chunks.append(offset)

                total_chunks = len(chunks)
                logger.info(f"将并发获取 {total_chunks} 个区块，从offset={start_offset}到最大offset={max_offset}")

                # 测试单个请求，确认API和参数正确
                test_offset = chunks[0] if chunks else start_offset
                logger.info(f"测试请求单个区块 (offset={test_offset})...")

                test_params = {
                    'limit': batch_size,
                    'page': f'a{test_offset}'
                }

                test_response = await client.get(self.base_url, params=test_params, headers=self.headers)
                test_response.raise_for_status()
                test_data = test_response.json()

                logger.info(f"测试请求成功，获取到 {len(test_data)} 条记录")

                # 如果测试请求成功，继续执行
                # 创建进度条
                completed_chunks = 0
                total_tags_fetched = 0
                pbar = tqdm(total=total_chunks, desc=f"进度 (0/{total_chunks})")

                # 限制并发数量，降低并发数以减少可能的问题
                semaphore = asyncio.Semaphore(min(5, self.concurrency))  # 降低并发数
                # 使用锁防止进度条更新冲突
                progress_lock = asyncio.Lock()

                async def process_offset_chunk(chunk_index, offset):
                    nonlocal completed_chunks, total_tags_fetched

                    async with semaphore:
                        chunk_id = chunk_index + 1
                        logger.info(f"开始处理offset区块 {chunk_id}/{total_chunks} (offset={offset})")

                        try:
                            # 构建请求参数
                            params = {
                                'limit': batch_size,
                                'page': f'a{offset}'
                            }

                            # 添加短暂延迟
                            await asyncio.sleep(0.5 + (time.time() % 1))

                            response = await client.get(self.base_url, params=params, headers=self.headers)

                            if response.status_code == 429:
                                retry_after = int(response.headers.get('Retry-After', 30))
                                logger.warning(f"请求频率受限，等待 {retry_after} 秒")
                                await asyncio.sleep(retry_after)
                                # 重试
                                return await process_offset_chunk(chunk_index, offset)

                            response.raise_for_status()
                            data = response.json()

                            # 检查是否获取到数据
                            batch_count = len(data)
                            logger.info(f"区块 {chunk_id} 获取到 {batch_count} 条记录")

                            if batch_count > 0:
                                # 保存到数据库
                                try:
                                    await self.save_batch_to_mongodb(data)
                                    logger.info(f"区块 {chunk_id} 数据已保存到数据库")
                                except Exception as e:
                                    logger.error(f"保存数据时出错: {e}")

                                # 使用锁安全更新进度
                                async with progress_lock:
                                    completed_chunks += 1
                                    total_tags_fetched += batch_count
                                    pbar.update(1)
                                    pbar.set_description(f"进度 ({completed_chunks}/{total_chunks}) 获取: {total_tags_fetched}")

                                return batch_count
                            else:
                                logger.info(f"区块 {chunk_id} 没有数据")
                                # 使用锁安全更新进度
                                async with progress_lock:
                                    completed_chunks += 1
                                    pbar.update(1)
                                    pbar.set_description(f"进度 ({completed_chunks}/{total_chunks}) 获取: {total_tags_fetched}")

                                return 0

                        except Exception as e:
                            logger.error(f"获取标签时出错 (offset={offset}): {e}")
                            # 使用锁安全更新进度
                            async with progress_lock:
                                completed_chunks += 1
                                pbar.update(1)
                                pbar.set_description(f"进度 ({completed_chunks}/{total_chunks}) 获取: {total_tags_fetched}")
                            return 0

                # 为了防止卡住，先处理少量区块测试
                initial_chunks = chunks[:min(5, len(chunks))]
                logger.info(f"先处理 {len(initial_chunks)} 个区块进行测试")

                # 创建初始任务
                initial_tasks = [
                    process_offset_chunk(i, offset)
                    for i, offset in enumerate(initial_chunks)
                ]

                # 执行初始任务
                initial_results = await asyncio.gather(*initial_tasks)
                initial_fetched = sum(filter(None, initial_results))
                logger.info(f"初始测试完成，获取了 {initial_fetched} 个标签")

                # 如果初始测试成功，处理剩余区块
                if initial_fetched > 0 or len(chunks) <= 5:
                    remaining_chunks = chunks[min(5, len(chunks)):]

                    if remaining_chunks:
                        logger.info(f"开始处理剩余 {len(remaining_chunks)} 个区块")

                        # 创建剩余任务
                        remaining_tasks = [
                            process_offset_chunk(i + min(5, len(chunks)), offset)
                            for i, offset in enumerate(remaining_chunks)
                        ]

                        # 执行剩余任务
                        remaining_results = await asyncio.gather(*remaining_tasks)
                        remaining_fetched = sum(filter(None, remaining_results))

                        total_fetched = initial_fetched + remaining_fetched
                    else:
                        total_fetched = initial_fetched
                else:
                    logger.error("初始测试未获取到标签，放弃处理剩余区块")
                    total_fetched = initial_fetched

                # 关闭进度条
                pbar.close()

                logger.info(f"总共并发重新获取并保存了 {total_fetched} 个标签")
                return total_fetched

            except Exception as e:
                logger.error(f"获取标签过程中发生错误: {e}")
                import traceback
                traceback.print_exc()
                return 0

async def main_async():
    # 创建获取器实例
    fetcher = None
    try:
        fetcher = DanbooruTagsFetcher(
            mongodb_uri="mongodb://8.153.97.53:3000/",
            db_name="danbooru",
            collection_name="tags",
            chunk_size=1000,
            concurrency=10
        )

        print("\n选择操作模式:")
        print("1. 增量更新标签 (只获取新增标签)")
        print("2. 重新获取所有标签 (从头开始)")
        # choice = input("请输入选项 (1/2，默认1): ").strip() or "1"
        choice = "2"
        if choice == "1":
            print("\n开始增量更新Danbooru标签...")
            total_fetched = await fetcher.fetch_all_tags()
        elif choice == "2":
            start_offset = input("请输入起始偏移量 (默认0): ").strip() or "0"
            try:
                start_offset = int(start_offset)
            except ValueError:
                print("无效的偏移量，使用默认值0")
                start_offset = 0

            print(f"\n开始从offset={start_offset}重新获取所有Danbooru标签...")
            total_fetched = await fetcher.fetch_all_tags_from_scratch(start_offset)
        else:
            print("无效的选项，使用默认的增量更新")
            total_fetched = await fetcher.fetch_all_tags()

        if total_fetched > 0:
            print(f"成功获取并保存 {total_fetched} 个标签")

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
    """
    自适应运行主异步函数
    - 如果在已有事件循环的环境(如Jupyter)中运行，则使用当前事件循环
    - 否则创建新的事件循环
    """
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