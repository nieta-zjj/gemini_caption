import requests
import logging
import asyncio
import httpx
from typing import Dict, List, Any, Optional, Set
import time
import random

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DanbooruRelatedTagsFetcher:
    """处理Danbooru相关标签并建立层级关系的类"""

    def __init__(self, overlap_threshold: float = 0.9995, categories: List[int] = None,
                max_retries: int = 3, retry_delay: float = 1.0):
        """
        初始化获取器

        参数:
            overlap_threshold: 确定父子关系的重叠系数阈值
            categories: 要查询的标签分类列表
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
        """
        self.overlap_threshold = overlap_threshold
        self.base_url = "https://danbooru.donmai.us/related_tag.json"
        self.categories = categories or [0, 1, 3, 4]  # 默认查询的分类
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    async def get_related_tags_async(self, tag_name: str) -> Dict[str, Any]:
        """
        异步获取标签的相关标签并处理关系

        参数:
            tag_name: 要查询的标签名称

        返回:
            包含标签及其关系的字典
        """
        try:
            # 使用 httpx 替代 aiohttp
            async with httpx.AsyncClient(timeout=60.0) as client:
                # 并行请求所有分类，增加重试机制
                tasks = [self._fetch_category_with_retry(client, tag_name, category)
                         for category in self.categories]
                results = await asyncio.gather(*tasks)

            # 合并所有分类结果
            main_tag = None
            all_related_tags = []

            # 提取主标签信息和所有相关标签
            for result in results:
                if result and 'query' in result:
                    # 查找主标签信息（如果还没找到）
                    if not main_tag:
                        for tag in result.get('related_tags', []):
                            if tag.get('tag', {}).get('name') == result.get('query'):
                                main_tag = tag.get('tag')
                                break

                    # 合并相关标签
                    all_related_tags.extend(result.get('related_tags', []))

            # 如果没找到主标签，记录警告
            if not main_tag:
                logger.warning(f"无法从响应中找到主标签 '{tag_name}' 的信息")
                # 创建一个基本的主标签信息
                main_tag = {"name": tag_name}

            # 去重相关标签（基于id）
            unique_tags = {}
            for tag in all_related_tags:
                tag_id = tag.get('tag', {}).get('id')
                # 如果是新的标签或有更高的相关性，则更新
                if tag_id not in unique_tags or tag.get('overlap_coefficient', 0) > unique_tags[tag_id].get('overlap_coefficient', 0):
                    unique_tags[tag_id] = tag

            # 处理关系
            return self._process_relationships(main_tag, list(unique_tags.values()))

        except Exception as e:
            logger.error(f"异步获取相关标签时出错: {e}")
            return {            }

    async def _fetch_category_with_retry(self, client, tag_name: str, category: int) -> Dict[str, Any]:
        """带重试机制的异步获取特定分类的相关标签"""
        retry_count = 0
        url = f"{self.base_url}?query={tag_name}&category={category}"

        while retry_count < self.max_retries:
            try:
                # 增加随机延迟，避免所有请求同时发送
                if retry_count > 0:
                    delay = self.retry_delay * (1 + random.random())
                    await asyncio.sleep(delay)

                response = await client.get(url)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # 太多请求
                    logger.warning(f"分类 {category} 请求频率过高，等待后重试")
                    await asyncio.sleep(2 + random.random() * 3)  # 更长的等待时间
                else:
                    logger.warning(f"获取分类 {category} 的标签失败，状态码: {response.status_code}")

            except httpx.TimeoutException:
                logger.warning(f"获取分类 {category} 的标签超时，重试中...")
            except Exception as e:
                logger.error(f"获取分类 {category} 的标签时出错: {e}")

            retry_count += 1

        logger.error(f"获取分类 {category} 的标签失败，已达到最大重试次数")
        return None

    def get_related_tags(self, tag_name: str) -> Dict[str, Any]:
        """
        同步获取标签的相关标签并处理关系（调用异步方法）

        参数:
            tag_name: 要查询的标签名称

        返回:
            包含标签及其关系的字典
        """
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # 如果没有事件循环，创建一个新的
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop.run_until_complete(self.get_related_tags_async(tag_name))

    def _process_relationships(self, main_tag: Optional[Dict[str, Any]],
                              related_tags: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        处理标签关系，确定父子关系和相关性

        参数:
            main_tag: 主标签信息
            related_tags: 相关标签列表

        返回:
            包含处理后关系的字典
        """
        if not main_tag:
            return {}

        # 准备结果结构
        result = {
            "_id": main_tag.get('id'),
            "parents": [],
            "children": [],
            "related": []
        }

        # 主标签的分类和帖子数
        main_category = main_tag.get('category')
        main_post_count = main_tag.get('post_count', 0)

        # 临时存储关系数据
        temp_parents = []
        temp_children = []
        temp_related = []

        # 分析每个相关标签
        for related in related_tags:
            # 跳过自身
            if related.get('tag', {}).get('id') == main_tag.get('id'):
                continue

            related_tag = related.get('tag', {})
            overlap = related.get('overlap_coefficient', 0)

            # 创建关系项 - 对于related使用新的格式
            related_item = {
                "id": related_tag.get('id'),
                "name": related_tag.get('name'),
                "category": related_tag.get('category'),
                "overlap_coefficient": round(overlap, 5),
                "cosine_similarity": round(related.get('cosine_similarity', 0), 5),
                "jaccard_similarity": round(related.get('jaccard_similarity', 0), 5),
                "frequency": round(related.get('frequency', 0), 5)
            }

            # 将所有标签添加到相关列表
            temp_related.append(related_item)

            # 确定关系类型 - 只有相同分类才建立父子关系
            if overlap >= self.overlap_threshold and related_tag.get('category') == main_category:
                # 判断父子关系方向
                if related_tag.get('post_count', 0) > main_post_count:
                    # 当前标签是子集，添加到父级 - 只保存标签名称
                    temp_parents.append(related_tag.get('name'))
                else:
                    # 当前标签是父级，添加到子集 - 只保存标签名称
                    temp_children.append(related_tag.get('name'))

        # 对related按overlap_coefficient排序
        temp_related = sorted(temp_related, key=lambda x: x['overlap_coefficient'], reverse=True)

        # 更新结果
        result['parents'] = temp_parents
        result['children'] = temp_children
        result['related'] = temp_related

        return result


async def get_related_tags_async(tag: str, process_relationships: bool = False,
                           overlap_threshold: float = 0.9995,
                           categories: List[int] = None,
                           max_retries: int = 3) -> Dict[str, Any]:
    """
    异步获取标签的相关标签

    参数:
        tag: 要查询的标签名称
        process_relationships: 是否处理并返回层级关系
        overlap_threshold: 关系阈值
        categories: 要查询的分类列表
        max_retries: 最大重试次数

    返回:
        相关标签列表或包含关系的字典
    """
    try:
        if process_relationships:
            # 使用新的类处理关系
            fetcher = DanbooruRelatedTagsFetcher(
                overlap_threshold=overlap_threshold,
                categories=categories,
                max_retries=max_retries
            )
            return await fetcher.get_related_tags_async(tag)
        else:
            # 异步获取所有分类的标签
            categories = categories or [0, 1, 3, 4]
            results = []

            # 使用 httpx 替代 aiohttp
            async with httpx.AsyncClient(timeout=60.0) as client:
                tasks = []
                for category in categories:
                    url = f"https://danbooru.donmai.us/related_tag.json?query={tag}&category={category}"
                    tasks.append(_fetch_category_simple_with_retry(client, url, max_retries))

                category_results = await asyncio.gather(*tasks)

                # 合并结果并去重
                seen_ids = set()
                for category_result in category_results:
                    if not category_result:
                        continue

                    for tag_data in category_result.get('related_tags', []):
                        tag_id = tag_data['tag']['id']
                        if tag_id not in seen_ids:
                            seen_ids.add(tag_id)
                            data = {
                                'id': tag_data['tag']['id'],
                                'name': tag_data['tag']['name'],
                                'category': tag_data['tag']['category'],
                                "is_deprecated": tag_data['tag']['is_deprecated'],
                                "cosine_similarity": round(tag_data['cosine_similarity'], 5),
                                "jaccard_similarity": round(tag_data['jaccard_similarity'], 5),
                                "overlap_coefficient": round(tag_data['overlap_coefficient'], 5),
                                "frequency": round(tag_data['frequency'], 5),
                            }
                            results.append(data)

            # 按overlap_coefficient排序
            return sorted(results, key=lambda x: x['overlap_coefficient'], reverse=True)

    except Exception as e:
        logger.error(f"异步获取相关标签时出错: {e}")
        return []


async def _fetch_category_simple_with_retry(client, url: str, max_retries: int = 3) -> Dict[str, Any]:
    """带重试机制的简单异步获取函数"""
    retry_count = 0

    while retry_count < max_retries:
        try:
            # 添加随机延迟，避免所有请求同时发送
            if retry_count > 0:
                await asyncio.sleep(1 + random.random() * 2)

            response = await client.get(url)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # 太多请求
                await asyncio.sleep(2 + random.random() * 3)  # 更长的等待时间
            else:
                logger.warning(f"获取URL {url} 失败，状态码: {response.status_code}")

        except httpx.TimeoutException:
            logger.warning(f"获取URL {url} 超时，重试中...")
        except Exception as e:
            logger.error(f"获取URL {url} 时出错: {e}")

        retry_count += 1

    logger.error(f"获取URL {url} 失败，已达到最大重试次数")
    return None


def get_related_tags(tag: str, process_relationships: bool = False,
                    overlap_threshold: float = 0.9995,
                    categories: List[int] = None,
                    max_retries: int = 3) -> Dict[str, Any]:
    """
    同步获取标签的相关标签（调用异步方法）

    参数:
        tag: 要查询的标签名称
        process_relationships: 是否处理并返回层级关系
        overlap_threshold: 关系阈值
        categories: 要查询的分类列表
        max_retries: 最大重试次数

    返回:
        相关标签列表或包含关系的字典
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        # 如果没有事件循环，创建一个新的
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(
        get_related_tags_async(
            tag,
            process_relationships=process_relationships,
            overlap_threshold=overlap_threshold,
            categories=categories,
            max_retries=max_retries
        )
    )

# 获取mongo中的danbooru的tags表的前1000条没有children的项目。对每个项目执行get_related_tags, 并将结果更新到原表中（以_id为更新匹配键）。使用异步，10线程

from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
from tqdm import tqdm

async def process_tags_batch(id_range=(0, 564803)):
    """
    批量处理MongoDB中没有children字段的标签，
    获取它们的相关标签，并更新数据库

    参数:
        id_range: 要处理的ID范围，格式为(min_id, max_id)
    """
    try:
        # 连接MongoDB
        print("正在连接MongoDB...")
        client = AsyncIOMotorClient("mongodb://8.153.97.53:27017/")
        db = client["danbooru"]
        collection = db["tags"]

        # 确保日志可见
        print(f"正在查询ID范围 {id_range[0]}-{id_range[1]} 内没有children字段的标签...")

        # 获取指定ID范围内没有children的所有记录，不再限制数量
        query = {
            "_id": {"$gte": id_range[0], "$lte": id_range[1]},
            "children": {"$exists": False},
            "category": 4,            # 只处理category为4的标签
            "post_count": {"$ne": 0}  # 只处理post_count不为0的标签
        }
        cursor = collection.find(query)
        print("正在加载所有匹配记录到内存...")
        tags = await cursor.to_list(length=None)  # 加载所有结果

        # 立即输出找到的标签数量，确保可见
        print(f"找到 {len(tags)} 条需要处理的标签记录")
        logger.info(f"找到 {len(tags)} 条需要处理的标签记录")

        if not tags:
            print("没有找到需要处理的标签")
            return

        # 添加处理计数器以便追踪进度
        processed_count = 0
        success_count = 0
        error_count = 0

        # 创建信号量来限制并发数
        semaphore = asyncio.Semaphore(32)  # 限制为32个并发任务

        async def process_tag(tag_doc):
            nonlocal processed_count, success_count, error_count
            async with semaphore:
                try:
                    tag_name = tag_doc.get("name")
                    if not tag_name:
                        print(f"跳过ID为 {tag_doc.get('_id')} 的标签，缺少name字段")
                        error_count += 1
                        return

                    # 每处理10个标签输出一次进度
                    if processed_count % 10 == 0:
                        print(f"正在处理第 {processed_count}/{len(tags)} 个标签: {tag_name}")

                    # 获取相关标签并处理关系
                    related_data = await get_related_tags_async(
                        tag_name,
                        process_relationships=True,
                        overlap_threshold=0.9995,
                        max_retries=3
                    )

                    if not related_data:
                        print(f"标签 {tag_name} 未获取到相关数据")
                        error_count += 1
                        return

                    # 更新MongoDB中的记录
                    # 只更新parents, children和related字段
                    update_data = {
                        "parents": related_data.get("parents", []),
                        "children": related_data.get("children", []),
                        "related": related_data.get("related", [])
                    }

                    result = await collection.update_one(
                        {"_id": tag_doc.get("_id")},
                        {"$set": update_data}
                    )

                    processed_count += 1

                    if result.modified_count > 0:
                        success_count += 1
                        # 每10个成功更新输出一次进度
                        if success_count % 10 == 0:
                            print(f"已成功更新 {success_count} 个标签")

                    # 添加随机延迟，避免请求过于集中
                    await asyncio.sleep(0.5 + random.random())

                except Exception as e:
                    print(f"处理标签 {tag_doc.get('name', '未知')} 时出错: {str(e)}")
                    error_count += 1

        # 创建所有标签的任务
        print("开始创建处理任务...")
        tasks = [process_tag(tag) for tag in tags]

        # 每30秒输出一次进度统计
        async def report_progress():
            while processed_count < len(tags):
                print(f"进度报告: 已处理 {processed_count}/{len(tags)} 个标签, 成功: {success_count}, 错误: {error_count}")
                await asyncio.sleep(30)  # 每30秒报告一次

        # 创建进度报告任务
        progress_task = asyncio.create_task(report_progress())

        # 处理所有标签
        print("开始处理所有标签...")
        for f in tqdm(asyncio.as_completed(tasks), total=len(tags), desc="处理标签"):
            await f

        # 取消进度报告任务
        progress_task.cancel()

        # 最终报告
        print(f"处理完成! 总计: {len(tags)}, 成功: {success_count}, 错误: {error_count}")
        logger.info(f"处理完成! 总计: {len(tags)}, 成功: {success_count}, 错误: {error_count}")

    except Exception as e:
        print(f"批量处理标签时发生错误: {str(e)}")
        logger.error(f"批量处理标签时发生错误: {e}")
    finally:
        # 关闭MongoDB连接
        print("关闭MongoDB连接...")
        client.close()

# 定义ID区间常量
ID_RANGES = [
    (0, 564803),           # 区间 0
    (564800, 717940),      # 区间 1
    (717900, 1167360),     # 区间 2
    (1167300, 1352414),    # 区间 3
    (1352400, 1507647),    # 区间 4
    (1507600, 1677828),    # 区间 5
    (1677800, 1846377),    # 区间 6
    (1846300, 2024493),    # 区间 7
    (2024400, 2194566),    # 区间 8
    (2194500, 3000000)     # 区间 9
]

def run_batch_processing(range_index=0):
    """
    运行批量处理任务的同步入口点

    参数:
        range_index: 要处理的ID区间索引(0-9)
            0: 0-564803
            1: 564800-717940
            2: 717900-1167360
            3: 1167300-1352414
            4: 1352400-1507647
            5: 1507600-1677828
            6: 1677800-1846377
            7: 1846300-2024493
            8: 2024400-2194566
            9: 2194500-3000000
    """
    try:
        # 确保索引在有效范围内
        if range_index < 0 or range_index >= len(ID_RANGES):
            print(f"错误: 区间索引必须在0-9之间，传入值: {range_index}")
            logger.error(f"无效的区间索引: {range_index}")
            return

        # 获取选定的ID区间
        selected_range = ID_RANGES[range_index]
        print(f"已选择处理区间 {range_index}: {selected_range[0]}-{selected_range[1]}")

        # 检查是否在Jupyter或其他已有事件循环的环境中运行
        try:
            # 尝试导入nest_asyncio模块，如果不存在则跳过
            import nest_asyncio
            nest_asyncio.apply()
            logger.info("应用nest_asyncio允许嵌套事件循环")
        except ImportError:
            logger.info("未安装nest_asyncio，如果在Jupyter中运行可能会出现事件循环错误")
            pass

        # 获取现有事件循环或创建新的
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # 在现有循环中运行或直接使用asyncio.run
        if loop.is_running():
            logger.info("检测到事件循环已运行，使用替代方法")

            # 创建专门运行异步任务的函数
            def run_async_task():
                # 在新线程中创建新的事件循环
                task_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(task_loop)
                try:
                    # 运行异步任务，传递选定的ID区间
                    task_loop.run_until_complete(process_tags_batch(selected_range))
                finally:
                    task_loop.close()

            # 创建并启动线程
            import threading
            thread = threading.Thread(target=run_async_task)
            thread.start()

            # 等待线程完成
            logger.info("等待异步处理任务完成...")
            thread.join()
            logger.info("异步处理任务已完成")
        else:
            loop.run_until_complete(process_tags_batch(selected_range))

    except Exception as e:
        logger.error(f"运行批处理时出错: {e}")

# 如果直接运行此脚本，则执行批量处理
if __name__ == "__main__":
    # 检测是否在Jupyter/IPython环境中运行
    try:
        # 正确方式：尝试导入IPython
        import IPython
        is_ipython = True
    except ImportError:
        is_ipython = False

    if is_ipython:
        # 在Jupyter环境中，使用默认区间或让用户显式调用函数
        print("检测到Jupyter/IPython环境，将使用默认区间0")
        print("如需处理其他区间，请直接调用 run_batch_processing(range_index)")
        print("例如: run_batch_processing(2) - 处理区间2 (717900-1167360)")

        # 展示可用区间
        for i, range_val in enumerate(ID_RANGES):
            print(f"区间 {i}: {range_val[0]}-{range_val[1]}")
    else:
        # 在常规Python环境中，使用命令行参数
        import sys

        range_index = 0  # 默认处理第一个区间
        if len(sys.argv) > 1:
            try:
                arg = sys.argv[1]
                range_index = int(arg)
                if range_index < 0 or range_index >= len(ID_RANGES):
                    print(f"警告: 索引 {range_index} 超出有效范围0-9，将使用默认值0")
                    range_index = 0
            except ValueError:
                print(f"无法解析参数 '{sys.argv[1]}' 为有效的区间索引，将使用默认值0")
                range_index = 0

        print(f"开始处理区间 {range_index}: {ID_RANGES[range_index][0]}-{ID_RANGES[range_index][1]}")
        run_batch_processing(range_index)


