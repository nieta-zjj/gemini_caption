from google import genai
from google.genai import types
import os
import json
import time
import asyncio
import logging
import httpx
from motor.motor_asyncio import AsyncIOMotorClient
from concurrent.futures import ThreadPoolExecutor
import json_repair
from typing import Union, Literal
from hfpics import HfPics

# 使用相对导入，确保作为包安装后能正确导入
try:
    # 当作为已安装的包导入时
    from gemini_caption.caption_promt_utils import CaptionPromptUtils
    from gemini_caption.file_utils import FileUtils
    from gemini_caption.character_analyzer import CharacterAnalyzer
except ImportError:
    try:
        # 当在src目录中时
        from .caption_promt_utils import CaptionPromptUtils
        from .file_utils import FileUtils
        from .character_analyzer import CharacterAnalyzer
    except ImportError:
        # 直接导入，当在脚本所在目录运行时
        from caption_promt_utils import CaptionPromptUtils
        from file_utils import FileUtils
        from character_analyzer import CharacterAnalyzer

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

caption_prompt_utils = CaptionPromptUtils()
file_utils = FileUtils()

class GeminiBatchCaption:
    """异步批量图像打标类"""

    def __init__(self,
                 api_key=None,
                 model_id="gemini-2.0-flash-lite-001",
                 mongodb_uri=None,
                 language="zh",
                 max_concurrency=5,
                 hf_repo="picollect/danbooru",
                 hf_cache_dir=None):
        """
        初始化异步批量图像打标类

        Args:
            api_key: Gemini API密钥
            model_id: 使用的模型ID
            mongodb_uri: MongoDB连接URI，如果为None则使用环境变量
            language: 输出语言
            max_concurrency: 最大并行处理数量
            hf_repo: Hugging Face数据集仓库名称
            hf_cache_dir: HFPics缓存目录路径，默认为None使用HFPics默认路径
        """
        # 初始化Gemini客户端
        if api_key:
            self.api_key = api_key
        else:
            os.environ["GOOGLE_CLOUD_PROJECT"] = "poised-runner-402505"
            self.api_key = os.getenv("GOOGLE_API_KEY")

        # 获取MongoDB连接URI
        if mongodb_uri is None:
            mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")

        self.mongodb_uri = mongodb_uri
        self.model_id = model_id
        self.language = language
        self.max_concurrency = max_concurrency
        self.character_analyzer = None  # 在运行时初始化
        self.retry_attempts = 3
        self.retry_delay = 5  # 秒

        # 初始化HFPics客户端
        if hf_cache_dir is None:
            # 如果cache_dir为None，不传递这个参数，让HFPics使用默认路径
            self.hf_pics = HfPics(repo=hf_repo)
        else:
            self.hf_pics = HfPics(repo=hf_repo, cache_dir=hf_cache_dir)
        logger.info(f"已初始化HFPics客户端，使用仓库: {hf_repo}")

        # 初始化Gemini客户端实例
        self.genai_client = genai.Client(api_key=self.api_key)

        # 文件类型到MIME类型的映射
        self.mime_type_map = {
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "webp": "image/webp"
        }

    async def initialize(self):
        """异步初始化方法"""
        # 初始化MongoDB连接
        self.mongo_client = AsyncIOMotorClient(self.mongodb_uri)
        self.db = self.mongo_client["gemini_captions_danbooru"]

        # 初始化角色分析器
        self.character_analyzer = CharacterAnalyzer(self.mongodb_uri)

        # 初始化HTTP客户端 (使用httpx代替aiohttp)
        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            follow_redirects=True
        )

        # 初始化并发控制信号量
        self.semaphore = asyncio.Semaphore(self.max_concurrency)

        logger.info(f"GeminiBatchCaption 初始化完成，最大并行数: {self.max_concurrency}")
        return self

    async def close(self):
        """关闭所有资源"""
        try:
            # 关闭HTTP客户端
            if hasattr(self, 'http_client') and self.http_client:
                await self.http_client.aclose()

            # 关闭MongoDB连接
            if hasattr(self, 'mongo_client') and self.mongo_client:
                self.mongo_client.close()

            logger.info("已关闭所有资源连接")
        except Exception as e:
            logger.error(f"关闭资源时发生错误: {str(e)}")

    def get_collection_for_id(self, dan_id):
        """
        根据ID获取对应的集合

        Args:
            dan_id: Danbooru图片ID

        Returns:
            对应的MongoDB集合
        """
        # 将ID转换为整数
        if isinstance(dan_id, str):
            dan_id = int(dan_id)

        # 计算集合名称
        collection_name = str(dan_id // 100000)
        return self.db[collection_name]

    async def get_danbooru_info_by_id(self, dan_id):
        """异步获取Danbooru信息"""
        try:
            # 获取角色参考信息
            character_reference_info = await self.character_analyzer.get_visualize_tree_by_pid(dan_id, "en")

            # 获取艺术家名称
            artist_name = await self.character_analyzer.pics.get_pic_artist_by_id(dan_id)
            # 确保artist_name是可迭代对象
            if artist_name is None or not hasattr(artist_name, '__iter__'):
                artist_name = []
            artist_name = "&".join(artist_name)

            # 获取标签
            danbooru_tags = " ".join(await self.character_analyzer.pics.get_pic_general_by_id(dan_id))

            return character_reference_info, artist_name, danbooru_tags
        except Exception as e:
            logger.error(f"获取Danbooru信息时出错: {str(e)}")
            return None, "", ""

    async def get_url_by_id(self, dan_id):
        """异步从MongoDB获取图片URL"""
        try:
            # 从MongoDB的danbooru数据库的pics表获取数据
            danbooru_db = self.mongo_client["danbooru"]
            pics_collection = danbooru_db["pics"]

            # 查询对应ID的记录
            pic_data = await pics_collection.find_one({"_id": int(dan_id)})

            if pic_data is None:
                error_result = {
                    "_id": dan_id,
                    "success": False,
                    "error": "Post not found in database",
                    "status_code": 404,
                    "created_at": time.time()
                }
                # 保存404错误到MongoDB
                collection = self.get_collection_for_id(dan_id)
                await collection.update_one(
                    {"_id": dan_id},
                    {"$set": error_result},
                    upsert=True
                )
                raise ValueError(f"Danbooru post {dan_id} not found in database (404)")

            # 从数据中构建URL
            # 使用md5和file_ext构建URL
            md5 = pic_data.get("md5")
            file_ext = pic_data.get("file_ext")

            if not md5 or not file_ext:
                error_result = {
                    "_id": dan_id,
                    "success": False,
                    "error": "Post URL not found",
                    "status_code": 404,
                    "created_at": time.time()
                }
                # 保存错误到MongoDB
                collection = self.get_collection_for_id(dan_id)
                await collection.update_one(
                    {"_id": dan_id},
                    {"$set": error_result},
                    upsert=True
                )
                raise ValueError(f"无法从数据库记录构建URL，缺少md5或file_ext，ID: {dan_id}")

            # 构建Danbooru URL (根据md5前两位字符分组)
            url = f"https://cdn.donmai.us/original/{md5[0:2]}/{md5[2:4]}/{md5}.{file_ext}"

            return url

        except Exception as e:
            if not isinstance(e, ValueError):  # 如果不是已处理的404错误
                logger.error(f"获取URL时出错: {str(e)}")
            raise

    async def download_image(self, image_url, dan_id=None):
        """
        异步下载图片

        Args:
            image_url: 图片URL
            dan_id: 已弃用，保留参数以兼容旧版接口

        Returns:
            bytes: 图片二进制内容，如果下载失败则返回None
        """
        # 从URL获取图片
        retry_delays = [1, 5, 30, 60, 300]

        for attempt, delay in enumerate(retry_delays):
            try:
                # 使用httpx替代aiohttp
                response = await self.http_client.get(image_url)
                if response.status_code == 200:
                    return response.content
                else:
                    logger.warning(f"获取图片失败，状态码: {response.status_code}")
            except Exception as e:
                logger.error(f"下载图片时出错: {str(e)}")

            if attempt < len(retry_delays) - 1:  # 如果不是最后一次尝试
                logger.debug(f"等待 {delay} 秒后重试...")
                await asyncio.sleep(delay)

        return None

    async def call_gemini_api(self, prompt, image_bytes, mime_type):
        """异步调用Gemini API"""
        caption = None
        last_error = None

        for attempt in range(self.retry_attempts):
            try:
                # 由于Gemini API可能不支持异步，使用线程池执行同步调用
                with ThreadPoolExecutor() as executor:
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        executor,
                        lambda: self.genai_client.models.generate_content(
                            model=self.model_id,
                            contents=[
                                prompt,
                                types.Part.from_bytes(data=image_bytes, mime_type=mime_type),
                            ],
                            config=types.GenerateContentConfig(
                                max_output_tokens=4096,
                                safety_settings=[
                                    types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="OFF"),
                                    types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"),
                                    types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
                                    types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
                                    types.SafetySetting(category="HARM_CATEGORY_CIVIC_INTEGRITY", threshold="OFF"),
                                ],
                            )
                        )
                    )

                caption = response.text
                break

            except Exception as e:
                last_error = e
                logger.warning(f"API调用错误: {str(e)}，重试 {attempt+1}/{self.retry_attempts}")
                # 增加重试延迟时间，避免频繁请求
                retry_time = self.retry_delay * (2 ** attempt)  # 指数退避
                await asyncio.sleep(retry_time)

        if caption is None:
            import traceback
            raise Exception(f"所有重试均失败: {str(last_error)} \n {traceback.format_exc()}")

        return caption

    async def process_single_id(self, dan_id, output_dir=None, save_image=False, skip_existing_check=False):
        """
        异步处理单个Danbooru ID

        Args:
            dan_id: Danbooru图片ID
            output_dir: 输出目录
            save_image: 是否保存下载的图片
            skip_existing_check: 是否跳过检查已存在记录，当在batch模式下使用时，应为True

        Returns:
            处理结果字典
        """
        # 使用信号量控制并发
        async with self.semaphore:
            start_time = time.time()
            logger.info(f"开始处理Danbooru ID: {dan_id}")

            try:
                # 获取对应的集合
                collection = self.get_collection_for_id(dan_id)

                # 检查是否已经处理过（除非跳过检查）
                if not skip_existing_check:
                    existing_doc = await collection.find_one({"_id": dan_id})
                    if existing_doc:
                        success = existing_doc.get("success", False)
                        status = "成功" if success else "失败"
                        logger.info(f"ID: {dan_id} 已有{status}记录，跳过")
                        return existing_doc

                # 首先尝试从HFPics获取图片
                logger.info(f"尝试从HFPics获取图片ID: {dan_id}")
                image_bytes = None
                image_url = None
                mime_type = "image/jpeg"  # 默认MIME类型
                file_extension = "jpg"    # 默认文件扩展名

                # 尝试使用HFPics获取图片
                try:
                    # 使用线程池执行同步调用
                    with ThreadPoolExecutor() as executor:
                        loop = asyncio.get_event_loop()
                        image_bytes = await loop.run_in_executor(
                            executor,
                            lambda: self.hf_pics.pic(int(dan_id), return_type="content")
                        )

                    if image_bytes:
                        logger.info(f"成功从HFPics获取图片ID: {dan_id}")

                        # 尝试从HFPics获取图片扩展名
                        try:
                            # 尝试获取图片原始路径，从中提取扩展名
                            with ThreadPoolExecutor() as executor:
                                pic_info = await loop.run_in_executor(
                                    executor,
                                    lambda: self.hf_pics.pic_info(int(dan_id))
                                )

                            if pic_info and 'pic_url' in pic_info:
                                pic_url = pic_info['pic_url']
                                image_url = pic_url  # 使用HFPics中记录的URL
                                # 从URL中提取文件扩展名
                                file_extension = os.path.splitext(pic_url)[1][1:].lower()
                                mime_type = self.mime_type_map.get(file_extension, "image/jpeg")
                        except Exception as e:
                            logger.warning(f"从HFPics获取图片信息失败: {str(e)}，使用默认MIME类型")
                except Exception as e:
                    logger.warning(f"从HFPics获取图片失败: {str(e)}，将尝试从Danbooru获取")

                # 如果从HFPics获取失败，则从Danbooru获取
                if image_bytes is None:
                    # 获取图片URL
                    image_url = await self.get_url_by_id(dan_id)
                    logger.debug(f"获取到图片URL: {image_url}")

                    # 获取文件的MIME类型
                    file_extension = os.path.splitext(image_url)[1][1:].lower()
                    mime_type = self.mime_type_map.get(file_extension, "image/jpeg")

                    # 下载图片
                    image_bytes = await self.download_image(image_url)

                # 如果所有重试都失败
                if image_bytes is None:
                    error_result = {
                        "_id": dan_id,
                        "success": False,
                        "error": "Failed to download image after all retries",
                        "status_code": 500,
                        "image_url": image_url,
                        "created_at": time.time()
                    }
                    await collection.update_one(
                        {"_id": dan_id},
                        {"$set": error_result},
                        upsert=True
                    )
                    raise Exception("无法获取图片内容，所有重试均失败")

                # 准备输出目录
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)

                # 如果需要保存图片
                if save_image and output_dir:
                    image_path = os.path.join(output_dir, f"{dan_id}.{file_extension}")
                    with open(image_path, 'wb') as f:
                        f.write(image_bytes)
                    logger.debug(f"图片已保存到: {image_path}")

                # 获取Danbooru信息
                character_reference_info, artist_name, danbooru_tags = await self.get_danbooru_info_by_id(dan_id)

                # 构建提示
                prompt = caption_prompt_utils.build_prompt(
                    artist_name=artist_name,
                    character_name="",
                    danbooru_tags=danbooru_tags,
                    language=self.language,
                    character_reference_info=character_reference_info
                )

                logger.info(f"开始为ID: {dan_id} 调用Gemini API...")

                # 调用API
                caption = await self.call_gemini_api(prompt, image_bytes, mime_type)

                # 处理API结果
                try:
                    caption_json = json_repair.loads(caption)
                except Exception as e:
                    logger.warning(f"JSON解析失败: {str(e)}，尝试使用原始文本")
                    caption_json = caption

                # 准备结果
                result = {
                    "_id": dan_id,
                    "image_url": image_url,
                    "prompt": prompt,
                    "caption": caption_json,
                    "artist": artist_name,
                    "tags": danbooru_tags,
                    "success": True,
                    "processing_time": time.time() - start_time,
                }

                # 保存到MongoDB
                try:
                    await collection.update_one(
                        {"_id": dan_id},
                        {"$set": result},
                        upsert=True
                    )
                    logger.debug(f"结果已保存到MongoDB，ID: {dan_id}，集合: {collection.name}")
                except Exception as e:
                    logger.error(f"保存到MongoDB失败: {str(e)}")

                # 如果需要保存到文件
                if output_dir:
                    result_path = os.path.join(output_dir, f"{dan_id}_caption.json")
                    with open(result_path, 'w', encoding='utf-8') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2)
                    logger.debug(f"结果已保存到: {result_path}")

                return result

            except Exception as e:
                error_msg = f"处理Danbooru ID {dan_id}时出错: {type(e).__name__}: {str(e)}"
                logger.error(error_msg)

                error_result = {
                    "_id": dan_id,
                    "success": False,
                    "error": error_msg,
                    "processing_time": time.time() - start_time
                }

                # 保存错误信息到MongoDB
                try:
                    collection = self.get_collection_for_id(dan_id)
                    await collection.update_one(
                        {"_id": dan_id},
                        {"$set": error_result},
                        upsert=True
                    )
                    logger.debug(f"错误信息已保存到MongoDB，ID: {dan_id}，集合: {collection.name}")
                except Exception as mongo_e:
                    logger.error(f"保存错误信息到MongoDB失败: {str(mongo_e)}")

                return error_result

    async def process_batch(self, start_id, end_id, output_dir=None, save_image=False):
        """
        批量处理一个范围的Danbooru ID

        首先会检查MongoDB中已存在的记录（无论成功还是失败），只处理尚未存在记录的ID

        Args:
            start_id: 起始ID
            end_id: 结束ID
            output_dir: 输出目录
            save_image: 是否保存下载的图片

        Returns:
            处理结果统计
        """
        if start_id > end_id:
            raise ValueError("起始ID必须小于或等于结束ID")

        total_count = end_id - start_id + 1
        success_count = 0
        error_count = 0
        skipped_count = 0
        already_processed_count = 0

        # 记录开始时间
        start_time = time.time()

        logger.info(f"开始批量处理 Danbooru ID范围: {start_id} - {end_id}，共 {total_count} 个")

        # 首先获取已经存在记录的ID列表（无论成功还是失败）
        existing_ids = set()

        # 计算涉及的集合范围
        start_collection = start_id // 100000
        end_collection = end_id // 100000

        logger.info(f"正在查询已处理过的记录，集合范围: {start_collection} - {end_collection}")

        for collection_num in range(start_collection, end_collection + 1):
            collection_name = str(collection_num)
            collection = self.db[collection_name]

            # 查询ID范围内的所有文档，无论成功还是失败
            query = {
                "_id": {"$gte": start_id, "$lte": end_id}
            }

            # 只获取_id字段以提高效率
            async for doc in collection.find(query, {"_id": 1}):
                existing_ids.add(doc["_id"])

        already_processed_count = len(existing_ids)
        logger.info(f"已找到 {already_processed_count} 个已存在记录，将跳过这些ID")

        # 计算需要处理的ID列表
        ids_to_process = [id for id in range(start_id, end_id + 1) if id not in existing_ids]
        remaining_count = len(ids_to_process)

        if remaining_count == 0:
            logger.info("所有ID都已经存在记录，没有需要处理的ID")
            return {
                "total": total_count,
                "success": 0,
                "error": 0,
                "skipped": 0,
                "already_processed": already_processed_count,
                "time": time.time() - start_time
            }

        logger.info(f"需要处理的ID数量: {remaining_count}")

        # 创建处理任务
        tasks = []
        for dan_id in ids_to_process:
            task = asyncio.create_task(self.process_single_id(dan_id, output_dir, save_image, skip_existing_check=True))
            tasks.append(task)

        # 处理所有任务结果
        for i, task in enumerate(asyncio.as_completed(tasks)):
            try:
                result = await task

                if i % 10 == 0 or i == remaining_count - 1:  # 每10个ID或最后一个ID时显示进度
                    progress = (i + 1) / remaining_count * 100
                    logger.info(f"进度: {progress:.2f}% ({i+1}/{remaining_count})")

                if result:
                    if result.get("success"):
                        success_count += 1
                    else:
                        if "already processed" in str(result.get("error", "")):
                            skipped_count += 1
                        else:
                            error_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"处理任务时出错: {str(e)}")

        end_time = time.time()

        # 统计结果
        stats = {
            "total": total_count,
            "success": success_count,
            "error": error_count,
            "skipped": skipped_count,
            "already_processed": already_processed_count,
            "time": end_time - start_time
        }

        logger.info(f"批量处理完成. 总计: {total_count}, 已有记录: {already_processed_count}, 本次成功: {success_count}, 本次错误: {error_count}, 本次跳过: {skipped_count}")

        return stats

# 批量处理入口点
async def run_batch_with_args(start_id, end_id, max_concurrency=5, api_key=None, model_id=None, language="zh",
                             mongodb_uri=None, output_dir="caption_results", save_image=False,
                             hf_repo="picollect/danbooru", hf_cache_dir=None):
    """
    使用指定参数运行批处理

    Args:
        start_id: 起始ID
        end_id: 结束ID
        max_concurrency: 最大并行处理数量
        api_key: Gemini API密钥
        model_id: 使用的模型ID
        language: 输出语言
        mongodb_uri: MongoDB连接URI
        output_dir: 输出目录
        save_image: 是否保存下载的图片
        hf_repo: Hugging Face数据集仓库名称
        hf_cache_dir: HFPics缓存目录路径

    Returns:
        处理结果统计
    """
    # 使用默认值
    if model_id is None:
        model_id = "gemini-2.0-flash-lite-001"

    if mongodb_uri is None:
        mongodb_uri = "mongodb://8.153.97.53:27815/"

    try:
        # 初始化批处理器
        batch_captioner = GeminiBatchCaption(
            api_key=api_key,
            model_id=model_id,
            language=language,
            mongodb_uri=mongodb_uri,
            max_concurrency=max_concurrency,
            hf_repo=hf_repo,
            hf_cache_dir=hf_cache_dir
        )

        # 异步初始化
        await batch_captioner.initialize()

        # 运行批处理
        stats = await batch_captioner.process_batch(
            start_id=start_id,
            end_id=end_id,
            output_dir=output_dir,
            save_image=save_image
        )

        # 输出详细统计信息
        total = stats.get("total", 0)
        already_processed = stats.get("already_processed", 0)
        success = stats.get("success", 0)
        error = stats.get("error", 0)
        skipped = stats.get("skipped", 0)
        time_spent = stats.get("time", 0)

        logger.info(f"批处理统计:")
        logger.info(f"  总ID数量: {total}")
        logger.info(f"  已有记录: {already_processed} ({(already_processed/total*100 if total else 0):.2f}%)")
        logger.info(f"  本次成功: {success} ({(success/total*100 if total else 0):.2f}%)")
        logger.info(f"  本次错误: {error} ({(error/total*100 if total else 0):.2f}%)")
        logger.info(f"  本次跳过: {skipped}")
        logger.info(f"  总耗时: {time_spent:.2f}秒")
        logger.info(f"  总结: 共处理 {already_processed + success + error} 个ID ({(already_processed + success + error)/total*100 if total else 0:.2f}%)，剩余 {total - already_processed - success - error - skipped} 个ID未处理")

        return stats

    finally:
        # 确保资源正确释放
        if 'batch_captioner' in locals():
            await batch_captioner.close()
            logger.info("已关闭所有资源连接")

# 命令行入口点
def main():
    """命令行入口函数"""
    import argparse

    parser = argparse.ArgumentParser(description='批量处理Danbooru图像并生成标题')
    parser.add_argument('--start-id', type=int, required=True, help='起始ID')
    parser.add_argument('--end-id', type=int, required=True, help='结束ID')
    parser.add_argument('--max-concurrency', type=int, default=100, help='最大并行处理数量')
    parser.add_argument('--api-key', type=str, help='Gemini API密钥')
    parser.add_argument('--model-id', type=str, default='gemini-2.0-flash-lite-001', help='使用的模型ID')
    parser.add_argument('--language', type=str, default='zh', help='输出语言 (en 或 zh)')
    parser.add_argument('--mongodb-uri', type=str, help='MongoDB连接URI')
    parser.add_argument('--output-dir', type=str, default=None, help='输出目录')
    parser.add_argument('--save-image', action='store_true', default=False, help='是否保存下载的图片')
    parser.add_argument('--hf-repo', type=str, default='picollect/danbooru', help='Hugging Face数据集仓库名称')
    parser.add_argument('--hf-cache-dir', type=str, help='HFPics缓存目录路径')

    args = parser.parse_args()

    # 运行批处理
    asyncio.run(run_batch_with_args(
        start_id=args.start_id,
        end_id=args.end_id,
        max_concurrency=args.max_concurrency,
        api_key=args.api_key,
        model_id=args.model_id,
        language=args.language,
        mongodb_uri=args.mongodb_uri,
        output_dir=args.output_dir,
        save_image=args.save_image,
        hf_repo=args.hf_repo,
        hf_cache_dir=args.hf_cache_dir
    ))

if __name__ == "__main__":
    main()

# 运行命令

