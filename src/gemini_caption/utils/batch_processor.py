'''
处理Gemini API批量图像描述任务的协调器
'''

import asyncio
import time
from typing import Dict, Any, Optional, Union, List, Set, Tuple
import os

# 导入日志工具和其他组件
from gemini_caption.utils.logger_utils import log_info, log_debug, log_warning, log_error
from gemini_caption.utils.image_processor import ImageProcessor
from gemini_caption.utils.gemini_api_client import GeminiApiClient
from gemini_caption.utils.caption_promt_utils import CaptionPromptUtils
from gemini_caption.mongo_collections import DanbooruPics, DanbooruGeminiCaptions

class BatchProcessor:
    """
    批量处理图片的类，协调其他组件进行并发处理
    """

    def __init__(self,
                 model_id: str = "gemini-2.0-flash-lite-001",
                 mongodb_uri: Optional[str] = None,
                 language: str = "zh",
                 max_concurrency: int = 5,
                 hf_repo: str = "picollect/danbooru",
                 hf_cache_dir: Optional[str] = None,
                 use_hfpics_first: bool = False,
                 project_id: str = "poised-runner-402505",
                 google_credentials_content: Optional[str] = None,
                 google_credentials_path: Optional[str] = None):
        """
        初始化批处理器

        Args:
            model_id: 使用的模型ID
            mongodb_uri: MongoDB连接URI
            language: 语言代码
            max_concurrency: 最大并发数
            hf_repo: HuggingFace仓库名称
            hf_cache_dir: HFPics缓存目录
            use_hfpics_first: 是否优先使用HFPics获取图片
            project_id: Google Cloud项目ID
            google_credentials_content: Google凭证内容
            google_credentials_path: Google凭证路径
        """
        self.max_concurrency = max_concurrency
        self.language = language
        self.mongodb_uri = mongodb_uri

        # 处理Google凭证
        if google_credentials_content or google_credentials_path:
            from gemini_caption.config import Config
            config_updates = {}
            if google_credentials_content:
                config_updates['GOOGLE_APPLICATION_CREDENTIALS_CONTENT'] = google_credentials_content
            if google_credentials_path:
                config_updates['GOOGLE_APPLICATION_CREDENTIALS'] = google_credentials_path
            Config.update_config(**config_updates)

        # 初始化组件
        self.image_processor = ImageProcessor(
            hf_repo=hf_repo,
            hf_cache_dir=hf_cache_dir,
            use_hfpics_first=use_hfpics_first
        )

        self.gemini_client = GeminiApiClient(
            model_id=model_id,
            project_id=project_id
        )

        self.caption_utils = CaptionPromptUtils()

        # 初始化数据库客户端
        self.danbooru_pics = DanbooruPics(client_url=mongodb_uri)
        self.danbooru_gemini_captions = DanbooruGeminiCaptions(mongodb_uri=mongodb_uri)

        # 用于控制并发的信号量
        self.semaphore = asyncio.Semaphore(max_concurrency)

        # 记录处理统计信息
        self.stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "start_time": 0,
            "end_time": 0,
            "total_time": 0,
            "avg_time_per_item": 0
        }

        log_info(f"批处理器初始化完成，最大并发数: {max_concurrency}")

    async def close(self):
        """关闭所有资源"""
        log_info("正在关闭批处理器资源...")
        self.danbooru_pics.close()
        await self.danbooru_gemini_captions.close()
        log_info("批处理器资源已关闭")

    async def process_single_id(self, dan_id: Union[str, int],
                               output_dir: Optional[str] = None,
                               save_image: bool = False,
                               skip_existing_check: bool = False,
                               custom_url: Optional[str] = None) -> Dict[str, Any]:
        """
        处理单个图片ID

        Args:
            dan_id: Danbooru图片ID
            output_dir: 输出目录
            save_image: 是否保存图片
            skip_existing_check: 是否跳过已存在结果的检查
            custom_url: 自定义URL，如果提供则优先使用

        Returns:
            处理结果字典
        """
        start_time = time.time()

        # 确保ID是整数
        if isinstance(dan_id, str):
            try:
                dan_id = int(dan_id)
            except ValueError:
                return {
                    "success": False,
                    "error": f"无效的ID格式: {dan_id}",
                    "processing_time": time.time() - start_time
                }

        log_info(f"开始处理ID: {dan_id}")

        # 检查是否已经处理过
        if not skip_existing_check:
            existing_result = await self.danbooru_gemini_captions.check_existing_result(dan_id)
            if existing_result:
                log_info(f"ID {dan_id} 已经处理过，跳过")
                return {
                    "success": True,
                    "skipped": True,
                    "existing_result": existing_result,
                    "processing_time": time.time() - start_time
                }

        # 获取图片URL
        url = custom_url
        if not url:
            url, status = await self.danbooru_pics.get_url_by_id(dan_id)
            if status != 200 or not url:
                log_warning(f"无法获取ID {dan_id} 的URL，状态码: {status}")
                return {
                    "success": False,
                    "error": f"无法获取URL，状态码: {status}",
                    "processing_time": time.time() - start_time
                }

        # 处理图片
        image_result = await self.image_processor.process_image_by_id(dan_id, custom_url=url)
        if not image_result["success"]:
            error_result = {
                "_id": dan_id,
                "success": False,
                "error": image_result.get("error", "图片处理失败"),
                "image_url": url,
                "processing_time": time.time() - start_time
            }
            await self.danbooru_gemini_captions.save_caption_result(dan_id, error_result)
            log_error(f"处理ID {dan_id} 的图片失败: {error_result['error']}")
            return error_result

        # 如果需要保存图片到本地
        if save_image and output_dir:
            image_path = os.path.join(output_dir, f"{dan_id}.{image_result['file_extension']}")
            await self.image_processor.save_image(image_result["image_bytes"], image_path)

        # 获取图片信息
        pic_data = await self.danbooru_pics.get_pic_data_by_id(dan_id)
        artist_name = pic_data.artist_tags
        character_name = pic_data.character_tags
        danbooru_tags = pic_data.general_tags

        # 获取角色参考信息（如果有CharacterAnalyzer）
        character_reference_info = None
        try:
            from gemini_caption.character_analyzer import CharacterAnalyzer
            analyzer = CharacterAnalyzer(client_url=self.mongodb_uri)
            character_reference_info = await analyzer.get_visualize_tree_by_pid(dan_id, self.language)
        except (ImportError, Exception) as e:
            log_debug(f"无法获取角色参考信息: {str(e)}")

        # 构建提示
        prompt = self.caption_utils.build_prompt(
            artist_name=artist_name,
            character_name=character_name,
            danbooru_tags=danbooru_tags,
            language=self.language,
            character_reference_info=character_reference_info
        )

        # 调用Gemini API
        api_result = await self.gemini_client.call_gemini_api(
            prompt,
            image_result["image_bytes"],
            image_result["mime_type"]
        )

        if not api_result["success"]:
            error_result = {
                "_id": dan_id,
                "success": False,
                "error": api_result.get("error", "API调用失败"),
                "image_url": url,
                "processing_time": time.time() - start_time
            }
            await self.danbooru_gemini_captions.save_caption_result(dan_id, error_result)
            log_error(f"处理ID {dan_id} 失败: {error_result['error']}")
            return error_result

        # 准备成功结果
        result = {
            "_id": dan_id,
            "image_url": url,
            "prompt": prompt,
            "caption": api_result["caption"],
            "artist": artist_name,
            "character": character_name,
            "tags": danbooru_tags,
            "success": True,
            "processing_time": time.time() - start_time,
        }

        # 保存到数据库
        await self.danbooru_gemini_captions.save_caption_result(dan_id, result)

        # 如果需要保存结果到本地文件
        if output_dir:
            await self.danbooru_gemini_captions.save_result_to_file(dan_id, result, output_dir)

        log_info(f"ID {dan_id} 处理完成，耗时: {result['processing_time']:.2f}秒")
        return result

    async def _process_id_with_semaphore(self, dan_id: Union[str, int],
                                        output_dir: Optional[str] = None,
                                        save_image: bool = False,
                                        custom_url: Optional[str] = None) -> Dict[str, Any]:
        """
        使用信号量控制并发，处理单个ID

        Args:
            dan_id: Danbooru图片ID
            output_dir: 输出目录
            save_image: 是否保存图片
            custom_url: 自定义URL，如果提供则优先使用

        Returns:
            处理结果字典
        """
        async with self.semaphore:
            try:
                return await self.process_single_id(
                    dan_id=dan_id,
                    output_dir=output_dir,
                    save_image=save_image,
                    skip_existing_check=False,
                    custom_url=custom_url
                )
            except Exception as e:
                log_error(f"处理ID {dan_id} 时发生异常: {str(e)}")
                return {
                    "success": False,
                    "error": f"处理异常: {str(e)}",
                    "_id": dan_id
                }

    async def process_batch(self, start_id: int, end_id: int,
                           output_dir: Optional[str] = None,
                           save_image: bool = False) -> Dict[str, Any]:
        """
        批量处理ID范围

        Args:
            start_id: 起始ID
            end_id: 结束ID
            output_dir: 输出目录
            save_image: 是否保存图片

        Returns:
            处理结果统计
        """
        log_info(f"开始批量处理ID范围: {start_id} - {end_id}")

        # 获取已经处理过的ID
        processed_ids = await self.danbooru_gemini_captions.get_processed_ids(start_id, end_id)
        log_info(f"已处理的ID数量: {len(processed_ids)}")

        # 获取有效的URL列表
        ids_to_process = []
        url_map = {}  # ID -> URL的映射

        # 从start_id到end_id遍历，每次处理一批
        batch_size = 10000
        for batch_start in range(start_id, end_id, batch_size):
            batch_end = min(batch_start + batch_size, end_id)
            id_batch = list(range(batch_start, batch_end))

            # 过滤掉已处理的ID
            id_batch = [id_value for id_value in id_batch if id_value not in processed_ids]

            if not id_batch:
                continue

            log_info(f"获取ID批次 {batch_start} - {batch_end - 1} 的URL信息")
            url_batch_result = await self.danbooru_pics.check_url_by_id_batch(id_batch)

            for id_value in id_batch:
                if id_value in url_batch_result:
                    data = url_batch_result[id_value]
                    if data.get("status") == 200 and data.get("url"):
                        ids_to_process.append(id_value)
                        url_map[id_value] = data.get("url")

        # 更新统计信息
        total_ids = end_id - start_id
        skipped_processed = len(processed_ids)
        skipped_no_url = total_ids - skipped_processed - len(ids_to_process)

        self.stats = {
            "total": total_ids,
            "success": 0,
            "failed": 0,
            "skipped": skipped_processed + skipped_no_url,
            "start_time": time.time(),
            "end_time": 0
        }

        log_info(f"总ID数: {total_ids}, 已处理: {skipped_processed}, 无URL: {skipped_no_url}, 待处理: {len(ids_to_process)}")

        # 如果没有需要处理的ID，直接返回
        if not ids_to_process:
            log_info("没有需要处理的ID，跳过批处理")
            self.stats["end_time"] = time.time()
            self.stats["total_time"] = self.stats["end_time"] - self.stats["start_time"]
            self.stats["avg_time_per_item"] = 0
            return self.stats

        # 创建任务列表
        tasks = []
        for id_value in ids_to_process:
            custom_url = url_map.get(id_value)
            task = self._process_id_with_semaphore(
                id_value,
                output_dir,
                save_image,
                custom_url
            )
            tasks.append(task)

        # 执行所有任务
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 统计结果
        for result in results:
            if isinstance(result, Exception):
                self.stats["failed"] += 1
                log_error(f"任务执行异常: {str(result)}")
            else:
                if result.get("success", False):
                    self.stats["success"] += 1
                else:
                    self.stats["failed"] += 1

        # 更新统计信息
        self.stats["end_time"] = time.time()
        self.stats["total_time"] = self.stats["end_time"] - self.stats["start_time"]
        self.stats["avg_time_per_item"] = self.stats["total_time"] / len(ids_to_process) if ids_to_process else 0

        log_info(f"ID范围处理完成。总计: {self.stats['total']}，成功: {self.stats['success']}，"
                f"失败: {self.stats['failed']}，跳过: {self.stats['skipped']}，"
                f"总耗时: {self.stats['total_time']:.2f}秒")

        return self.stats

    async def process_batch_by_key_with_range(self, key: int, start_id: Optional[int] = None, end_id: Optional[int] = None,
                                  output_dir: Optional[str] = None,
                                  save_image: bool = False) -> Dict[str, Any]:
        """
        按键值批量处理，支持在key内部设置范围

        Args:
            key: ID范围键值 (start = key*100000)
            start_id: 起始ID偏移量，如果不为None，则使用key*100000+start_id作为起始ID
            end_id: 结束ID偏移量，如果不为None，则使用key*100000+end_id作为结束ID
            output_dir: 输出目录
            save_image: 是否保存图片

        Returns:
            处理结果统计
        """
        # 计算ID范围
        base_id = key * 100000
        range_start = base_id + start_id if start_id is not None else base_id
        range_end = base_id + end_id if end_id is not None else base_id + 100000

        return await self.process_batch(
            start_id=range_start,
            end_id=range_end,
            output_dir=output_dir,
            save_image=save_image
        )

    async def process_batch_by_key(self, key: int,
                                  output_dir: Optional[str] = None,
                                  save_image: bool = False) -> Dict[str, Any]:
        """
        按键值批量处理（每个key对应100000个ID）

        Args:
            key: ID范围键值 (start = key*100000, end = (key+1)*100000)
            output_dir: 输出目录
            save_image: 是否保存图片

        Returns:
            处理结果统计
        """
        # 计算ID范围
        start_id = key * 100000
        end_id = (key + 1) * 100000

        return await self.process_batch(
            start_id=start_id,
            end_id=end_id,
            output_dir=output_dir,
            save_image=save_image
        )

    async def process_id_list(self, id_list: List[Union[str, int]],
                             output_dir: Optional[str] = None,
                             save_image: bool = False) -> Dict[str, Any]:
        """
        处理ID列表

        Args:
            id_list: ID列表
            output_dir: 输出目录
            save_image: 是否保存图片

        Returns:
            处理结果统计
        """
        if not id_list:
            return {
                "success": False,
                "error": "ID列表为空",
                "total": 0,
                "processed": 0
            }

        log_info(f"开始处理ID列表，共 {len(id_list)} 个ID")

        # 转换所有ID为整数
        int_id_list = []
        for id_value in id_list:
            try:
                int_id_list.append(int(id_value))
            except (ValueError, TypeError):
                log_warning(f"无效的ID格式: {id_value}，跳过")

        # 获取已经处理过的ID
        processed_ids = set()
        min_id = min(int_id_list) if int_id_list else 0
        max_id = max(int_id_list) if int_id_list else 0

        if max_id > min_id:
            processed_ids = await self.danbooru_gemini_captions.get_processed_ids(min_id, max_id + 1)

        log_info(f"已处理的ID数量: {len(processed_ids)}")

        # 获取URL信息
        log_info(f"获取ID列表的URL信息")
        url_batch_result = await self.danbooru_pics.check_url_by_id_batch(int_id_list)

        # 找出需要处理的ID
        ids_to_process = []
        url_map = {}  # 用于存储ID->URL的映射

        for id_value in int_id_list:
            if id_value in url_batch_result and id_value not in processed_ids:
                data = url_batch_result[id_value]

                # 只处理状态为200的ID（URL可获取）
                if data.get("status") == 200 and data.get("url"):
                    ids_to_process.append(id_value)
                    url_map[id_value] = data.get("url")

        # 统计信息
        skipped_processed = len(processed_ids.intersection(set(int_id_list)))
        skipped_no_url = len(int_id_list) - skipped_processed - len(ids_to_process)

        log_info(f"有效ID: {len(int_id_list)}, 已处理: {skipped_processed}, 无URL: {skipped_no_url}, 待处理: {len(ids_to_process)}")

        # 重置统计信息
        self.stats = {
            "total": len(id_list),
            "success": 0,
            "failed": 0,
            "skipped": skipped_processed + skipped_no_url,
            "start_time": time.time(),
            "end_time": 0
        }

        # 如果没有需要处理的ID，直接返回
        if not ids_to_process:
            log_info("没有需要处理的ID，跳过批处理")
            self.stats["end_time"] = time.time()
            self.stats["total_time"] = self.stats["end_time"] - self.stats["start_time"]
            self.stats["avg_time_per_item"] = 0
            return self.stats

        # 创建任务列表
        tasks = []
        for id_value in ids_to_process:
            # 使用缓存的URL，避免再次查询
            custom_url = url_map.get(id_value)
            task = self._process_id_with_semaphore(
                id_value,
                output_dir,
                save_image,
                custom_url
            )
            tasks.append(task)

        # 执行所有任务
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 统计结果
        for result in results:
            if isinstance(result, Exception):
                self.stats["failed"] += 1
                log_error(f"任务执行异常: {str(result)}")
            else:
                if result.get("success", False):
                    self.stats["success"] += 1
                else:
                    self.stats["failed"] += 1

        # 更新统计信息
        self.stats["end_time"] = time.time()
        self.stats["total_time"] = self.stats["end_time"] - self.stats["start_time"]
        self.stats["avg_time_per_item"] = self.stats["total_time"] / len(ids_to_process) if ids_to_process else 0

        log_info(f"ID列表处理完成。总计: {self.stats['total']}，成功: {self.stats['success']}，"
                f"失败: {self.stats['failed']}，跳过: {self.stats['skipped']}，"
                f"总耗时: {self.stats['total_time']:.2f}秒")

        return self.stats