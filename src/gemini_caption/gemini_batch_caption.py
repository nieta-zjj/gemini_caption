'''
Gemini批量图像描述主程序

提供命令行界面和高级接口，用于批量处理图像描述任务
'''

import os
import asyncio
import argparse
from typing import Optional, Dict, Any, Union, List
import time

# 导入自定义模块
from gemini_caption.utils.logger_utils import log_info, log_debug, log_warning, log_error, logger_utils
from gemini_caption.utils.batch_processor import BatchProcessor
from gemini_caption.config import Config

class GeminiBatchCaption:
    """
    Gemini批量图像描述主类

    整合各个组件，提供高级接口进行批量图像描述处理
    """

    def __init__(self,
                 model_id: Optional[str] = None,
                 mongodb_uri: Optional[str] = None,
                 language: Optional[str] = None,
                 max_concurrency: Optional[int] = None,
                 hf_repo: Optional[str] = None,
                 hf_cache_dir: Optional[str] = None,
                 use_hfpics_first: Optional[bool] = None,
                 log_level: Optional[str] = None,
                 project_id: Optional[str] = None):
        """
        初始化批量图像描述器

        Args:
            model_id: 使用的模型ID
            mongodb_uri: MongoDB连接URI
            language: 输出语言
            max_concurrency: 最大并发数
            hf_repo: HuggingFace仓库名称
            hf_cache_dir: HFPics缓存目录
            use_hfpics_first: 是否优先使用HFPics获取图片
            log_level: 日志级别 (debug, info, warning, error)
            project_id: Google Cloud项目ID
        """
        # 加载配置
        config = Config.get_config()

        # 使用传入的参数或配置参数
        self.model_id = model_id or config.MODEL_ID
        self.mongodb_uri = mongodb_uri or config.MONGODB_URI
        self.language = language or config.LANGUAGE
        self.max_concurrency = max_concurrency or config.MAX_CONCURRENCY
        self.hf_repo = hf_repo or config.HF_REPO
        self.hf_cache_dir = hf_cache_dir
        self.use_hfpics_first = use_hfpics_first if use_hfpics_first is not None else config.USE_HFPICS_FIRST
        self.project_id = project_id or "poised-runner-402505"

        # 设置日志级别
        log_level = log_level or config.LOG_LEVEL
        logger_utils.set_log_level(log_level)
        if config.LOG_FILE:
            logger_utils.setup_file_handler(config.LOG_FILE)

        log_info(f"初始化GeminiBatchCaption，并发数: {self.max_concurrency}，语言: {self.language}")

        # 批处理器将在initialize中创建
        self.batch_processor = None

    async def initialize(self):
        """异步初始化方法"""
        try:
            # 创建批处理器
            self.batch_processor = BatchProcessor(
                model_id=self.model_id,
                mongodb_uri=self.mongodb_uri,
                language=self.language,
                max_concurrency=self.max_concurrency,
                hf_repo=self.hf_repo,
                hf_cache_dir=self.hf_cache_dir,
                use_hfpics_first=self.use_hfpics_first,
                project_id=self.project_id
            )
            log_info("GeminiBatchCaption初始化完成")
            return self
        except Exception as e:
            log_error(f"初始化失败: {str(e)}")
            raise

    async def close(self):
        """关闭资源"""
        if self.batch_processor:
            await self.batch_processor.close()
        log_info("GeminiBatchCaption资源已关闭")

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
        if not self.batch_processor:
            log_error("批处理器未初始化，请先调用initialize()")
            return {"success": False, "error": "批处理器未初始化"}

        return await self.batch_processor.process_single_id(
            dan_id=dan_id,
            output_dir=output_dir,
            save_image=save_image,
            skip_existing_check=skip_existing_check,
            custom_url=custom_url
        )

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
        if not self.batch_processor:
            log_error("批处理器未初始化，请先调用initialize()")
            return {"success": False, "error": "批处理器未初始化"}

        return await self.batch_processor.process_batch(
            start_id=start_id,
            end_id=end_id,
            output_dir=output_dir,
            save_image=save_image
        )

    async def process_batch_by_key(self, key: int,
                                 output_dir: Optional[str] = None,
                                 save_image: bool = False) -> Dict[str, Any]:
        """
        按键值批量处理（每个key对应100000个ID）

        Args:
            key: 键值
            output_dir: 输出目录
            save_image: 是否保存图片

        Returns:
            处理结果统计
        """
        if not self.batch_processor:
            log_error("批处理器未初始化，请先调用initialize()")
            return {"success": False, "error": "批处理器未初始化"}

        return await self.batch_processor.process_batch_by_key(
            key=key,
            output_dir=output_dir,
            save_image=save_image
        )

# 提供给外部调用的异步函数
async def run_batch_with_args(key: Optional[int] = None,
                             start_id: Optional[int] = None,
                             end_id: Optional[int] = None,
                             max_concurrency: Optional[int] = None,
                             model_id: Optional[str] = None,
                             language: Optional[str] = None,
                             mongodb_uri: Optional[str] = None,
                             output_dir: str = "caption_results",
                             save_image: bool = False,
                             hf_repo: Optional[str] = None,
                             hf_cache_dir: Optional[str] = None,
                             use_hfpics_first: Optional[bool] = None,
                             log_level: Optional[str] = None,
                             log_file: Optional[str] = None,
                             project_id: Optional[str] = None) -> Dict[str, Any]:
    """
    使用给定参数运行批处理任务

    Args:
        key: 区间键值
        start_id: 起始ID
        end_id: 结束ID
        max_concurrency: 最大并发数
        model_id: 使用的模型ID
        language: 输出语言
        mongodb_uri: MongoDB连接URI
        output_dir: 输出目录
        save_image: 是否保存图片
        hf_repo: HuggingFace仓库名称
        hf_cache_dir: HFPics缓存目录
        use_hfpics_first: 是否优先使用HFPics获取图片
        log_level: 日志级别
        log_file: 日志文件路径
        project_id: Google Cloud项目ID

    Returns:
        处理结果统计
    """
    # 设置日志
    config = Config.get_config()
    log_level = log_level or config.LOG_LEVEL
    logger_utils.set_log_level(log_level)
    if log_file:
        logger_utils.setup_file_handler(log_file)
    elif config.LOG_FILE:
        logger_utils.setup_file_handler(config.LOG_FILE)

    # 准备输出目录
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # 创建批处理器
    batch_captioner = None
    result = None

    try:
        log_info(f"开始批处理任务")

        # 初始化批处理器
        batch_captioner = await GeminiBatchCaption(
            model_id=model_id,
            mongodb_uri=mongodb_uri,
            language=language,
            max_concurrency=max_concurrency,
            hf_repo=hf_repo,
            hf_cache_dir=hf_cache_dir,
            use_hfpics_first=use_hfpics_first,
            log_level=log_level,
            project_id=project_id
        ).initialize()

        # 根据参数选择处理方法 - 只允许两种模式
        if key is not None and start_id is None and end_id is None:
            # 按key处理全部
            result = await batch_captioner.process_batch_by_key(
                key=key,
                output_dir=output_dir,
                save_image=save_image
            )
        elif key is None and start_id is not None and end_id is not None:
            # 按ID范围处理
            result = await batch_captioner.process_batch(
                start_id=start_id,
                end_id=end_id,
                output_dir=output_dir,
                save_image=save_image
            )
        else:
            log_error("只能使用key参数或同时使用start_id和end_id参数，不允许混合使用")
            result = {"success": False, "error": "参数组合错误，只允许使用key或同时使用start_id和end_id"}

        return result
    except Exception as e:
        log_error(f"批处理任务失败: {str(e)}")
        return {"success": False, "error": str(e)}
    finally:
        # 确保资源被释放
        if batch_captioner:
            await batch_captioner.close()
        log_info("批处理任务结束")

def main():
    """命令行入口函数"""
    parser = argparse.ArgumentParser(description="Gemini批量图像描述工具")

    # 添加参数
    parser.add_argument("--key", type=int, help="区间键值")
    parser.add_argument("--start-id", type=int, help="起始ID")
    parser.add_argument("--end-id", type=int, help="结束ID")
    parser.add_argument("--max-concurrency", type=int, help="最大并发数")
    parser.add_argument("--model-id", help="使用的模型ID")
    parser.add_argument("--language", help="输出语言")
    parser.add_argument("--mongodb-uri", help="MongoDB连接URI")
    parser.add_argument("--output-dir", default=None, help="输出目录")
    parser.add_argument("--save-image", action="store_true", default=False, help="是否保存图片")
    parser.add_argument("--hf-repo", help="HuggingFace仓库名称")
    parser.add_argument("--hf-cache-dir", help="HFPics缓存目录")
    parser.add_argument("--use-hfpics-first", action="store_true", default=False, help="是否优先使用HFPics获取图片")
    parser.add_argument("--log-level", choices=["debug", "info", "warning", "error"], default="info", help="日志级别")
    parser.add_argument("--log-file", help="日志文件路径")
    parser.add_argument("--project-id", help="Google Cloud项目ID")

    args = parser.parse_args()

    # 检查必要参数 - 只允许两种方式
    has_key = args.key is not None
    has_start_end = args.start_id is not None and args.end_id is not None

    if not has_key and not has_start_end:
        parser.error("必须提供--key参数或同时提供--start-id和--end-id参数")

    if has_key and (args.start_id is not None or args.end_id is not None):
        parser.error("不能同时使用--key参数和--start-id/--end-id参数，请只使用一种方式")

    # 创建事件循环并运行批处理任务
    loop = asyncio.get_event_loop()
    try:
        result = loop.run_until_complete(run_batch_with_args(
            key=args.key,
            start_id=args.start_id,
            end_id=args.end_id,
            max_concurrency=args.max_concurrency,
            model_id=args.model_id,
            language=args.language,
            mongodb_uri=args.mongodb_uri,
            output_dir=args.output_dir,
            save_image=args.save_image,
            hf_repo=args.hf_repo,
            hf_cache_dir=args.hf_cache_dir,
            use_hfpics_first=args.use_hfpics_first,
            log_level=args.log_level,
            log_file=args.log_file,
            project_id=args.project_id
        ))

        # 输出结果
        if result.get("success", 0) > 0:
            log_info(f"批处理任务完成，成功: {result.get('success', 0)}，"
                    f"失败: {result.get('failed', 0)}，总计: {result.get('total', 0)}")
        else:
            log_error(f"批处理任务未成功完成: {result.get('error', '未知错误')}")

    finally:
        loop.close()

if __name__ == "__main__":
    main()