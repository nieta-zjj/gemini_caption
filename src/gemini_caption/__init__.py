"""
Gemini Caption 模块

提供使用Google Gemini对图像进行批量描述的功能
"""

# 版本信息
__version__ = "0.2.1"

# 导出主要类和函数
from gemini_caption.gemini_batch_caption import GeminiBatchCaption, run_batch_with_args
from gemini_caption.utils.batch_processor import BatchProcessor
from gemini_caption.utils.image_processor import ImageProcessor
from gemini_caption.utils.gemini_api_client import GeminiApiClient
from gemini_caption.utils.caption_promt_utils import CaptionPromptUtils
from gemini_caption.utils.logger_utils import log_info, log_debug, log_warning, log_error, LoggerUtils

# 为方便使用导出main函数
from gemini_caption.gemini_batch_caption import main as batch_main

# 导出其他可能存在的模块
try:
    # 尝试导入单图像处理模块
    from gemini_caption.gemini_single_caption import GeminiSingleCaption, run_single_with_args, main
    from gemini_caption.utils.file_utils import FileUtils
    from gemini_caption.utils.character_analyzer import CharacterAnalyzer
except ImportError:
    pass