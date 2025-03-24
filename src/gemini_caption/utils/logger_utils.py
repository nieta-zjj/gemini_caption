import logging
import os
from typing import Optional

# 配置日志格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("gemini_caption")

class LoggerUtils:
    """
    处理日志记录的工具类
    """
    def __init__(self):
        self.is_kaggle = os.environ.get("KAGGLE_KERNEL_RUN_TYPE", None) is not None

    @staticmethod
    def set_log_level(level: str):
        """设置日志级别"""
        level_map = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR
        }
        logger.setLevel(level_map.get(level.lower(), logging.INFO))

    def log_info(self, message: str):
        """记录信息级别日志"""
        if self.is_kaggle:
            print(f"[INFO] {message}")
        else:
            logger.info(message)

    def log_debug(self, message: str):
        """记录调试级别日志"""
        if self.is_kaggle:
            print(f"[DEBUG] {message}")
        else:
            logger.debug(message)

    def log_warning(self, message: str):
        """记录警告级别日志"""
        if self.is_kaggle:
            print(f"[WARNING] {message}")
        else:
            logger.warning(message)

    def log_error(self, message: str):
        """记录错误级别日志"""
        if self.is_kaggle:
            print(f"[ERROR] {message}")
        else:
            logger.error(message)

    @staticmethod
    def setup_file_handler(log_file: Optional[str] = None, level: int = logging.INFO):
        """设置文件日志处理器"""
        if log_file:
            # 确保日志目录存在
            os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)

            # 创建文件处理器
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(level)
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)

            # 添加到根日志器
            logger.addHandler(file_handler)
            logger.info(f"已设置文件日志处理器: {log_file}")

# 创建LoggerUtils实例
logger_utils = LoggerUtils()

# 导出方便使用的函数
log_info = logger_utils.log_info
log_debug = logger_utils.log_debug
log_warning = logger_utils.log_warning
log_error = logger_utils.log_error