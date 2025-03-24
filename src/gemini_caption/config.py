'''
配置管理模块

提供全局配置项的管理，包括MongoDB连接、模型设置、区域配置等。
支持从环境变量加载配置，并处理Google应用凭证。
'''
import os
from gemini_caption.utils.logger_utils import log_info, log_debug, log_warning, log_error
from typing import Optional, Dict, Any

class Config:
    MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")

    KEY = int(os.getenv("KEY", "0")) if os.getenv("KEY", "0").isdigit() else None

    MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "100"))

    MODEL_ID = os.getenv("MODEL_ID", "gemini-2.0-flash-lite-001")

    LANGUAGE = os.getenv("LANGUAGE", "zh")
    if LANGUAGE not in ["zh", "en"]:
        LANGUAGE = "zh"

    HF_REPO = os.getenv("HF_REPO", "picollect/danbooru")
    USE_HFPICS_FIRST = bool(int(os.getenv("USE_HFPICS_FIRST", "0")))

    LOG_LEVEL = os.getenv("LOG_LEVEL", "info")
    LOG_FILE = os.getenv("LOG_FILE", None)

    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/kaggle/working/credentials.json")
    GOOGLE_APPLICATION_CREDENTIALS_CONTENT = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_CONTENT", None)

    GOOGLE_REGION = [   "us-east5",
                        "us-south1",
                        "us-central1",
                        "us-west4",
                        "us-east1",
                        "us-east4",
                        "us-west1",
                        "europe-west4",
                        "europe-west9",
                        "europe-west1",
                        "europe-southwest1",
                        "europe-west8",
                        "europe-north1",
                        "europe-central2",
                    ]

    def __init__(self):
        self._initialize_credentials()
        pass

    def _initialize_credentials(self):
        """初始化Google凭证"""
        if self.GOOGLE_APPLICATION_CREDENTIALS_CONTENT is not None:
            try:
                # 确保目录存在
                credentials_dir = os.path.dirname(self.GOOGLE_APPLICATION_CREDENTIALS)
                if credentials_dir and not os.path.exists(credentials_dir):
                    os.makedirs(credentials_dir, exist_ok=True)
                    log_info(f"已创建凭证目录: {credentials_dir}")

                # 写入凭证内容到文件
                with open(self.GOOGLE_APPLICATION_CREDENTIALS, "w") as f:
                    f.write(self.GOOGLE_APPLICATION_CREDENTIALS_CONTENT)
                log_info(f"已将凭证内容写入到 {self.GOOGLE_APPLICATION_CREDENTIALS}")
            except PermissionError as e:
                log_error(f"写入凭证文件时权限错误: {str(e)}")
                log_error("请确保应用具有写入指定目录的权限")
                raise
            except Exception as e:
                log_error(f"写入凭证文件时出错: {str(e)}")
                raise

        else:
            # 检查是否存在文件且有内容
            if os.path.exists(self.GOOGLE_APPLICATION_CREDENTIALS):
                if os.path.getsize(self.GOOGLE_APPLICATION_CREDENTIALS) > 0:
                    log_info(f"已找到凭证文件 {self.GOOGLE_APPLICATION_CREDENTIALS}")
                else:
                    log_warning(f"凭证文件存在但为空: {self.GOOGLE_APPLICATION_CREDENTIALS}")
                    raise ValueError("凭证文件为空")
            else:
                raise ValueError(f"凭证文件不存在: {self.GOOGLE_APPLICATION_CREDENTIALS}，且GOOGLE_APPLICATION_CREDENTIALS_CONTENT为空")

    @classmethod
    def get_config(cls):
        """获取配置单例实例"""
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance

    @classmethod
    def update_config(cls, **kwargs) -> 'Config':
        """
        更新配置参数，支持动态修改配置项

        Args:
            **kwargs: 要更新的配置参数，键为配置名称，值为新的配置值

        Returns:
            Config: 配置单例实例
        """
        instance = cls.get_config()

        # 更新配置项
        for key, value in kwargs.items():
            if hasattr(instance, key):
                setattr(instance, key, value)
                log_info(f"已更新配置项 {key}: {value}")
            else:
                log_warning(f"未知配置项: {key}")

        return instance

    def to_dict(self) -> Dict[str, Any]:
        """
        将配置转换为字典格式

        Returns:
            Dict[str, Any]: 包含所有配置项的字典
        """
        config_dict = {}
        for key in dir(self):
            # 排除私有属性、方法和内置属性
            if not key.startswith('_') and not callable(getattr(self, key)):
                config_dict[key] = getattr(self, key)
        return config_dict
