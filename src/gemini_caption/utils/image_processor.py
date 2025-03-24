import os
import time
import httpx
import asyncio
import random
import subprocess
import tempfile
from typing import Tuple, Optional, Dict, Any, Union
from hfpics import HfPics

# 导入日志工具
from gemini_caption.utils.logger_utils import log_info, log_debug, log_warning, log_error

class ImageProcessor:
    """
    负责图片下载和处理的类
    """

    def __init__(self,
                 hf_repo: str = "picollect/danbooru",
                 hf_cache_dir: Optional[str] = None,
                 use_hfpics_first: bool = False,
                 use_wget: bool = True):
        """
        初始化图片处理器

        Args:
            hf_repo: HuggingFace仓库名称
            hf_cache_dir: HfPics缓存目录
            use_hfpics_first: 是否优先使用HfPics获取图片
            use_wget: 是否优先使用系统wget工具下载图片
        """
        self.use_hfpics_first = use_hfpics_first
        self.use_wget = use_wget

        # 初始化HFPics客户端
        if hf_cache_dir is None:
            # 不传递cache_dir参数，让HFPics使用默认路径
            self.hf_pics = HfPics(repo=hf_repo)
        else:
            self.hf_pics = HfPics(repo=hf_repo, cache_dir=hf_cache_dir)
        log_info(f"已初始化HFPics客户端，使用仓库: {hf_repo}")

        # 检查wget是否可用
        if self.use_wget:
            self._check_wget_available()

        # 文件类型到MIME类型的映射
        self.mime_type_map = {
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "webp": "image/webp",
            "gif": "image/gif"
        }

    def _check_wget_available(self):
        """
        检查系统中是否安装了wget工具
        """
        try:
            # 使用subprocess检查wget命令是否可用
            subprocess.run(["wget", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
            log_info("wget工具可用，将优先使用wget下载图片")
        except (subprocess.SubprocessError, FileNotFoundError):
            log_warning("wget工具不可用，将使用httpx库下载图片")
            self.use_wget = False

    def get_random_headers(self) -> Dict[str, str]:
        """
        生成随机的浏览器请求头

        Returns:
            随机生成的请求头字典
        """
        # 随机选择浏览器和版本
        browsers = [
            # Chrome
            {
                "name": "Google Chrome",
                "version_prefix": random.choice(["110", "111", "112", "113", "114", "115", "116", "120", "131", "132"]),
                "chromium_version": random.choice(["110", "111", "112", "113", "114", "115", "116", "120", "131", "132"]),
                "platform": random.choice(["Windows", "Macintosh", "X11"]),
                "os_version": random.choice(["10.0", "11.0"]) if "Windows" else "Intel Mac OS X 10_15_7" if "Macintosh" else "Linux x86_64"
            },
            # Edge
            {
                "name": "Microsoft Edge",
                "version_prefix": random.choice(["110", "111", "112", "113", "114", "115", "116", "120"]),
                "chromium_version": random.choice(["110", "111", "112", "113", "114", "115", "116", "120"]),
                "platform": random.choice(["Windows", "Macintosh", "X11"]),
                "os_version": random.choice(["10.0", "11.0"]) if "Windows" else "Intel Mac OS X 10_15_7" if "Macintosh" else "Linux x86_64"
            }
        ]

        selected_browser = random.choice(browsers)

        # 构建版本号
        minor_version = f"{random.randint(0, 9999)}.{random.randint(0, 99)}"
        full_version = f"{selected_browser['version_prefix']}.0.{minor_version}"

        # 生成对应平台的User-Agent
        user_agent = ""
        if selected_browser["platform"] == "Windows":
            user_agent = f"Mozilla/5.0 (Windows NT {selected_browser['os_version']}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{full_version} Safari/537.36"
            # 添加浏览器特定标识
            if selected_browser["name"] == "Microsoft Edge":
                user_agent += f" Edg/{full_version}"
        elif selected_browser["platform"] == "Macintosh":
            user_agent = f"Mozilla/5.0 (Macintosh; {selected_browser['os_version']}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{full_version} Safari/537.36"
            if selected_browser["name"] == "Microsoft Edge":
                user_agent += f" Edg/{full_version}"
        else:  # X11
            user_agent = f"Mozilla/5.0 (X11; {selected_browser['os_version']}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{full_version} Safari/537.36"
            if selected_browser["name"] == "Microsoft Edge":
                user_agent += f" Edg/{full_version}"

        # 生成sec-ch-ua
        sec_ch_ua = f'"Not A(Brand";v="8", "Chromium";v="{selected_browser["chromium_version"]}", "{selected_browser["name"]}";v="{selected_browser["version_prefix"]}"'

        # 只保留必要的请求头
        headers = {
            "User-Agent": user_agent,
            "Referer": "https://danbooru.donmai.us/",
            "sec-ch-ua": sec_ch_ua,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": f'"{selected_browser["platform"]}"',
        }

        return headers

    async def download_with_wget(self, image_url: str) -> Optional[bytes]:
        """
        使用wget工具下载图片

        Args:
            image_url: 图片URL

        Returns:
            图片字节内容，下载失败则返回None
        """
        try:
            # 创建临时文件用于保存下载内容
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_path = temp_file.name

            # 生成随机的User-Agent
            headers = self.get_random_headers()
            user_agent = headers.get('User-Agent', '')
            referer = headers.get('Referer', '')

            # 构建wget命令
            cmd = [
                "wget",
                "--quiet",  # 安静模式
                "--tries=3",  # 重试3次
                "--timeout=60",  # 超时时间60秒
                "--user-agent=" + user_agent,
                "--referer=" + referer,
                "-O", temp_path,  # 输出到临时文件
                image_url
            ]

            log_debug(f"使用wget下载图片，命令: {' '.join(cmd)}")

            # 执行wget命令
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            # 等待命令执行完成
            stdout, stderr = await process.communicate()

            # 检查wget返回状态
            if process.returncode != 0:
                log_warning(f"wget下载失败，返回码: {process.returncode}, 错误: {stderr.decode().strip()}")
                # 删除临时文件
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass
                return None

            # 读取下载的文件内容
            with open(temp_path, 'rb') as f:
                image_content = f.read()

            # 删除临时文件
            try:
                os.unlink(temp_path)
            except OSError as e:
                log_warning(f"删除临时文件失败: {str(e)}")

            return image_content

        except Exception as e:
            log_error(f"使用wget下载图片时出错: {str(e)}")
            return None

    async def download_image(self, image_url: str, dan_id: Optional[int] = None) -> Tuple[Optional[bytes], str, str]:
        """
        下载图片

        Args:
            image_url: 图片URL
            dan_id: Danbooru图片ID，用于日志记录

        Returns:
            图片字节内容、MIME类型和文件扩展名的元组
        """
        log_info(f"开始下载图片: {image_url}" + (f", ID: {dan_id}" if dan_id else ""))

        # 获取文件的MIME类型和扩展名
        file_extension = os.path.splitext(image_url)[1][1:].lower()
        mime_type = self.mime_type_map.get(file_extension, "image/jpeg")

        image_bytes = None

        # 如果启用了wget，首先尝试使用wget下载
        if self.use_wget:
            log_debug("尝试使用wget下载图片...")
            image_bytes = await self.download_with_wget(image_url)
            if image_bytes:
                log_info("使用wget成功下载图片")
                return image_bytes, mime_type, file_extension

        # 如果wget下载失败或未启用wget，使用httpx下载
        if not image_bytes:
            log_debug("使用httpx下载图片...")
            # 重试间隔（秒）
            retry_delays = [1, 5, 30, 60, 300]

            # 尝试获取图片内容
            for attempt, delay in enumerate(retry_delays):
                try:
                    # 每次请求生成新的随机请求头
                    headers = self.get_random_headers()
                    log_debug(f"正在获取图片内容... 尝试 {attempt + 1}/{len(retry_delays)}")
                    log_debug(f"使用User-Agent: {headers.get('User-Agent', '未知')}")

                    async with httpx.AsyncClient(timeout=60.0, headers=headers, follow_redirects=True) as client:
                        response = await client.get(image_url)
                        if response.status_code == 200:
                            image_bytes = response.content
                            break
                        else:
                            log_warning(f"获取失败，状态码: {response.status_code}")
                except Exception as e:
                    log_error(f"获取图片时出错: {str(e)}")

                if attempt < len(retry_delays) - 1:  # 如果不是最后一次尝试
                    log_debug(f"等待 {delay} 秒后重试...")
                    await asyncio.sleep(delay)

        return image_bytes, mime_type, file_extension

    async def get_image_from_hfpics(self, dan_id: Union[str, int]) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
        """
        从HFPics获取图片

        Args:
            dan_id: Danbooru图片ID

        Returns:
            图片字节内容、MIME类型和文件扩展名的元组
        """
        try:
            # 确保ID是整数
            dan_id = int(dan_id)
            log_info(f"尝试从HFPics获取图片ID: {dan_id}")

            # 获取图片内容
            image_bytes = self.hf_pics.pic(dan_id, return_type="content")

            if not image_bytes:
                log_warning(f"从HFPics获取图片ID {dan_id} 失败，返回为空")
                return None, None, None

            log_info(f"成功从HFPics获取图片ID: {dan_id}")

            # 尝试获取图片扩展名和MIME类型
            try:
                pic_info = self.hf_pics.pic_info(dan_id)
                if pic_info and 'pic_url' in pic_info:
                    pic_url = pic_info['pic_url']
                    file_extension = os.path.splitext(pic_url)[1][1:].lower()
                    mime_type = self.mime_type_map.get(file_extension, "image/jpeg")
                    return image_bytes, mime_type, file_extension
            except Exception as e:
                log_warning(f"从HFPics获取图片信息失败: {str(e)}，使用默认MIME类型")

            # 默认返回jpeg格式
            return image_bytes, "image/jpeg", "jpg"

        except Exception as e:
            log_warning(f"从HFPics获取图片失败: {str(e)}")
            return None, None, None

    async def save_image(self, image_bytes: bytes, output_path: str) -> bool:
        """
        保存图片到本地文件

        Args:
            image_bytes: 图片字节内容
            output_path: 输出文件路径

        Returns:
            是否成功保存
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

            # 写入文件
            with open(output_path, 'wb') as f:
                f.write(image_bytes)

            log_debug(f"图片已保存到: {output_path}")
            return True
        except Exception as e:
            log_error(f"保存图片失败: {str(e)}")
            return False

    async def process_image_by_id(self, dan_id: Union[str, int], custom_url: Optional[str] = None) -> Dict[str, Any]:
        """
        根据ID处理图片，从HFPics或URL获取

        Args:
            dan_id: Danbooru图片ID
            custom_url: 自定义URL，如果提供则优先使用

        Returns:
            包含图片信息的字典
        """
        # 将ID统一为整数类型
        dan_id = int(dan_id)
        image_bytes = None
        mime_type = None
        file_extension = None
        image_url = custom_url
        result = {"success": False}

        # 如果设置了优先使用HFPics且没有提供自定义URL
        if self.use_hfpics_first and not custom_url:
            image_bytes, mime_type, file_extension = await self.get_image_from_hfpics(dan_id)

            # 如果从HFPics成功获取图片
            if image_bytes:
                result = {
                    "success": True,
                    "image_bytes": image_bytes,
                    "mime_type": mime_type,
                    "file_extension": file_extension,
                    "source": "hfpics"
                }
                return result

        # 如果没有从HFPics获取或未设置优先使用HFPics
        if custom_url:
            # 使用提供的URL
            image_url = custom_url
        else:
            # 这里需要从外部获取URL，所以返回需要获取URL的信息
            return {
                "success": False,
                "need_url": True,
                "message": "需要从外部获取图片URL"
            }

        # 如果有URL，下载图片
        if image_url:
            image_bytes, mime_type, file_extension = await self.download_image(image_url, dan_id)

            if image_bytes:
                result = {
                    "success": True,
                    "image_bytes": image_bytes,
                    "mime_type": mime_type,
                    "file_extension": file_extension,
                    "image_url": image_url,
                    "source": "url"
                }
            else:
                result = {
                    "success": False,
                    "error": "下载图片失败",
                    "image_url": image_url
                }

        return result