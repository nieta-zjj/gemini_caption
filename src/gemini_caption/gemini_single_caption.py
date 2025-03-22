from google import genai
from google.genai import types
import os
import json
import time
import requests
import json_repair
from pymongo import MongoClient
import logging
from typing import Union, Literal
from concurrent.futures import ThreadPoolExecutor
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

class GeminiSingleCaption:
    def __init__(self,
                 api_key=None,
                 model_id="gemini-2.0-flash-lite-001",
                 mongodb_uri=None,
                 language="zh",
                 hf_repo="picollect/danbooru",
                 hf_cache_dir=None):
        """
        初始化同步单图像打标类

        Args:
            api_key: Gemini API密钥
            model_id: 使用的模型ID
            mongodb_uri: MongoDB连接URI，如果为None则使用环境变量
            language: 输出语言
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

        self.model_id = model_id
        self.language = language
        self.character_analyzer = CharacterAnalyzer(mongodb_uri)
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

        # 初始化MongoDB连接
        self.client = MongoClient(mongodb_uri)
        self.db = self.client["gemini_captions_danbooru"]  # 修改为新的数据库名
        # 不再在初始化时创建集合，而是在处理每个ID时动态选择

        # 文件类型到MIME类型的映射
        self.mime_type_map = {
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "webp": "image/webp"
        }

    def __del__(self):
        """析构函数，确保资源正确释放"""
        self.close()

    def close(self):
        """关闭所有打开的资源"""
        try:
            # 关闭MongoDB连接
            if hasattr(self, 'client'):
                self.client.close()
            if hasattr(self, 'character_analyzer') and self.character_analyzer:
                if hasattr(self.character_analyzer, 'pics') and self.character_analyzer.pics:
                    if hasattr(self.character_analyzer.pics, 'client') and self.character_analyzer.pics.client:
                        self.character_analyzer.pics.client.close()
                if hasattr(self.character_analyzer, 'tags') and self.character_analyzer.tags:
                    if hasattr(self.character_analyzer.tags, 'client') and self.character_analyzer.tags.client:
                        self.character_analyzer.tags.client.close()
            logger.info("已关闭数据库连接")
        except Exception as e:
            logger.error(f"关闭资源时发生错误: {str(e)}")

    def get_danbooru_info_by_id(self, dan_id):
        """获取 Danbooru 信息"""
        # 将异步方法转换为同步方法
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            character_reference_info = loop.run_until_complete(
                self.character_analyzer.get_visualize_tree_by_pid(dan_id, "en")
            )
            artist_name = loop.run_until_complete(
                self.character_analyzer.pics.get_pic_artist_by_id(dan_id)
            )
            # 确保artist_name是可迭代对象
            if artist_name is None or not hasattr(artist_name, '__iter__'):
                artist_name = []
            artist_name = "&".join(artist_name)
            danbooru_tags = " ".join(loop.run_until_complete(
                self.character_analyzer.pics.get_pic_general_by_id(dan_id)
            ))
            return character_reference_info, artist_name, danbooru_tags
        finally:
            loop.close()

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

    def get_url_by_id(self, dan_id):
        """从Danbooru获取图片URL"""
        request_url = f"https://danbooru.donmai.us/posts/{dan_id}.json"
        response = requests.get(request_url)

        if response.status_code == 404:
            error_result = {
                "_id": dan_id,
                "success": False,
                "error": "Post not found",
                "status_code": 404,
                "created_at": time.time()
            }
            # 保存404错误到MongoDB
            collection = self.get_collection_for_id(dan_id)
            collection.update_one(
                {"_id": dan_id},
                {"$set": error_result},
                upsert=True
            )
            raise ValueError(f"Danbooru post {dan_id} not found (404)")

        if response.status_code != 200:
            raise Exception(f"获取Danbooru信息失败，状态码: {response.status_code}")

        data = response.json()
        url = None

        try:
            url = data.get("large_file_url")
            if not url:
                url = data.get("file_url")
        except Exception:
            pass

        if not url:
            error_result = {
                "_id": dan_id,
                "success": False,
                "error": "Post URL not found",
                "status_code": 404,
                "created_at": time.time()
            }
            # 保存404错误到MongoDB
            collection = self.get_collection_for_id(dan_id)
            collection.update_one(
                {"_id": dan_id},
                {"$set": error_result},
                upsert=True
            )
            raise ValueError(f"无法从Danbooru获取图片URL，ID: {dan_id}")

        # 如果URL是相对路径，添加域名
        if url.startswith("/"):
            url = f"https://danbooru.donmai.us{url}"

        return url

    def process_single_id(self, dan_id, output_dir=None, save_image=True):
        """
        处理单个Danbooru ID

        Args:
            dan_id: Danbooru图片ID
            output_dir: 输出目录
            save_image: 是否保存下载的图片

        Returns:
            处理结果字典
        """
        start_time = time.time()
        logger.info(f"开始处理Danbooru ID: {dan_id}")

        try:
            # 获取对应的集合
            collection = self.get_collection_for_id(dan_id)

            # 首先尝试从HFPics获取图片
            logger.info(f"尝试从HFPics获取图片ID: {dan_id}")
            image_bytes = None
            image_url = None
            mime_type = "image/jpeg"  # 默认MIME类型
            file_extension = "jpg"    # 默认文件扩展名

            # 尝试使用HFPics获取图片
            try:
                image_bytes = self.hf_pics.pic(int(dan_id), return_type="content")

                if image_bytes:
                    logger.info(f"成功从HFPics获取图片ID: {dan_id}")

                    # 尝试从HFPics获取图片扩展名
                    try:
                        # 尝试获取图片原始路径，从中提取扩展名
                        pic_info = self.hf_pics.pic_info(int(dan_id))

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
                image_url = self.get_url_by_id(dan_id)
                logger.debug(f"获取到图片URL: {image_url}")

                # 获取文件的MIME类型
                file_extension = os.path.splitext(image_url)[1][1:].lower()
                mime_type = self.mime_type_map.get(file_extension, "image/jpeg")

                # 重试间隔（秒）
                retry_delays = [1, 5, 30, 60, 300]

                # 尝试获取图片内容
                for attempt, delay in enumerate(retry_delays):
                    try:
                        logger.debug(f"正在获取图片内容... 尝试 {attempt + 1}/{len(retry_delays)}")
                        response = requests.get(image_url)
                        if response.status_code == 200:
                            image_bytes = response.content
                            break
                        else:
                            logger.warning(f"获取失败，状态码: {response.status_code}")
                    except Exception as e:
                        logger.error(f"获取图片时出错: {str(e)}")

                    if attempt < len(retry_delays) - 1:  # 如果不是最后一次尝试
                        logger.debug(f"等待 {delay} 秒后重试...")
                        time.sleep(delay)

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
                collection.update_one(
                    {"_id": dan_id},
                    {"$set": error_result},
                    upsert=True
                )
                raise Exception("无法获取图片内容，所有重试均失败")

            # 准备输出目录
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            # 如果需要保存图片
            image_path = None
            if save_image and output_dir:
                image_path = os.path.join(output_dir, f"{dan_id}.{file_extension}")
                with open(image_path, 'wb') as f:
                    f.write(image_bytes)
                logger.debug(f"图片已保存到: {image_path}")

            # 获取Danbooru信息
            character_reference_info, artist_name, danbooru_tags = self.get_danbooru_info_by_id(dan_id)

            # 构建提示
            prompt = caption_prompt_utils.build_prompt(
                artist_name=artist_name,
                character_name="",
                danbooru_tags=danbooru_tags,
                language=self.language,
                character_reference_info=character_reference_info
            )

            logger.info("开始调用Gemini API...")

            # 添加请求限制和重试机制
            caption = None
            last_error = None

            for attempt in range(self.retry_attempts):
                try:
                    # 调用 API
                    response = self.genai_client.models.generate_content(
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
                        ),
                    )

                    caption = response.text
                    break

                except ConnectionError as e:
                    # 网络连接错误，适合重试
                    last_error = e
                    logger.warning(f"网络错误，重试 {attempt+1}/{self.retry_attempts}")
                    time.sleep(self.retry_delay)

                except Exception as e:
                    # 其他错误
                    last_error = e
                    logger.warning(f"API调用错误: {str(e)}，重试 {attempt+1}/{self.retry_attempts}")
                    # 增加重试延迟时间，避免频繁请求
                    retry_time = self.retry_delay * (2 ** attempt)  # 指数退避
                    time.sleep(retry_time)

            # 如果所有重试都失败
            if caption is None:
                raise Exception(f"所有重试均失败: {str(last_error)}")

            # 准备结果
            result = {
                "_id": dan_id,
                "image_url": image_url,
                "prompt": prompt,
                "caption": json_repair.loads(caption),
                "artist": artist_name,
                "tags": danbooru_tags,
                "success": True,
                "processing_time": time.time() - start_time,
            }

            # 保存到MongoDB
            try:
                collection.update_one(
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
                collection.update_one(
                    {"_id": dan_id},
                    {"$set": error_result},
                    upsert=True
                )
                logger.debug(f"错误信息已保存到MongoDB，ID: {dan_id}，集合: {collection.name}")
            except Exception as mongo_e:
                logger.error(f"保存错误信息到MongoDB失败: {str(mongo_e)}")

            return error_result

def main():
    api_key = ""
    model_id = "gemini-2.0-flash-lite-001"
    language = "zh"
    mongodb_uri = "mongodb://8.153.97.53:27815/"
    output_dir = "caption_results"
    save_image = False
    dan_id = 1994
    hf_repo = "picollect/danbooru"
    hf_cache_dir = None

    try:
        # 初始化打标器
        captioner = GeminiSingleCaption(
            api_key=api_key,
            model_id=model_id,
            language=language,
            mongodb_uri=mongodb_uri,
            hf_repo=hf_repo,
            hf_cache_dir=hf_cache_dir
        )

        # 处理指定ID
        result = captioner.process_single_id(
            dan_id=dan_id,
            output_dir=output_dir,
            save_image=save_image
        )

        if result["success"]:
            logger.info(f"成功处理Danbooru ID: {dan_id}")
            logger.info(f"生成的标题: {result['caption']}")
        else:
            logger.error(f"处理失败: {result['error']}")

    finally:
        # 确保资源正确释放
        if 'captioner' in locals():
            captioner.close()
            logger.info("已关闭所有资源连接")

if __name__ == "__main__":
    main()