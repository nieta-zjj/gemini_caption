from google import genai
from google.genai import types
import json
import time
import json_repair
from typing import Dict, Any, Optional, Union, List
import random
import traceback  # 添加traceback模块导入
import google.auth.exceptions  # 添加OAuth认证错误模块
import asyncio
import uuid  # 添加uuid模块导入

# 导入日志工具
from gemini_caption.utils.logger_utils import log_info, log_debug, log_warning, log_error

class GeminiApiClient:
    """
    处理与Gemini API的交互
    """

    def __init__(self,
                 model_id: str = "gemini-2.0-flash-lite-001",
                 project_id: str = "poised-runner-402505",
                 regions: Optional[List[str]] = None,
                 retry_attempts: int = 3,
                 retry_delay: int = 5):
        """
        初始化Gemini API客户端

        Args:
            model_id: 使用的模型ID
            project_id: Google Cloud项目ID
            regions: 可用的区域列表，如果为None则使用默认区域
            retry_attempts: 重试次数
            retry_delay: 重试延迟（秒）
        """
        self.model_id = model_id
        self.project_id = project_id
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay

        # 默认区域列表
        self.regions = regions or [
            "us-east5", "us-south1", "us-central1", "us-west4",
            "us-east1", "us-east4", "us-west1", "europe-west4",
            "europe-west9", "europe-west1", "europe-southwest1",
            "europe-west8", "europe-north1", "europe-central2"
        ]

        # 保存客户端实例
        self._clients = {}

        try:
            # 记录提示信息以帮助解决OAuth认证问题
            log_info("如果遇到OAuth认证作用域问题，请确保环境中设置了GOOGLE_APPLICATION_CREDENTIALS环境变量，并且服务账号有正确的权限")
            log_info(f"已准备好Gemini客户端配置，使用模型: {self.model_id}")
        except Exception as e:
            log_error(f"初始化Gemini客户端失败: {str(e)}")
            raise

    def _create_new_client(self) -> genai.Client:
        """
        创建一个新的Gemini客户端实例

        Returns:
            genai.Client: 新创建的客户端实例
        """
        # 从区域列表中随机选择一个
        region = random.choice(self.regions)

        # 创建客户端实例
        client = genai.Client(
            vertexai=True,
            project=self.project_id,
            location=region
        )

        log_debug(f"创建新客户端实例，使用区域: {region}")
        return client

    def _call_gemini_sync(self, prompt: str, image_bytes: bytes, mime_type: str, task_id: str = None) -> Dict[str, Any]:
        """
        同步调用Gemini API进行图像描述

        Args:
            prompt: 提示文本
            image_bytes: 图像数据
            mime_type: 图像MIME类型
            task_id: 任务ID，用于日志跟踪

        Returns:
            包含API响应结果的字典
        """
        start_time = time.time()
        if task_id is None:
            task_id = str(uuid.uuid4())[:8]  # 生成简短的任务ID

        log_info(f"[任务 {task_id}] 开始同步调用Gemini API...")

        caption = None
        last_error = None

        # 创建一个新的客户端实例
        client = self._create_new_client()

        for attempt in range(self.retry_attempts):
            try:
                if attempt > 0:
                    # 如果不是第一次尝试，则重新创建客户端
                    client = self._create_new_client()
                    log_info(f"[任务 {task_id}] 重试使用新客户端，尝试次数 {attempt+1}/{self.retry_attempts}")

                # 调用API
                response = client.models.generate_content(
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

                # 检查响应是否有效
                if not response:
                    last_error = Exception(f"API返回无效响应: 响应对象为None")
                    log_warning(f"[任务 {task_id}] API返回空响应对象，重试 {attempt+1}/{self.retry_attempts}")
                    self._delay_retry_sync(attempt)
                    continue

                # 记录完整响应对象信息，帮助调试
                log_debug(f"[任务 {task_id}] API响应对象: {repr(response)}")
                log_debug(f"[任务 {task_id}] API响应对象类型: {type(response)}")
                log_debug(f"[任务 {task_id}] API响应对象属性: {dir(response)}")

                # 检查响应是否有text属性
                if not hasattr(response, 'text'):
                    last_error = Exception(f"API响应对象没有text属性: {repr(response)}")
                    log_warning(f"[任务 {task_id}] API响应缺少text属性，重试 {attempt+1}/{self.retry_attempts}")
                    self._delay_retry_sync(attempt)
                    continue

                # 检查text是否为空
                if not response.text:
                    # 检查是否因为内容政策被拦截
                    prohibited_content = False
                    policy_violation_reason = ""

                    if hasattr(response, 'candidates') and response.candidates:
                        for i, candidate in enumerate(response.candidates):
                            if hasattr(candidate, 'finish_reason'):
                                if candidate.finish_reason == types.FinishReason.PROHIBITED_CONTENT:
                                    prohibited_content = True
                                    policy_violation_reason = "PROHIBITED_CONTENT"
                                    log_warning(f"[任务 {task_id}] 生成内容被安全过滤器拦截: {policy_violation_reason}")
                                elif candidate.finish_reason == types.FinishReason.SAFETY:
                                    prohibited_content = True
                                    policy_violation_reason = "SAFETY"
                                    log_warning(f"[任务 {task_id}] 生成内容被安全过滤器拦截: {policy_violation_reason}")
                                elif candidate.finish_reason:
                                    log_debug(f"[任务 {task_id}] 响应候选项 {i} 完成原因: {candidate.finish_reason}")

                            if hasattr(candidate, 'safety_ratings') and candidate.safety_ratings:
                                log_debug(f"[任务 {task_id}] 响应候选项 {i} 安全评级: {candidate.safety_ratings}")

                    if prohibited_content:
                        # 对于内容政策违规，提供更具体的错误信息
                        log_warning(f"[任务 {task_id}] API响应因内容政策违规被拦截: {repr(response)}，原因: {policy_violation_reason}")

                        # 对于内容违规，直接返回结果，不再重试
                        processing_time = time.time() - start_time
                        log_info(f"[任务 {task_id}] 检测到内容政策违规，不进行重试，直接返回结果。处理时间: {processing_time:.2f}秒")

                        return {
                            "success": False,
                            "error": f"内容政策违规被拦截: {policy_violation_reason}",
                            "error_type": "ContentPolicyViolation",
                            "processing_time": processing_time,
                            "status_code": 999  # 特定状态码表示内容政策违规
                        }
                    else:
                        # 其他空响应情况
                        last_error = Exception(f"API响应text为空: {repr(response)}")
                        log_warning(f"[任务 {task_id}] API响应text为空，重试 {attempt+1}/{self.retry_attempts}")
                        self._delay_retry_sync(attempt)
                        continue

                # 提取响应文本
                caption = response.text
                log_debug(f"[任务 {task_id}] API返回响应: {caption[:100]}..." if len(caption) > 100 else caption)
                break

            except ConnectionError as e:
                # 网络连接错误，适合重试
                last_error = e
                error_stack = traceback.format_exc()  # 获取完整错误栈
                log_warning(f"[任务 {task_id}] 网络错误，重试 {attempt+1}/{self.retry_attempts}\n错误详情: {str(e)}\n错误栈: {error_stack}")
                self._delay_retry_sync(attempt)

            except google.auth.exceptions.RefreshError as e:
                # 处理OAuth认证错误
                last_error = e
                error_stack = traceback.format_exc()

                # 检查是否是作用域问题
                if 'invalid_scope' in str(e).lower():
                    log_error(f"[任务 {task_id}] OAuth认证作用域错误，请检查服务账号权限: {str(e)}")
                    log_error("解决方案: 1) 确保设置了GOOGLE_APPLICATION_CREDENTIALS环境变量; 2) 确保服务账号有正确的权限; 3) 检查服务账号密钥是否有效")

                    # 尝试更新Google应用默认凭据
                    try:
                        import os
                        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
                        if creds_path:
                            log_info(f"当前使用的凭据文件路径: {creds_path}")
                        else:
                            log_warning("未设置GOOGLE_APPLICATION_CREDENTIALS环境变量")
                    except Exception as cred_err:
                        log_warning(f"检查凭据信息时出错: {str(cred_err)}")

                log_warning(f"[任务 {task_id}] 认证错误，重试 {attempt+1}/{self.retry_attempts}\n错误详情: {str(e)}\n错误栈: {error_stack}")
                self._delay_retry_sync(attempt)

            except Exception as e:
                # 其他错误
                last_error = e
                error_stack = traceback.format_exc()  # 获取完整错误栈
                log_warning(f"[任务 {task_id}] API调用错误，重试 {attempt+1}/{self.retry_attempts}\n错误类型: {type(e).__name__}\n错误详情: {str(e)}\n错误栈: {error_stack}")
                self._delay_retry_sync(attempt)

        # 处理结果
        if caption is not None:
            processing_time = time.time() - start_time
            log_info(f"[任务 {task_id}] Gemini API调用成功，处理时间: {processing_time:.2f}秒")

            try:
                # 尝试解析JSON响应
                parsed_caption = json_repair.loads(caption)
                return {
                    "success": True,
                    "caption": parsed_caption,
                    "raw_response": caption,
                    "processing_time": processing_time,
                    "status_code": 200
                }
            except Exception as e:
                error_stack = traceback.format_exc()
                log_warning(f"[任务 {task_id}] JSON解析失败\n错误类型: {type(e).__name__}\n错误详情: {str(e)}\n错误栈: {error_stack}")
                return {
                    "success": False,
                    "raw_response": caption,
                    "processing_time": processing_time,
                    "parse_error": str(e),
                    "parse_error_stack": error_stack,
                    "error": f"JSON解析失败: {str(e)}",
                    "status_code": 400
                }
        else:
            # 所有重试都失败
            processing_time = time.time() - start_time

            # 如果last_error仍为None，说明发生了未捕获的失败情况
            if last_error is None:
                last_error = Exception("所有API尝试均失败，但未捕获到具体异常。可能是API返回了无效响应或请求超时")
                log_warning(f"[任务 {task_id}] 检测到异常情况：所有尝试均失败但未捕获到具体异常")

            error_type = type(last_error).__name__
            error_message = str(last_error)
            try:
                error_stack = traceback.format_exception(type(last_error), last_error, last_error.__traceback__)
                error_stack = "".join(error_stack)
            except Exception as stack_err:
                error_stack = f"无法获取错误栈: {str(stack_err)}"

            log_error(f"[任务 {task_id}] 所有Gemini API调用重试均失败\n错误类型: {error_type}\n错误详情: {error_message}\n错误栈: {error_stack}")

            return {
                "success": False,
                "error": f"API调用失败: {error_message}",
                "error_type": error_type,
                "error_stack": error_stack,
                "processing_time": processing_time,
                "status_code": 500
            }

    def _delay_retry_sync(self, attempt: int):
        """
        同步实现指数退避的重试延迟

        Args:
            attempt: 当前尝试次数（从0开始）
        """
        # 使用指数退避策略
        retry_time = self.retry_delay * (2 ** attempt)  # 指数退避
        log_debug(f"等待 {retry_time} 秒后重试...")
        time.sleep(retry_time)  # 同步等待

    async def call_gemini_api(self, prompt: str, image_bytes: bytes, mime_type: str) -> Dict[str, Any]:
        """
        异步调用Gemini API进行图像描述

        使用asyncio.to_thread将同步API调用转换为异步任务

        Args:
            prompt: 提示文本
            image_bytes: 图像数据
            mime_type: 图像MIME类型

        Returns:
            包含API响应结果的字典
        """
        # 生成任务ID
        task_id = str(uuid.uuid4())[:8]

        log_info(f"[任务 {task_id}] 使用线程池异步调用Gemini API...")

        # 使用asyncio.to_thread将同步函数转为异步执行
        try:
            result = await asyncio.to_thread(
                self._call_gemini_sync,
                prompt,
                image_bytes,
                mime_type,
                task_id
            )
            return result
        except Exception as e:
            log_error(f"[任务 {task_id}] 异步线程执行失败: {str(e)}")
            return {
                "success": False,
                "error": f"异步线程执行异常: {str(e)}",
                "error_type": type(e).__name__,
                "status_code": 500
            }