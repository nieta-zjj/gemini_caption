from google import genai
from google.genai import types
import json
import time
import json_repair
from typing import Dict, Any, Optional, Union, List
import random
import traceback  # 添加traceback模块导入

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

        # 初始化Gemini客户端（使用Vertex AI）
        try:
            # 从区域列表中随机选择一个
            region = random.choice(self.regions)
            self.genai_client = genai.Client(
                vertexai=True,
                project=self.project_id,
                location=region
            )
            log_info(f"成功初始化Gemini客户端，使用模型: {self.model_id}，区域: {region}")
        except Exception as e:
            log_error(f"初始化Gemini客户端失败: {str(e)}")
            raise

    async def call_gemini_api(self, prompt: str, image_bytes: bytes, mime_type: str) -> Dict[str, Any]:
        """
        调用Gemini API进行图像描述

        Args:
            prompt: 提示文本
            image_bytes: 图像数据
            mime_type: 图像MIME类型

        Returns:
            包含API响应结果的字典
        """
        start_time = time.time()
        log_info("开始调用Gemini API...")

        # 添加请求限制和重试机制
        caption = None
        last_error = None

        for attempt in range(self.retry_attempts):
            try:
                # 每次重试使用不同区域
                if attempt > 0:
                    # 重新随机选择区域初始化客户端
                    region = random.choice(self.regions)
                    log_info(f"重试使用新区域: {region}")
                    self.genai_client = genai.Client(
                        vertexai=True,
                        project=self.project_id,
                        location=region
                    )

                # 调用API
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

                # 检查响应是否有效
                if not response:
                    last_error = Exception(f"API返回无效响应: 响应对象为None")
                    log_warning(f"API返回空响应对象，重试 {attempt+1}/{self.retry_attempts}")
                    await self._delay_retry(attempt)
                    continue

                # 记录完整响应对象信息，帮助调试
                log_debug(f"API响应对象: {repr(response)}")
                log_debug(f"API响应对象类型: {type(response)}")
                log_debug(f"API响应对象属性: {dir(response)}")

                # 检查响应是否有text属性
                if not hasattr(response, 'text'):
                    last_error = Exception(f"API响应对象没有text属性: {repr(response)}")
                    log_warning(f"API响应缺少text属性，重试 {attempt+1}/{self.retry_attempts}")
                    await self._delay_retry(attempt)
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
                                    log_warning(f"生成内容被安全过滤器拦截: {policy_violation_reason}")
                                elif candidate.finish_reason == types.FinishReason.SAFETY:
                                    prohibited_content = True
                                    policy_violation_reason = "SAFETY"
                                    log_warning(f"生成内容被安全过滤器拦截: {policy_violation_reason}")
                                elif candidate.finish_reason:
                                    log_debug(f"响应候选项 {i} 完成原因: {candidate.finish_reason}")

                            if hasattr(candidate, 'safety_ratings') and candidate.safety_ratings:
                                log_debug(f"响应候选项 {i} 安全评级: {candidate.safety_ratings}")

                    if prohibited_content:
                        # 对于内容政策违规，提供更具体的错误信息
                        log_warning(f"API响应因内容政策违规被拦截: {repr(response)}，原因: {policy_violation_reason}")

                        # 对于内容违规，直接返回结果，不再重试
                        processing_time = time.time() - start_time
                        log_info(f"检测到内容政策违规，不进行重试，直接返回结果。处理时间: {processing_time:.2f}秒")

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
                        log_warning(f"API响应text为空，重试 {attempt+1}/{self.retry_attempts}")
                        await self._delay_retry(attempt)
                        continue

                # 提取响应文本
                caption = response.text
                log_debug(f"API返回响应: {caption[:100]}..." if len(caption) > 100 else caption)
                break

            except ConnectionError as e:
                # 网络连接错误，适合重试
                last_error = e
                error_stack = traceback.format_exc()  # 获取完整错误栈
                log_warning(f"网络错误，重试 {attempt+1}/{self.retry_attempts}\n错误详情: {str(e)}\n错误栈: {error_stack}")
                await self._delay_retry(attempt)

            except Exception as e:
                # 其他错误
                last_error = e
                error_stack = traceback.format_exc()  # 获取完整错误栈
                log_warning(f"API调用错误，重试 {attempt+1}/{self.retry_attempts}\n错误类型: {type(e).__name__}\n错误详情: {str(e)}\n错误栈: {error_stack}")
                await self._delay_retry(attempt)

        # 处理结果
        if caption is not None:
            processing_time = time.time() - start_time
            log_info(f"Gemini API调用成功，处理时间: {processing_time:.2f}秒")

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
                log_warning(f"JSON解析失败\n错误类型: {type(e).__name__}\n错误详情: {str(e)}\n错误栈: {error_stack}")
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
                log_warning("检测到异常情况：所有尝试均失败但未捕获到具体异常")

            error_type = type(last_error).__name__
            error_message = str(last_error)
            try:
                error_stack = traceback.format_exception(type(last_error), last_error, last_error.__traceback__)
                error_stack = "".join(error_stack)
            except Exception as stack_err:
                error_stack = f"无法获取错误栈: {str(stack_err)}"

            log_error(f"所有Gemini API调用重试均失败\n错误类型: {error_type}\n错误详情: {error_message}\n错误栈: {error_stack}")

            return {
                "success": False,
                "error": f"API调用失败: {error_message}",
                "error_type": error_type,
                "error_stack": error_stack,
                "processing_time": processing_time,
                "status_code": 500
            }

    async def _delay_retry(self, attempt: int):
        """
        实现指数退避的重试延迟

        Args:
            attempt: 当前尝试次数（从0开始）
        """
        import asyncio
        # 使用指数退避策略
        retry_time = self.retry_delay * (2 ** attempt)  # 指数退避
        log_debug(f"等待 {retry_time} 秒后重试...")
        await asyncio.sleep(retry_time)