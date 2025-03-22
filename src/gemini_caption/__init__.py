"""
Gemini Caption Generator

A package for generating captions for Danbooru images using Google's Gemini API.
Supports both single image captioning and batch processing with concurrent execution.
"""

__version__ = "0.1.0"

# 导出主要的类和函数
try:
    from .gemini_batch_caption import GeminiBatchCaption, run_batch_with_args, main
    from .gemini_single_caption import GeminiSingleCaption
    from .caption_promt_utils import CaptionPromptUtils
    from .file_utils import FileUtils
    from .character_analyzer import CharacterAnalyzer
except ImportError:
    pass