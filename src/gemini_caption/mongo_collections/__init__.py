'''
MongoDB集合操作模块
提供各种MongoDB集合的操作接口
'''

from gemini_caption.mongo_collections.danbooru_gemini_captions import DanbooruGeminiCaptions
from gemini_caption.mongo_collections.danbooru_pics import DanbooruPics
from gemini_caption.mongo_collections.danbooru_tags import DanbooruTags
from gemini_caption.mongo_collections.danbooru_pics_model import DanbooruPicDoc

__all__ = [
    'DanbooruGeminiCaptions',
    'DanbooruPics',
    'DanbooruTags',
    'DanbooruPicDoc'
]
