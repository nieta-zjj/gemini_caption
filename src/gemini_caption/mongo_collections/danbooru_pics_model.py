'''
Danbooru图片文档模型

定义Danbooru图片数据的模型结构，提供数据验证和URL构建功能。
使用Pydantic模型确保数据一致性和类型安全。
'''

from typing import List, Optional
from pydantic import BaseModel, Field, model_validator
from gemini_caption.utils.logger_utils import log_warning

class DanbooruPicDoc(BaseModel):
    """
    Danbooru图片文档模型

    属性:
        _id/id: 图片唯一ID
        *_tags: 各类标签集合
        status: 状态码 (200=存在, 404=不存在, 405=无法下载)
        url: 图片URL (仅当status=200时有值)
    """
    _id: int
    id: int = Field(..., alias='_id')

    general_tags: Optional[List[str]] = Field(default_factory=list, description="一般标签")
    character_tags: Optional[List[str]] = Field(default_factory=list, description="角色标签")
    artist_tags: Optional[List[str]] = Field(default_factory=list, description="艺术家标签")
    copyright_tags: Optional[List[str]] = Field(default_factory=list, description="版权标签")
    meta_tags: Optional[List[str]] = Field(default_factory=list, description="元数据标签")

    height: Optional[int] = 0
    width: Optional[int] = 0

    created_at: str = ""

    md5: Optional[str] = ""
    file_ext: Optional[str] = ""
    file_size: Optional[int] = 0
    url: Optional[str] = ""

    has_children: Optional[bool] = False

    fav_count: Optional[int] = 0
    score: Optional[int] = 0
    rating: Optional[str] = ""

    parent_id: Optional[int] = 0
    pixiv_id: Optional[int] = 0
    source: Optional[str] = ""

    status: int = 404


    @model_validator(mode='after')
    def set_status_based_on_md5(self) -> 'DanbooruPicDoc':
        """根据md5字段的存在与否设置正确的status值并生成url"""
        # 处理md5为None的情况
        if self.md5 is None:
            self.md5 = ""

        # 处理file_ext为None的情况
        if self.file_ext is None:
            self.file_ext = ""

        # 如果有md5但不为空字符串，视为有效md5
        if self.md5:
            self.status = 200
            # 只有当status为200时才生成url
            self.url = self.build_url()
        elif self.md5 == "" or self.file_ext == "gif":
            self.status = 405
        # 如果没设置status或status为0，则使用默认值404
        elif not self.status:
            self.status = 404
        if 'gif' in self.url:
            self.status = 405
            self.url = ""
        return self


    @model_validator(mode='after')
    def ensure_tags_are_lists(self) -> 'DanbooruPicDoc':
        """确保所有标签字段都是列表类型，将None值转换为空列表"""
        tag_fields = [
            'general_tags', 'character_tags', 'artist_tags',
            'copyright_tags', 'meta_tags'
        ]

        for field in tag_fields:
            if getattr(self, field) is None:
                setattr(self, field, [])

        return self


    def build_url(self) -> Optional[str]:
        """根据md5和文件扩展名构建URL"""
        # 处理None值
        if self.md5 is None or self.file_ext is None:
            return None

        # 处理空字符串
        if not self.md5 or not self.file_ext:
            return None

        # 构建Danbooru URL (根据md5前两位字符分组)
        return f"https://cdn.donmai.us/original/{self.md5[0:2]}/{self.md5[2:4]}/{self.md5}.{self.file_ext}"


    @classmethod
    def from_id(cls, id: int) -> 'DanbooruPicDoc':
        """
        只传入_id，其余字段使用默认值创建对象

        Args:
            id: 图片ID

        Returns:
            创建的DanbooruPicDoc对象，status为404，其他字段为默认值
        """
        return cls(_id=id)
