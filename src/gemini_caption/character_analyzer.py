from typing import List, Optional, Any, Dict, Union, Tuple
import logging

# 使用相对导入，确保作为包安装后能正确导入
try:
    # 当作为已安装的包导入时
    from gemini_caption.danbooru_pics import DanbooruPics
    from gemini_caption.danbooru_tags import DanbooruTags
except ImportError:
    try:
        # 当在src目录中时
        from .danbooru_pics import DanbooruPics
        from .danbooru_tags import DanbooruTags
    except ImportError:
        # 直接导入，当在脚本所在目录运行时
        from danbooru_pics import DanbooruPics
        from danbooru_tags import DanbooruTags

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CharacterAnalyzer:
    """
    整合DanbooruPics和DanbooruTags的功能，专门用于角色分析和可视化
    """

    def __init__(self, client_url: str = None):
        """
        初始化角色分析器

        Args:
            client_url: MongoDB连接URL，如果为None则使用环境变量
        """
        # 如果未提供连接URL，则使用环境变量
        if client_url is None:
            import os
            client_url = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")

        self.pics = DanbooruPics(client_url)
        self.tags = DanbooruTags(client_url)

    async def cross_verify_character(self, id: Union[str, int]) -> Dict[str, Dict[str, List[str]]]:
        """
        交叉验证角色信息

        Args:
            id: 图像ID

        Returns:
            角色验证结果字典
        """
        character = await self.pics.get_pic_character_by_id(id)
        main_series = await self.pics.get_pic_series_by_id(id)
        main_general = await self.pics.get_pic_general_by_id(id)
        char_dict = {}

        for char in character:
            character_stats = await self.pics.extract_character_stats(char)
            if character_stats:
                char_dict[char] = {"attribute":[], "series":[]}
                attribute, series_dict = character_stats
                for attr in attribute:
                    if attr in main_general:
                        char_dict[char]["attribute"].append(attr)
                    else:
                        if hasattr(self.pics, "character_stats") and hasattr(self.pics.character_stats, "general"):
                            attr_doc = await self.pics.character_stats.general.find_one({"name": attr})
                            if attr_doc and attr_doc.get("frequency", 0) > 0.5:
                                char_dict[char]["attribute"].append(attr)

                for series in series_dict:
                    if series in main_series:
                        char_dict[char]["series"].append(series)
        return char_dict

    async def build_tree_by_tags(self, tags: List[str]) -> Dict[str, Any]:
        """
        根据标签列表构建树结构

        Args:
            tags: 标签列表

        Returns:
            树结构字典
        """
        tree = {}
        root_tags = []

        # 首先找出所有根标签
        for tag in sorted(tags):
            if await self.tags.judge_root_tag(tag):
                root_tags.append(tag)

        children_tags = []
        for root_tag in root_tags:
            children = await self.tags.get_children_tags(root_tag)
            tree[root_tag] = [child for child in children if child in tags]
            for tag in tree[root_tag]:
                if tag in tags:
                    children_tags.append(tag)

        # 从树中移除作为子标签的节点
        for tag in children_tags:
            if tag in tree:
                del tree[tag]

        return tree

    async def visualize_tree(self, char_dict: Dict[str, Dict[str, List[str]]], language: str = "en") -> Optional[str]:
        """
        可视化角色关系树

        Args:
            char_dict: 角色信息字典
            language: 语言代码，支持"en"或"zh"

        Returns:
            格式化的树结构字符串
        """
        if not char_dict:
            return None

        def build_section(tag: str, level: int = 0) -> str:
            """构建单个标签的文本部分"""
            indent = "  " * level
            attribute = ", ".join(char_dict[tag]["attribute"]) or ("无" if language == "zh" else "None")
            series = ", ".join(char_dict[tag]["series"]) or ("无" if language == "zh" else "None")

            if language == "zh":
                return f"{indent}• {tag}\n{indent}  │ 角色特征: {attribute}\n{indent}  └─ 作品系列: {series}"
            return f"{indent}• {tag}\n{indent}  │ Features: {attribute}\n{indent}  └─ Series: {series}"

        # 构建树结构
        tree = await self.build_tree_by_tags(list(char_dict.keys()))

        output = []
        tip_message = {
            "zh": '''\n\n提示：带缩进的角色通常是上级的形态/皮肤版本，应优先识别具体形态。若同时存在父级和子级角色，请同时在描述中指出。
      这些是一些可能出现在画面中的角色的参考，你可以根据他们的平时的通常特征进行人物判断，提供的信息中子级角色通常是父级角色的某个形态或是皮肤，能判断出子级角色的话就不要重复判断父级角色，除非两者都出现''',
            "en": '''\n\nTip: Indented roles are usually alternative forms/skins of parent characters. Prefer identifying specific forms, but include both if coexisting.
      This could be the character's name, or it could refer to a specific outfit or state of the character. When describing, naturally mention the character's name and do not forget this. The model will know the character's features once the name is provided, so you can simplify the description of the character's inherent traits or omit them, provided you are certain which character is in the scene."
        '''
        }

        # 构建公共头部
        if language == "zh":
            output.append("角色检索参考信息表：图片中很大概率会出现以下标签的角色，请根据参考信息进行角色判断，把判断在画面的角色自然的在描述中提到其名称，可以看情况选择合适的提到出自哪个系列，提到系列时如果角色标签中带有系列名的话请酌情去除角色中的系列名，如果是皮肤或是特殊形态在你确定的情况下也可以提到")
            output.append("══════════════════")
        else:
            output.append("Character Search Reference Information Table: The following characters are likely to appear in the image, please identify them based on the reference information, and naturally mention the character's name in the description, you can choose the appropriate series to mention according to the situation, if the character's tag contains the series name, please remove the series name according to the situation")
            output.append("═══════════════════════════")

        # 递归构建树结构
        def build_tree_output(node: str, depth: int = 0) -> List[str]:
            """递归构建树节点输出"""
            result = []
            result.append(build_section(node, depth))
            for child in tree.get(node, []):
                result.extend(build_tree_output(child, depth+1))
            return result

        # 对每个根节点构建树输出
        for root_tag in tree:
            output.extend(build_tree_output(root_tag))

        # 添加提示信息
        output.append(tip_message[language])

        # 组合最终输出
        final_output = "\n".join(output)
        if language == "zh":
            return f"\n{final_output}\n"
        return f"\n{final_output}\n"

    async def get_visualize_tree_by_pid(self, pid: Union[str, int], language: str = "en") -> Optional[str]:
        """
        根据图像ID获取并可视化角色关系树

        此方法首先通过cross_verify_character获取角色信息，然后将其可视化为树结构。
        整个过程是异步的，适合在高并发环境中使用。

        Args:
            pid: 图像ID，可以是字符串或整数
            language: 输出语言代码，支持"en"（英语）或"zh"（中文）

        Returns:
            格式化的树结构字符串，如果没有找到角色信息则返回None

        Example:
            ```python
            tree = await analyzer.get_visualize_tree_by_pid("12345", "zh")
            print(tree)
            ```
        """
        # 获取角色信息
        char_dict = await self.cross_verify_character(pid)
        # 如果找到角色信息，则进行可视化
        if char_dict:
            return await self.visualize_tree(char_dict, language)
        return None