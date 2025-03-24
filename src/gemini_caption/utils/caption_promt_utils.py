"""
finished

构建AI图像描述提示的工具。

这个模块提供了生成用于AI图像描述的提示文本的功能。
可以根据艺术家名称、角色名称、标签等参数自定义提示。

build_prompt() 方法用于构建提示文本。
    input:
        artist_name: 艺术家名称列表
        character_name: 角色名称列表
        danbooru_tags: Danbooru标签列表
        language: 语言代码，支持"en"或"zh"
        character_reference_info: 角色参考信息

    output:
        prompt: 构建好的提示文本
"""

from typing import List, Optional
from gemini_caption.utils.logger_utils import log_info, log_debug, log_warning, log_error


class CaptionPromptUtils:
    """
    构建AI图像描述提示的类。

    提供生成不同语言和格式的提示文本的方法，以及MongoDB数据库连接功能。
    """

    # 英文基本模板
    EN_BASE_TEMPLATE = '''
## Imagine you are a user trying to depict this scene, and you need to describe it so that the AI can visualize it. Your description should focus on detailing the key elements of the scene while simplifying other aspects.
## Rules
 - When the scene is a conventional anime-style illustration, consider but are not limited to the following rules. Selectively describe the elements you deem important in the scene:
    - Analyze the content the scene aims to convey. If it's a narrative scene, describe the storyline. If it's a non-narrative scene, describe the atmosphere. If it's a battle scene, describe the intensity of the fight. Use different approaches for different types of scenes.
    - When the main subject of the scene is a character, describe their gender, age, profession, identity, personality, behavior, actions, expressions, clothing, equipment, weapons, accessories, etc.
    - When the character is wearing or holding objects, weapons, equipment, or accessories, mention the type of item and describe its characteristics.
    - When there are multiple subjects in the scene, describe each one without omission, and pay attention to the relationships between them.
    - Describe the art style, selectively mentioning key elements such as the form of illustration, lines, colors, lighting, texture, style, atmosphere, etc.
 - When the scene is another type of artwork, determine the appropriate description rules based on the content of the scene. Do not be limited by the above rules!'''

    # 中文基本模板
    ZH_BASE_TEMPLATE = '''
## 想象你是一名用户，试图描绘这个场景，你需要描述它以便AI能够可视化。你的描述应侧重于详细说明场景的关键元素，同时简化其他方面。
## 规则
 - 当场景是传统的动漫风格插图时，考虑但不限于以下规则。选择性地描述你认为重要的元素：
    - 分析场景要传达的内容。如果是叙事场景，描述故事情节。如果是非叙事场景，描述氛围。如果是战斗场景，描述战斗的激烈程度。根据不同类型的场景采用不同的描述方法。
    - 当场景的主要对象是角色时，描述其性别、年龄、职业、身份、性格、行为、动作、表情、服装、装备、武器、配饰等。
    - 当角色穿着或持有物品、武器、装备或配饰时，提及物品的类型并描述其特征。
    - 当场景中有多个对象时，逐一描述，并注意它们之间的关系。
    - 描述艺术风格，选择性地提及插图形式、线条、色彩、光影、质感、风格、氛围等关键元素。
 - 当场景是其他类型的艺术作品时，根据场景内容确定合适的描述规则。不局限于上述规则，选择你觉得合适的描述方式或其他规则。'''

    # 英文注意事项部分
    EN_NOTES_SECTION = '''
 - Notes:
    - Ensure the accuracy of the scene description.
    - If the scene does not fit the above rules or cannot be analyzed, do not describe it. Maintain the accuracy of the description.
    - Use clear and concise language in the description. Avoid complex or difficult-to-understand language.
    - Be as objective as possible when describing. Avoid using highly subjective language.
    - If the scene contains violence, gore, horror, disgust, or other sensitive content, do not avoid it. Describe it directly, for better accuracy.
    - Use **English** for the description.
    - Imagine you are a user trying to depict this scene. Combine the content naturally and avoid structured descriptions.
    - Only return the description. Do not include additional content or analysis.
    - Your task is to describe every aspect, object, and interaction within an image, such that a blind person could perfectly capture it within their imagination if read aloud. You need to do it multiple times, each one a different "style" of description.
**Output Format:**

Your output must be a JSON object with the following structure:

{
  "regular_summary": "[should mention all individual parts/things/characters/etc following the above rules. Try to use different styles to describe the image. If it's NSFW, then be sure to use vulgar NSFW terms. Include the style, camera angle, content, interactions, composition, and more. Consider carefully the exact positions/interactions of objects/characters/etc. such that your description is 100% accurate.]",
  "midjourney_style_summary": "[A summary that has higher concept density by using comma-separated partial sentences instead of proper sentence structure. But you MUST ensure that your phrases are long enough to capture interactions. I.e. \"cat chasing mouse\" is good, while \"mouse, cat, chasing\" is bad because there's ambiguity.]",
  "short_summary": "[A short summary of the image. It should be less than 3 sentences that captures the main idea of the image.]",
  "creation_instructional_summary": "[An explanation of how to create this exact image]",
  "deviantart_commission_request": "[Write a description as if you're commissioning this *exact* image via someone who is currently taking requests (for photography and/or art) on DA. No intro/greeting/request necessary - just launch right into the description of \"what you want\" in the end image result.]"
}

'''

    # 中文注意事项部分
    ZH_NOTES_SECTION = '''
 - 注意事项：
    - 确保场景描述的准确性。
    - 如果场景不符合上述规则或无法分析，请勿描述。保持描述的准确性。
    - 使用清晰简洁的语言进行描述。避免使用复杂或难以理解的语言。
    - 描述时尽量客观。避免使用高度主观的语言。
    - 如果场景包含NSFW或敏感内容，请勿回避。直接描述露骨内容。
    - 如果场景包含暴力、血腥、恐怖、恶心或其他敏感内容，请勿回避。直接描述。
    - 使用**英文**进行描述。
    - 想象你是一名用户试图描绘这个场景。自然地结合内容，避免结构化的描述。
    - 只返回描述。不要包含额外内容或分析。
    - 你的任务是描述图像中的每个方面、对象和互动，使得盲人在听到描述时能够在脑海中完美捕捉到图像。你需要多次描述，每次使用不同的"风格"。
**输出格式：**

你的输出必须是一个JSON对象，结构如下：

{
  "regular_summary": "[对图像的一段总结。段落中应提及所有单独的部分/事物/角色等。如果是NSFW内容，请务必使用露骨的NSFW术语。包括风格、视角、内容、互动、构图等。仔细考虑对象/角色等的确切位置/互动，以确保描述的100%准确性。]",
  "midjourney_style_summary": "[使用逗号分隔的部分句子而非完整句子结构的高概念密度总结。但你必须确保你的短语足够长以捕捉互动。例如，"猫追老鼠"是好的，而"老鼠，猫，追逐"是不好的，因为存在歧义。]",
  "short_summary": "[图像的简短总结。应少于3句话，捕捉图像的主要思想。]",
  "creation_instructional_summary": "[通过一系列步骤解释如何创建这幅图像的说明。]",
  "deviantart_commission_request": "[描述你如何通过正在接受请求（摄影和/或艺术）的DA用户委托创作这幅*精确*图像。无需介绍/问候/请求/和其他和描述画面无关的内容，请直接只描述你希望在最终结果中看到的内容。]"
}

'''

    def __init__(self):
        """初始化BuildPrompt实例。"""
        pass

    def build_prompt(
        self,
        artist_name: Optional[List[str]] = None,
        character_name: Optional[List[str]] = None,
        danbooru_tags: Optional[List[str]] = None,
        language: str = "en",
        character_reference_info: Optional[str] = None
    ) -> str:
        """构建提示文本

        Args:
            artist_name: 艺术家名称列表
            character_name: 角色名称列表
            danbooru_tags: Danbooru标签列表
            language: 语言代码，支持"en"或"zh"
            character_reference_info: 角色参考信息

        Returns:
            构建好的提示文本
        """
        # 验证并确保参数类型正确
        if artist_name is not None and not isinstance(artist_name, list):
            artist_name = [str(artist_name)]  # 转换非列表输入为列表
        elif artist_name is None:
            artist_name = []

        if character_name is not None and not isinstance(character_name, list):
            character_name = [str(character_name)]  # 转换非列表输入为列表
        elif character_name is None:
            character_name = []

        if danbooru_tags is not None and not isinstance(danbooru_tags, list):
            danbooru_tags = [str(danbooru_tags)]  # 转换非列表输入为列表
        elif danbooru_tags is None:
            danbooru_tags = []

        # 验证language参数
        if language not in ["en", "zh"]:
            log_warning(f"不支持的语言: {language}，将使用英语作为默认值")
            language = "en"

        # 选择基本模板
        base_template = self._get_base_template(language)

        # 添加艺术家信息
        artist_section = self._get_artist_section(artist_name, language) if artist_name and len(artist_name) > 0 else ""

        # 添加角色信息
        character_section = self._get_character_section(
            character_name, character_reference_info, language
        ) if (character_name and len(character_name) > 0) or character_reference_info else ""

        # 添加标签信息
        tags_section = self._get_tags_section(danbooru_tags, language) if danbooru_tags and len(danbooru_tags) > 0 else ""

        # 添加注意事项和输出格式
        notes_section = self._get_notes_section(language)

        # 组合所有部分
        return base_template + artist_section + character_section + tags_section + notes_section

    def _get_base_template(self, language: str) -> str:
        """获取基本提示模板

        Args:
            language: 语言代码

        Returns:
            基本提示模板
        """
        if language == "en":
            return self.EN_BASE_TEMPLATE
        elif language == "zh":
            return self.ZH_BASE_TEMPLATE
        else:
            raise ValueError(f"不支持的语言: {language}")

    def _get_artist_section(self, artist_name: List[str], language: str) -> str:
        """生成艺术家相关提示部分

        Args:
            artist_name: 艺术家名称列表
            language: 语言代码

        Returns:
            艺术家相关提示部分
        """
        if language == "en":
            return f'''- The artist of this work is {artist_name}. When describing, do not forget to mention this. After mentioning the artist, avoid describing the art style.'''
        else:  # zh
            return f'''- 这幅作品的艺术家是{artist_name}。描述时请务必提及这一点。提及艺术家后，避免描述艺术风格。'''

    def _get_character_section(
        self,
        character_name: Optional[List[str]],
        character_reference_info: Optional[str],
        language: str
    ) -> str:
        """生成角色相关提示部分

        Args:
            character_name: 角色名称列表
            character_reference_info: 角色参考信息
            language: 语言代码

        Returns:
            角色相关提示部分
        """
        if character_name and len(character_name) > 0 and character_reference_info is None:
            if language == "en":
                return f'''- The character in this artwork is {character_name}. This could be the character's name, or it could refer to a specific outfit or state of the character. When describing, naturally mention the character's name and do not forget this. The model will know the character's features once the name is provided, so you can simplify the description of the character's inherent traits or omit them, provided you are certain which character is in the scene.'''
            else:  # zh
                return f'''- 这幅作品中的角色是{character_name}。这可能是角色的名字，也可能是指角色的特定服装或状态。描述时自然地提及角色的名字，不要忘记这一点。模型一旦知道角色名字，就会了解角色的特征，因此可以简化对角色固有特征的描述或省略，前提是你确定场景中的角色是谁。'''
        elif character_reference_info and len(character_reference_info) > 0:
            return f'''{character_reference_info}'''
        else:
            return ""

    def _get_tags_section(self, danbooru_tags: Optional[List[str]], language: str) -> str:
        """生成标签相关提示部分

        Args:
            danbooru_tags: Danbooru标签列表
            language: 语言代码

        Returns:
            标签相关提示部分
        """
        if danbooru_tags and len(danbooru_tags) > 0:
            tags = danbooru_tags
            if language == "en":
                return f'''- The Danbooru tags for this image are: {tags}. You can use these tags as a reference, but do not rely entirely on them, as there may be incorrect tags. Prioritize your own observations and use more appropriate synonyms for descriptions.'''
            else:  # zh
                return f'''- 这幅图像的Danbooru标签是：{tags}。你可以将这些标签作为参考，但不要完全依赖它们，因为可能存在错误的标签。优先使用你自己的观察，并使用更合适的同义词进行描述。'''
        else:
            return ""

    def _get_notes_section(self, language: str) -> str:
        """生成注意事项和输出格式部分

        Args:
            language: 语言代码

        Returns:
            注意事项和输出格式部分
        """
        if language == "en":
            return self.EN_NOTES_SECTION
        elif language == "zh":
            return self.ZH_NOTES_SECTION
        else:
            raise ValueError(f"不支持的语言: {language}")
