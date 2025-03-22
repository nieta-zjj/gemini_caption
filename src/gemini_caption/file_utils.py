import os
from typing import Optional

image_ext = [".png", ".jpg", ".jpeg", ".webp"]

class FileUtils:
    def __init__(self):
        pass

    async def get_id_from_path(self, image_path: str, prefix: str = "danbooru") -> Optional[str]:
        """
        从图像路径提取Danbooru ID

        支持多种命名模式：
        1. prefix_id.ext (例如 danbooru_12345.jpg)
        2. id.ext (例如 12345.jpg)
        3. prefix_id_其他信息.ext (例如 danbooru_12345_character.jpg)
        4. prefix-id.ext (例如 danbooru-12345.jpg)

        Args:
            image_path: 图像文件路径
            prefix: ID前缀，默认为"danbooru"

        Returns:
            提取的Danbooru ID，如果无法提取则返回None
        """
        try:
            # 检查文件是否为图像
            _, ext = os.path.splitext(image_path)
            if ext.lower() not in image_ext:
                return None

            # 提取文件名（不带扩展名）
            image_name = os.path.basename(image_path).split(".")[0]

            # 模式1: prefix_id.ext
            if image_name.startswith(prefix + "_"):
                parts = image_name.split("_")
                if len(parts) >= 2 and parts[1].isdigit():
                    return parts[1]

            # 模式2: 纯数字的文件名，可能就是ID
            if image_name.isdigit():
                return image_name

            # 模式3: prefix_id_其他信息.ext
            if "_" in image_name:
                parts = image_name.split("_")
                if len(parts) >= 2 and parts[0] == prefix and parts[1].isdigit():
                    return parts[1]

            # 模式4: prefix-id.ext
            if "-" in image_name:
                parts = image_name.split("-")
                if len(parts) >= 2 and parts[0] == prefix and parts[1].isdigit():
                    return parts[1]

            # 尝试直接从文件名中提取数字部分
            import re
            match = re.search(r'(\d+)', image_name)
            if match:
                return match.group(1)

            # 如果没有匹配任何模式，则返回原始文件名
            return image_name
        except Exception as e:
            print(f"从路径提取ID时出错: {image_path}, 错误: {str(e)}")
            return None