# Gemini Caption

Gemini Caption是一个使用Google Gemini API对图像进行批量描述的工具，特别适合处理Danbooru图片集。该工具支持多种图像获取方式，提供了灵活的批处理选项，并能够与MongoDB数据库集成以存储描述结果。

## 安装

### 使用pip安装

```bash
pip install git+https://github.com/nieta-zjj/gemini_caption.git
```

## 命令行使用

### 基本用法

```bash
# 需要先设置环境变量GOOGLE_APPLICATION_CREDENTIALS或GOOGLE_APPLICATION_CREDENTIALS_CONTENT
gemini_caption --key 5 --max_concurrency 10 --mongodb_uri $mongodb_uri
```

### 完整参数选项

```bash
# 环境变量可选设置
# Google API凭证（至少有一个）
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json  # Google凭证文件路径
GOOGLE_APPLICATION_CREDENTIALS_CONTENT="..."              # Google凭证JSON内容

# 数据库配置
MONGODB_URI=mongodb://localhost:27017/                    # MongoDB连接URI

# 处理控制
KEY=0                                                     # 处理特定键对应的图片批次
MAX_CONCURRENCY=100                                       # 最大并发处理数量

# 模型配置
MODEL_ID=gemini-2.0-flash-lite-001                        # Gemini模型ID
LANGUAGE=zh                                               # 描述生成的语言，支持"en"或"zh"

# HuggingFace设置
HF_REPO=picollect/danbooru                                # HuggingFace仓库名称
USE_HFPICS_FIRST=0                                        # 是否优先使用HfPics获取图片(0:否, 1:是)

# 日志设置
LOG_LEVEL=info                                            # 日志级别(debug/info/warning/error)
LOG_FILE=                                                 # 日志文件路径，默认为None表示不输出到文件
```

```bash
# 命令行参数(优先级高于环境变量)
gemini_caption [选项]

选项:
  --key INT                 处理特定键对应的图片批次
  --start_id INT            处理起始ID
  --end_id INT              处理结束ID
  --max_concurrency INT     最大并发处理数量
  --model_id TEXT           Gemini模型ID
  --language TEXT           描述生成的语言，支持"en"或"zh"
  --mongodb_uri TEXT        MongoDB连接URI
  --output_dir TEXT         结果输出目录
  --save_image              保存图像到本地
  --hf_repo TEXT           HuggingFace仓库名称
  --hf_cache_dir TEXT       HuggingFace缓存目录
  --use_hfpics_first        优先使用HfPics获取图片
  --log_level TEXT          日志级别(debug/info/warning/error)
  --log_file TEXT           日志文件路径
  --project_id TEXT         Google Cloud项目ID
```

## 模块功能说明

### 主要模块

1. **gemini_batch_caption.py**
   - 提供命令行界面和高级接口，用于批量处理图像描述任务
   - 核心类：`GeminiBatchCaption`
   - 主要方法：`process_batch`, `process_single_id`, `process_batch_by_key`

2. **utils/batch_processor.py**
   - 批处理核心逻辑
   - 处理单个或多个图片ID的描述生成任务
   - 支持并发处理和结果收集

3. **utils/image_processor.py**
   - 图像下载和处理
   - 支持从URL或HuggingFace获取图像
   - 提供图像保存功能

4. **utils/gemini_api_client.py**
   - 与Google Gemini API交互
   - 处理API请求，包含重试和错误处理逻辑
   - 解析API响应结果

5. **utils/caption_promt_utils.py**
   - 构建AI图像描述提示文本
   - 支持自定义提示生成，包括艺术家、角色、标签信息

### 数据处理模块

1. **mongo_collections/**
   - **danbooru_gemini_captions.py**: 管理描述结果存储和检索
   - **danbooru_pics.py**: 提供图片数据访问接口
   - **danbooru_tags.py**: 标签结构和关系处理
   - **danbooru_pics_model.py**: 图片数据模型定义

2. **utils/character_analyzer.py**
   - 角色分析和可视化
   - 创建角色关系树
   - 提供角色信息验证

3. **utils/file_utils.py**
   - 图像文件路径处理
   - 从文件名提取ID功能

4. **utils/logger_utils.py**
   - 日志记录工具
   - 支持多种日志级别和输出方式