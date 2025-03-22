# Gemini Caption Generator

使用Google Gemini API为Danbooru图像生成描述的工具。支持单张图像处理和批量并行处理。

## 功能特点

- 使用Google Gemini API进行高质量图像描述生成
- 支持单张图像和批量处理
- 异步并行处理，大幅提高效率
- 自动将结果保存到MongoDB数据库
- 智能跳过已处理的图像，避免重复
- 完善的错误处理和重试机制

## 安装方法

```bash
# 从本地安装
pip install .

# 或直接从GitHub安装
pip install git+https://github.com/nieta-zjj/gemini_caption.git
```

## 使用方法

### 命令行工具

安装后可以直接使用命令行工具进行批量处理：

```bash
gemini_caption --start-id 1 --end-id 10 --max-concurrency 5 --api-key YOUR_API_KEY --mongodb-uri "mongodb://user:password@host:port/"
```

### 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| --start-id | 起始ID | 必填 |
| --end-id | 结束ID | 必填 |
| --max-concurrency | 最大并行处理数量 | 5 |
| --api-key | Gemini API密钥 | 环境变量 |
| --model-id | 使用的模型ID | gemini-2.0-flash-lite-001 |
| --language | 输出语言 (en或zh) | zh |
| --mongodb-uri | MongoDB连接URI | 环境变量 |
| --output-dir | 输出目录 | 不保存文件 |
| --save-image | 是否保存下载的图片 | False |

## 编程方式使用

```python
import asyncio
from gemini_batch_caption import run_batch_with_args

asyncio.run(run_batch_with_args(
    start_id=1,
    end_id=10,
    max_concurrency=5,
    api_key="YOUR_API_KEY",
    mongodb_uri="mongodb://user:password@host:port/"
))
```

## 环境变量

可以设置以下环境变量避免每次指定参数：

- `GOOGLE_API_KEY`: Gemini API密钥
- `MONGODB_URI`: MongoDB连接URI

## 许可证

MIT
