from setuptools import setup, find_packages

setup(
    name="gemini_caption",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,

    # 依赖项
    install_requires=[
        "json-repair",
        "google-genai",
        "pymongo",
        "requests",
        "motor",
        "httpx",
        "argparse",
    ],

    # 命令行入口点
    entry_points={
        'console_scripts': [
            'gemini_caption=gemini_caption.gemini_batch_caption:main',
        ],
    },

    # 元数据
    author="shiertier&qianqian",
    author_email="shiertier@nieta.art",
    description="Batch caption generator using Google Gemini API for Danbooru images",
    keywords="gemini, caption, ai, danbooru, batch",
    python_requires=">=3.7",
)