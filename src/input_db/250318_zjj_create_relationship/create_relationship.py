import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.operations import UpdateOne
from tqdm import tqdm
import logging
from typing import List, Dict, Any, Tuple, Union

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DanbooruTags:
    """分析Danbooru标签关系并创建层次结构的类"""

    def __init__(self, mongodb_uri="mongodb://8.153.97.53:3000/",
                 db_name="danbooru_db",
                 tags_collection="related_tags",
                 output_collection="tag_relationships",
                 overlap_threshold=0.9995):
        """初始化分析器"""
        self.overlap_threshold = overlap_threshold
        try:
            self.client = AsyncIOMotorClient(mongodb_uri)
            self.db = self.client[db_name]
            self.tags_collection = self.db[tags_collection]
            self.output_collection = self.db[output_collection]
            logger.info("MongoDB连接初始化成功")
        except Exception as e:
            logger.error(f"MongoDB连接初始化失败: {e}")
            raise

    async def initialize_db(self):
        """初始化数据库，创建索引"""
        try:
            # 使用异步方式创建索引
            await self.output_collection.create_index([("name", 1)])
            logger.info("关系表索引创建成功")
        except Exception as e:
            logger.error(f"索引创建失败: {e}")
            raise

    async def analyze_tag_relationships(self, batch_size=100):
        """分析标签关系并创建层次结构"""
        total_processed = 0
        total_tags = await self.tags_collection.count_documents({})
        logger.info(f"总共有 {total_tags} 个标签需要分析")

        cursor = self.tags_collection.find({}, no_cursor_timeout=True)

        # 使用tqdm创建进度条
        pbar = tqdm(total=total_tags, desc="分析标签关系")

        batch = []
        async for tag_data in cursor:
            relationship_data = await self._process_tag(tag_data)
            if relationship_data:
                batch.append(relationship_data)

            if len(batch) >= batch_size:
                await self._save_batch(batch)
                total_processed += len(batch)
                pbar.update(len(batch))
                batch = []

        # 保存最后一批
        if batch:
            await self._save_batch(batch)
            total_processed += len(batch)
            pbar.update(len(batch))

        pbar.close()
        logger.info(f"完成分析 {total_processed} 个标签关系")
        return total_processed

    async def _process_tag(self, tag_data):
        """处理单个标签的关系数据"""
        if not tag_data or 'tag' not in tag_data or 'related_tags' not in tag_data:
            return None

        main_tag = tag_data['tag']
        related_tags = tag_data['related_tags']

        # 准备基本结构
        result = {
            "_id": main_tag.get("id"), #tag_data.get("_id"),
            "name": main_tag.get("name"),
            "tag_id": main_tag.get("id"),
            "category": main_tag.get("category"),
            "post_count": main_tag.get("post_count"),
            "parents": [],
            "children": [],
            "related": [],
        }

        # 分析关系
        for related in related_tags:
            if 'tag' not in related:
                continue

            # 跳过自身关系
            if related['tag'].get('id') == main_tag.get('id'):
                continue

            related_tag = related['tag']
            overlap = related.get('overlap_coefficient', 0)
            frequency = related.get('frequency', 0)
            cosine = related.get('cosine_similarity', 0)
            jaccard = related.get('jaccard_similarity', 0)

            # 关系类型判断
            relationship_item = {
                "name": related_tag.get('name'),
                "tag_id": related_tag.get('id'),
                "category": related_tag.get('category'),
                "relationship_strength": overlap
            }

            # 如果overlap接近1，且一个tag的post_count远小于另一个，那么小的是大的子集
            if overlap >= self.overlap_threshold:
                # 判断是父级还是子级
                if related_tag.get('post_count', 0) > main_tag.get('post_count', 0):
                    # 当前标签是子集，添加到父级
                    result['parents'].append(relationship_item)
                else:
                    # 当前标签是父级，添加到子集
                    result['children'].append(relationship_item)
            # 中等相关性的标签
            elif overlap >= 0.6:
                result['related'].append(relationship_item)

        # 按关系强度排序
        result['parents'] = sorted(result['parents'], key=lambda x: x['relationship_strength'], reverse=True)
        result['children'] = sorted(result['children'], key=lambda x: x['relationship_strength'], reverse=True)
        result['related'] = sorted(result['related'], key=lambda x: x['relationship_strength'], reverse=True)

        return result

    async def _save_batch(self, batch):
        """批量保存关系数据"""
        if not batch:
            return

        try:
            # 使用pymongo.operations.UpdateOne进行批量写入
            operations = []
            for item in batch:
                # 获取_id用于过滤条件
                doc_id = item['_id']

                # 使用字典推导式创建不包含_id的副本
                update_data = {k: v for k, v in item.items() if k != '_id'}

                # 创建UpdateOne操作
                operations.append(
                    UpdateOne(
                        {'_id': doc_id},  # 过滤条件
                        {'$set': update_data},  # 更新操作
                        upsert=True  # 如果不存在则插入
                    )
                )

            # 执行批量写入
            result = await self.output_collection.bulk_write(operations)
            logger.debug(f"批量保存结果: 更新 {result.modified_count}, 插入 {result.upserted_count}")
        except Exception as e:
            logger.error(f"批量保存数据时出错: {e}")
            import traceback
            traceback.print_exc()

    async def get_sample_relationships(self, limit=5):
        """获取样例关系数据"""
        samples = []
        async for doc in self.output_collection.find().sort('post_count', -1).limit(limit):
            samples.append(doc)
        return samples

    async def close(self):
        """关闭MongoDB连接"""
        self.client.close()
        logger.info("MongoDB连接已关闭")


async def main_async():
    """主异步函数"""
    analyzer = None
    try:
        analyzer = DanbooruTags(
            overlap_threshold=0.9995  # 设置为99.95%的阈值
        )

        # 初始化数据库
        await analyzer.initialize_db()

        # 分析标签关系
        total_processed = await analyzer.analyze_tag_relationships()
        logger.info(f"共处理了 {total_processed} 个标签关系")

        # 显示样例
        print("\n标签关系样例:")
        samples = await analyzer.get_sample_relationships(3)
        for sample in samples:
            print(f"\n标签: {sample.get('name')}, 类别: {sample.get('category')}, 帖子数: {sample.get('post_count')}")
            print(f"  父级标签: {len(sample.get('parents', []))}")
            print(f"  子级标签: {len(sample.get('children', []))}")
            print(f"  相关标签: {len(sample.get('related', []))}")

            # 显示部分子标签
            if sample.get('children'):
                print("  子标签示例:")
                for child in sample.get('children')[:3]:
                    print(f"    - {child.get('name')} ({child.get('relationship_strength'):.4f})")

    except Exception as e:
        logger.error(f"处理标签关系时出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if analyzer:
            await analyzer.close()



def main():
    """主函数"""
    try:
        asyncio.run(main_async())
    except Exception as e:
        logger.error(f"运行主函数时出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()