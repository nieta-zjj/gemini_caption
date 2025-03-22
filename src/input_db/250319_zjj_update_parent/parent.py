import asyncio
import time
from motor.motor_asyncio import AsyncIOMotorClient

# 连接到MongoDB数据库
CONNECTION_STRING = "mongodb://8.153.97.53:27815/"
DB_NAME = "danbooru"
COLLECTION_NAME = "tags"
MAX_CONCURRENT_TASKS = 100

# 统计更新计数
update_count = 0
total_docs = 0
processed_docs = 0

async def update_child_parent(collection, child_name, parent_name):
    """为单个子标签更新父引用"""
    global update_count
    result = await collection.update_one(
        {"name": child_name},
        {"$addToSet": {"parent": parent_name}},
        upsert=False
    )
    if result.modified_count > 0:
        print(f"  - 更新子标签: {child_name}")
        return 1
    return 0

async def process_parent(collection, doc):
    """处理单个父标签及其所有子标签"""
    global processed_docs, update_count

    parent_name = doc.get("name")
    children = doc.get("children", [])

    print(f"正在处理父标签: {parent_name}，包含 {len(children)} 个子标签")

    # 并发处理所有子标签
    tasks = [update_child_parent(collection, child, parent_name) for child in children]
    results = await asyncio.gather(*tasks)

    # 更新计数
    count = sum(results)
    update_count += count
    processed_docs += 1

    return count

async def main():
    global total_docs, processed_docs, update_count

    # 连接到MongoDB
    client = AsyncIOMotorClient(CONNECTION_STRING)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    try:
        # 步骤1：查找所有包含非空children字段的文档
        cursor = collection.find({"children": {"$exists": True, "$ne": []}})

        # 计算总文档数
        total_docs = await collection.count_documents({"children": {"$exists": True, "$ne": []}})
        print(f"找到 {total_docs} 个包含非空children字段的文档")

        if total_docs == 0:
            print("没有找到需要处理的文档，请检查查询条件")
            return

        start_time = time.time()

        # 获取所有需要处理的文档
        docs = await cursor.to_list(length=None)

        # 创建信号量限制并发任务数
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

        async def process_with_semaphore(doc):
            async with semaphore:
                return await process_parent(collection, doc)

        # 创建所有任务
        tasks = [process_with_semaphore(doc) for doc in docs]

        # 设置进度报告任务
        async def report_progress():
            last_processed = 0
            while processed_docs < total_docs:
                await asyncio.sleep(2)  # 每2秒更新一次进度
                if processed_docs > last_processed:
                    elapsed_time = time.time() - start_time
                    if processed_docs > 0:
                        estimated_total = (elapsed_time / processed_docs) * total_docs
                        remaining_time = estimated_total - elapsed_time
                        print(f"进度: {processed_docs}/{total_docs} ({processed_docs/total_docs*100:.2f}%), "
                              f"已用时间: {elapsed_time:.2f}秒, 预计剩余: {remaining_time:.2f}秒")
                    last_processed = processed_docs

        # 启动进度报告
        progress_task = asyncio.create_task(report_progress())

        # 执行所有任务
        await asyncio.gather(*tasks)

        # 取消进度报告任务
        progress_task.cancel()

        elapsed_time = time.time() - start_time
        print(f"双向父子关系更新完成，共处理了 {processed_docs} 个父标签，更新了 {update_count} 个子标签文档")
        print(f"总耗时: {elapsed_time:.2f}秒，平均每秒处理 {processed_docs/elapsed_time:.2f} 个父标签")

    except Exception as e:
        print(f"发生错误: {str(e)}")

    finally:
        # 关闭连接
        client.close()

if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())