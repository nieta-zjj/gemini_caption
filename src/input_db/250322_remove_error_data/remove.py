# {
#   success: false,
#   status_code: { $exists: false }
# }

# mongodb://8.153.97.53:27815/
# gemini_captions_danbooru

import pymongo
import argparse

def main(auto_yes=False):
    client = pymongo.MongoClient("mongodb://8.153.97.53:27815/")
    db = client["gemini_captions_danbooru"]

    # 定义要匹配的条件
    query = {
        "success": False,
        "status_code": { "$exists": False },
        "prompt": { "$exists": False }
    }

    # 获取所有集合名称
    collection_names = db.list_collection_names()

    if not collection_names:
        print("数据库中没有集合。")
        return

    total_deleted = 0

    # 逐个处理每个集合
    for collection_name in collection_names:
        collection = db[collection_name]

        # 统计符合条件的文档数量
        count = collection.count_documents(query)

        if count == 0:
            print(f"\n集合 {collection_name}: 没有符合条件的文档需要删除。")
            continue

        print(f"\n集合 {collection_name}: 发现 {count} 个符合条件的文档。")

        # 根据auto_yes参数决定是否询问用户
        if not auto_yes:
            confirmation = input(f"是否删除集合 {collection_name} 中的这些文档？(y/n，默认y): ").strip().lower()
            should_delete = confirmation == '' or confirmation == 'y'
        else:
            print(f"自动确认：将删除集合 {collection_name} 中的文档")
            should_delete = True

        if should_delete:
            # 执行删除操作
            result = collection.delete_many(query)
            deleted = result.deleted_count
            total_deleted += deleted
            print(f"已从 {collection_name} 中删除 {deleted} 个文档")
        else:
            print(f"已跳过集合 {collection_name}")

    print(f"\n操作完成！总共删除了 {total_deleted} 个文档。")

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='删除MongoDB中符合条件的文档')
    parser.add_argument('-y', '--yes', action='store_true',
                        help='自动确认所有删除操作，不再询问')
    args = parser.parse_args()

    # 执行主函数，传入auto_yes参数
    main(auto_yes=args.yes)