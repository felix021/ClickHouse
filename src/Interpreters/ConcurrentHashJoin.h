#pragma once

#include <condition_variable>
#include <memory>
#include <optional>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/IJoin.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool_fwd.h>

namespace DB
{

/**
 * Can run addBlockToJoin() parallelly to speedup the join process. On test, it almose linear speedup by
 * the degree of parallelism.
 *
 * The default HashJoin is not thread safe for inserting right table's rows and run it in a single thread. When
 * the right table is large, the join process is too slow.
 *
 * We create multiple HashJoin instances here. In addBlockToJoin(), one input block is split into multiple blocks
 * corresponding to the HashJoin instances by hashing every row on the join keys. And make a guarantee that every HashJoin
 * instance is written by only one thread.
 *
 * When come to the left table matching, the blocks from left table are alse split into different HashJoin instances.
 *
 */
class ConcurrentHashJoin : public IJoin
{

public:
    explicit ConcurrentHashJoin(
        ContextPtr context_,
        std::shared_ptr<TableJoin> table_join_,
        size_t slots_,
        const Block & right_sample_block,
        bool any_take_last_row_ = false);

    ~ConcurrentHashJoin() override;

    std::string getName() const override { return "ConcurrentHashJoin"; }
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool addBlockToJoin(const Block & block, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) override;
    void setTotals(const Block & block) override;
    const Block & getTotals() const override;
    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;
    bool supportParallelJoin() const override { return true; }

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

private:
    struct InternalHashJoin
    {
        std::mutex mutex;
        std::unique_ptr<HashJoin> data;
    };

    ContextPtr context;
    std::shared_ptr<TableJoin> table_join;
    size_t slots;
    std::unique_ptr<ThreadPool> pool;
    std::vector<std::shared_ptr<InternalHashJoin>> hash_joins;

    std::mutex totals_mutex;
    Block totals;

    IColumn::Selector selectDispatchBlock(const Strings & key_columns_names, const Block & from_block);
    Blocks dispatchBlock(const Strings & key_columns_names, const Block & from_block);
};

}
