#include <Storages/SharedMergeTree/SharedMergeTreeSink.h>

#include <Core/Block.h>
#include <DataTypes/ObjectUtils.h>
#include <IO/Operators.h>
#include <Interpreters/PartLog.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Storages/MergeTree/InsertBlockInfo.h>
#include <Storages/StorageSharedMergeTree.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEventsScope.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>


namespace ProfileEvents
{
    extern const Event DuplicatedInsertedBlocks;
}

namespace DB
{

namespace FailPoints
{
    extern const char smt_commit_write_zk_fail_before_op[];
    extern const char smt_commit_write_zk_fail_after_op[];
    extern const char smt_insert_retry_timeout[];
}

namespace ErrorCodes
{
    extern const int INSERT_WAS_DEDUPLICATED;
    extern const int DUPLICATE_DATA_PART;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_ZOOKEEPER_ERROR;
    extern const int QUERY_WAS_CANCELLED;
    extern const int UNKNOWN_STATUS_OF_INSERT;
    extern const int TIMEOUT_EXCEEDED;
}

template <bool async_insert>
struct SharedMergeTreeSinkImpl<async_insert>::DelayedChunk
{
    using BlockInfo = std::conditional_t<async_insert, AsyncInsertBlockInfo, SyncInsertBlockInfo>;
    struct Partition : public BlockInfo
    {
        MergeTreeDataWriter::TemporaryPart temp_part;
        UInt64 elapsed_ns;
        ProfileEvents::Counters part_counters;

        Partition() = default;
        Partition(LoggerPtr log_,
                  MergeTreeDataWriter::TemporaryPart && temp_part_,
                  UInt64 elapsed_ns_,
                  BlockIDsType && block_id_,
                  BlockWithPartition && block_,
                  std::optional<BlockWithPartition> && unmerged_block_with_partition_,
                  ProfileEvents::Counters && part_counters_)
            : BlockInfo(log_, std::move(block_id_), std::move(block_), std::move(unmerged_block_with_partition_)),
              temp_part(std::move(temp_part_)),
              elapsed_ns(elapsed_ns_),
              part_counters(std::move(part_counters_))
        {}
    };

    DelayedChunk() = default;

    std::vector<Partition> partitions;
};

template <bool async_insert>
SharedMergeTreeSinkImpl<async_insert>::SharedMergeTreeSinkImpl(
    StorageSharedMergeTree & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    size_t max_parts_per_block_,
    bool deduplicate_,
    ContextPtr context_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , max_parts_per_block(max_parts_per_block_)
    , deduplicate(deduplicate_)
    , log(getLogger(storage.getLogName() + " (Shared Sink)"))
    , context(context_)
    , storage_snapshot(storage.getStorageSnapshotWithoutData(metadata_snapshot, context_))
{
}

namespace
{
    /// Convert block id vector to string. Output at most 50 ids.
    template<typename T>
    inline String toString(const std::vector<T> & vec)
    {
        size_t size = vec.size();
        if (size > 50) size = 50;
        return fmt::format("({})", fmt::join(vec.begin(), vec.begin() + size, ","));
    }
}

template <bool async_insert>
SharedMergeTreeSinkImpl<async_insert>::~SharedMergeTreeSinkImpl() = default;

template <bool async_insert>
void SharedMergeTreeSinkImpl<async_insert>::consume(Chunk & chunk)
{
    if (num_blocks_processed > 0)
        storage.delayInsertOrThrowIfNeeded(&storage.partial_shutdown_event, context, false);

    auto block = getHeader().cloneWithColumns(chunk.getColumns());

    const auto & settings = context->getSettingsRef();

    ZooKeeperWithFaultInjectionPtr zookeeper = ZooKeeperWithFaultInjection::createInstance(
        settings.insert_keeper_fault_injection_probability,
        settings.insert_keeper_fault_injection_seed,
        storage.getZooKeeper(),
        "SharedMergeTreeSinkImpl::consume",
        log);

    if (!storage_snapshot->object_columns.empty())
        convertDynamicColumnsToTuples(block, storage_snapshot);

    AsyncInsertInfoPtr async_insert_info;

    if constexpr (async_insert)
    {
        auto async_insert_info_prt = chunk.getChunkInfos().get<AsyncInsertInfo>();
        if (async_insert_info_prt)
            async_insert_info = std::dynamic_pointer_cast<AsyncInsertInfo>(async_insert_info_prt->clone());
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No chunk info for async inserts");
    }

    String block_dedup_token;
    auto token_info = chunk.getChunkInfos().get<DeduplicationToken::TokenInfo>();
    if (!token_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "TokenInfo is expected for consumed chunk in SharedMergeTreeSink for table: {}",
            storage.getStorageID().getNameForLogs());

    if (token_info->isDefined())
        block_dedup_token = token_info->getToken();

    auto part_blocks = DB::MergeTreeDataWriter::splitBlockIntoParts(std::move(block), max_parts_per_block, metadata_snapshot, context, async_insert_info);

    using DelayedPartition = typename SharedMergeTreeSinkImpl<async_insert>::DelayedChunk::Partition;
    using DelayedPartitions = std::vector<DelayedPartition>;
    DelayedPartitions partitions;

    size_t streams = 0;

    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        ProfileEvents::Counters part_counters;
        ProfileEventsScope profile_events_scope(&part_counters);

        /// Some merging algorithms can modify the block which loses the information about the async insert offsets
        /// when preprocessing or filtering data for async inserts deduplication we want to use the initial, unmerged block
        std::optional<BlockWithPartition> unmerged_block;

        if constexpr (async_insert)
        {
            /// we copy everything but offsets which we move because they are only used by async insert
            if (settings.optimize_on_insert && storage.writer.getMergingMode() != MergeTreeData::MergingParams::Mode::Ordinary)
                unmerged_block.emplace(Block(current_block.block), Row(current_block.partition), std::move(current_block.offsets), std::move(current_block.tokens));
        }

        /// Write part to the filesystem under temporary name. Calculate a checksum.
        auto temp_part = storage.writer.writeTempPart(current_block, metadata_snapshot, context, /* may_exist = */ false);

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!temp_part.part)
            continue;

        BlockIDsType block_id;

        if constexpr (async_insert)
        {
            auto get_block_id = [&](BlockWithPartition & block_)
            {
                block_id = AsyncInsertBlockInfo::getHashesForBlocks(block_, temp_part.part->info.partition_id);
                LOG_TRACE(log, "async insert part, part id {}, block id {}, offsets {}, size {}", temp_part.part->info.partition_id, toString(block_id), toString(block_.offsets), block_.offsets.size());
            };
            get_block_id(unmerged_block ? *unmerged_block : current_block);
        }
        else
        {
            if (deduplicate)
            {
                /// We add the hash from the data and partition identifier to deduplication ID.
                /// That is, do not insert the same data to the same partition twice.
                block_id = temp_part.part->getZeroLevelPartBlockID(block_dedup_token);
                LOG_DEBUG(log, "Wrote block with ID '{}', {} rows", block_id, current_block.block.rows());
            }
            else
            {
                LOG_DEBUG(log, "Wrote block with {} rows", current_block.block.rows());
            }

            if (!token_info->isDefined())
            {
                chassert(temp_part.part);
                const auto hash_value = temp_part.part->getPartBlockIDHash();
                token_info->addChunkHash(toString(hash_value.items[0]) + "_" + toString(hash_value.items[1]));
            }
        }

        UInt64 elapsed_ns = watch.elapsed();

        size_t max_insert_delayed_streams_for_parallel_write = DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE;
        if (settings.max_insert_delayed_streams_for_parallel_write.changed)
            max_insert_delayed_streams_for_parallel_write = settings.max_insert_delayed_streams_for_parallel_write;

        /// In case of too much columns/parts in block, flush explicitly.
        streams += temp_part.streams.size();
        if (streams > max_insert_delayed_streams_for_parallel_write)
        {
            finishDelayedChunk(zookeeper);
            delayed_chunk = std::make_unique<SharedMergeTreeSinkImpl::DelayedChunk>();
            delayed_chunk->partitions = std::move(partitions);
            finishDelayedChunk(zookeeper);

            streams = 0;
            partitions = DelayedPartitions{};
        }

        if constexpr (!async_insert)
        {
            /// Reset earlier to free memory.
            current_block.block.clear();
            current_block.partition.clear();
        }

        partitions.emplace_back(DelayedPartition(
            log,
            std::move(temp_part),
            elapsed_ns,
            std::move(block_id),
            std::move(current_block),
            std::move(unmerged_block),
            std::move(part_counters) /// profile_events_scope must be reset here.
        ));
    }

    if (!token_info->isDefined())
    {
        token_info->finishChunkHashes();
    }

    finishDelayedChunk(zookeeper);
    delayed_chunk = std::make_unique<SharedMergeTreeSinkImpl::DelayedChunk>();
    delayed_chunk->partitions = std::move(partitions);

    ++num_blocks_processed;
}

template <>
void SharedMergeTreeSinkImpl<false>::finishDelayedChunk(const ZooKeeperWithFaultInjectionPtr & zookeeper)
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        partition.temp_part.finalize();
        ProfileEventsScope profile_events_scope;
        auto & part = partition.temp_part.part;

        try
        {
            commitPart(zookeeper, part, partition.block_id, false);

            /// Set a special error code if the block is duplicate
            int error = (deduplicate && part->is_duplicate) ? ErrorCodes::INSERT_WAS_DEDUPLICATED : 0;
            auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
            PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot), ExecutionStatus(error));
            StorageSharedMergeTree::incrementInsertedPartsProfileEvent(part->getType());
        }
        catch (...)
        {
            auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
            PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot), ExecutionStatus::fromCurrentException(__PRETTY_FUNCTION__));

            throw;
        }
    }

    delayed_chunk.reset();
}

template <>
void SharedMergeTreeSinkImpl<true>::finishDelayedChunk(const ZooKeeperWithFaultInjectionPtr & zookeeper)
{
    if (!delayed_chunk)
        return;

    for (auto & partition: delayed_chunk->partitions)
    {
        int retry_times = 0;
        /// users may have lots of same inserts. It will be helpful to deduplicate in advance.
        if (partition.filterSelfDuplicate())
        {
            LOG_TRACE(log, "found duplicated inserts in the block");
            partition.block_with_partition.partition = std::move(partition.temp_part.part->partition.value);
            partition.temp_part.cancel();
            partition.temp_part = storage.writer.writeTempPart(partition.block_with_partition, metadata_snapshot, context);
        }

        /// reset the cache version to zero for every partition write.
        cache_version = 0;
        while (true)
        {
            partition.temp_part.finalize();
            auto conflict_block_ids = commitPart(zookeeper, partition.temp_part.part, partition.block_id, false).first;
            if (conflict_block_ids.empty())
            {
                auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
                PartLog::addNewPart(
                    storage.getContext(),
                    PartLog::PartLogEntry(partition.temp_part.part, partition.elapsed_ns, counters_snapshot),
                    ExecutionStatus(0));
                break;
            }

            storage.async_block_ids_cache.triggerCacheUpdate();
            ++retry_times;
            LOG_DEBUG(log, "Found duplicate block IDs: {}, retry times {}", toString(conflict_block_ids), retry_times);
            /// partition clean conflict
            partition.filterBlockDuplicate(conflict_block_ids, false);
            if (partition.block_with_partition.block.rows() == 0)
            {
                auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
                PartLog::addNewPart(
                    storage.getContext(),
                    PartLog::PartLogEntry(partition.temp_part.part, partition.elapsed_ns, counters_snapshot),
                    ExecutionStatus(ErrorCodes::INSERT_WAS_DEDUPLICATED));
                break;
            }
            partition.block_with_partition.partition = std::move(partition.temp_part.part->partition.value);
            /// partition.temp_part is already finalized, no need to call cancel
            partition.temp_part = storage.writer.writeTempPart(partition.block_with_partition, metadata_snapshot, context);
        }
    }

    delayed_chunk.reset();
}

template <bool async_insert>
void SharedMergeTreeSinkImpl<async_insert>::writeExistingPart(MergeTreeData::MutableDataPartPtr & part)
{
    /// NOTE: No delay in this case. That's Ok.

    auto origin_zookeeper = storage.getZooKeeper();
    auto zookeeper = std::make_shared<ZooKeeperWithFaultInjection>(origin_zookeeper);
    ProfileEventsScope profile_events_scope;

    Stopwatch watch;

    try
    {
        part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
        String block_id = deduplicate ? fmt::format("{}_{}", part->info.partition_id, part->checksums.getTotalChecksumHex()) : "";
        commitPart(zookeeper, part, BlockIDsType{block_id}, true);
        PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, watch.elapsed(), profile_events_scope.getSnapshot()));
    }
    catch (...)
    {
        PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, watch.elapsed(), profile_events_scope.getSnapshot()), ExecutionStatus::fromCurrentException(__PRETTY_FUNCTION__));
        throw;
    }
}

template <bool async_insert>
std::pair<std::vector<std::string>, bool> SharedMergeTreeSinkImpl<async_insert>::commitPart(
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    MergeTreeData::MutableDataPartPtr & part,
    const BlockIDsType & block_id,
    bool writing_existing_part)
{
    const auto & settings = context->getSettingsRef();
    ZooKeeperRetriesInfo zookeeper_retries_info = ZooKeeperRetriesInfo(
        settings.insert_keeper_max_retries, settings.insert_keeper_retry_initial_backoff_ms, settings.insert_keeper_retry_max_backoff_ms);

    ZooKeeperRetriesControl retries_ctl_for_commit_part("commitPart", log, zookeeper_retries_info, context->getProcessListElement());
    /// It is possible that we alter a part with different types of source columns.
    /// In this case, if column was not altered, the result type will be different with what we have in metadata.
    /// For now, consider it is ok. See 02461_alter_update_respect_part_column_type_bug for an example.
    ///
    /// metadata_snapshot->check(part->getColumns());

    /// Allocate new block number and check for duplicates
    const bool deduplicate_block = !block_id.empty();
    bool part_is_duplicate = false;

    std::vector<String> conflict_block_ids;

    bool hardware_error_when_commit = false;
    std::optional<EphemeralLockInZooKeeper> block_number_lock;
    bool tried_to_commit = false;
    auto disable_part_blobs_removal_in_destructor = [&part, &part_is_duplicate]
    {
        part->is_temp = false;
        part->is_duplicate = false;
        part_is_duplicate = false;
    };

    /// NOTE: this one will be called only in case when retries finishes with Keeper exception
    /// if it will be some other exception this function will not be called.
    retries_ctl_for_commit_part.actionAfterLastFailedRetry([&] ()
    {
        /// We tried to commit part, but have no idea about the status.
        /// It's unsafe to remove blobs, better to leave garbage on FS
        if (tried_to_commit)
        {
            /// To avoid deletion in desturctor
            disable_part_blobs_removal_in_destructor();

            throw Exception(ErrorCodes::UNKNOWN_STATUS_OF_INSERT, "We tried to commit part, but ZooKeeper is unavailable, insert status is unknown, client must retry");
        }
    });

    retries_ctl_for_commit_part.retryLoop([&]()
    {
        /// When we attach existing parts it's okay to be in read-only mode
        /// For example during RESTORE REPLICA.
        if (!writing_existing_part)
            storage.waitForNotReadonly(context);

        zookeeper->setKeeper(storage.getZooKeeper());

        auto partition_id = part->info.partition_id;

        BlockIDsType block_id_path;
        if constexpr (async_insert)
        {
            /// prefilter by cache
            conflict_block_ids = storage.async_block_ids_cache.detectConflicts(block_id, cache_version);
            if (!conflict_block_ids.empty())
            {
                cache_version = 0;
                return;
            }

            for (const auto & single_block_id : block_id)
                block_id_path.push_back(storage.zookeeper_path + "/async_blocks/" + single_block_id);
        }
        else
        {
            if (deduplicate_block)
                block_id_path = storage.zookeeper_path + "/blocks/" + block_id;
        }

        if (retries_ctl_for_commit_part.isRetry() && hardware_error_when_commit)
        {
            fiu_do_on(FailPoints::smt_insert_retry_timeout,
            {
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Injected timeout exceeded");
            });

            /// check that info about the part was actually written in zk
            std::string virtual_part_path = fs::path(storage.getZooKeeperVirtualPartsPathInPartition(part->info.partition_id)) / part->name;
            std::string outdated_part_path = fs::path(storage.getZooKeeperOutdatedPartsPath()) / part->name;

            std::string condemned_part_path = fs::path(storage.getZooKeeperCondemnedPartsPath()) / part->name;

            zookeeper->sync(fs::path(storage.getZooKeeperOutdatedPartsPath()));

            if (zookeeper->anyExists({virtual_part_path, outdated_part_path, condemned_part_path}))
            {
                LOG_DEBUG(log, "Part was successfully committed on previous iteration: part_id={}", part->name);
                /// To avoid deletion in desturctor
                storage.updateDiskMetadataCacheForPart(part->name);
                storage.addCurrentPart(part->name);
                disable_part_blobs_removal_in_destructor();
                if (block_number_lock)
                    block_number_lock->assumeUnlocked();
                return;
            }
            else
            {
                auto virtual_parts = storage.getFreshVirtualParts(storage.getZooKeeper(), /* can_read_from_leader=*/ false);
                /// Maybe someone already detached the part?
                if (virtual_parts.isCoveredByDropRange(part->info))
                {
                    LOG_INFO(log, "Part {} was concurrently covered by drop range, exiting without removing blobs", part->name);
                    disable_part_blobs_removal_in_destructor();
                    retries_ctl_for_commit_part.stopRetries();
                    if (block_number_lock)
                        block_number_lock->assumeUnlocked();

                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Currently inserting part was dropped by concurrent ALTER PARTITION query");
                }
                else
                {
                    LOG_TRACE(log, "Part {} doesn't exist, but could be merged. We will retry insert relying on deduplication hash", part->name);
                }
            }

            if (block_number_lock && !writing_existing_part)
                block_number_lock = storage.allocateBlockNumber(partition_id, zookeeper, block_id_path);
            hardware_error_when_commit = false;
        }
        else
        {
            block_number_lock = storage.allocateBlockNumber(partition_id, zookeeper, block_id_path);
        }

        /// Prepare transaction to ZooKeeper
        /// It will simultaneously add information about the part to all the necessary places in ZooKeeper and remove block_number_lock.
        Coordination::Requests ops;

        /// The first request is to check that there is no newer connection from this replica.
        /// This can happen if the session got a long queue of requests, we disconnected and connected again but the old session is still alive on Keeper
        /// and Keeper keeps processing that long queue of requests. In such a case we want the old requests to fail.
        ops.emplace_back(zkutil::makeCheckRequest(fs::path(storage.replica_path) / "host", storage.replica_host_node_version));

        Int64 block_number = 0;
        size_t block_unlock_op_idx = std::numeric_limits<size_t>::max();

        bool is_already_existing_part = false;
        if (block_number_lock)
        {
            if constexpr (async_insert)
            {
                /// The truth is that we always get only one path from block_number_lock.
                /// This is a restriction of Keeper. Here I would like to use vector because
                /// I wanna keep extensibility for future optimization, for instance, using
                /// cache to resolve conflicts in advance.
                String conflict_path = block_number_lock->getConflictPath();
                if (!conflict_path.empty())
                {
                    LOG_TRACE(log, "Cannot get lock, the conflict path is {}", conflict_path);
                    conflict_block_ids.push_back(conflict_path);
                    return;
                }
            }

            is_already_existing_part = false;
            block_number = block_number_lock->getNumber();

            /// Set part attributes according to part_number. Prepare an entry for log.

            part->info.min_block = block_number;
            part->info.max_block = block_number;
            part->info.level = 0;
            part->info.mutation = 0;

            part->setName(part->getNewName(part->info));

            /// Deletes the information that the block number is used for writing.
            block_unlock_op_idx = ops.size();
            block_number_lock->getUnlockOp(ops);
        }
        else if constexpr (!async_insert)
        {
            is_already_existing_part = true;

            /// This part could be already removed. We will try to find some covering part locally,
            /// otherwise will try to load covering part, otherwise will just update all parts state.
            String existing_part_name = zookeeper->get(storage.zookeeper_path + "/blocks/" + block_id);

            /// Does it exist locally?
            if (auto covering_part = storage.getActiveContainingPart(existing_part_name); covering_part != nullptr)
            {
                LOG_INFO(
                    log,
                    "Block with ID {} already exists as part {}, but we have covering part locally {}",
                    block_id,
                    existing_part_name,
                    covering_part->name);
            }
            else
            {
                bool part_or_covering_part_loaded = false;
                for (size_t i = 0; i < 5; ++i)
                {
                    /// Ok, we don't have anything related to this part locally, let's try to find covering part
                    /// First update virtual parts, what if part was merged
                    auto part_to_load = storage.virtual_parts.getContainingCurrentPart(existing_part_name);
                    std::string virtual_part_path
                        = fs::path(storage.getZooKeeperVirtualPartsPathInPartition(part->info.partition_id)) / part_to_load;

                    if (zookeeper->exists(virtual_part_path))
                    {
                        try
                        {
                            LOG_INFO(log, "Block with ID {} already exists as part {}, will try to load part {}", block_id, existing_part_name, part_to_load);
                            storage.updateDiskMetadataCacheForPart(part_to_load);
                            storage.addCurrentPart(part_to_load);
                            part_or_covering_part_loaded = true;
                            break;
                        }
                        catch (const Exception & ex)
                        {
                            LOG_INFO(log, "Block with ID {} already exists as part {}, but we failed to load it or covering part, because of '{}', will update virtual parts and try one more time", block_id, existing_part_name, ex.message());
                        }
                    }
                    else
                    {
                        LOG_INFO(log, "Block with ID {} already exists as part {}, but we didn't find covering part {} in virtual parts, will update virtual parts and try one more time", block_id, existing_part_name, part_to_load);
                    }

                    storage.updatePartsWithConnection(zookeeper->getKeeper(), StorageSharedMergeTree::UpdatePartsOptions{.sync_zookeeper = true});
                }

                if (!part_or_covering_part_loaded)
                {
                    LOG_INFO(log, "Block with ID {} already exists as part {}, but we failed to load it or covering part, will load new parts state", block_id, existing_part_name);
                    storage.updatePartsAndLoad({.update_mutations = false});
                }
            }
            part_is_duplicate = true;
            retries_ctl_for_commit_part.stopRetries();

            return;
        }

        ops.emplace_back(zkutil::makeSetRequest(storage.getZooKeeperVirtualPartsPath(), "", -1));

        MergeTreeData::Transaction transaction(storage, NO_TRANSACTION_RAW); /// If you can not add a part to ZK, we'll remove it back from the working set.
        bool added = false;

        LOG_DEBUG(log, "Preparing part {} for commit in memory", part->name);
        try
        {
            /// Rename part outside of lock scope
            part->renameTo(part->name, false, false);
            {
                auto lock = storage.lockParts();
                added = storage.addTempPart(part, transaction, lock, nullptr);
            }
        }
        catch (const Exception & e)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            if (e.code() != ErrorCodes::DUPLICATE_DATA_PART
                && e.code() != ErrorCodes::PART_IS_TEMPORARILY_LOCKED)
                throw;
        }

        if (!added)
        {
            if (is_already_existing_part)
            {
                transaction.rollbackPartsToTemporaryState();
                LOG_INFO(log, "Part {} is duplicate; ignoring it.", part->name);
                part_is_duplicate = true;
                return;
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part with name {} is already written by concurrent request."
                    " It should not happen for non-duplicate data parts because unique names are assigned for them. It's a bug",
                    part->name);
        }

        bool with_partition = false;
        ZooKeeperRetriesControl retries_ctl_for_commit_txn(
            "commitPartTransaction", log, zookeeper_retries_info, context->getProcessListElement());
        retries_ctl_for_commit_txn.retryLoop([&]() -> void
        {
            Coordination::Requests commit_part_ops = storage.getCommitPartOps(part->name, part->info.partition_id, block_id_path, with_partition);
            /// Information about the part.
            MetaInKeeperCommitOptions<ZooKeeperWithFaultInjection> options;
            options.zookeeper = zookeeper.get();
            options.additional_requests.insert(options.additional_requests.end(), ops.begin(), ops.end());
            options.additional_requests.insert(options.additional_requests.end(), commit_part_ops.begin(), commit_part_ops.end());
            options.may_retry = true;
            options.need_rollback_blobs = false;

            MergeTreeData::DataPartsVector vector;

            fiu_do_on(FailPoints::smt_commit_write_zk_fail_after_op,
            {
                zookeeper->forceFailureAfterOperation();
            });

            fiu_do_on(FailPoints::smt_commit_write_zk_fail_before_op,
            {
                zookeeper->forceFailureBeforeOperation();
            });

            LOG_DEBUG(log, "Committing part {} with partition {}", part->name, with_partition);
            auto outcome = getKeeperOutcome(transaction.tryCommit(options, vector));
            const auto & [code, responses, _] = outcome;

            if (code != Coordination::Error::ZOK)
            {
                tried_to_commit = true;
                if (Coordination::isHardwareError(code))
                {
                    transaction.rollbackPartsToTemporaryState();
                    /// We will decide on the next retry. If it will not happen
                    /// we are safe.
                    disable_part_blobs_removal_in_destructor();

                    retries_ctl_for_commit_txn.stopRetries();

                    retries_ctl_for_commit_txn.setKeeperError(
                        {code, "Insert of part {} failed with zookeeper session error {}", part->name, toString(code)});
                    hardware_error_when_commit = true;
                    return;
                }

                auto failed_operation_index = zkutil::getFailedOpIndex(code, responses);
                LOG_TRACE(log, "Committing part {} with partition {} failed with code {} (failed op {} (total ops {}))",
                          part->name, with_partition, toString(code), failed_operation_index, responses.size());

                /// First op from getCommitPartOps() is the block id dedup.
                size_t deduplication_position = responses.size() - commit_part_ops.size();
                const auto failed_on_deduplication_path = [&]
                {
                    if constexpr (async_insert)
                    {
                        return failed_operation_index >= deduplication_position && failed_operation_index < deduplication_position + block_id_path.size();
                    }
                    else
                    {
                        return failed_operation_index == deduplication_position;
                    }
                };

                if (code == Coordination::Error::ZNODEEXISTS && deduplicate_block && failed_on_deduplication_path())
                {
                    ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks);
                    part->is_duplicate = true;

                    if constexpr (async_insert)
                    {
                        conflict_block_ids = std::vector<String>({commit_part_ops[failed_operation_index - deduplication_position]->getPath()});
                        LOG_TRACE(log, "conflict when committing, the conflict block IDs are {}", toString(conflict_block_ids));
                    }
                    else
                    {
                        transaction.rollbackPartsToTemporaryState();
                        LOG_INFO(log, "Block with ID {} already exists; ignoring it.", block_id);
                    }

                    return;
                }
                else if (code == Coordination::Error::ZNONODE && responses.rbegin()[1]->error == Coordination::Error::ZNONODE)
                {
                    /// This is normal when we first insert a data to a new partition. Do not backoff.
                    with_partition = true;
                    LOG_DEBUG(log, "Path of virtual parts do not exist, will retry unconditionally and create this path");
                    retries_ctl_for_commit_txn.unconditional_retry = true;
                    return;
                }
                else if (code == Coordination::Error::ZNODEEXISTS && responses.rbegin()[2]->error == Coordination::Error::ZNODEEXISTS)
                {
                    with_partition = false;

                    retries_ctl_for_commit_txn.setKeeperError({code, "Path of virtual parts exists"});
                    return;
                }
                else if (code == Coordination::Error::ZNONODE && zkutil::getFailedOpIndex(code, responses) == outcome.additionalOpIdx(block_unlock_op_idx))
                {
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                                    "Insert query (for block {}) was cancelled by concurrent ALTER PARTITION", block_number_lock->getPath());
                }
                else
                {
                    throw Exception(ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR, "Got unexpected ZooKeeper error {} (at index {}) for part {}",
                                    toString(code), failed_operation_index, part->name);
                }
            }
            else /// ZOK
            {
                if (block_number_lock)
                    block_number_lock->assumeUnlocked();

                storage.updateDiskMetadataCacheForPart(part->name);

                storage.merge_selecting_task->schedule();
                return;
            }
        });
    });

    if (!conflict_block_ids.empty())
        return {std::move(conflict_block_ids), part_is_duplicate};

    LOG_DEBUG(log, "Part {} processed", part->name);

    return {std::move(conflict_block_ids), part_is_duplicate};
}

template <bool async_insert>
void SharedMergeTreeSinkImpl<async_insert>::onStart()
{
    storage.delayInsertOrThrowIfNeeded(&storage.partial_shutdown_event, context, true);
}

template <bool async_insert>
void SharedMergeTreeSinkImpl<async_insert>::onFinish()
{
    if (!delayed_chunk)
        return; /// If there is no delayed chunk we do not need to get session and check its expiration.

    const auto & settings = context->getSettingsRef();

    ZooKeeperWithFaultInjectionPtr zookeeper = ZooKeeperWithFaultInjection::createInstance(
        settings.insert_keeper_fault_injection_probability,
        settings.insert_keeper_fault_injection_seed,
        storage.getZooKeeper(),
        "SharedMergeTreeSinkImpl::onFinish",
        log);

    finishDelayedChunk(zookeeper);
}

template class SharedMergeTreeSinkImpl<true>;
template class SharedMergeTreeSinkImpl<false>;

}
