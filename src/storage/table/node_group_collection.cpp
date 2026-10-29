#include "storage/table/node_group_collection.h"

#include <mutex>
#include <thread>

#include "common/exception/buffer_manager.h"
#include "common/vector/value_vector.h"
#include "storage/shadow_file.h"
#include "storage/table/chunked_node_group.h"
#include "storage/table/column.h"
#include "storage/table/csr_node_group.h"
#include "storage/table/table.h"
#include "transaction/transaction.h"

using namespace lbug::common;
using namespace lbug::transaction;

namespace lbug {
namespace storage {

NodeGroupCollection::NodeGroupCollection(MemoryManager& mm, const std::vector<LogicalType>& types,
    const bool enableCompression, ResidencyState residency,
    const VersionRecordHandler* versionRecordHandler)
    : mm{mm}, enableCompression{enableCompression}, numTotalRows{0},
      types{LogicalType::copy(types)}, residency{residency}, stats{std::span{types}},
      versionRecordHandler(versionRecordHandler) {
    const auto lock = nodeGroups.lock();
    for (auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        numTotalRows += nodeGroup->getNumRows();
    }
}

void NodeGroupCollection::append(const Transaction* transaction,
    const std::vector<ValueVector*>& vectors) {
    const auto numRowsToAppend = vectors[0]->state->getSelVector().getSelSize();
    DASSERT(numRowsToAppend == vectors[0]->state->getSelVector().getSelSize());
    for (auto i = 1u; i < vectors.size(); i++) {
        DASSERT(vectors[i]->state->getSelVector().getSelSize() == numRowsToAppend);
    }
    const auto lock = nodeGroups.lock();
    if (nodeGroups.isEmpty(lock)) {
        auto newGroup =
            std::make_unique<NodeGroup>(mm, 0, enableCompression, LogicalType::copy(types));
        nodeGroups.appendGroup(lock, std::move(newGroup));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        auto lastNodeGroup = nodeGroups.getLastGroup(lock);
        if (!lastNodeGroup || lastNodeGroup->isFull()) {
            auto newGroup = std::make_unique<NodeGroup>(mm, nodeGroups.getNumGroups(lock),
                enableCompression, LogicalType::copy(types));
            nodeGroups.appendGroup(lock, std::move(newGroup));
        }
        lastNodeGroup = nodeGroups.getLastGroup(lock);
        const auto numToAppendInNodeGroup =
            std::min(numRowsToAppend - numRowsAppended, lastNodeGroup->getNumRowsLeftToAppend());
        lastNodeGroup->moveNextRowToAppend(numToAppendInNodeGroup);
        pushInsertInfo(transaction, lastNodeGroup, numToAppendInNodeGroup);
        numTotalRows += numToAppendInNodeGroup;
        lastNodeGroup->append(transaction, vectors, numRowsAppended, numToAppendInNodeGroup);
        numRowsAppended += numToAppendInNodeGroup;
    }
    stats.update(vectors);
}

void NodeGroupCollection::append(const Transaction* transaction,
    const std::vector<column_id_t>& columnIDs, const NodeGroupCollection& other) {
    const auto otherLock = other.nodeGroups.lock();
    for (auto& nodeGroup : other.nodeGroups.getAllGroups(otherLock)) {
        append(transaction, columnIDs, *nodeGroup);
    }
    mergeStats(columnIDs, other.getStats(otherLock));
}

void NodeGroupCollection::append(const Transaction* transaction,
    const std::vector<column_id_t>& columnIDs, const NodeGroup& nodeGroup) {
    DASSERT(nodeGroup.getDataTypes().size() == columnIDs.size());
    const auto lock = nodeGroups.lock();
    if (nodeGroups.isEmpty(lock)) {
        auto newGroup =
            std::make_unique<NodeGroup>(mm, 0, enableCompression, LogicalType::copy(types));
        nodeGroups.appendGroup(lock, std::move(newGroup));
    }
    const auto numChunkedGroupsToAppend = nodeGroup.getNumChunkedGroups();
    node_group_idx_t numChunkedGroupsAppended = 0;
    while (numChunkedGroupsAppended < numChunkedGroupsToAppend) {
        const auto chunkedGroupToAppend = nodeGroup.getChunkedNodeGroup(numChunkedGroupsAppended);
        const auto numRowsToAppendInChunkedGroup = chunkedGroupToAppend->getNumRows();
        row_idx_t numRowsAppendedInChunkedGroup = 0;
        while (numRowsAppendedInChunkedGroup < numRowsToAppendInChunkedGroup) {
            auto lastNodeGroup = nodeGroups.getLastGroup(lock);
            if (!lastNodeGroup || lastNodeGroup->isFull()) {
                auto newGroup = std::make_unique<NodeGroup>(mm, nodeGroups.getNumGroups(lock),
                    enableCompression, LogicalType::copy(types));
                nodeGroups.appendGroup(lock, std::move(newGroup));
            }
            lastNodeGroup = nodeGroups.getLastGroup(lock);
            const auto numToAppendInBatch =
                std::min(numRowsToAppendInChunkedGroup - numRowsAppendedInChunkedGroup,
                    lastNodeGroup->getNumRowsLeftToAppend());
            lastNodeGroup->moveNextRowToAppend(numToAppendInBatch);
            pushInsertInfo(transaction, lastNodeGroup, numToAppendInBatch);
            numTotalRows += numToAppendInBatch;
            lastNodeGroup->append(transaction, columnIDs, *chunkedGroupToAppend,
                numRowsAppendedInChunkedGroup, numToAppendInBatch);
            numRowsAppendedInChunkedGroup += numToAppendInBatch;
        }
        numChunkedGroupsAppended++;
    }
}

std::pair<offset_t, offset_t> NodeGroupCollection::appendToLastNodeGroupAndFlushWhenFull(
    Transaction* transaction, const std::vector<column_id_t>& columnIDs,
    InMemChunkedNodeGroup& chunkedGroup, PageAllocator& pageAllocator) {
    NodeGroup* lastNodeGroup = nullptr;
    offset_t startOffset = 0;
    offset_t numToAppend = 0;
    bool directFlushWhenAppend = false;
    {
        const auto lock = nodeGroups.lock();
        startOffset = numTotalRows;
        if (nodeGroups.isEmpty(lock)) {
            nodeGroups.appendGroup(lock,
                std::make_unique<NodeGroup>(mm, nodeGroups.getNumGroups(lock), enableCompression,
                    LogicalType::copy(types)));
        }
        lastNodeGroup = nodeGroups.getLastGroup(lock);
        auto numRowsLeftInLastNodeGroup = lastNodeGroup->getNumRowsLeftToAppend();
        if (numRowsLeftInLastNodeGroup == 0) {
            nodeGroups.appendGroup(lock,
                std::make_unique<NodeGroup>(mm, nodeGroups.getNumGroups(lock), enableCompression,
                    LogicalType::copy(types)));
            lastNodeGroup = nodeGroups.getLastGroup(lock);
            numRowsLeftInLastNodeGroup = lastNodeGroup->getNumRowsLeftToAppend();
        }
        numToAppend = std::min(chunkedGroup.getNumRows(), numRowsLeftInLastNodeGroup);
        lastNodeGroup->moveNextRowToAppend(numToAppend);
        // If the node group is empty now and the chunked group is full, we can directly flush it.
        directFlushWhenAppend =
            numToAppend == numRowsLeftInLastNodeGroup && lastNodeGroup->getNumRows() == 0;
        pushInsertInfo(transaction, lastNodeGroup, numToAppend);
        numTotalRows += numToAppend;
        if (!directFlushWhenAppend) {
            // TODO(Guodong): Further optimize on this. Should directly figure out startRowIdx to
            // start appending into the node group and pass in as param.
            lastNodeGroup->append(transaction, columnIDs, chunkedGroup, 0, numToAppend);
        }
    }
    if (directFlushWhenAppend) {
        auto flushedGroup = chunkedGroup.flush(transaction, pageAllocator);

        // If there are deleted columns that haven't been vacuumed yet,
        // we need to add extra columns to the chunked group
        // to ensure that the number of columns is consistent with the rest of the node group
        auto groupToMerge = std::make_unique<ChunkedNodeGroup>(mm, *flushedGroup,
            lastNodeGroup->getDataTypes(), columnIDs);

        DASSERT(lastNodeGroup->getNumChunkedGroups() == 0);
        lastNodeGroup->merge(transaction, std::move(groupToMerge));
    }
    return {startOffset, numToAppend};
}

row_idx_t NodeGroupCollection::getNumTotalRows() const {
    const auto lock = nodeGroups.lock();
    return numTotalRows;
}

NodeGroup* NodeGroupCollection::getOrCreateNodeGroup(const Transaction* transaction,
    node_group_idx_t groupIdx, NodeGroupDataFormat format) {
    const auto lock = nodeGroups.lock();
    while (groupIdx >= nodeGroups.getNumGroups(lock)) {
        const auto currentGroupIdx = nodeGroups.getNumGroups(lock);
        nodeGroups.appendGroup(lock, format == NodeGroupDataFormat::REGULAR ?
                                         std::make_unique<NodeGroup>(mm, currentGroupIdx,
                                             enableCompression, LogicalType::copy(types)) :
                                         std::make_unique<CSRNodeGroup>(mm, currentGroupIdx,
                                             enableCompression, LogicalType::copy(types)));
        // push an insert of size 0 so that we can roll back the creation of this node group if
        // needed
        pushInsertInfo(transaction, nodeGroups.getLastGroup(lock), 0);
    }
    DASSERT(groupIdx < nodeGroups.getNumGroups(lock));
    return nodeGroups.getGroup(lock, groupIdx);
}

void NodeGroupCollection::addColumn(TableAddColumnState& addColumnState,
    PageAllocator* pageAllocator) {
    DASSERT((pageAllocator == nullptr) == (residency == ResidencyState::IN_MEMORY));
    const auto lock = nodeGroups.lock();
    auto& newColumnStats = stats.addNewColumn(addColumnState.propertyDefinition.getType());
    for (const auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        nodeGroup->addColumn(addColumnState, pageAllocator, &newColumnStats);
    }
    types.push_back(addColumnState.propertyDefinition.getType().copy());
}

uint64_t NodeGroupCollection::getEstimatedMemoryUsage() const {
    auto estimatedMemUsage = 0u;
    const auto lock = nodeGroups.lock();
    for (const auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        estimatedMemUsage += nodeGroup->getEstimatedMemoryUsage();
    }
    return estimatedMemUsage;
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
bool NodeGroupCollection::hasRetainedHorizon() const {
    const auto lock = nodeGroups.lock();
    for (const auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        if (nodeGroup->hasRetainedHorizon()) {
            return true;
        }
    }
    return false;
}

void NodeGroupCollection::checkpoint(MemoryManager& memoryManager,
    NodeGroupCheckpointState& state) {
    checkpointInParallel(memoryManager, state, nullptr /*makeState*/, 1 /*numThreads*/);
}

namespace {
// The page allocator of one node group checkpoint run beside others. It holds the frees back
// until the group succeeds: a failed group then frees no page its restored chunks still use,
// and gives back the pages it allocated, so it can be checkpointed again in the same checkpoint.
class GroupCheckpointAllocator final : public PageAllocator {
public:
    explicit GroupCheckpointAllocator(PageAllocator& allocator)
        : PageAllocator{allocator.getDataFH()}, allocator{allocator} {}

    PageRange allocatePageRange(page_idx_t numPages) override {
        const auto range = allocator.allocatePageRange(numPages);
        if (numPages > 0) {
            std::unique_lock lck{mtx};
            allocated.push_back(range);
        }
        return range;
    }
    void freePageRange(PageRange range) override {
        std::unique_lock lck{mtx};
        frees.push_back(range);
    }
    PageAllocator& getBackingAllocator() override { return allocator; }

    void commit() {
        std::unique_lock lck{mtx};
        for (const auto& range : frees) {
            allocator.freePageRange(range);
        }
        frees.clear();
        allocated.clear();
    }
    // The failed group's chunks reference none of the pages it allocated. They are freed like
    // any page freed by a checkpoint: reusable once the checkpoint is durable.
    void rollback() {
        std::unique_lock lck{mtx};
        for (const auto& range : allocated) {
            allocator.freePageRange(range);
        }
        frees.clear();
        allocated.clear();
    }

private:
    PageAllocator& allocator;
    std::mutex mtx;
    std::vector<PageRange> allocated;
    std::vector<PageRange> frees;
};

bool isOutOfMemory(const std::exception_ptr& error) {
    try {
        std::rethrow_exception(error);
    } catch (const BufferManagerException&) {
        return true;
    } catch (...) {
        return false;
    }
}
} // namespace

void NodeGroupCollection::checkpointInParallel(MemoryManager& memoryManager,
    NodeGroupCheckpointState& state,
    const std::function<std::unique_ptr<NodeGroupCheckpointState>(PageAllocator&)>& makeState,
    uint64_t numThreads) {
    DASSERT(residency == ResidencyState::ON_DISK);
    const auto lock = nodeGroups.lock();
    const auto& groups = nodeGroups.getAllGroups(lock);
    numThreads = std::min<uint64_t>(numThreads, groups.size());
    if (numThreads < 2) {
        for (const auto& nodeGroup : groups) {
            nodeGroup->checkpoint(memoryManager, state);
        }
    } else {
        // Each group is checkpointed whole on one thread, so its page-free deferral (thread
        // local) and its shadow-file savepoint (attributed per thread) cover just its own work.
        std::atomic<size_t> nextGroup{0};
        std::atomic<bool> failed{false};
        std::vector<std::exception_ptr> errors(numThreads);
        std::vector<uint8_t> done(groups.size(), 0);
        std::vector<std::thread> workers;
        workers.reserve(numThreads);
        for (auto t = 0u; t < numThreads; t++) {
            workers.emplace_back([&, t] {
                GroupCheckpointAllocator allocator{state.pageAllocator};
                try {
                    const auto groupState = makeState(allocator);
                    while (!failed.load(std::memory_order_relaxed)) {
                        const auto i = nextGroup.fetch_add(1);
                        if (i >= groups.size()) {
                            break;
                        }
                        groups[i]->checkpoint(memoryManager, *groupState);
                        allocator.commit();
                        done[i] = 1;
                    }
                } catch (...) {
                    allocator.rollback();
                    errors[t] = std::current_exception();
                    failed.store(true, std::memory_order_relaxed);
                }
            });
        }
        for (auto& worker : workers) {
            worker.join();
        }
        if (failed.load()) {
            // A failed group dropped its shadow pages, leaving holes between those of the groups
            // that ran beside it: close them while no thread writes shadow pages. The groups that
            // succeeded keep their work, as when groups run one after another.
            if (!state.columns.empty() && state.columns[0]->getShadowFile() != nullptr) {
                state.columns[0]->getShadowFile()->compactShadowPages();
            }
            for (auto& error : errors) {
                if (error && !isOutOfMemory(error)) {
                    std::rethrow_exception(error);
                }
            }
            // Groups running beside each other ran out of memory. A failed group undid all its
            // work (CSRNodeGroup::checkpoint rethrows a buffer manager exception only then), so
            // checkpoint it, and the groups not started, one at a time as without threads.
            for (auto i = 0u; i < groups.size(); i++) {
                if (!done[i]) {
                    groups[i]->checkpoint(memoryManager, state);
                }
            }
        }
    }
    std::vector<LogicalType> typesAfterCheckpoint;
    for (auto i = 0u; i < state.columnIDs.size(); i++) {
        typesAfterCheckpoint.push_back(types[state.columnIDs[i]].copy());
    }
    types = std::move(typesAfterCheckpoint);
}

void NodeGroupCollection::reclaimStorage(PageAllocator& pageAllocator) const {
    const auto lock = nodeGroups.lock();
    for (auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        nodeGroup->reclaimStorage(pageAllocator);
    }
}

void NodeGroupCollection::rollbackInsert(row_idx_t numRows_, bool updateNumRows) {
    const auto lock = nodeGroups.lock();

    // remove any empty trailing node groups after the rollback
    const auto numGroupsToRemove = nodeGroups.getNumEmptyTrailingGroups(lock);
    nodeGroups.removeTrailingGroups(lock, numGroupsToRemove);

    if (updateNumRows) {
        DASSERT(numRows_ <= numTotalRows);
        numTotalRows -= numRows_;
    }
}

void NodeGroupCollection::pushInsertInfo(const Transaction* transaction, const NodeGroup* nodeGroup,
    row_idx_t numRows) {
    pushInsertInfo(transaction, nodeGroup->getNodeGroupIdx(), nodeGroup->getNumRows(), numRows,
        versionRecordHandler, false);
};

void NodeGroupCollection::pushInsertInfo(const Transaction* transaction,
    node_group_idx_t nodeGroupIdx, row_idx_t startRow, row_idx_t numRows,
    const VersionRecordHandler* versionRecordHandler, bool incrementNumRows) {
    // we only append to the undo buffer if the node group collection is persistent
    if (residency == ResidencyState::ON_DISK && transaction->shouldAppendToUndoBuffer()) {
        transaction->pushInsertInfo(nodeGroupIdx, startRow, numRows, versionRecordHandler);
    }
    if (incrementNumRows) {
        numTotalRows += numRows;
    }
}

void NodeGroupCollection::serialize(Serializer& ser) {
    ser.writeDebuggingInfo("node_groups");
    nodeGroups.serializeGroups(ser);
    ser.writeDebuggingInfo("stats");
    stats.serialize(ser);
}

void NodeGroupCollection::deserialize(Deserializer& deSer, MemoryManager& memoryManager) {
    std::string key;
    deSer.validateDebuggingInfo(key, "node_groups");
    DASSERT(residency == ResidencyState::ON_DISK);
    nodeGroups.deserializeGroups(memoryManager, deSer, types);
    deSer.validateDebuggingInfo(key, "stats");
    stats.deserialize(deSer);
    numTotalRows = 0;
    const auto lock = nodeGroups.lock();
    for (auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        numTotalRows += nodeGroup->getNumRows();
    }
}

} // namespace storage
} // namespace lbug
