#include "storage/table/rel_table_data.h"

#include <algorithm>
#include <tuple>

#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/enums/rel_direction.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/table/node_group.h"
#include "storage/table/rel_table.h"
#include "transaction/transaction.h"

using namespace lbug::catalog;
using namespace lbug::common;
using namespace lbug::transaction;

namespace lbug {
namespace storage {

PersistentVersionRecordHandler::PersistentVersionRecordHandler(RelTableData* relTableData,
    catalog::Catalog* ownerCatalog)
    : VersionRecordHandler(ownerCatalog), relTableData(relTableData) {}

void PersistentVersionRecordHandler::applyFuncToChunkedGroups(version_record_handler_op_t func,
    node_group_idx_t nodeGroupIdx, row_idx_t startRow, row_idx_t numRows,
    transaction_t commitTS) const {
    if (nodeGroupIdx < relTableData->getNumNodeGroups()) {
        auto& nodeGroup = relTableData->getNodeGroupNoLock(nodeGroupIdx)->cast<CSRNodeGroup>();
        if (auto* persistentChunkedGroup = nodeGroup.getPersistentChunkedGroup()) {
            std::invoke(func, *persistentChunkedGroup, startRow, numRows, commitTS);
        }
    }
}

void PersistentVersionRecordHandler::rollbackInsert(main::ClientContext* context,
    node_group_idx_t nodeGroupIdx, row_idx_t startRow, row_idx_t numRows) const {
    VersionRecordHandler::rollbackInsert(context, nodeGroupIdx, startRow, numRows);
    relTableData->rollbackGroupCollectionInsert(numRows, true);
}

InMemoryVersionRecordHandler::InMemoryVersionRecordHandler(RelTableData* relTableData,
    catalog::Catalog* ownerCatalog)
    : VersionRecordHandler(ownerCatalog), relTableData(relTableData) {}

void InMemoryVersionRecordHandler::applyFuncToChunkedGroups(version_record_handler_op_t func,
    node_group_idx_t nodeGroupIdx, row_idx_t startRow, row_idx_t numRows,
    transaction_t commitTS) const {
    auto* nodeGroup = relTableData->getNodeGroupNoLock(nodeGroupIdx);
    nodeGroup->applyFuncToChunkedGroups(func, startRow, numRows, commitTS);
}

void InMemoryVersionRecordHandler::rollbackInsert(main::ClientContext* context,
    node_group_idx_t nodeGroupIdx, row_idx_t startRow, row_idx_t numRows) const {
    VersionRecordHandler::rollbackInsert(context, nodeGroupIdx, startRow, numRows);
    auto* nodeGroup = relTableData->getNodeGroupNoLock(nodeGroupIdx);
    const auto numRowsToRollback = std::min(numRows, nodeGroup->getNumRows() - startRow);
    nodeGroup->rollbackInsert(startRow);
    relTableData->rollbackGroupCollectionInsert(numRowsToRollback, false);
}

RelTableData::RelTableData(FileHandle* dataFH, MemoryManager* mm, ShadowFile* shadowFile,
    const RelGroupCatalogEntry& relGroupEntry, const RelTableCatalogInfo& relTableInfo,
    Table& table, RelDataDirection direction, table_id_t nbrTableID, bool enableCompression)
    : table{table}, mm{mm}, shadowFile{shadowFile}, enableCompression{enableCompression},
      direction{direction}, multiplicity{relTableInfo.getMultiplicity(direction)},
      persistentVersionRecordHandler(this,
          table.getOwnerCatalogName().empty() ? nullptr : relGroupEntry.getOwningCatalog()),
      inMemoryVersionRecordHandler(this,
          table.getOwnerCatalogName().empty() ? nullptr : relGroupEntry.getOwningCatalog()) {
    initCSRHeaderColumns(dataFH);
    initPropertyColumns(relGroupEntry, nbrTableID, dataFH);
    // default to using the persistent version record handler
    // if we want to use the in-memory handler, we will explicitly pass it into
    // nodeGroups.pushInsertInfo()
    nodeGroups = std::make_unique<NodeGroupCollection>(*mm, getColumnTypes(), enableCompression,
        ResidencyState::ON_DISK, &persistentVersionRecordHandler);
}

void RelTableData::initCSRHeaderColumns(FileHandle* dataFH) {
    // No NULL values is allowed for the csr length and offset column.
    auto csrOffsetColumnName = StorageUtils::getColumnName("", StorageUtils::ColumnType::CSR_OFFSET,
        RelDirectionUtils::relDirectionToString(direction));
    csrHeaderColumns.offset = std::make_unique<Column>(csrOffsetColumnName, LogicalType::UINT64(),
        dataFH, mm, shadowFile, enableCompression, false /* requireNullColumn */);
    auto csrLengthColumnName = StorageUtils::getColumnName("", StorageUtils::ColumnType::CSR_LENGTH,
        RelDirectionUtils::relDirectionToString(direction));
    csrHeaderColumns.length = std::make_unique<Column>(csrLengthColumnName, LogicalType::UINT64(),
        dataFH, mm, shadowFile, enableCompression, false /* requireNullColumn */);
}

void RelTableData::initPropertyColumns(const RelGroupCatalogEntry& relGroupEntry,
    table_id_t nbrTableID, FileHandle* dataFH) {
    const auto maxColumnID = relGroupEntry.getMaxColumnID();
    columns.resize(maxColumnID + 1);
    auto nbrIDColName = StorageUtils::getColumnName("NBR_ID", StorageUtils::ColumnType::DEFAULT,
        RelDirectionUtils::relDirectionToString(direction));
    auto nbrIDColumn =
        std::make_unique<InternalIDColumn>(nbrIDColName, dataFH, mm, shadowFile, enableCompression);
    columns[NBR_ID_COLUMN_ID] = std::move(nbrIDColumn);
    for (auto& property : relGroupEntry.getProperties()) {
        const auto columnID = relGroupEntry.getColumnID(property.getName());
        const auto colName = StorageUtils::getColumnName(property.getName(),
            StorageUtils::ColumnType::DEFAULT, RelDirectionUtils::relDirectionToString(direction));
        columns[columnID] = ColumnFactory::createColumn(colName, property.getType().copy(), dataFH,
            mm, shadowFile, enableCompression);
    }
    // Set common tableID for nbrIDColumn and relIDColumn.
    columns[NBR_ID_COLUMN_ID]->cast<InternalIDColumn>().setCommonTableID(nbrTableID);
    columns[REL_ID_COLUMN_ID]->cast<InternalIDColumn>().setCommonTableID(table.getTableID());
}

bool RelTableData::update(Transaction* transaction, ValueVector& boundNodeIDVector,
    const ValueVector& relIDVector, column_id_t columnID, const ValueVector& dataVector) const {
    DASSERT(boundNodeIDVector.state->getSelVector().getSelSize() == 1);
    DASSERT(relIDVector.state->getSelVector().getSelSize() == 1);
    const auto boundNodePos = boundNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = relIDVector.state->getSelVector()[0];
    if (boundNodeIDVector.isNull(boundNodePos) || relIDVector.isNull(relIDPos)) {
        return false;
    }
    const auto [source, rowIdx] = findMatchingRow(transaction, boundNodeIDVector, relIDVector);
    // A stale or uninitialized rel ID can point to a row that does not exist (e.g. an invalid
    // ID left over from a MERGE pattern lookup). Skip the update instead of corrupting memory.
    if (rowIdx == INVALID_ROW_IDX || source == CSRNodeGroupScanSource::NONE) {
        return false;
    }
    DASSERT(rowIdx != INVALID_ROW_IDX);
    const auto boundNodeOffset = boundNodeIDVector.getValue<nodeID_t>(boundNodePos).offset;
    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(boundNodeOffset);
    if (nodeGroupIdx >= getNumNodeGroups()) {
        return false;
    }
    auto& csrNodeGroup = getNodeGroup(nodeGroupIdx)->cast<CSRNodeGroup>();
    csrNodeGroup.update(transaction, source, rowIdx, columnID, dataVector);
    return true;
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
bool RelTableData::delete_(Transaction* transaction, ValueVector& boundNodeIDVector,
    const ValueVector& relIDVector) {
    const auto boundNodePos = boundNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = relIDVector.state->getSelVector()[0];
    if (boundNodeIDVector.isNull(boundNodePos) || relIDVector.isNull(relIDPos)) {
        return false;
    }
    const auto [source, rowIdx] = findMatchingRow(transaction, boundNodeIDVector, relIDVector);
    return deleteRow(transaction,
        RelRowLookup{boundNodeIDVector.getValue<nodeID_t>(boundNodePos).offset,
            relIDVector.getValue<nodeID_t>(relIDPos).offset, source, rowIdx});
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
bool RelTableData::deleteRow(Transaction* transaction, const RelRowLookup& lookup) {
    if (lookup.rowIdx == INVALID_ROW_IDX || lookup.source == CSRNodeGroupScanSource::NONE) {
        return false;
    }
    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(lookup.boundNodeOffset);
    if (nodeGroupIdx >= getNumNodeGroups()) {
        return false;
    }
    auto& csrNodeGroup = getNodeGroup(nodeGroupIdx)->cast<CSRNodeGroup>();
    bool isDeleted = csrNodeGroup.delete_(transaction, lookup.source, lookup.rowIdx);
    if (isDeleted && transaction->shouldAppendToUndoBuffer()) {
        transaction->pushDeleteInfo(nodeGroupIdx, lookup.rowIdx, 1,
            getVersionRecordHandler(lookup.source));
    }
    return isDeleted;
}

void RelTableData::addColumn(TableAddColumnState& addColumnState, PageAllocator& pageAllocator) {
    auto& definition = addColumnState.propertyDefinition;
    auto column = ColumnFactory::createColumn(definition.getName(), definition.getType().copy(),
        pageAllocator.getDataFH(), mm, shadowFile, enableCompression);
    {
        std::unique_lock lck{columnsMtx};
        columns.push_back(std::move(column));
    }
    nodeGroups->addColumn(addColumnState, &pageAllocator);
}

std::pair<CSRNodeGroupScanSource, row_idx_t> RelTableData::findMatchingRow(Transaction* transaction,
    ValueVector& boundNodeIDVector, const ValueVector& relIDVector) const {
    DASSERT(boundNodeIDVector.state->getSelVector().getSelSize() == 1);
    DASSERT(relIDVector.state->getSelVector().getSelSize() == 1);
    const auto boundNodePos = boundNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = relIDVector.state->getSelVector()[0];
    const auto boundNodeOffset = boundNodeIDVector.getValue<nodeID_t>(boundNodePos).offset;
    const auto relOffset = relIDVector.getValue<nodeID_t>(relIDPos).offset;
    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(boundNodeOffset);
    if (nodeGroupIdx >= getNumNodeGroups()) {
        // The bound node's node group does not exist (e.g. the rel table is still empty), so
        // there cannot be a matching row.
        return {CSRNodeGroupScanSource::NONE, INVALID_ROW_IDX};
    }

    // Deleting or updating many rels of one node scans its list once per rel, which is quadratic
    // for a node with millions of rels. Long lists are scanned once per transaction and cached.
    {
        std::unique_lock lck{rowLookupMtx};
        if (rowLookupTransactionID != transaction->getID()) {
            rowLookupCache.clear();
            rowLookupTransactionID = transaction->getID();
        }
        const auto cachedList = rowLookupCache.find(boundNodeOffset);
        if (cachedList != rowLookupCache.end()) {
            const auto cachedRow = cachedList->second.find(relOffset);
            if (cachedRow != cachedList->second.end()) {
                return cachedRow->second;
            }
            // Not in the cache: the rel was inserted after the list was cached, so scan.
        }
    }
    std::vector<std::pair<offset_t, RowLocation>> scannedRows;
    DataChunk scanChunk(1);
    // RelID output vector.
    scanChunk.insert(0, std::make_shared<ValueVector>(LogicalType::INTERNAL_ID()));
    std::vector columnIDs = {REL_ID_COLUMN_ID, ROW_IDX_COLUMN_ID};
    auto scanState = std::make_unique<RelTableScanState>(*mm, &boundNodeIDVector,
        std::vector{&scanChunk.getValueVectorMutable(0)}, scanChunk.state, true /*randomLookup*/);
    scanState->setToTable(transaction, &table, columnIDs, {}, direction);
    scanState->initState(transaction, getNodeGroup(nodeGroupIdx));
    row_idx_t matchingRowIdx = INVALID_ROW_IDX;
    auto source = CSRNodeGroupScanSource::NONE;
    const auto scannedIDVector = scanState->outputVectors[0];
    while (true) {
        const auto scanResult = scanState->nodeGroup->scan(transaction, *scanState);
        if (scanResult == NODE_GROUP_SCAN_EMPTY_RESULT) {
            break;
        }
        const auto scanSource = scanState->nodeGroupScanState->cast<CSRNodeGroupScanState>().source;
        for (auto i = 0u; i < scanState->outState->getSelVector().getSelSize(); i++) {
            const auto pos = scanState->outState->getSelVector()[i];
            const auto rowIdxPos = scanState->rowIdxVector->state->getSelVector()[i];
            const auto rowIdx = scanState->rowIdxVector->getValue<row_idx_t>(rowIdxPos);
            const auto scannedRelOffset = scannedIDVector->getValue<internalID_t>(pos).offset;
            scannedRows.emplace_back(scannedRelOffset, RowLocation{scanSource, rowIdx});
            if (scannedRelOffset == relOffset) {
                matchingRowIdx = rowIdx;
                source = scanSource;
            }
        }
        // A short list stops at its match; a long one is scanned to its end to be cached.
        if (matchingRowIdx != INVALID_ROW_IDX &&
            scannedRows.size() < ROW_LOOKUP_CACHE_MIN_LIST_SIZE) {
            return {source, matchingRowIdx};
        }
    }
    if (scannedRows.size() >= ROW_LOOKUP_CACHE_MIN_LIST_SIZE) {
        std::unique_lock lck{rowLookupMtx};
        if (rowLookupTransactionID == transaction->getID()) {
            rowLookupCache[boundNodeOffset] =
                std::unordered_map<offset_t, RowLocation>(scannedRows.begin(), scannedRows.end());
        }
    }
    return {source, matchingRowIdx};
}

void RelTableData::findMatchingRows(Transaction* transaction, table_id_t boundTableID,
    std::vector<RelRowLookup>& lookups) const {
    std::sort(lookups.begin(), lookups.end(), [](const auto& a, const auto& b) {
        return std::tie(a.boundNodeOffset, a.relOffset) < std::tie(b.boundNodeOffset, b.relOffset);
    });
    // One scan state for the whole batch: moving to the next bound node of the same node group
    // only reads that node's CSR header entry again.
    ValueVector boundNodeIDVector(LogicalType::INTERNAL_ID());
    const auto boundNodeState = std::make_shared<DataChunkState>();
    boundNodeState->getSelVectorUnsafe().setSelSize(1);
    boundNodeIDVector.setState(boundNodeState);
    DataChunk scanChunk(1);
    scanChunk.insert(0, std::make_shared<ValueVector>(LogicalType::INTERNAL_ID()));
    std::vector columnIDs = {REL_ID_COLUMN_ID, ROW_IDX_COLUMN_ID};
    auto scanState = std::make_unique<RelTableScanState>(*mm, &boundNodeIDVector,
        std::vector{&scanChunk.getValueVectorMutable(0)}, scanChunk.state, true /*randomLookup*/);
    scanState->setToTable(transaction, &table, columnIDs, {}, direction);
    const auto scannedIDVector = scanState->outputVectors[0];
    const auto numNodeGroups = getNumNodeGroups();
    auto begin = lookups.begin();
    while (begin != lookups.end()) {
        const auto boundNodeOffset = begin->boundNodeOffset;
        auto end = begin;
        uint64_t numRelsToFind = 0;
        for (; end != lookups.end() && end->boundNodeOffset == boundNodeOffset; ++end) {
            numRelsToFind += end == begin || (end - 1)->relOffset != end->relOffset;
        }
        const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(boundNodeOffset);
        if (nodeGroupIdx >= numNodeGroups) {
            begin = end;
            continue;
        }
        boundNodeIDVector.setValue<nodeID_t>(0, nodeID_t{boundNodeOffset, boundTableID});
        scanState->initState(transaction, getNodeGroup(nodeGroupIdx));
        // Scan the list until every rel looked up in it is found; most lists end sooner.
        while (numRelsToFind > 0 &&
               scanState->nodeGroup->scan(transaction, *scanState) != NODE_GROUP_SCAN_EMPTY_RESULT) {
            const auto scanSource =
                scanState->nodeGroupScanState->cast<CSRNodeGroupScanState>().source;
            for (auto i = 0u; i < scanState->outState->getSelVector().getSelSize(); i++) {
                const auto pos = scanState->outState->getSelVector()[i];
                const auto relOffset = scannedIDVector->getValue<internalID_t>(pos).offset;
                auto it = std::lower_bound(begin, end, relOffset,
                    [](const RelRowLookup& lookup, offset_t offset) {
                        return lookup.relOffset < offset;
                    });
                if (it == end || it->relOffset != relOffset ||
                    it->source != CSRNodeGroupScanSource::NONE) {
                    continue;
                }
                const auto rowIdxPos = scanState->rowIdxVector->state->getSelVector()[i];
                const auto rowIdx = scanState->rowIdxVector->getValue<row_idx_t>(rowIdxPos);
                // The same rel can be looked up more than once (e.g. listed twice in a DELETE).
                for (; it != end && it->relOffset == relOffset; ++it) {
                    it->source = scanSource;
                    it->rowIdx = rowIdx;
                }
                numRelsToFind--;
            }
        }
        begin = end;
    }
}

bool RelTableData::checkIfNodeHasRels(Transaction* transaction,
    ValueVector* srcNodeIDVector) const {
    DASSERT(srcNodeIDVector->state->isFlat());
    const auto nodeIDPos = srcNodeIDVector->state->getSelVector()[0];
    const auto nodeOffset = srcNodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (nodeGroupIdx >= getNumNodeGroups()) {
        return false;
    }
    DataChunk scanChunk(1);
    // RelID output vector.
    scanChunk.insert(0, std::make_shared<ValueVector>(LogicalType::INTERNAL_ID()));
    std::vector columnIDs = {REL_ID_COLUMN_ID};
    auto scanState = std::make_unique<RelTableScanState>(*mm, srcNodeIDVector,
        std::vector{&scanChunk.getValueVectorMutable(0)}, scanChunk.state, true /*randomLookup*/);
    scanState->setToTable(transaction, &table, columnIDs, {}, direction);
    scanState->initState(transaction, getNodeGroup(nodeGroupIdx));
    while (true) {
        const auto scanResult = scanState->nodeGroup->scan(transaction, *scanState);
        if (scanResult == NODE_GROUP_SCAN_EMPTY_RESULT) {
            break;
        }
        if (scanState->outState->getSelVector().getSelSize() > 0) {
            return true;
        }
    }
    return false;
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
void RelTableData::pushInsertInfo(const Transaction* transaction, const CSRNodeGroup& nodeGroup,
    row_idx_t numRows_, CSRNodeGroupScanSource source) {
    // we shouldn't be appending directly to the to the persistent data
    // unless we are performing batch insert and the persistent chunked group is empty
    DASSERT(source != CSRNodeGroupScanSource::COMMITTED_PERSISTENT ||
            !nodeGroup.getPersistentChunkedGroup() ||
            nodeGroup.getPersistentChunkedGroup()->getNumRows() == 0);

    const auto [startRow, shouldIncrementNumRows] =
        (source == CSRNodeGroupScanSource::COMMITTED_PERSISTENT) ?
            std::make_pair(static_cast<row_idx_t>(0), false) :
            std::make_pair(nodeGroup.getNumRows(), true);

    nodeGroups->pushInsertInfo(transaction, nodeGroup.getNodeGroupIdx(), startRow, numRows_,
        getVersionRecordHandler(source), shouldIncrementNumRows);
}

void RelTableData::checkpoint(const std::vector<column_id_t>& columnIDs,
    PageAllocator& pageAllocator, const Transaction* snapshotTxn, transaction_t retentionHorizon,
    const std::vector<transaction_t>& openStartTSs, uint64_t numThreads) {
    std::vector<Column*> checkpointColumnPtrs;
    {
        std::unique_lock lck{columnsMtx};
        std::vector<std::unique_ptr<Column>> checkpointColumns;
        for (auto i = 0u; i < columnIDs.size(); i++) {
            const auto columnID = columnIDs[i];
            checkpointColumns.push_back(std::move(columns[columnID]));
        }
        columns = std::move(checkpointColumns);
        for (const auto& column : columns) {
            checkpointColumnPtrs.push_back(column.get());
        }
    }

    CSRNodeGroupCheckpointState state{columnIDs, std::move(checkpointColumnPtrs), pageAllocator, mm,
        csrHeaderColumns.offset.get(), csrHeaderColumns.length.get()};
    state.transaction = snapshotTxn;
    state.retentionHorizon = retentionHorizon;
    state.openStartTSs = openStartTSs;
    // A merge into populated node groups (a COPY that extends a table) rewrites each touched
    // group: run them on several threads, one group per thread.
    const auto makeState =
        [&](PageAllocator& groupAllocator) -> std::unique_ptr<NodeGroupCheckpointState> {
        auto groupState = std::make_unique<CSRNodeGroupCheckpointState>(state.columnIDs,
            state.columns, groupAllocator, mm, state.csrOffsetColumn, state.csrLengthColumn);
        groupState->transaction = state.transaction;
        groupState->retentionHorizon = state.retentionHorizon;
        groupState->openStartTSs = state.openStartTSs;
        return groupState;
    };
    nodeGroups->checkpointInParallel(*mm, state, makeState, numThreads);
}

void RelTableData::serialize(Serializer& serializer) const {
    nodeGroups->serialize(serializer);
}

void RelTableData::deserialize(Deserializer& deSerializer, MemoryManager& memoryManager) {
    nodeGroups->deserialize(deSerializer, memoryManager);
}

const VersionRecordHandler* RelTableData::getVersionRecordHandler(
    CSRNodeGroupScanSource source) const {
    if (source == CSRNodeGroupScanSource::COMMITTED_PERSISTENT) {
        return &persistentVersionRecordHandler;
    } else {
        DASSERT(source == CSRNodeGroupScanSource::COMMITTED_IN_MEMORY);
        return &inMemoryVersionRecordHandler;
    }
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
void RelTableData::rollbackGroupCollectionInsert(row_idx_t numRows_, bool isPersistent) {
    nodeGroups->rollbackInsert(numRows_, !isPersistent);
}

void RelTableData::reclaimStorage(PageAllocator& pageAllocator) const {
    nodeGroups->reclaimStorage(pageAllocator);
}

} // namespace storage
} // namespace lbug
