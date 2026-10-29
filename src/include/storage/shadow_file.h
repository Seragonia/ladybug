#pragma once

#include <atomic>
#include <functional>
#include <optional>
#include <shared_mutex>

#include "common/types/uuid.h"
#include "storage/file_handle.h"

namespace lbug {
namespace storage {

class BufferManager;
class StorageManager;

struct ShadowPageRecord {
    common::file_idx_t originalFileIdx = common::INVALID_PAGE_IDX;
    common::page_idx_t originalPageIdx = common::INVALID_PAGE_IDX;

    void serialize(common::Serializer& serializer) const;
    static ShadowPageRecord deserialize(common::Deserializer& deserializer);
};

struct ShadowFileHeader {
    common::uuid databaseID{0};
    common::page_idx_t numShadowPages = 0;
    // The database that owns the pending checkpoint bundle this shadow file belongs to. A shadow
    // flushed while another database runs the checkpoint (a graph's or partition child's shadow
    // during a parent checkpoint) carries that parent's ID, so a standalone open of the child file
    // can refuse to decide the bundle's fate instead of discarding its shadow. Zero means the
    // header predates bundle ownership stamping and is treated as the file's own bundle.
    common::uuid ownerDatabaseID{0};
};
static_assert(std::is_trivially_copyable_v<ShadowFileHeader>);

class BufferManager;
// NOTE: Only the checkpointing thread may create, publish or clear shadow pages. Read
// transactions running concurrently with a checkpoint may only use readShadowVersionIfExists.
class ShadowFile {
public:
    static constexpr common::uuid CHECKPOINT_BUNDLE_DATABASE_ID{
        common::int128_t{UINT64_MAX, INT64_MAX}};

    ShadowFile(BufferManager& bm, common::VirtualFileSystem* vfs, const std::string& databasePath);

    // Mutex protocol: hasShadowPage, getShadowPage, clearShadowPage, createShadowPage and
    // publishShadowPage are for checkpointing threads only. A checkpoint may rewrite several node
    // groups (and the columns of one) on several threads, each creating shadow pages for pages
    // no other thread writes, so these take mtx: shared to read the map, exclusively to modify it.
    // Read transactions running concurrently with a checkpoint must not call these and must use
    // readShadowVersionIfExists, which takes mtx shared.
    // TODO(Guodong): Remove originalFile param.
    bool hasShadowPage(common::file_idx_t originalFile, common::page_idx_t originalPage) const;
    void clearShadowPage(common::file_idx_t originalFile, common::page_idx_t originalPage);
    common::page_idx_t getShadowPage(common::file_idx_t originalFile,
        common::page_idx_t originalPage) const;
    // The shadow page of originalPage, or INVALID_PAGE_IDX if it has none.
    common::page_idx_t findShadowPage(common::file_idx_t originalFile,
        common::page_idx_t originalPage) const;
    // Adds a new shadow page for originalPage. The page is not visible to
    // readShadowVersionIfExists (nor hasShadowPage) until publishShadowPage is called, so the
    // caller can fill it with the contents of the original page first. Must not be called while
    // a published shadow page of a column data page is pinned (see mtx).
    common::page_idx_t createShadowPage(common::file_idx_t originalFile,
        common::page_idx_t originalPage);
    void publishShadowPage(common::file_idx_t originalFile, common::page_idx_t originalPage,
        common::page_idx_t shadowPageIdx);
    // A savepoint names the shadow pages created under it: those created by a thread while it
    // holds a SavepointScope of it. Rolling back to it drops exactly those pages, without
    // disturbing shadow pages created by already-checkpointed tables or by node groups
    // checkpointed concurrently on other threads. Used to undo a partially completed node group
    // checkpoint (see LadybugDB/ladybug#1051).
    struct ShadowSavepoint {
        uint64_t id = 0; // 0: no savepoint
    };
    ShadowSavepoint createSavepoint();
    // Attributes the shadow pages the current thread creates to a savepoint while in scope.
    class SavepointScope {
    public:
        explicit SavepointScope(ShadowSavepoint savepoint);
        ~SavepointScope();
        SavepointScope(const SavepointScope&) = delete;
        SavepointScope& operator=(const SavepointScope&) = delete;
        // The savepoint the current thread creates shadow pages under.
        static ShadowSavepoint current();

    private:
        uint64_t previous;
    };
    // Drops the shadow pages created under the savepoint: their map entries, so readers stop
    // seeing them at once, and their frames (evicted without flushing, so their contents never
    // reach the data file). Their records are removed when they are last in the record list;
    // records with others' records after them are left as holes until compactShadowPages.
    // Must not be called while the savepoint's own pages are pinned, nor while a published
    // shadow page of a column data page is pinned by the calling thread (see mtx).
    void rollbackToSavepoint(ShadowSavepoint savepoint);
    // Closes the holes left by rollbackToSavepoint: moves the shadow pages after a hole into it,
    // preserving the invariant that shadowPageRecords[i] describes shadow page i+1 (see
    // flushAll). Only when no other thread creates or writes shadow pages, e.g. once the threads
    // checkpointing node groups in parallel have joined.
    void compactShadowPages();
    // Reads the shadow version of a page if the current checkpoint has one, and returns false
    // without calling readOp otherwise. Unlike the functions above, this is safe to call from
    // read transactions running concurrently with a checkpoint: a checkpoint publishes the new
    // metadata of in-place updated column chunks before the shadow pages are applied to the data
    // file, so those readers must see the shadow version of such pages.
    bool readShadowVersionIfExists(common::file_idx_t originalFile, common::page_idx_t originalPage,
        const std::function<void(uint8_t*)>& readOp) const;

    FileHandle& getShadowingFH() const { return *shadowingFH; }

    void applyShadowPages(StorageManager& storageManager, main::ClientContext& context) const;

    // `ownerDatabaseID` is the ID of the database running the checkpoint, stamped into the shadow
    // header (see ShadowFileHeader::ownerDatabaseID).
    void flushAll(const common::uuid& ownerDatabaseID) const;
    // Clear any buffer in the WAL writer. Also truncate the WAL file to 0 bytes.
    void clear(BufferManager& bm, bool syncParentDirectory = true);
    bool hasShadowingFH() const { return shadowingFH != nullptr; }
    void setDatabasePath(const std::string& databasePath);
    // Reset the WAL writer to nullptr, and remove the WAL file if it exists.
    void reset();

    // Replay shadow page records from the shadow file to the original data file. This is used
    // during recovery.
    static void replayShadowPageRecords(main::ClientContext& context);
    // Same as above, but for an arbitrary database file (e.g. a partition child's data file)
    // rather than the main database path. `databasePath` is the DATA file path; the shadow
    // path is derived from it.
    static void replayShadowPageRecords(main::ClientContext& context,
        const std::string& databasePath,
        std::optional<common::uuid> legacyDatabaseID = std::nullopt);
    // Variant for files whose handle is already open and locked by the given storage manager
    // (partition children during recovery): avoids taking a second lock on the data file.
    static void replayShadowPageRecordsForStorageManager(main::ClientContext& context,
        StorageManager& storageManager,
        std::optional<common::uuid> legacyDatabaseID = std::nullopt);

private:
    FileHandle* getOrCreateShadowingFH();
    void truncateNoLock(size_t numRecords);

    static void replayShadowPageRecordsCore(common::FileInfo& shadowFileInfo,
        common::FileInfo& dataFileInfo, uint64_t maxDatabasePages,
        std::optional<common::uuid> legacyDatabaseID = std::nullopt);

private:
    BufferManager& bm;
    std::string shadowFilePath;
    common::VirtualFileSystem* vfs;
    // This is the file handle for the shadow file. It is created lazily when the first shadow page
    // is created.
    FileHandle* shadowingFH;
    // The map caches shadow page idxes for pages in original files.
    std::unordered_map<common::file_idx_t,
        std::unordered_map<common::page_idx_t, common::page_idx_t>>
        shadowPagesMap;
    std::vector<ShadowPageRecord> shadowPageRecords;
    // The savepoint each shadow page was created under, parallel to shadowPageRecords; ROLLED_BACK
    // for the holes rollbackToSavepoint leaves.
    static constexpr uint64_t ROLLED_BACK = UINT64_MAX;
    std::vector<uint64_t> shadowPageSavepoints;
    std::atomic<uint64_t> nextSavepointID{1};
    // Protects shadowPagesMap, shadowPageRecords and shadowPageSavepoints against concurrent
    // readers (see readShadowVersionIfExists) and checkpointing threads.
    // Readers hold mtx shared while spinning in optimisticRead on a locked shadow page of a page
    // they reach through ColumnReadWriter::readFromPage (column data pages). A checkpointing
    // thread therefore must not hold such a published shadow page pinned while it takes mtx
    // exclusively (create, publish, clear or reset), or it would deadlock with those readers.
    // Another checkpointing thread may: it releases the page without taking mtx.
    mutable std::shared_mutex mtx;
    // Fast path for readers: false when there is no shadow page.
    std::atomic<bool> hasShadowPages{false};
};

} // namespace storage
} // namespace lbug
