// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RAFT_LOG_STORE_
#define PIKA_RAFT_LOG_STORE_

#include <leveldb/comparator.h>
#include <leveldb/db.h>
#include <libnuraft/log_store.hxx>
#include <mutex>

/// LevelDB comparator for ordering NuRaft log indices.
class RaftLogIdxComparator : public leveldb::Comparator {
 public:
	/// Compare the order of the two given LevelDB keys when converted to
	/// uint64_t.
	/// \param a first key.
	/// \param b second key.
	/// \return 0 if a == b. -1 if a < b. 1 if a > b.
	[[nodiscard]] auto Compare(const leveldb::Slice& a,
															const leveldb::Slice& b) const
			-> int override;

	/// Return the comparator name.
	/// \return "IndexComparator".
	[[nodiscard]] auto Name() const -> const char* override;

	/// Not implemented.
	void FindShortestSeparator(
			std::string* /* start */,
			const leveldb::Slice& /* limit */) const override;

	/// Not implemented.
	void FindShortSuccessor(std::string* /* key */) const override;
};

/// NuRaft log_store implementation using LevelDB.
class PikaRaftLogStore : public nuraft::log_store {
 public:
	PikaRaftLogStore() = default;
	~PikaRaftLogStore() override = default;

	PikaRaftLogStore(const PikaRaftLogStore& other) = delete;
	auto operator=(const PikaRaftLogStore& other) -> PikaRaftLogStore& = delete;

	PikaRaftLogStore(PikaRaftLogStore&& other) noexcept;
	auto operator=(PikaRaftLogStore&& other) noexcept -> PikaRaftLogStore&;

	/// Load the log store from the given LevelDB database directory.
	/// \param db_dir database directory.
	/// \return true if loading the database succeeded.
	[[nodiscard]] auto load(const std::string& db_dir) -> bool;

	/// Return the log index of the next empty log entry.
	/// \return log index.
	[[nodiscard]] auto next_slot() const -> uint64_t override;

	/// Return the first log index stored by the log store.
	/// \return log index.
	[[nodiscard]] auto start_index() const -> uint64_t override;

	/// Return the last log entry in the log store. Returns an empty log
	/// entry at index zero if the log store is empty.
	/// \return log entry.
	[[nodiscard]] auto last_entry() const
			-> nuraft::ptr<nuraft::log_entry> override;

	/// Append the given log entry to the end of the log.
	/// \param entry log entry to append.
	/// \return index of the appended log entry.
	auto append(nuraft::ptr<nuraft::log_entry>& entry)
			-> uint64_t override;

	/// Write a log entry at the given index.
	/// \param index log index at which to write the entry.
	/// \param entry log entry to write.
	void write_at(uint64_t index,
									nuraft::ptr<nuraft::log_entry>& entry) override;

	/// List of log entries.
	using log_entries_t
			= nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>;

	/// Return the log entries in the given range of indices.
	/// \param start first log entry to retrieve.
	/// \param end last log entry to retrieve (exclusive).
	/// \return list of log entries.
	[[nodiscard]] auto log_entries(uint64_t start, uint64_t end)
			-> log_entries_t override;

	/// Return the log entry at the given index. Returns a null log entry
	/// if there is no log entry at the given index.
	/// \param index log index.
	/// \return log entry.
	[[nodiscard]] auto entry_at(uint64_t index)
			-> nuraft::ptr<nuraft::log_entry> override;

	/// Return the log term associated with the log entry at the given
	/// index.
	/// \param index log index.
	/// \return log term.
	[[nodiscard]] auto term_at(uint64_t index) -> uint64_t override;

	/// Serialize the given number of log entries from the given index.
	/// \param index starting log index.
	/// \param cnt number of log entries to serialize. Must be positive.
	/// \return buffer containing serialized log entries.
	[[nodiscard]] auto pack(uint64_t index, int32_t cnt)
			-> nuraft::ptr<nuraft::buffer> override;

	/// Deserialize the given log entries and write them starting at the
	/// given log index.
	/// \param index log index at which to write the first log entry.
	/// \param pack serialized log entries.
	void apply_pack(uint64_t index, nuraft::buffer& pack) override;

	/// Delete log entries from the start of the log up to the given log
	/// index.
	/// \param last_log_index last log index to delete (inclusive).
	/// \return true.
	auto compact(uint64_t last_log_index) -> bool override;

	/// Flush any buffered writes to disk.
	/// \return true if the flush was successful.
	auto flush() -> bool override;

	private:
	std::unique_ptr<leveldb::DB> m_db{};
	mutable std::mutex m_db_mut{};
	uint64_t m_next_idx{};
	uint64_t m_start_idx{};

	leveldb::ReadOptions m_read_opt;
	leveldb::WriteOptions m_write_opt;

	RaftLogIdxComparator m_cmp;
};

#endif
