// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_raft_log_store.h"

#include <array>
#include <cstring>
#include <leveldb/write_batch.h>
#include <libnuraft/buffer_serializer.hxx>

auto RaftLogIdxComparator::Compare(const leveldb::Slice& a,
                                const leveldb::Slice& b) const -> int {
	assert(a.size() == b.size());
	uint64_t a_val{0};
	uint64_t b_val{0};
	std::memcpy(&a_val, a.data(), sizeof(a_val));
	std::memcpy(&b_val, b.data(), sizeof(b_val));
	if(a_val == b_val) {
		return 0;
	}

	if(a_val < b_val) {
		return -1;
	}

	return 1;
}

auto RaftLogIdxComparator::Name() const -> const char* {
    return "IndexComparator";
}

void RaftLogIdxComparator::FindShortestSeparator(
    std::string* /* start */,
    const leveldb::Slice& /* limit */) const {}

void RaftLogIdxComparator::FindShortSuccessor(std::string* /* key */) const {}

template<bool First>
auto get_first_or_last_index(leveldb::DB* db,
															const leveldb::ReadOptions& opt) -> uint64_t {
	auto it = std::unique_ptr<leveldb::Iterator>(db->NewIterator(opt));

	if constexpr(First) {
		it->SeekToFirst();
		if(!it->Valid()) {
				return 1;
		}
	} else {
		it->SeekToLast();
		if(!it->Valid()) {
				return 0;
		}
	}

	const auto last_log_key_slice = it->key();

	uint64_t ret{};
	assert(last_log_key_slice.size() == sizeof(ret));
	std::memcpy(&ret, last_log_key_slice.data(), sizeof(ret));

	return ret;
}

auto PikaRaftLogStore::load(const std::string& db_dir) -> bool {
	m_write_opt.sync = false;

	leveldb::Options opt;
	opt.create_if_missing = true;
	opt.comparator = &m_cmp;

	leveldb::DB* db_ptr{};
	const auto res = leveldb::DB::Open(opt, db_dir, &db_ptr);
	if(!res.ok()) {
			return false;
	}

	{
			std::lock_guard<std::mutex> l(m_db_mut);
			m_db.reset(db_ptr);

			m_next_idx
					= get_first_or_last_index<false>(m_db.get(), m_read_opt) + 1;
			m_start_idx
					= get_first_or_last_index<true>(m_db.get(), m_read_opt);
	}

	return true;
}

auto PikaRaftLogStore::next_slot() const -> uint64_t {
	std::lock_guard<std::mutex> l(m_db_mut);
	return m_next_idx;
}

auto PikaRaftLogStore::start_index() const -> uint64_t {
	std::lock_guard<std::mutex> l(m_db_mut);
	return m_start_idx;
}

auto log_entry_from_slice(const leveldb::Slice& slice)
	-> nuraft::ptr<nuraft::log_entry> {
	auto buf = nuraft::buffer::alloc(slice.size());
	std::memcpy(buf->data_begin(), slice.data(), buf->size());
	auto entry = nuraft::log_entry::deserialize(*buf);
	assert(entry);
	return entry;
}

auto PikaRaftLogStore::last_entry() const -> nuraft::ptr<nuraft::log_entry> {
	nuraft::ptr<nuraft::log_entry> last_entry;
	{
		std::lock_guard<std::mutex> l(m_db_mut);
		auto it = std::unique_ptr<leveldb::Iterator>(
				m_db->NewIterator(m_read_opt));

		it->SeekToLast();

		if(!it->Valid()) {
				auto null_entry
						= nuraft::cs_new<nuraft::log_entry>(0, nullptr);
				return null_entry;
		}

		const auto last_entry_slice = it->value();
		last_entry = log_entry_from_slice(last_entry_slice);
	}

	return last_entry;
}

using data_slice = std::pair<leveldb::Slice, std::vector<char>>;

auto get_key_slice(uint64_t key) -> data_slice {
	std::vector<char> key_arr(sizeof(key));
	std::memcpy(key_arr.data(), &key, key_arr.size());
	auto slice = leveldb::Slice(key_arr.data(), key_arr.size());
	return std::make_pair(slice, std::move(key_arr));
}

auto get_value_slice(nuraft::ptr<nuraft::log_entry>& entry) -> data_slice {
	const auto buf = entry->serialize();
	std::vector<char> value_buf(buf->size());
	std::memcpy(value_buf.data(), buf->data_begin(), value_buf.size());
	auto slice = leveldb::Slice(value_buf.data(), value_buf.size());
	return std::make_pair(slice, std::move(value_buf));
}

auto PikaRaftLogStore::append(nuraft::ptr<nuraft::log_entry>& entry) -> uint64_t {
	const auto value = get_value_slice(entry);

	{
		std::lock_guard<std::mutex> l(m_db_mut);
		const auto key = get_key_slice(m_next_idx);
		const auto status = m_db->Put(m_write_opt, key.first, value.first);
		assert(status.ok());

		m_next_idx++;
		return m_next_idx - 1;
	}
}

void PikaRaftLogStore::write_at(uint64_t index,
													nuraft::ptr<nuraft::log_entry>& entry) {
	const auto key = get_key_slice(index);
	const auto value = get_value_slice(entry);
	leveldb::WriteBatch batch;
	batch.Put(key.first, value.first);

	{
		std::lock_guard<std::mutex> l(m_db_mut);

		std::vector<data_slice::second_type> data_slices(m_next_idx
																											- index);
		for(uint64_t i = index + 1; i < m_next_idx; i++) {
			auto del_key = get_key_slice(i);
			batch.Delete(del_key.first);
			data_slices[i - index] = std::move(del_key.second);
		}

		const auto status = m_db->Write(m_write_opt, &batch);
		assert(status.ok());

		m_next_idx = index + 1;
	}
}

auto PikaRaftLogStore::log_entries(uint64_t start, uint64_t end)
	-> log_entries_t {
	const auto first_key = get_key_slice(start);
	auto ret = nuraft::cs_new<log_entries_t::element_type>(end - start);

	{
		std::lock_guard<std::mutex> l(m_db_mut);
		auto it = std::unique_ptr<leveldb::Iterator>(
			m_db->NewIterator(m_read_opt));

		it->Seek(first_key.first);

		for(size_t i{0}; i < ret->size(); [&]() {
					it->Next();
					i++;
			}()) {
			assert(it->Valid());
			const auto val_slice = it->value();
			auto entry = log_entry_from_slice(val_slice);
			assert(entry);
			(*ret)[i] = std::move(entry);
		}
	}

	return ret;
}

auto PikaRaftLogStore::entry_at(uint64_t index)
	-> nuraft::ptr<nuraft::log_entry> {
	const auto key = get_key_slice(index);
	std::string val;

	{
		std::lock_guard<std::mutex> l(m_db_mut);
		const auto status = m_db->Get(m_read_opt, key.first, &val);
		if(!status.ok()) {
			assert(status.IsNotFound());
			auto null_entry
				= nuraft::cs_new<nuraft::log_entry>(0, nullptr);
			return null_entry;
		}
	}

	const auto val_slice = leveldb::Slice(val.data(), val.size());
	auto ret = log_entry_from_slice(val_slice);
	return ret;
}

auto PikaRaftLogStore::term_at(uint64_t index) -> uint64_t {
	const auto entry = entry_at(index);
	return entry->get_term();
}

auto PikaRaftLogStore::pack(uint64_t index, int32_t cnt)
	-> nuraft::ptr<nuraft::buffer> {
	assert(cnt >= 0);
	const auto entries
		= log_entries(index, index + static_cast<uint64_t>(cnt));

	std::vector<nuraft::ptr<nuraft::buffer>> bufs(
		static_cast<size_t>(cnt));

	size_t i{0};
	size_t total_len{0};
	for(const auto& entry : *entries) {
		auto buf = entry->serialize();
		total_len += buf->size();
		bufs[i] = std::move(buf);
		i++;
	}

	auto ret = nuraft::buffer::alloc(
		sizeof(uint64_t) + static_cast<size_t>(cnt) * sizeof(uint64_t)
		+ total_len);
	nuraft::buffer_serializer bs(ret);

	bs.put_u64(static_cast<uint64_t>(cnt));

	for(const auto& buf : bufs) {
		bs.put_u64(buf->size());
		bs.put_raw(buf->data_begin(), buf->size());
	}

	return ret;
}

void PikaRaftLogStore::apply_pack(uint64_t index, nuraft::buffer& pack) {
	nuraft::buffer_serializer bs(pack);

	const auto cnt = bs.get_u64();

	std::vector<nuraft::ptr<nuraft::log_entry>> entries(cnt);

	for(size_t i{0}; i < cnt; i++) {
		const auto len = bs.get_u64();
		auto buf = nuraft::buffer::alloc(len);
		bs.get_buffer(buf);
		auto entry = nuraft::log_entry::deserialize(*buf);
		assert(entry);
		entries[i] = std::move(entry);
	}

	std::vector<data_slice::second_type> data_slices(entries.size() * 2);
	leveldb::WriteBatch batch;
	for(size_t i{0}; i < entries.size(); i++) {
		auto key = get_key_slice(index + i);
		auto val = get_value_slice(entries[i]);
		batch.Put(key.first, val.first);

		// Store the backing data or it will get deleted when we go out of
		// scope
		data_slices[i * 2] = std::move(key.second);
		data_slices[(i * 2) + 1] = std::move(val.second);
	}

	{
		std::lock_guard<std::mutex> l(m_db_mut);
		const auto status = m_db->Write(m_write_opt, &batch);
		assert(status.ok());

		m_start_idx
			= get_first_or_last_index<true>(m_db.get(), m_read_opt);
		m_next_idx
			= get_first_or_last_index<false>(m_db.get(), m_read_opt) + 1;
	}
}

auto PikaRaftLogStore::compact(uint64_t last_log_index) -> bool {
	leveldb::WriteBatch batch;

	{
		std::lock_guard<std::mutex> l(m_db_mut);

		const auto n_elems = last_log_index - m_start_idx + 1;
		std::vector<data_slice::second_type> data_slices(n_elems);
		for(uint64_t i{m_start_idx}; i <= last_log_index; i++) {
				auto key = get_key_slice(i);
				batch.Delete(key.first);
				data_slices[i - m_start_idx] = std::move(key.second);
		}

		const auto status = m_db->Write(m_write_opt, &batch);
		assert(status.ok());

		m_start_idx = last_log_index + 1;
		m_next_idx = std::max(m_next_idx, m_start_idx);
	}

	return true;
}

auto PikaRaftLogStore::flush() -> bool {
	// LevelDB does not provide a way to issue a single "flush" call. As a
	// workaround make a dummy write with no lasting effects where sync =
	// true.

	leveldb::WriteOptions sync_opts;
	sync_opts.sync = true;

	leveldb::WriteBatch batch;

	// Log entry 0 is always empty so we're not overwriting anything
	// important
	std::array<char, sizeof(uint64_t)> dummy_key_data{};
	leveldb::Slice dummy_key_slice(dummy_key_data.data(),
																	dummy_key_data.size());
	batch.Put(dummy_key_slice, dummy_key_slice);
	batch.Delete(dummy_key_slice);

	{
		std::lock_guard<std::mutex> l(m_db_mut);
		const auto status = m_db->Write(sync_opts, &batch);
		return status.ok();
	}
}
