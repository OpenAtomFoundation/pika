package metrics

import (
	"regexp"
)

func RegisterRocksDB() {
	Register(collectRocksDBMetrics)
}

var collectRocksDBMetrics = map[string]MetricConfig{
	// memtables num
	"num_immutable_mem_table": {
		Parser: &regexParser{
			name:   "num_immutable_mem_table",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?num_immutable_mem_table:(?P<num_immutable_mem_table>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "num_immutable_mem_table",
			Help:      "Number of immutable memtables not yet flushed.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "num_immutable_mem_table",
		},
	},
	"num_immutable_mem_table_flushed": {
		Parser: &regexParser{
			name:   "num_immutable_mem_table_flushed",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?num_immutable_mem_table_flushed:(?P<num_immutable_mem_table_flushed>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "num_immutable_mem_table_flushed",
			Help:      "Number of immutable memtables that have been flushed.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "num_immutable_mem_table_flushed",
		},
	},
	"mem_table_flush_pending": {
		Parser: &regexParser{
			name:   "mem_table_flush_pending",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?mem_table_flush_pending:(?P<mem_table_flush_pending>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "mem_table_flush_pending",
			Help:      "Returns 1 if there is a pending memtable flush; otherwise returns 0.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "mem_table_flush_pending",
		},
	},
	"num_running_flushes": {
		Parser: &regexParser{
			name:   "num_running_flushes",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?num_running_flushes:(?P<num_running_flushes>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "num_running_flushes",
			Help:      "Number of currently running flush operations.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "num_running_flushes",
		},
	},

	// compaction
	"compaction_pending": {
		Parser: &regexParser{
			name:   "compaction_pending",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?compaction_pending:(?P<compaction_pending>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "compaction_pending",
			Help:      "Returns 1 if at least one compaction operation is pending; otherwise returns 0.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "compaction_pending",
		},
	},
	"num_running_compactions": {
		Parser: &regexParser{
			name:   "num_running_compactions",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?num_running_compactions:(?P<num_running_compactions>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "num_running_compactions",
			Help:      "Number of running compactions.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "num_running_compactions",
		},
	},

	// background errors
	"background_errors": {
		Parser: &regexParser{
			name:   "background_errors",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?background_errors:(?P<background_errors>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "background_errors",
			Help:      "Total number of background errors.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "background_errors",
		},
	},

	// memtables size
	"cur_size_active_mem_table": {
		Parser: &regexParser{
			name:   "cur_size_active_mem_table",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?cur_size_active_mem_table:(?P<cur_size_active_mem_table>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "cur_size_active_mem_table",
			Help:      "Approximate size, in bytes, of the active memtable.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "cur_size_active_mem_table",
		},
	},
	"cur_size_all_mem_tables": {
		Parser: &regexParser{
			name:   "cur_size_all_mem_tables",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?cur_size_all_mem_tables:(?P<cur_size_all_mem_tables>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "cur_size_all_mem_tables",
			Help:      "Total size in bytes of memtables not yet flushed, including the current active memtable and the unflushed immutable memtables.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "cur_size_all_mem_tables",
		},
	},
	"size_all_mem_tables": {
		Parser: &regexParser{
			name: "size_all_mem_tables",
			// TODO: need fix size_all_mem_tables contains wrong data type starting with cur
			// issue: https://github.com/OpenAtomFoundation/pika/issues/1752
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?size_all_mem_tables:(?P<size_all_mem_tables>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "size_all_mem_tables",
			Help:      "Total size in bytes of all memtables, including the active memtable, unflushed immutable memtables, and pinned immutable memtables.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "size_all_mem_tables",
		},
	},

	// keys
	"estimate_num_keys": {
		Parser: &regexParser{
			name:   "estimate_num_keys",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?estimate_num_keys:(?P<estimate_num_keys>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "estimate_num_keys",
			Help:      "Estimated number of keys in active memtable, unflushed immutable memtables, and flushed SST files.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "estimate_num_keys",
		},
	},

	// table readers mem
	"estimate_table_readers_mem": {
		Parser: &regexParser{
			name:   "estimate_table_readers_mem",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?estimate_table_readers_mem:(?P<estimate_table_readers_mem>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "estimate_table_readers_mem",
			Help:      "Estimated memory size used for reading SST files, excluding block cache (such as filter and index blocks).",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "estimate_table_readers_mem",
		},
	},

	// pending compaction bytes
	"estimate_pending_compaction_bytes": {
		Parser: &regexParser{
			name:   "estimate_pending_compaction_bytes",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?estimate_pending_compaction_bytes:(?P<estimate_pending_compaction_bytes>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "estimate_pending_compaction_bytes",
			Help:      "Estimated total number of bytes that compression needs to rewrite to bring all levels down below the target size. Has no effect on compression other than level-based compression.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "estimate_pending_compaction_bytes",
		},
	},

	// snapshots
	"num_snapshots": {
		Parser: &regexParser{
			name:   "num_snapshots",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?num_snapshots:(?P<num_snapshots>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "num_snapshots",
			Help:      "Number of unreleased snapshots in the database.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "num_snapshots",
		},
	},

	// version
	"num_live_versions": {
		Parser: &regexParser{
			name:   "num_live_versions",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?num_live_versions:(?P<num_live_versions>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "num_live_versions",
			Help:      "Number of current versions. More current versions usually indicate more SST files being used by iterators or incomplete compactions.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "num_live_versions",
		},
	},
	"current_super_version_number": {
		Parser: &regexParser{
			name:   "current_super_version_number",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?current_super_version_number:(?P<current_super_version_number>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "current_super_version_number",
			Help:      "Current number of the LSM version. It is a uint64_t integer that increments after any changes in the LSM tree. This number is not preserved after restarting the database and starts from 0 after a database restart.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "current_super_version_number",
		},
	},

	// live data size
	"estimate_live_data_size": {
		Parser: &regexParser{
			name:   "estimate_live_data_size",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?estimate_live_data_size:(?P<estimate_live_data_size>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "estimate_live_data_size",
			Help:      "Estimated size of the activity data in bytes. For BlobDB, it also includes the actual live bytes in the version's blob file.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "estimate_live_data_size",
		},
	},

	// sst files
	"total_sst_files_size": {
		Parser: &regexParser{
			name:   "total_sst_files_size",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?total_sst_files_size:(?P<total_sst_files_size>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "total_sst_files_size",
			Help:      "Total size (in bytes) of all SST files. Note: If there are too many files, it may slow down the online query.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "total_sst_files_size",
		},
	},
	"live_sst_files_size": {
		Parser: &regexParser{
			name:   "live_sst_files_size",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?live_sst_files_size:(?P<live_sst_files_size>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "live_sst_files_size",
			Help:      "Total size in bytes of all SST files belonging to the latest LSM tree.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "live_sst_files_size",
		},
	},

	// block cache
	"block_cache_capacity": {
		Parser: &regexParser{
			name:   "block_cache_capacity",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?block_cache_capacity:(?P<block_cache_capacity>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "block_cache_capacity",
			Help:      "The capacity of the block cache.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "block_cache_capacity",
		},
	},
	"block_cache_usage": {
		Parser: &regexParser{
			name:   "block_cache_usage",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?block_cache_usage:(?P<block_cache_usage>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "block_cache_usage",
			Help:      "Memory size occupied by entries in the block cache.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "block_cache_usage",
		},
	},
	"block_cache_pinned_usage": {
		Parser: &regexParser{
			name:   "block_cache_pinned_usage",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?block_cache_pinned_usage:(?P<block_cache_pinned_usage>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "block_cache_pinned_usage",
			Help:      "Memory size occupied by pinned entries in the block cache.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "block_cache_pinned_usage",
		},
	},

	// blob files
	"num_blob_files": {
		Parser: &regexParser{
			name:   "num_blob_files",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?num_blob_files:(?P<num_blob_files>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "num_blob_files",
			Help:      "The number of blob files in the current version.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "num_blob_files",
		},
	},
	"blob_stats": {
		Parser: &regexParser{
			name:   "blob_stats",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?blob_stats:(?P<blob_stats>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "blob_stats",
			Help:      "The total and size of all blob files, and the total amount of garbage (in bytes) in blob files in the current version.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "blob_stats",
		},
	},
	"total_blob_file_size": {
		Parser: &regexParser{
			name:   "total_blob_file_size",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?total_blob_file_size:(?P<total_blob_file_size>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "total_blob_file_size",
			Help:      "The total size of all blob files across all versions.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "total_blob_file_size",
		},
	},
	"live_blob_file_size": {
		Parser: &regexParser{
			name:   "live_blob_file_size",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?live_blob_file_size:(?P<live_blob_file_size>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "live_blob_file_size",
			Help:      "The total size of all blob files in the current version.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "live_blob_file_size",
		},
	},
	// column family stats
	"cf_l0_file_count_limit_delays_with_ongoing_compaction": {
		Parser: &regexParser{
			name:   "cf_l0_file_count_limit_delays_with_ongoing_compaction",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?cf-l0-file-count-limit-delays-with-ongoing-compaction: (?P<cf_l0_file_count_limit_delays_with_ongoing_compaction>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "cf_l0_file_count_limit_delays_with_ongoing_compaction",
			Help:      "Write slowdown caused by l0 file count limit while there is ongoing L0 compaction.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "cf_l0_file_count_limit_delays_with_ongoing_compaction",
		},
	},
	"cf_l0_file_count_limit_stops_with_ongoing_compaction": {
		Parser: &regexParser{
			name:   "cf_l0_file_count_limit_stops_with_ongoing_compaction",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?cf-l0-file-count-limit-stops-with-ongoing-compaction: (?P<cf_l0_file_count_limit_stops_with_ongoing_compaction>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "cf_l0_file_count_limit_stops_with_ongoing_compaction",
			Help:      "Write stop caused by l0 file count limit while there is ongoing L0 compaction.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "cf_l0_file_count_limit_stops_with_ongoing_compaction",
		},
	},
	"compaction": {
		Parser: &regexParser{
			name:   "compaction",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_compaction\.L(?P<compaction_level>\d+)\.(?P<info_type>\w+): (?P<compaction>[\.\d]+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "compaction",
			Help:      "\r#The all metrics of compaction_L<N>:\r#compaction.L<N>.AvgSec	Average time spent per Compact.\r#compaction.L<N>.CompCount	The number of times Compact has been accumulated.\r#compaction.L<N>.CompMergeCPU	CPU time used in compression, in seconds.\r#compaction.L<N>.CompSec	Compact cumulative time, in seconds.\r#compaction.L<N>.CompactedFiles	Number of files that have completed compact.\r#compaction.L<N>.KeyDrop	Number of keys deleted in compact.\r#compaction.L<N>.KeyIn	Number of records compared during the compaction process.\r#compaction.L<N>.MovedGB	During the compaction process, the number of bytes moved to level n+1.\r#compaction.L<N>.NumFiles	Total number of sst files.\r#compaction.L<N>.RblobGB	The size of data read from the blob file by the compaction, in GB.\r#compaction.L<N>.ReadGB	Read size in GB.\r#compaction.L<N>.ReadMBps	Read rate in MBps.\r#compaction.L<N>.RnGB	When performing compact, read the size of the current layer file in GB.\r#compaction.L<N>.Rnp1GB	When performing compact, read the size of the next level file in GB.\r#compaction.L<N>.Score	Score, the higher the score, the higher the priority.\r#compaction.L<N>.SizeBytes	Total size of SST in bytes.\r#compaction.L<N>.WblobGB	The size of the blob file written during compaction, in GB.\r#compaction.L<N>.WnewGB	WNP1- Rnp1.\r#compaction.L<N>.WriteAmp	Total bytes written to level<N+1>/(total bytes read from level<N>).\r#compaction.L<N>.WriteGB	The size of the table file written during the compaction period, in GB.\r#compaction.L<N>.WriteMBps	Data write rate in compaction.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type", "info_type", "compaction_level"},
			ValueName: "compaction",
		},
	},
	"compaction_Sum": {
		Parser: &regexParser{
			name:   "compaction_Sum",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?compaction\.Sum\.(?P<info_type>\w+): (?P<compaction_Sum>[\.\d]+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "compaction_Sum",
			Help:      "The all metrics of compaction_Sum.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type", "info_type"},
			ValueName: "compaction_Sum",
		},
	},
	"l0_file_count_limit_delays": {
		Parser: &regexParser{
			name:   "l0_file_count_limit_delays",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?l0-file-count-limit-delays: (?P<l0_file_count_limit_delays>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "l0_file_count_limit_delays",
			Help:      "Stalling writes because we have immutable memtables (waiting for flush).",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "l0_file_count_limit_delays",
		},
	},
	"l0_file_count_limit_stops": {
		Parser: &regexParser{
			name:   "l0_file_count_limit_stops",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?l0-file-count-limit-stops: (?P<l0_file_count_limit_stops>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "l0_file_count_limit_stops",
			Help:      "Stopping writes because we have level-0 files.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "l0_file_count_limit_stops",
		},
	},
	"memtable_limit_delays": {
		Parser: &regexParser{
			name:   "memtable_limit_delays",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?memtable-limit-delays: (?P<memtable_limit_delays>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "memtable_limit_delays",
			Help:      "Stalling writes because we have immutable memtables.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "memtable_limit_delays",
		},
	},
	"memtable_limit_stops": {
		Parser: &regexParser{
			name:   "memtable_limit_stops",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?memtable-limit-stops: (?P<memtable_limit_stops>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "memtable_limit_stops",
			Help:      "Stopping writes because we have immutable memtables.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "memtable_limit_stops",
		},
	},
	"pending_compaction_bytes_delays": {
		Parser: &regexParser{
			name:   "pending_compaction_bytes_delays",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?pending-compaction-bytes-delays: (?P<pending_compaction_bytes_delays>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "pending_compaction_bytes_delays",
			Help:      "Stopping writes because of estimated pending compaction.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "pending_compaction_bytes_delays",
		},
	},
	"pending_compaction_bytes_stops": {
		Parser: &regexParser{
			name:   "pending_compaction_bytes_stops",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?pending-compaction-bytes-stops: (?P<pending_compaction_bytes_stops>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "pending_compaction_bytes_stops",
			Help:      "Stalling writes because of estimated pending compaction.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "pending_compaction_bytes_stops",
		},
	},
	"total_delays": {
		Parser: &regexParser{
			name:   "total_delays",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?total-delays: (?P<total_delays>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "total_delays",
			Help:      "Total delays count.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "total_delays",
		},
	},
	"total_stops": {
		Parser: &regexParser{
			name:   "total_stops",
			reg:    regexp.MustCompile(`(?P<data_type>\w+)_.*?total-stops: (?P<total_stops>\d+)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "total_stops",
			Help:      "Total stops count.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "data_type"},
			ValueName: "total_stops",
		},
	},
}
