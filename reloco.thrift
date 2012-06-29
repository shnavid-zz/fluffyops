const i32 PROCESS_MIN_AGE_SECONDS = 5
const bool AUTOCREATE_TOKENS = true

exception NotAuthorisedException {
    1: string errorMessage,
}

exception AuthTimeoutException {
    1: string errorMessage,
}

struct ProcCpuStats {
  1: double usr = 0,
  2: double sys = 0,
}

struct ProcMemoryStats {
  1: i64 vms = 0,
  2: i64 rss = 0,
}

struct ProcGroupStats {
  1: required string pg_id,
  2: i32 processes = 1,
  3: ProcCpuStats cpu,
  4: ProcMemoryStats mem,
}

service ProcStatsService {
  void ping();

  string authorize( 1:string token )
     throws ( 1:NotAuthorisedException e ),

  string get_process_group(1: required string token, 2: required string username, 3: required string name);

  oneway void store_bulk(1: required string token, 2: required list<ProcGroupStats> pgs_list);
}
