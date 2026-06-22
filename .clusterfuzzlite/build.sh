#!/bin/bash -eu
# Register every native Go fuzz target here so ClusterFuzzLite builds and runs it
# on each PR. When you add a new FuzzXxx target, append a compile line below with
# its package import path and a unique binary name.

# Root package (github.com/rbaliyan/ledger)
compile_native_go_fuzzer github.com/rbaliyan/ledger FuzzValidateName fuzz_validate_name
compile_native_go_fuzzer github.com/rbaliyan/ledger FuzzJSONCodecRoundTrip fuzz_json_codec_round_trip
compile_native_go_fuzzer github.com/rbaliyan/ledger FuzzZstdUnmarshal fuzz_zstd_unmarshal
compile_native_go_fuzzer github.com/rbaliyan/ledger FuzzZstdRoundTrip fuzz_zstd_round_trip
compile_native_go_fuzzer github.com/rbaliyan/ledger FuzzFieldMapperUpcast fuzz_field_mapper_upcast
compile_native_go_fuzzer github.com/rbaliyan/ledger FuzzUpcastChain fuzz_upcast_chain

# MongoDB package (github.com/rbaliyan/ledger/mongodb)
compile_native_go_fuzzer github.com/rbaliyan/ledger/mongodb FuzzDecodeCursor fuzz_decode_cursor

# Bridge package (github.com/rbaliyan/ledger/bridge)
compile_native_go_fuzzer github.com/rbaliyan/ledger/bridge FuzzMutationEventDecode fuzz_mutation_event_decode
compile_native_go_fuzzer github.com/rbaliyan/ledger/bridge FuzzInt64CodecRoundTrip fuzz_int64_codec_round_trip

# gRPC adapter package (github.com/rbaliyan/ledger/ledgerpb)
compile_native_go_fuzzer github.com/rbaliyan/ledger/ledgerpb FuzzParseIntID fuzz_parse_int_id
