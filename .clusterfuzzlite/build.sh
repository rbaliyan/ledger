#!/bin/bash -eu
compile_native_go_fuzzer github.com/rbaliyan/ledger FuzzValidateName fuzz_validate_name
compile_native_go_fuzzer github.com/rbaliyan/ledger FuzzJSONCodecRoundTrip fuzz_json_codec_round_trip
