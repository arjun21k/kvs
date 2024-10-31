# kvs

The following table indicates the host-side and/or dpu-side code for different KVS designs.

|       | Host | DPU |
| ----------- | ----------- | ---------- |
| CPU-only    | main-x86    |
| DPU-only    |             | main-aarch64 |
| DPU-KV-lat  | main-x86-lat | main-aarch-lat |
| DPU-KV-sav  | main-x86-rs | main-aarch-rs |
| DPU-KV-shrd | main-x86    | main-aarch64|
| DPU-KV-dual | main-x86-dual | main-aarch64-dual|

Our code builds on top of MICA2 KVS (https://github.com/efficient/mica2). The major code modifications for each design can be found under src/mica/datagram/datagram_server_impl.h file and the KVS configuration can be found (and modified) under src/mica/test/server.json file (e.g., number of cores). The instructions to compile and build can be found respective branch's README file. The KVS client also uses "main-x86" branch and its configuration can be found (and changed) under src/mica/test/netbench.json file (e.g., GET:PUT ratio, number of cores). The KVS server reports the throughput and the KVS client reports the average latency.

The code uses DPDK version 21.08 which is publicly available at https://github.com/DPDK/dpdk. 
