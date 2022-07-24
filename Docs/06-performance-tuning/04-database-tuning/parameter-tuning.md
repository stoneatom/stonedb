---
id: parameter-tuning
sidebar_position: 7.43
---

# Database parameter tuning

## stonedb_insert_buffer_size
- Description: This parameter specifies the insert buffer size, expressed in MB.
- Default value: 512
- Value range: 512 to 10000
- Recommended value: If operations of inserting bulk data exist, we recommend that you set the parameter to 2048.
## stonedb_ini_servermainheapsize

- Description: This parameter specifies the size of heap memory on the server, expressed in MB.
- Default value: 0, which indicates half the size of the physical memory.
- Value range: 0 to 1000000
- Recommended value: 0
## stonedb_distinct_cache_size

- Description: This parameter specifies the amount of the Group Distinct Cache, expressed in MB.
- Default value: 64
- Value range: 64 to 256
- Recommended value: 128
## stonedb_bg_load_threads

- Description: This parameter specifies the number of worker threads that load data from the insert buffer to the background thread pool.
- Default value: 0
- Value range: 0 to 100
- Recommended value: We recommend that you set the parameter to half the number of CPU cores.
## stonedb_load_threads

- Description: This parameter specifies the number of worker threads in the StoneDB Load thread pool.
- Default value: 0
- Value range: 0 to 100
- Recommended value: We recommend that you set the parameter to the number of the CPU cores.
## stonedb_query_threads

- Description: This parameter specifies the number of worker threads in the StoneDB query thread pool.
- Default value: 0
- Value range: 0 to 100
- Recommended value: We recommend that you set the parameter to the number of the CPU cores.
