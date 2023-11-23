This folder is used for IMCS for tianmu. IMCS means In-memory column store, it is acted as the secondary engine of MySQL server.
The data is organized in column-base format, and in hierarchy. The data is organized in row, tile, bucket, chunck, and the data
fistly reads from innodb and stores into IMCS.

The data layout is as following:
|rows|--->|tiles|--->|buckets|--->|chuncks|. 

The memory was used by IMCS allocated when IMCS engine initialization phase. If the server starts to enable the secondary engine,
the IMCS engine start to do initialization. At initialization phase, IMCS will do the following:
1:) To check which status the IMCS is in. The status includes: 1:) in reconvery; 2:) shutting down; 3:) starting; 4:)running;.

2:) To allocate the memory used by IMCS.  

3:) If it is in reconvery, then IMCS will do reconvery. Read the data from disk-based data into IMCS, that will not do secondary-
load operation again (that will save time because imcs will read the data directly from disk, not only inndob and convert it from
row-based format to column-based format).

4:) If it is in normal status, the imcs will allocate the memory from server instance. if imcs can not allocates to much memory 
the minimum size of memory will be allocated.

5:) If the IMCS runs out of memory, the multi-hierarchy storage paradigm will be introuduced. the cold data in IMCS will swapped
out, and will make some spaces for the latest data.

6:) Other initialization will be done.

7:) If it initializes sucess, then set the status to succeed, otherwise, errors throw.

PS: where the data(memory) is located should be considered carefully.
