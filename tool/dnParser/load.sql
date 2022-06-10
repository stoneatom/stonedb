create table debug_dpn (
       `used` int,
       `local` int,
       synced int,
       null_compressed int,
       data_compressed int,
       no_compress int,
       base bigint,
       addr bigint,
       len bigint,
       nr int,
       nn int,
       xmin bigint,
       xmax bigint,
       min bigint,
       max bigint,
       maxlen bigint
);
load data infile '/tmp/dpn.data' into table debug_dpn fields terminated by ';' lines terminated by '\n';
