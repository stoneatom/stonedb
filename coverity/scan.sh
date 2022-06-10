#!/bin/bash -x
#-*- coding: utf-8 -*-

# 将编译命令写到一个文件中，例如build.sh:
# #!/bin/bash
# make clean
# make -j 20

# 注意需要完全编译，确保没有上一次的编译结果,编译脚本返回0时认为编译成功
# 将扫描脚本下载后和build.sh放到同一目录，比如scan.sh,然后执行
# ./scan.sh build.sh
# 程序会自动下载工具并上传结果等待扫描，如出现网络错误需连接vpn


WORKDIR=cov_scan
PWD=`pwd`
BUILD_TOOL_MD5=5760984119d68e7f383cc7b9910c18fb
BUILD_TOOL=build-system-linux64-87
BUILD_TOOL_FILE=$BUILD_TOOL
BUILD_TOOL_FILE+=".zip"
META_FILE=$WORKDIR/idir/AV4tLjOp_fLu-ZGEhdNj/cov_meta.json
BUILD_LOG=$WORKDIR/idir/AV4tLjOp_fLu-ZGEhdNj
BUILD_LOG+=".build_log"

FTP_SERVER=100.81.153.152
FTP_USER=088108
FTP_PASSWORD=TqX0aRrl

BUILD_TOOL_URL=ftp://build:system@$FTP_SERVER/$BUILD_TOOL_FILE
PROJECT_ID=AV4tLjOp_fLu-ZGEhdNj
PROJECT_IDIR=AV4tLjOp_fLu-ZGEhdNj.tar.gz

ensure_command_exists(){
    command -v $1 >/dev/null 2>&1 || { echo >&2 "required command $1 not installed.  Aborting."; exit 1; }
}

check_last_sh(){
    status_code=`curl -o /dev/null -s --write-out '%{http_code}' http://10.101.235.49:5000/check_scan_sh_update?project_id=$1`
    if [ $status_code -eq '200' ]; then
        last_project_id=`curl -s http://10.101.235.49:5000/check_scan_sh_update?project_id=$1`
        if [ "$last_project_id" == "$1" ]; then
            echo 'we are the latest version'
        else
            echo "==========  ERROR =================" 
            echo "need update,remove your cov_scan directory and execute command below to get the latest scan script"
            echo "wget \"http://10.101.235.49:5000/download_scan_sh?project_id=$last_project_id&staff_id=088108&platform=linux\" -O scan.sh"
            exit 1;
        fi
    else
        echo "==========  ERROR =================" 
        echo 'check_last_sh got' $status_code
        exit 1;
    fi
}

ensure_command_exists curl
ensure_command_exists wget
ensure_command_exists tee
ensure_command_exists unzip
ensure_command_exists md5sum
ensure_command_exists tar
ensure_command_exists awk
ensure_command_exists lftp
ensure_command_exists sed
ensure_command_exists uuidgen

check_last_sh AV4tLjOp_fLu-ZGEhdNj

if [ "$#" -ne 1 ]; then
    echo "USAGE: $0 build.sh"
    echo "write your build commands in build.sh,ensure make clean first";exit 1;
fi

if [ ! -d "$WORKDIR" ]; then
    echo "workdir not exists"
    mkdir -p $WORKDIR
    cd $WORKDIR
    wget $BUILD_TOOL_URL
    if [ $? -eq 0 ]; then
        echo "build system downloaded"
    else
        echo "build-system download failed"; exit 1;
    fi

    md5=`md5sum $BUILD_TOOL_FILE | awk '{ print $1 }'`
    echo "$md5"
    if [ "$md5" == "$BUILD_TOOL_MD5" ]; then
        echo "build-system md5sum ok"
    else
        echo ""
        echo "==========  ERROR =================" 
        echo "build-system md5sum failed";
        echo "execute wget \"http://10.101.235.49:5000/download_scan_sh?project_id=AV4tLjOp_fLu-ZGEhdNj&staff_id=088108&platform=linux\" -O scan.sh"
        exit 1;
    fi
    unzip $BUILD_TOOL_FILE
    mv $BUILD_TOOL/* .
    bin/cov-configure --template --gcc
    bin/cov-configure --template --compiler cc
    bin/cov-configure --template --compiler c++
    bin/cov-configure --template --java
    bin/cov-configure --template --clang
    bin/cov-configure --comptype prefix --compiler ccache
    bin/cov-configure --comptype gcc --template --compiler arm-linux-gnueabihf-gcc
    bin/cov-configure --comptype gcc --template --compiler x86_64-linux-android-gcc
    bin/cov-configure --comptype gcc --template --compiler i686-linux-gnu-gcc
    bin/cov-configure --comptype gcc --template --compiler arm-none-linux-gnueabi-gcc
    bin/cov-configure --comptype gcc --template --compiler arm-linux-gnueabi-gcc
    bin/cov-configure --comptype gcc --template --compiler    csky-elf-gcc
    bin/cov-configure --comptype gcc --template --compiler    csky-abiv2-elf-gcc
    bin/cov-configure --comptype gcc --template --compiler arm-linux-androideabi-gcc
    bin/cov-configure --comptype gcc --template --compiler aarch64-linux-android-gcc
    bin/cov-configure --comptype gcc --template --compiler arm-eabi-gcc
    bin/cov-configure --comptype gcc --template --compiler aarch64-poky-linux-gcc
    bin/cov-configure --comptype gcc --template --compiler mipsel-openwrt-linux-uclibc-gcc
    bin/cov-configure --comptype gcc --template --compiler xtensa-esp32-elf-gcc
    cd ..
else
    echo "workdir exists"
fi

lftp -c "open ftp://$FTP_USER:$FTP_PASSWORD@$FTP_SERVER; ls $PROJECT_IDIR"
if [ $? -eq 0 ]; then
    echo "scan task for project $PROJECT_ID pending"; exit 1;
fi
rm -rf $WORKDIR/idir

echo "building cov_meta.json"
if [ -f cov_meta.json.sys ]; then
    rm cov_meta.json.sys
fi
task_id=`uuidgen|sed 's/.*/\L&/'`
if [ -f cov_meta.json ]; then
    cat <<EOF > cov_meta.json.sys
{
    "philStone": {
        "task_id": "$task_id"
    },<philstone-meta-ends>
EOF
    cat cov_meta.json >> cov_meta.json.sys
    sed -i -e '$!N;s/<philstone-meta-ends>.*\n{//g;P;D' cov_meta.json.sys
else
    cat <<EOF > cov_meta.json.sys
{
    "philStone": {
        "task_id": "$task_id"
    }
}
EOF
fi



echo "building..."
mkdir -p $WORKDIR/idir
touch $BUILD_LOG
$WORKDIR/bin/cov-build --dir $WORKDIR/idir/$PROJECT_ID bash $@ | tee $BUILD_LOG

if test ${PIPESTATUS[0]} -ne 0; then
    echo "==========  ERROR =================" 
    echo "build failed"; exit 1;
fi

if grep -q "\[WARNING\] No files were emitted" $WORKDIR/idir/$PROJECT_ID/build-log.txt; then
    echo ""
    echo "==========  ERROR =================" 
    echo "no file emmited,forget make clean?"; exit 1;
fi

echo "build success,uploading"
mv $BUILD_LOG $WORKDIR/idir/$PROJECT_ID/
mv $PWD/cov_meta.json.sys $META_FILE

tar cvzf $PROJECT_IDIR  $WORKDIR/idir/$PROJECT_ID > /dev/null
if [ $? != 0 ]; then
    echo ""
    echo "==========  ERROR =================" 
    echo "compress package failed"; exit 1;
fi

lftp -c "open ftp://$FTP_USER:$FTP_PASSWORD@$FTP_SERVER;put $PROJECT_IDIR;quit"
if [ $? != 0 ]; then
    echo ""
    echo "==========  ERROR =================" 
    echo "upload idir failed"; exit 1;
else
    echo "upload complete"
    rm -f $PROJECT_IDIR
fi

