#!/bin/bash

DATA_DIR=/usr/local/stonedb/data
TABLE_DIR_SUFFIX="bht"
ABS_FILE="ab_switch"

function print_warning() {
    echo ""
    echo "##########################################################"
    echo "#                                                        #"
    echo "#      Please make sure StoneDB has been stopped         #"
    echo "#                                                        #"
    echo "##########################################################"
}

function toggleAB {
    if [ $# -ne 2 ]
    then
        echo "Usage: toggle db_name tab_name"
        exit 1
    fi
    dir="$DATA_DIR/$1/$2.$TABLE_DIR_SUFFIX"
    if [[ ! -w "$dir" ]]; then
        echo "Usage: table does not exist or cannot be accessed"
        exit 1
    fi

    print_warning
    echo -n "Please confirm to switch the table version: [Y/N]: "
    read answer
    if [[ "$answer" != "Y" && "$answer" != "y" ]]
    then
        echo "Bye"
        exit 0
    fi

    if [[ -f "$dir/$ABS_FILE" ]]; then
        rm "$dir/$ABS_FILE"
        echo "B => A"
    else
        touch "$dir/$ABS_FILE"
        echo "A => B"
    fi
}

if [ "$1" == "toggle" ]
then
    toggleAB "$2" "$3"
    exit 0
fi

if [ "$1" != "purge" ]
then
    echo "Invalid option. Supported options:"
    echo "      purge"
    exit 1
fi

if [[ ! -d "$DATA_DIR" ]]; then
    echo "$DATA_DIR is not a directory"
fi

if [[ ! -r "$DATA_DIR" ]]; then
    echo "$DATA_DIR is not readable by user $USER"
fi

print_warning

echo "List stage files"

echo "--------------------------------------------"
find $DATA_DIR -type f -name '*.S'
if [ $? -ne 0 ]; then
    echo "Failed to find files under $DATA_DIR"
    exit 1
fi
echo "--------------------------------------------"

NUM=$(find $DATA_DIR -type f -name '*.S' | wc -l)

if [ $NUM = '0' ]
then
    echo "No stage file found"
exit 0
fi


echo -n "Please confirm to remove above files: [Y/N]: "
read answer
if [[ "$answer" = "Y" || "$answer" = "y" ]]
then
    find $DATA_DIR -type f -name '*.S' -delete
    echo "Done!"
else
    echo "Bye"
fi
