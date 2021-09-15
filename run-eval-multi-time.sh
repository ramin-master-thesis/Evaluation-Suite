#!/bin/bash
NUMBER_OF_REPEAT=${1:-10}
PARTITION_COUNT=${2-2}
PARTITION_METHOD=${3-"murmur2"}

PARTITION_FOLDER="$PARTITION_COUNT"_partitions
CONFIG_FILE="$PARTITION_METHOD"-evaluation.yaml
FOLDER_NAME="$NUMBER_OF_REPEAT"_times_"$PARTITION_METHOD"_"$PARTITION_COUNT"_partitions
mkdir ./output/"$FOLDER_NAME"

for ((i = 1; i <= NUMBER_OF_REPEAT; ++i)); do
  python3 -m main ./configs/$PARTITION_FOLDER/$CONFIG_FILE
  REPEAT_FOLDER=repeat_"$i"
  mkdir ./output/"$FOLDER_NAME"/$REPEAT_FOLDER
  mv ./output/"$PARTITION_METHOD"/* ./output/"$FOLDER_NAME"/$REPEAT_FOLDER
done
