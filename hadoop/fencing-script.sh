#!/bin/bash
# fence-script.sh
# This script fences the failed NameNode by killing its container.

FAILED_NN_CONTAINER=$1  # Pass the container name as an argument

# Kill the failed NameNode container
docker kill $FAILED_NN_CONTAINER

if [ $? -eq 0 ]; then
  echo "Successfully fenced NameNode container: $FAILED_NN_CONTAINER"
  exit 0
else
  echo "Failed to fence NameNode container: $FAILED_NN_CONTAINER"
  exit 1
fi