#!/bin/bash

BUILD_DIR="build"
BUILD_TYPE="Debug"
RUN_AFTER_BUILD=0
RUN_TESTS=0
CLEAN=0

case "$1" in
  release)
    BUILD_TYPE="Release"
    ;;
  drun)
    BUILD_TYPE="Debug"
    RUN_AFTER_BUILD=1
    ;;
  test)
    BUILD_TYPE="Debug"
    RUN_TESTS=1
    ;;
  clean)
    CLEAN=1
    ;;
  help)
    echo "Usage: ./build.sh [release | drun | test | clean]"
    ;;
esac

if [ "$CLEAN" -eq 1 ]; then
  echo "Cleaning build directory..."
  rm -rf $BUILD_DIR
  exit 0
fi

echo "Building in $BUILD_TYPE mode..."
cmake -B $BUILD_DIR -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DCMAKE_MODULE_PATH=/usr/share/doc/check/examples/cmake
cd $BUILD_DIR && make

if [ "$RUN_AFTER_BUILD" -eq 1 ]; then
  ./jugad-cli --verbose 3
fi

if [ "$RUN_TESTS" -eq 1 ]; then
  ctest --test-dir . --output-on-failure
fi