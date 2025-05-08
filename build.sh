#!/bin/bash

BUILD_DIR="build"
BUILD_TYPE="Debug"
RUN_AFTER_BUILD=0
RUN_TESTS=0
CLEAN=0
DEBUG_WITH_GDB=0
DEBUG_WITH_LLDB=0
VERBOSE_LEVEL=0
VERBOSE_MAKE=0
NUM_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)
CMAKE_ARGS=""
DEFAULT_CORE_PATH="$(pwd)/src/core.jcl"
SYSTEM_CORE_PATH="/usr/local/share/jugadbase/core.jcl"
OS="$(uname -s)"
case "$OS" in
    Linux*)
        SYSTEM_CORE_PATH="/usr/local/share/jugadbase/core.jcl"
        ;;
    Darwin*)
        SYSTEM_CORE_PATH="/usr/local/share/jugadbase/core.jcl"
        ;;
    CYGWIN*|MINGW*|MSYS*)
        SYSTEM_CORE_PATH="$(cygpath -u "$APPDATA")/jugadbase/core.jcl"
        ;;
    *)
        echo "Unsupported OS: $OS"
        exit 1
        ;;
esac


if [[ "$OSTYPE" == "darwin"* ]]; then
    LINKER_FLAGS="-L/opt/homebrew/lib"
    echo "System detected as macOS (Darwin), setting linker flags to: $LINKER_FLAGS"
else
    LINKER_FLAGS="-L/usr/lib"
    echo "System detected as Linux or other UNIX-like, setting linker flags to: $LINKER_FLAGS"
fi

print_help() {
    echo "Enhanced Build Script"
    echo "Usage: ./build.sh [options] [commands]"
    echo ""
    echo "Commands (can be combined):"
    echo "  release     Build in Release mode"
    echo "  debug       Build in Debug mode (default)"
    echo "  drun        Build and run after build"
    echo "  test        Build and run tests"
    echo "  clean       Clean build directory"
    echo "  gdb         Debug with GDB"
    echo "  lldb        Debug with LLDB"
    echo ""
    echo "Options:"
    echo "  -v, --verbose LEVEL    Set verbosity level (0-3, default: 0)"
    echo "  -j, --jobs CORES       Set number of cores for parallel build (default: auto)"
    echo "  -b, --build-dir DIR    Set build directory (default: build)"
    echo "  -d, --build-type TYPE  Set build type (Debug/Release)"
    echo "  -c, --cmake-args ARGS  Additional CMake arguments (in quotes)"
    echo "  -l, --linker-flags FLAGS  Additional linker flags (in quotes)"
    echo "  --verbose-make         Enable verbose make output"
    echo ""
    echo "Examples:"
    echo "  ./build.sh clean gdb -v 2     Clean, build, and debug with GDB"
    echo "  ./build.sh clean lldb -v 2    Clean, build, and debug with LLDB"
    echo "  ./build.sh clean release drun Run clean release build"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        release)
            BUILD_TYPE="Release"
            shift
            ;;
        debug)
            BUILD_TYPE="Debug"
            shift
            ;;
        drun)
            RUN_AFTER_BUILD=1
            shift
            ;;
        test)
            RUN_TESTS=1
            shift
            ;;
        clean)
            CLEAN=1
            shift
            ;;
        gdb)
            DEBUG_WITH_GDB=1
            shift
            ;;
        lldb)
            DEBUG_WITH_LLDB=1
            shift
            ;;
        help|--help|-h)
            print_help
            exit 0
            ;;
        -v|--verbose)
            if [[ $# -gt 1 && "$2" =~ ^[0-3]$ ]]; then
                VERBOSE_LEVEL="$2"
                shift 2
            else
                echo "Error: Verbosity level must be between 0-3"
                exit 1
            fi
            ;;
        -j|--jobs)
            if [[ $# -gt 1 && "$2" =~ ^[0-9]+$ ]]; then
                NUM_CORES="$2"
                shift 2
            else
                echo "Error: Jobs parameter must be a positive number"
                exit 1
            fi
            ;;
        -b|--build-dir)
            if [[ $# -gt 1 ]]; then
                BUILD_DIR="$2"
                shift 2
            else
                echo "Error: Build directory not specified"
                exit 1
            fi
            ;;
        -d|--build-type)
            if [[ $# -gt 1 && ("$2" == "Debug" || "$2" == "Release") ]]; then
                BUILD_TYPE="$2"
                shift 2
            else
                echo "Error: Build type must be Debug or Release"
                exit 1
            fi
            ;;
        -c|--cmake-args)
            if [[ $# -gt 1 ]]; then
                CMAKE_ARGS="$2"
                shift 2
            else
                echo "Error: CMake arguments not specified"
                exit 1
            fi
            ;;
        -l|--linker-flags)
            if [[ $# -gt 1 ]]; then
                LINKER_FLAGS="$2"
                shift 2
            else
                echo "Error: Linker flags not specified"
                exit 1
            fi
            ;;
        --verbose-make)
            VERBOSE_MAKE=1
            shift
            ;;
        *)
            echo "Unknown option: $1"
            print_help
            exit 1
            ;;
    esac
done

if [[ "$RELEASE_BUILD" == "1" ]]; then
    echo "Release build: installing core.jcl to system path"
    mkdir -p "$(dirname "$SYSTEM_CORE_PATH")"
    cp "$DEFAULT_CORE_PATH" "$SYSTEM_CORE_PATH" || {
        echo "Error: Could not copy core.jcl to $SYSTEM_CORE_PATH"
        exit 1
    }
    CORE_JCL_PATH="$SYSTEM_CORE_PATH"
else
    CORE_JCL_PATH="$DEFAULT_CORE_PATH"
fi

if [ "$DEBUG_WITH_GDB" -eq 1 ] && [ "$DEBUG_WITH_LLDB" -eq 1 ]; then
    echo "Error: Cannot use both GDB and LLDB simultaneously."
    exit 1
fi

if [ "$CLEAN" -eq 1 ]; then
    echo "Cleaning build directory..."
    rm -rf "$BUILD_DIR"
    [ "$DEBUG_WITH_GDB" -eq 0 ] && [ "$DEBUG_WITH_LLDB" -eq 0 ] && [ "$RUN_TESTS" -eq 0 ] && [ "$RUN_AFTER_BUILD" -eq 0 ] && exit 0
fi

mkdir -p "$BUILD_DIR"

CMAKE_CMD="cmake -B $BUILD_DIR -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DCMAKE_MODULE_PATH=/usr/share/doc/check/examples/cmake" 
[ -n "$CMAKE_ARGS" ] && CMAKE_CMD="$CMAKE_CMD $CMAKE_ARGS"
[ -n "$LINKER_FLAGS" ] && CMAKE_CMD="$CMAKE_CMD -DCMAKE_EXE_LINKER_FLAGS=\"$LINKER_FLAGS\""

CMAKE_CMD="$CMAKE_CMD -DCORE_JCL_PATH=$CORE_JCL_PATH"

echo "$> $CMAKE_CMD"
eval "$CMAKE_CMD"

MAKE_CMD="make -C $BUILD_DIR -j$NUM_CORES"
[ "$VERBOSE_MAKE" -eq 1 ] && MAKE_CMD="$MAKE_CMD VERBOSE=1"

echo "Building in $BUILD_TYPE mode using $NUM_CORES cores..."
eval "$MAKE_CMD"

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build successful!"

if [ "$RUN_AFTER_BUILD" -eq 1 ]; then
    EXEC_CMD="$BUILD_DIR/jugad-cli"
    [ "$VERBOSE_LEVEL" -gt 0 ] && EXEC_CMD="$EXEC_CMD --verbose $VERBOSE_LEVEL"
    echo "Running: $EXEC_CMD"
    eval "$EXEC_CMD"
fi

if [ "$DEBUG_WITH_GDB" -eq 1 ]; then
    EXEC_CMD="gdb --args $BUILD_DIR/jugad-cli"
    [ "$VERBOSE_LEVEL" -gt 0 ] && EXEC_CMD="$EXEC_CMD --verbose $VERBOSE_LEVEL"
    echo "Starting GDB debugging session: $EXEC_CMD"
    eval "$EXEC_CMD"
fi

if [ "$DEBUG_WITH_LLDB" -eq 1 ]; then
    EXEC_CMD="lldb $BUILD_DIR/jugad-cli"
    [ "$VERBOSE_LEVEL" -gt 0 ] && EXEC_CMD="$EXEC_CMD -- --verbose $VERBOSE_LEVEL"
    echo "Starting LLDB debugging session: $EXEC_CMD"
    eval "$EXEC_CMD"
fi

if [ "$RUN_TESTS" -eq 1 ]; then
    echo "Running tests..."
    cd "$BUILD_DIR" && ctest --test-dir . --output-on-failure
fi

exit 0