#!/bin/bash

BUILD_DIR="build"
BUILD_TYPE="Debug"
RUN_AFTER_BUILD=0
RUN_TESTS=0
CLEAN=0
DEBUG_WITH_GDB=0
DEBUG_WITH_LLDB=0
DEBUG_WITH_VALGRIND=0
VERBOSE_LEVEL=0
VERBOSE_MAKE=0
NUM_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)
CMAKE_ARGS=""
DEFAULT_CORE_PATH="$(pwd)/src/db/core.jcl"
SYSTEM_CORE_PATH="/usr/local/share/jugadbase/core.jcl"
VALGRIND_LOG_FILE=""
OS="$(uname -s)"

export CORE_JCL_PATH="$DEFAULT_CORE_PATH"
echo "CORE_JCL_PATH is: $CORE_JCL_PATH"

# --- Helper Functions ---

print_help() {
    echo "Enhanced Build Script with Valgrind Support"
    echo "Usage: ./build.sh [options] [commands]"
    echo ""
    echo "Commands (can be combined):"
    echo "  release           Build in Release mode"
    echo "  debug             Build in Debug mode (default)"
    echo "  drun              Build and run after build"
    echo "  test              Build and run tests"
    echo "  clean             Clean build directory"
    echo "  gdb               Debug with GDB"
    echo "  lldb              Debug with LLDB"
    echo "  valgrind          Run with Valgrind memory profiling"
    echo ""
    echo "Options:"
    echo "  -v, --verbose LEVEL    Set verbosity level (0-3, default: 0)"
    echo "  -j, --jobs CORES       Set number of cores for parallel build (default: auto)"
    echo "  -b, --build-dir DIR    Set build directory (default: build)"
    echo "  -d, --build-type TYPE  Set build type (Debug/Release)"
    echo "  -c, --cmake-args ARGS  Additional CMake arguments (in quotes)"
    echo "  -l, --linker-flags FLAGS Additional linker flags (in quotes)"
    echo "  --verbose-make         Enable verbose make output"
    echo "  --valgrind-log FILE    Specify Valgrind log file (default: \${BUILD_DIR}/valgrind.log)"
    echo ""
    echo "Examples:"
    echo "  ./build.sh clean gdb -v 2"
    echo "  ./build.sh clean lldb -v 2"
    echo "  ./build.sh clean release drun"
    echo "  ./build.sh clean valgrind"
    echo "  ./build.sh valgrind --valgrind-log memory.log"
}

# Function to print the license
print_license() {
    echo "MIT License"
    echo ""
    echo "Copyright (c) 2025 Sitanshu Shrestha"
    echo ""
    echo "Permission is hereby granted, free of charge, to any person obtaining a copy"
    echo "of this software and associated documentation files (the \"Software\"), to deal"
    echo "in the Software without restriction, including without limitation the rights"
    echo "to use, copy, modify, merge, publish, distribute, sublicense, and/or sell"
    echo "copies of the Software, and to permit persons to whom the Software is"
    echo "furnished to do so, subject to the following conditions:"
    echo ""
    echo "The above copyright notice and this permission notice shall be included in all"
    echo "copies or substantial portions of the Software."
    echo ""
    echo "THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR"
    echo "IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,"
    echo "FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE"
    echo "AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER"
    echo "LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,"
    echo "OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE"
    echo "SOFTWARE."
}

# Function to check for Valgrind availability
check_valgrind() {
    if ! command -v valgrind &> /dev/null; then
        echo "Error: Valgrind is not installed or not in PATH."
        echo "On Ubuntu/Debian: sudo apt-get install valgrind"
        echo "On CentOS/RHEL:   sudo yum install valgrind"
        echo "On Fedora:        sudo dnf install valgrind"
        exit 1
    fi
}

# --- Dependency Management ---

# Function to ensure dependencies are present
ensure_deps() {
    echo "Checking for dependencies..."

    # Create deps folder if it doesn't exist
    if [ ! -d "deps" ]; then
        echo "Creating deps/ directory."
        mkdir deps
    fi

    # Check for uthash.h and download if not present
    if [ ! -f "deps/uthash.h" ]; then
        echo "Downloading uthash.h..."
        curl -L "https://raw.githubusercontent.com/troydhanson/uthash/master/src/uthash.h" -o "deps/uthash.h"
    fi

    # Check for libsodium and clone if not present
    if [ ! -d "deps/libsodium" ]; then
        echo "Cloning libsodium..."
        git clone https://github.com/jedisct1/libsodium.git deps/libsodium
    fi

    echo "Dependencies are in place."
}

# --- Main Script Logic ---

# Ensure dependencies are met before proceeding
ensure_deps

# Loop through all arguments to find commands
for arg in "$@"; do
    case "$arg" in
        release) BUILD_TYPE="Release" ;;
        debug) BUILD_TYPE="Debug" ;;
        drun) RUN_AFTER_BUILD=1 ;;
        test) RUN_TESTS=1 ;;
        clean) CLEAN=1 ;;
        gdb) DEBUG_WITH_GDB=1 ;;
        lldb) DEBUG_WITH_LLDB=1 ;;
        valgrind) DEBUG_WITH_VALGRIND=1 ;;
        help|--help|-h)
            print_help
            exit 0
            ;;
    esac
done

# Parse options
while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--verbose)
            if [[ $# -gt 1 && "$2" =~ ^[0-3]$ ]]; then
                VERBOSE_LEVEL="$2"
                shift 2
            else
                echo "Error: Verbosity level must be between 0-3" >&2
                exit 1
            fi
            ;;
        -j|--jobs)
            if [[ $# -gt 1 && "$2" =~ ^[0-9]+$ ]]; then
                NUM_CORES="$2"
                shift 2
            else
                echo "Error: Jobs parameter must be a positive number" >&2
                exit 1
            fi
            ;;
        -b|--build-dir)
            if [[ $# -gt 1 ]]; then
                BUILD_DIR="$2"
                shift 2
            else
                echo "Error: Build directory not specified" >&2
                exit 1
            fi
            ;;
        -d|--build-type)
            if [[ $# -gt 1 && ("$2" == "Debug" || "$2" == "Release") ]]; then
                BUILD_TYPE="$2"
                shift 2
            else
                echo "Error: Build type must be Debug or Release" >&2
                exit 1
            fi
            ;;
        -c|--cmake-args)
            if [[ $# -gt 1 ]]; then
                CMAKE_ARGS="$2"
                shift 2
            else
                echo "Error: CMake arguments not specified" >&2
                exit 1
            fi
            ;;
        -l|--linker-flags)
            if [[ $# -gt 1 ]]; then
                LINKER_FLAGS="$2"
                shift 2
            else
                echo "Error: Linker flags not specified" >&2
                exit 1
            fi
            ;;
        --verbose-make)
            VERBOSE_MAKE=1
            shift
            ;;
        --valgrind-log)
            if [[ $# -gt 1 ]]; then
                VALGRIND_LOG_FILE="$2"
                shift 2
            else
                echo "Error: Valgrind log file not specified" >&2
                exit 1
            fi
            ;;
        *)
            shift
            ;;
    esac
done

# Set default Valgrind log file if not specified
if [ "$DEBUG_WITH_VALGRIND" -eq 1 ] && [ -z "$VALGRIND_LOG_FILE" ]; then
    VALGRIND_LOG_FILE="${BUILD_DIR}/valgrind.log"
fi

if [ "$BUILD_TYPE" == "Release" ]; then
    echo "Release build: installing core.jcl to system path"
    mkdir -p "$(dirname "$SYSTEM_CORE_PATH")"
    cp "$DEFAULT_CORE_PATH" "$SYSTEM_CORE_PATH" || {
        echo "Error: Could not copy core.jcl to $SYSTEM_CORE_PATH" >&2
        exit 1
    }
    CORE_JCL_PATH="$SYSTEM_CORE_PATH"
else
    CORE_JCL_PATH="$DEFAULT_CORE_PATH"
fi

# Check for conflicting debug options
debug_options_count=0
[ "$DEBUG_WITH_GDB" -eq 1 ] && ((debug_options_count++))
[ "$DEBUG_WITH_LLDB" -eq 1 ] && ((debug_options_count++))
[ "$DEBUG_WITH_VALGRIND" -eq 1 ] && ((debug_options_count++))

if [ "$debug_options_count" -gt 1 ]; then
    echo "Error: Cannot use multiple debugging tools simultaneously (GDB, LLDB, Valgrind)." >&2
    exit 1
fi

# Check Valgrind availability if requested
if [ "$DEBUG_WITH_VALGRIND" -eq 1 ]; then
    check_valgrind
fi

# --- Execution ---

if [ "$CLEAN" -eq 1 ]; then
    echo "Cleaning build directory..."
    rm -rf "$BUILD_DIR"
    if [ "$DEBUG_WITH_GDB" -eq 0 ] && \
       [ "$DEBUG_WITH_LLDB" -eq 0 ] && \
       [ "$DEBUG_WITH_VALGRIND" -eq 0 ] && \
       [ "$RUN_TESTS" -eq 0 ] && \
       [ "$RUN_AFTER_BUILD" -eq 0 ]; then
        exit 0
    fi
fi

mkdir -p "$BUILD_DIR"
cp src/db/core.jcl "$BUILD_DIR"

# Build with CMake and Make
CMAKE_CMD="cmake -B \"$BUILD_DIR\" -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DCMAKE_MODULE_PATH=/usr/share/doc/check/examples/cmake"
[ -n "$CMAKE_ARGS" ] && CMAKE_CMD+=" $CMAKE_ARGS"
[ -n "$LINKER_FLAGS" ] && CMAKE_CMD+=" -DCMAKE_EXE_LINKER_FLAGS=\"$LINKER_FLAGS\""

echo "$> $CMAKE_CMD"
eval "$CMAKE_CMD"

MAKE_CMD="make -C \"$BUILD_DIR\" -j$NUM_CORES"
[ "$VERBOSE_MAKE" -eq 1 ] && MAKE_CMD+=" VERBOSE=1"

echo "Building in $BUILD_TYPE mode using $NUM_CORES cores..."
eval "$MAKE_CMD"

if [ $? -ne 0 ]; then
    echo "Build failed!" >&2
    exit 1
fi

echo "Build successful!"

# --- Post-Build Actions ---

if [ "$RUN_AFTER_BUILD" -eq 1 ]; then
    EXEC_CMD="\"$BUILD_DIR/jugad-cli\""
    [ "$VERBOSE_LEVEL" -gt 0 ] && EXEC_CMD+=" --verbose $VERBOSE_LEVEL"
    echo "Running: $EXEC_CMD"
    eval "$EXEC_CMD"
fi

if [ "$DEBUG_WITH_GDB" -eq 1 ]; then
    EXEC_CMD="gdb --args \"$BUILD_DIR/jugad-cli\""
    [ "$VERBOSE_LEVEL" -gt 0 ] && EXEC_CMD+=" --verbose $VERBOSE_LEVEL"
    echo "Starting GDB debugging session: $EXEC_CMD"
    eval "$EXEC_CMD"
fi

if [ "$DEBUG_WITH_LLDB" -eq 1 ]; then
    EXEC_CMD="lldb \"$BUILD_DIR/jugad-cli\""
    [ "$VERBOSE_LEVEL" -gt 0 ] && EXEC_CMD+=" -- --verbose $VERBOSE_LEVEL"
    echo "Starting LLDB debugging session: $EXEC_CMD"
    eval "$EXEC_CMD"
fi

if [ "$DEBUG_WITH_VALGRIND" -eq 1 ]; then
    echo "Starting Valgrind memory profiling session..."
    echo "Log file: $VALGRIND_LOG_FILE"

    mkdir -p "$(dirname "$VALGRIND_LOG_FILE")"

    VALGRIND_CMD=(
        "valgrind"
        "--tool=memcheck"
        "--leak-check=full"
        "--show-leak-kinds=definite,possible"
        "--track-origins=yes"
        "--error-exitcode=1"
        "--log-file=$VALGRIND_LOG_FILE"
        "$BUILD_DIR/jugad-cli"
    )

    [ "$VERBOSE_LEVEL" -gt 0 ] && VALGRIND_CMD+=("--verbose" "$VERBOSE_LEVEL")

    echo "Running: ${VALGRIND_CMD[*]}"
    "${VALGRIND_CMD[@]}"

    echo ""
    echo "Valgrind analysis complete!"
    echo "Log saved to: $VALGRIND_LOG_FILE"
    echo ""
    echo "Quick summary:"
    if [ -f "$VALGRIND_LOG_FILE" ]; then
        echo "=== Memory Leak Summary ==="
        grep -E "(definitely lost|indirectly lost|possibly lost|still reachable)" "$VALGRIND_LOG_FILE" | tail -5
        echo ""
        echo "=== Error Summary ==="
        grep -E "ERROR SUMMARY" "$VALGRIND_LOG_FILE" | tail -1
        echo ""
        echo "For full details, check: $VALGRIND_LOG_FILE"
    else
        echo "Warning: Log file was not created or is empty."
    fi
fi

if [ "$RUN_TESTS" -eq 1 ]; then
    echo "Running tests..."
    (cd "$BUILD_DIR" && ctest --test-dir . --output-on-failure)
fi

print_license

exit 0