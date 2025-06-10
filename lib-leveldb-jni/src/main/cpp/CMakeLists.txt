# Sets the minimum version of CMake required to build the native
# library. You should either keep the default value or only pass a
# value of 3.4.0 or higher.
cmake_minimum_required(VERSION 3.22.1)

# Declares the project name.
project(leveldb-jni)

# --- Control subproject options ---
# Disable building tests and benchmarks for leveldb and snappy,
# as they are likely causing target conflicts with GoogleTest/Benchmark.
# We only want to build the libraries themselves.
set(LEVELDB_BUILD_TESTS OFF CACHE BOOL "Disable LevelDB tests" FORCE)
set(LEVELDB_BUILD_BENCHMARKS OFF CACHE BOOL "Disable LevelDB benchmarks" FORCE)
set(SNAPPY_BUILD_TESTS OFF CACHE BOOL "Disable Snappy tests" FORCE)
set(SNAPPY_BUILD_BENCHMARKS OFF CACHE BOOL "Disable Snappy benchmarks" FORCE)
# --- End control subproject options ---

# --- Ensure LevelDB builds with Snappy support ---
# By building Snappy as a subdirectory, the 'snappy' target will be available.
# We can also explicitly set HAVE_SNAPPY for LevelDB's CMake to recognize it.
set(HAVE_SNAPPY ON CACHE BOOL "Enable Snappy support in LevelDB" FORCE)
# --- End Snappy support for LevelDB ---


# Add LevelDB source directory as a subdirectory.
# This assumes you have the LevelDB source code in a directory named "leveldb"
# at the same level as your jni directory.
# You will need to download the LevelDB source and place it there.
# Example: lib-leveldb-jni/src/main/jni/leveldb
add_subdirectory(snappy)
add_subdirectory(leveldb)

# Specifies the C++ standard.
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Includes the directory containing leveldb_jni.h
include_directories(.)

# Creates and names a library, sets it as STATIC, and
# specifies the relative paths to its source code.
# You can define multiple libraries, and CMake builds them for you.
# Gradle automatically packages shared libraries with your APK.
add_library( # Sets the name of the library.
        leveldb-jni

        # Sets the library as a shared library.
        SHARED

        # Provides a relative path to your source file(s).
        leveldb_jni.cc)

# Searches for a specified prebuilt library and stores the path as a
# variable. Because CMake includes system libraries in the search path by
# default, you only need to specify the name of the public NDK library
# you want to add. CMake verifies that the library exists before
# completing its build.
find_library( # Sets the name of the path variable.
        log-lib

        # Specifies the name of the NDK library that
        # you want CMake to locate.
        log)

# Specifies libraries CMake should link to your target library. You
# can link multiple libraries, such as libraries you define in this
# build script, prebuilt third-party libraries, or system libraries.
target_link_libraries( # Specifies the target library.
        leveldb-jni

        # Links the LevelDB library (built from the subdirectory).
        leveldb
        snappy

        # Links the log library
        # to the target library.
        ${log-lib})