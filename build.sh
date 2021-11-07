echo Building output

mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=debug
cmake --build .