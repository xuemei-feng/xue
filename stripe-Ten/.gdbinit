gdb ./project/cmake/build/run_proxy
set args 0.0.0.0:50005
b proxy.cpp:308
b proxy.cpp:399
b proxy.cpp:403
b proxy.cpp:126
b proxy.cpp:407
r

p std::string(block_key)
p block_id