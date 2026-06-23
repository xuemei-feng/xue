cd project/src/proto
/root/xue/project/third_party/grpc/bin/protoc --proto_path=. --grpc_out=. --plugin=protoc-gen-grpc=./../../third_party/grpc/bin/grpc_cpp_plugin coordinator.proto
/root/xue/project/third_party/grpc/bin/protoc --proto_path=. --cpp_out=. coordinator.proto
/root/xue/project/third_party/grpc/bin/protoc --proto_path=. --grpc_out=. --plugin=protoc-gen-grpc=./../../third_party/grpc/bin/grpc_cpp_plugin proxy.proto
/root/xue/project/third_party/grpc/bin/protoc --proto_path=. --cpp_out=. proxy.proto
/root/xue/project/third_party/grpc/bin/protoc --proto_path=. --grpc_out=. --plugin=protoc-gen-grpc=./../../third_party/grpc/bin/grpc_cpp_plugin datanode.proto
/root/xue/project/third_party/grpc/bin/protoc --proto_path=. --cpp_out=. datanode.proto