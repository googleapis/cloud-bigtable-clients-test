To regenerate the proto, run:
```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go get github.com/googleapis/googleapis
export PATH="$PATH:$HOME/go/bin"
Protoc -I{path to googleapis go pkg installed above} -I. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative test_proxy.proto
```