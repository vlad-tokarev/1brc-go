
build:
	go build -gcflags -m -o program ./cmd/main.go

build-pgo:
	go build -gcflags -m -o programpgo -pgo=cpu.prof ./cmd/main.go