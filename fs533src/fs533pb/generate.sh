protoc heartbeat_package.proto --go_out=. --go_opt=paths=source_relative 
protoc membershipservice_definition.proto --go_out=plugins=grpc:. 
protoc fileservice_definition.proto --go_out=plugins=grpc:. 