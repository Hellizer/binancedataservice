CONFFILE = config/data-serv-conf.json

default: clean build

build:
	-@ mkdir build || echo "build folder exists"
	-@ mkdir build/config || echo "config folder exists"
	cp $(CONFFILE) build/$(CONFFILE)
	go build -o build/data-serv cmd/main.go

run: 
	@build/data-serv 

clean:
	rm -rf build

debug:
	-@go run cmd/main.go

# deploy:
# 	scp -P 5051 -r build/* developer@ts1.itkn.ru:Blockchain


.PHONY: build 