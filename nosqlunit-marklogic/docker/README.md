## Build MarkLogic Docker image ##
1. Download MarkLogic [developer edition](https://developer.marklogic.com/products) (requires registration) to the current directory.
2. Build the docker image:
```bash
docker build -t marklogic:9.0-8.2 .
```
3. Create the docker container (two test ports are forwarded to the host):
```bash
docker run --platform linux -d --name=marklogic --hostname=marklogic.local -p 8000-8002:8000-8002 -p 9001-9002:9001-9002 marklogic:9.0-8.2
```
#### Note: 
To change to your time zone add an enviroment variable:
```bash
-e "TZ=Europe/Berlin"
```
To mount a volume to the host add an argument:
```bash
-v /docker/MarkLogic:/home/ml
```