FROM ubuntu:22.04 

RUN apt-get update && apt-get install -y build-essential

WORKDIR /workdir

COPY . /workdir

RUN mkdir build && make bplus_main_compile