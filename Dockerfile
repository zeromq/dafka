FROM ubuntu:latest
MAINTAINER dafka Developers <zeromq-dev@lists.zeromq.org>

RUN DEBIAN_FRONTEND=noninteractive apt-get update -y -q
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y -q --force-yes build-essential git-core libtool autotools-dev autoconf automake pkg-config unzip libkrb5-dev cmake

RUN useradd -d /home/zmq -m -s /bin/bash zmq
RUN echo "zmq ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER zmq

RUN DEBIAN_FRONTEND=noninteractive apt-get install -y -q --force-yes \
     libzmq3-dev \
     libleveldb-dev

WORKDIR /home/zmq/tmp-deps
RUN git clone --quiet https://github.com/zeromq/czmq.git czmq
WORKDIR /home/zmq/tmp-deps/czmq
RUN ./autogen.sh 2> /dev/null
RUN ./configure --quiet --without-docs
RUN make
RUN sudo make install
RUN sudo ldconfig

WORKDIR /home/zmq
RUN git clone --quiet https://github.com/zeromq/dafka dafka
WORKDIR /home/zmq/dafka
RUN ./autogen.sh 2> /dev/null
RUN ./configure --quiet --without-docs
RUN make
RUN sudo make install
RUN sudo ldconfig
