FROM ubuntu:22.04

WORKDIR /workspace

COPY ./build_support/packages.sh packages.sh

RUN chmod +x packages.sh && \
    ./packages.sh -y


