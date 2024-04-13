FROM ubuntu:22.04

WORKDIR /workspace

COPY ./build_support/packages.sh packages.sh

RUN chmod +x packages.sh && \
    ./packages.sh -y

# Install apache arrow lib
RUN  apt-get install -y -V ca-certificates lsb-release wget && \
  wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt install -y -V  libarrow-dev
RUN apt install -y -V  libarrow-glib-dev
RUN apt install -y -V  libarrow-dataset-dev
RUN apt install -y -V  libarrow-dataset-glib-dev
RUN apt install -y -V  libarrow-acero-dev
RUN apt install -y -V  libarrow-flight-dev
RUN apt install -y -V  libarrow-flight-glib-dev
RUN apt install -y -V  libarrow-flight-sql-dev
RUN apt install -y -V  libarrow-flight-sql-glib-dev
RUN apt install -y -V  libgandiva-dev
RUN apt install -y -V  libgandiva-glib-dev
RUN apt install -y -V  libparquet-dev
RUN apt install -y -V  libparquet-glib-dev

RUN apt-get install -y gdb ninja-build tar curl zip unzip
RUN git clone https://github.com/microsoft/vcpkg && export VCPKG_FORCE_SYSTEM_BINARIES=true && ./vcpkg/bootstrap-vcpkg.sh

RUN export VCPKG_FORCE_SYSTEM_BINARIES=true && ./vcpkg/vcpkg install lz4 re2 zstd




