#!/bin/bash

## =================================================================
## PROJTEMP PACKAGE INSTALLATION
##
## This script will install all the packages that are needed to
## build and run the DBMS.
##
## Supported environments:
##  * Ubuntu 22.04 (x86-64)
##  * macOS 11 Big Sur (x86-64 or ARM)
##  * macOS 12 Monterey (x86-64 or ARM)
## =================================================================

main() {
  set -o errexit

    if [ "$1" == "-y" ] 
    then 
        install
    else
        echo "PACKAGES WILL BE INSTALLED. THIS MAY BREAK YOUR EXISTING TOOLCHAIN."
        echo "YOU ACCEPT ALL RESPONSIBILITY BY PROCEEDING."
        read -p "Proceed? [Y/n] : " yn
    
        case $yn in
            Y|y) install;;
            *) ;;
        esac
    fi

    echo "Script complete."
}

install() {
  set -x
  UNAME=$(uname | tr "[:lower:]" "[:upper:]" )

  case $UNAME in
    DARWIN) install_mac ;;

    LINUX)
      version=$(cat /etc/os-release | grep VERSION_ID | cut -d '"' -f 2)
      case $version in
        18.04) install_linux ;;
        20.04) install_linux ;;
        22.04) install_linux ;;
        *) give_up ;;
      esac
      ;;

    *) give_up ;;
  esac
}

give_up() {
  set +x
  echo "Unsupported distribution '$UNAME'"
  echo "Please contact our support team for additional help."
  echo "Be sure to include the contents of this message."
  echo "Platform: $(uname -a)"
  echo
  echo
  exit 1
}

install_mac() {
  # Install Homebrew.
  if test ! $(which brew); then
    echo "Installing Homebrew (https://brew.sh/)"
    ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
  fi
  # Update Homebrew.
  brew update
  # Install packages.
  brew ls --versions cmake || brew install cmake
  brew ls --versions coreutils || brew install coreutils
  brew ls --versions doxygen || brew install doxygen
  brew ls --versions git || brew install git
  (brew ls --versions llvm | grep 15) || brew install llvm@15
  brew ls --versions libelf || brew install libelf
  brew ls --version arrow || brew install arrow
}

install_linux() {
  # Update apt-get.
  apt-get -y update
  # Install packages.
  apt-get -y install \
      build-essential \
      clang-15 \
      clang-format-15 \
      clang-tidy-15 \
      cmake \
      doxygen \
      git \
      pkg-config \
      zlib1g-dev \
      libelf-dev \
      libdwarf-dev

  # Install apache arrow lib
  apt-get install -y -V ca-certificates lsb-release wget
  wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
  apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
  apt-get -y update
  apt install -y -V \
      libarrow-dev \
      libarrow-glib-dev \
      libarrow-dataset-dev \
      libarrow-dataset-glib-dev \
      libarrow-acero-dev \
      libarrow-flight-dev \
      libarrow-flight-glib-dev \
      libarrow-flight-sql-dev \
      libarrow-flight-sql-glib-dev \
      libgandiva-dev \
      libgandiva-glib-dev \
      libparquet-dev \
      libparquet-glib-dev


}

main "$@"
