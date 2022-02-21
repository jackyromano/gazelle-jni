FROM amr-cache-registry.caas.intel.com/cache/library/ubuntu:20.04


ENV http_proxy=http://proxy-chain.intel.com:911
ENV https_proxy=http://proxy-chain.intel.com:912
ENV MAVEN_OPTS="-Dhttp.proxyHost=proxy-chain.intel.com -Dhttp.proxyPort=911 -Dhttp.nonProxyHosts=”localhost|*.intel.com -Dhttps.proxyHost=proxy-chain.intel.com -Dhttps.proxyPort=912"

# setup APT proxies

RUN echo 'Acquire::http::Proxy "http://proxy-dmz.intel.com:911/"; Acquire::https::Proxy "http://proxy-dmz.intel.com:912/";' > /etc/apt/apt.conf.d/proxy

# Update the base image OS and install wget and python
RUN apt-get update
RUN apt-get install apt-utils
RUN apt-get install -y wget python less numactl sudo vim 
RUN apt-get install -y openjdk-8-jdk
RUN apt-get install -y maven llvm-dev llvm-runtime clang clang-format
RUN apt-get install -y build-essential
RUN apt-get install -y git
RUN apt-get install -y cmake
RUN apt-get install -y libssl-dev libcurl4 libcurl4-openssl-dev zlib1g-dev zlib1g curl
RUN apt-get install -y libboost-dev
RUN apt-get install -y tig
RUN apt-get install -y ccache
RUN apt-get install -y htop
RUN apt-get install -y ctags
RUN apt-get install -y bash-completion
RUN apt-get install -y software-properties-common

RUN echo "installating NB dependencies"
RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | sudo tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null
RUN wget -qO - https://download.neuroblade.ai/debian/gpg/neuroblade.gpg.key | sudo apt-key add -
RUN wget -qO - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
RUN apt-key list
COPY neuroblade.list /etc/apt/sources.list.d
RUN add-apt-repository ppa:ubuntu-toolchain-r/test -y
RUN apt update
RUN apt install -y gcc-8 g++-8 gcc-9 g++-9 gcc-10 g++-10 clang-12 lldb-12 lld-12 clang-tidy-12 nvme-cli cmake maven openjdk-8-jdk-headless python3
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 100 --slave /usr/bin/g++ g++ /usr/bin/g++-10 --slave /usr/bin/gcov gcov /usr/bin/gcov-10
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 90 --slave /usr/bin/g++ g++ /usr/bin/g++-9 --slave /usr/bin/gcov gcov /usr/bin/gcov-9
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 80 --slave /usr/bin/g++ g++ /usr/bin/g++-8 --slave /usr/bin/gcov gcov /usr/bin/gcov-8

# Enable no passwd for sudo
COPY sudoers /etc/sudoers

# select java version
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64


# Specify the entrypoint to start
ENTRYPOINT tail -f /dev/null
