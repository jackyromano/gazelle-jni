FROM amr-cache-registry.caas.intel.com/cache/library/ubuntu:20.04


ENV http_proxy=http://proxy-chain.intel.com:911
ENV https_proxy=http://proxy-chain.intel.com:912
ENV MAVEN_OPTS="-Dhttp.proxyHost=proxy-chain.intel.com -Dhttp.proxyPort=911 -Dhttp.nonProxyHosts=”localhost|*.intel.com -Dhttps.proxyHost=proxy-chain.intel.com -Dhttps.proxyPort=912"

# setup APT proxies

RUN echo 'Acquire::http::Proxy "http://proxy-dmz.intel.com:911/"; Acquire::https::Proxy "http://proxy-dmz.intel.com:912/";' > /etc/apt/apt.conf.d/proxy

# Update the base image OS and install wget and python
RUN apt-get update
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

# Enable no passwd for sudo
COPY sudoers /etc/sudoers

# select java version
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64


# Specify the entrypoint to start
ENTRYPOINT tail -f /dev/null
