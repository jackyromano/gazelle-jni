FROM dbio-runner-vm1.iil.intel.com:5000/gazelle-jni-ci-image:latest

ARG USER_NAME
ARG USER_ID
ARG HOME

ENV http_proxy=http://proxy-chain.intel.com:911
ENV https_proxy=http://proxy-chain.intel.com:912
# setup APT proxies

RUN useradd  -m -u ${USER_ID} ${USER_NAME} && echo "${USER_NAME}:docker" | chpasswd && adduser $USER_NAME sudo

USER $USER_NAME
WORKDIR $HOME


# Specify the entrypoint to start
ENTRYPOINT tail -f /dev/null
