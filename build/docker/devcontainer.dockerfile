################################################################################
#                                     BUILD                                    #
################################################################################

FROM ubuntu:22.04 as build

# Keep the bulk of the APT installs in one layer to reduce image size. The
# PPA setup and the toolchain install get their own layers so that a network
# failure in either one does not invalidate the cached base install.
ENV GCC_VERSION=13
ARG DEBIAN_FRONTEND="noninteractive"
ARG TZ="America/Los_Angeles"
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tar \
    wget \
    zip \
    unzip \
    git \
    make \
    rename \
    software-properties-common \
    gnupg \
    build-essential \
    ca-certificates \
    libgnutls30 \
    tzdata \
    language-pack-en \
    default-jre \
    default-jdk \
    protobuf-compiler \
    python3

# Add the ubuntu-toolchain-r PPA by hand rather than with
# `add-apt-repository ppa:ubuntu-toolchain-r/test`: that resolves the PPA
# through the Launchpad API at api.launchpad.net, which is unreachable from
# some build networks and hangs until the TCP connect times out. The PPA's
# archive and the keyserver are all that is actually needed.
RUN mkdir -p /etc/apt/keyrings \
    && curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x60c317803a41ba51845e371a1e9377a2ba9ef27f" \
       | gpg --dearmor -o /etc/apt/keyrings/ubuntu-toolchain-r.gpg \
    && . /etc/os-release \
    && echo "deb [signed-by=/etc/apt/keyrings/ubuntu-toolchain-r.gpg]" \
       "https://ppa.launchpadcontent.net/ubuntu-toolchain-r/test/ubuntu" \
       "${UBUNTU_CODENAME} main" \
       > /etc/apt/sources.list.d/ubuntu-toolchain-r-test.list

RUN apt-get update && apt-get install -y --no-install-recommends \
       gcc-${GCC_VERSION} \
       g++-${GCC_VERSION} \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-${GCC_VERSION} 90 \
       --slave /usr/bin/g++ g++ /usr/bin/g++-${GCC_VERSION} \
    && apt-get install -y --no-install-recommends --only-upgrade libstdc++6

# Create a symlink so 'python' points to 'python3'
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install Bazelisk
ENV USE_BAZEL_VERSION=7.6.1
ARG TARGETARCH
RUN wget -O /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/download/v1.17.0/bazelisk-linux-${TARGETARCH} \
    && chmod +x /usr/local/bin/bazel
ENV EXTRA_BAZEL_ARGS="--tool_java_runtime_version=local_jdk"

# Install google-cloud-sdk to get gcloud.
RUN curl https://sdk.cloud.google.com > install.sh                          && \
    bash install.sh --disable-prompts                                       && \
    ln -s /root/google-cloud-sdk/bin/gcloud /usr/bin/gcloud                 && \
    ln -s /root/google-cloud-sdk/bin/gsutil /usr/bin/gsutil

ENV GCLOUD_DIR="/usr/bin"

# Configure gcloud to use emulator locally.
ENV SPANNER_EMULATOR_HOST=localhost:9010
RUN gcloud config configurations create emulator                            && \
    gcloud config set auth/disable_credentials true                         && \
    gcloud config set account emulator-account                              && \
    gcloud config set project emulator-project                              && \
    gcloud config set api_endpoint_overrides/spanner http://localhost:9020/
