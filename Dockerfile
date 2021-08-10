# Base image.
FROM ubuntu

# Non-interactive mode.
ARG DEBIAN_FRONTEND=noninteractive

# Install core packages.
RUN apt-get update && apt-get install -y \
  libfuse-dev \
  python3.9 \
  python3-pip

# Install dev packages.
RUN apt-get update && apt-get install -y \
  fish \
  git \
  htop \
  less \
  tmux \
  tree \
  vim \
  zip

# Install python packages.
RUN python3.9 -m pip install \
  docformatter \
  fusepy \
  pytest \
  yapf

# Set the Pythonpath.
VOLUME ["/code"]
ENV PYTHONPATH=/code
