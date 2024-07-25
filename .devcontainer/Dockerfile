FROM mcr.microsoft.com/vscode/devcontainers/python:3.9-slim

# Set the working directory in the container
WORKDIR /workspace

# Install necessary dependencies
RUN apt-get update && apt-get install -y wget bzip2 && rm -rf /var/lib/apt/lists/*

# Install Miniconda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh \
    && bash miniconda.sh -b -p /opt/conda \
    && rm miniconda.sh \
    && /opt/conda/bin/conda init bash \
    && /opt/conda/bin/conda install -n base conda

# Ensure conda is in the PATH
ENV PATH /opt/conda/bin:$PATH

# Install ESA Climate Toolbox in a new environment
RUN conda create --name ect --channel conda-forge esa-climate-toolbox jupyter -y && conda clean -afy

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set the default shell to bash
SHELL ["/bin/bash", "-c"]

# Automatically activate the environment when starting the container
RUN echo "conda activate ect" >> ~/.bashrc
