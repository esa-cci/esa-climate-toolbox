# Use the Miniconda base image
FROM continuumio/miniconda3

# Create a non-root user (vscode)
RUN useradd -m vscode

# Set the working directory in the container
WORKDIR /workspace

# Update conda and install the ESA Climate Toolbox, Jupyter, and ipykernel in a new environment
RUN conda update -n base -c defaults conda -y \
    && conda create --name ect --channel conda-forge esa-climate-toolbox jupyter ipykernel -y \
    && conda clean -afy

# Install the Conda environment as a Jupyter kernel
RUN /opt/conda/envs/ect/bin/python -m ipykernel install --name "ect" --user

# Set environment variables
ENV PATH /opt/conda/envs/ect/bin:$PATH
ENV CONDA_DEFAULT_ENV=ect
ENV PYTHONUNBUFFERED=1

# Set the default shell to bash
SHELL ["/bin/bash", "-c"]

# Ensure that the Git repository is up-to-date
RUN git config --global pull.ff only && \
    git config --global init.defaultBranch main && \
    git -C /workspace pull origin main || true

# Automatically activate the environment when starting the container
RUN echo "conda activate ect" >> /home/vscode/.bashrc

# Switch to non-root user
USER vscode
