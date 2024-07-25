# Development Environment Setup

Welcome to the development environment setup for the ESA Climate Toolbox. This document provides instructions on how to set up and run the Codespace using Visual Studio Code, GitHub Codespaces, and the beta 'Open in Jupyter Lab' feature.

## Prerequisites

Before you begin, ensure that you have the following:

- A GitHub account with access to the repository.
- Permission to use GitHub Codespaces.
- Familiarity with Visual Studio Code and basic terminal commands.

## Getting Started

### 1. Open the Codespace

1. Navigate to the GitHub repository for the ESA Climate Toolbox.
2. Click on the **Code** button and select **Open with Codespaces**.
3. If a Codespace does not already exist, create a new one by clicking **+ New Codespace**.

### 2. Environment Setup

The Codespace is configured to use a custom Docker image based on Miniconda. The setup includes the necessary dependencies and tools, including the ESA Climate Toolbox and Jupyter Lab.

#### Key Files and Configuration

- **Dockerfile**: Specifies the base image and dependencies.
- **.devcontainer.json**: Configures the development environment, including extensions and environment variables.

### 3. Using the Codespace

#### Opening in Jupyter Lab (Beta)

1. After the Codespace has started, you can use the beta 'Open in Jupyter Lab' option:
   - Look for the option in the GitHub Codespaces interface.
   - Click on 'Open in Jupyter Lab' to access a Jupyter Lab environment directly in your browser.

2. This feature allows for seamless interaction with Jupyter notebooks without manually starting the Jupyter Lab server.

#### Manual Activation and Usage

If needed, you can still manually activate the Conda environment and start Jupyter Lab:

1. **Activate the Conda Environment**: The environment should be activated automatically. If not, you can manually activate it:
   ```bash
   conda activate ect
