# Run ESA CCI Toolkit on GitHub Codespace

This guide will help run the ESA CCI Toolboox on GitHub Codespace. 
Each GitHub account has a free tier for Codespaces which should be sufficient 
to run this demo.

## Prerequisites

Before you begin, ensure that you have the following:

- A GitHub account with access to the repository.
- Permission to use GitHub Codespaces.
- Familiarity with Visual Studio Code and basic terminal commands.

## Getting Started

### 1. Open the Codespace

1. Navigate to the GitHub repository for the ESA CCI Toolbox.
2. Click on the **Code** button and select **Open with Codespaces**.
3. If a Codespace does not already exist, create a new one by clicking 
   **+ New Codespace** or **[Create codespace on main]**.

This should spawn the codespace in a new browser tab.
![Click [Create codespace on main]](<images/Screenshot 2024-09-06 at 5.11.58 PM.png>)

### 2. Install Docker Extension

Once you start your Codespace, it should look like this.

![Codespace](<images/Screenshot 2024-10-15 at 3.59.17 PM.png>)

If you are asked, you should install the Docker extension - click [Install].

![click [Install]](<images/Screenshot 2024-09-06 at 5.15.17 PM.png>)

### 3. Open Notebook

In the Explorer pane, open up the notebooks folder, and select the notebook 
you would like to open.

![select the notebook](<images/Screenshot 2024-09-06 at 5.17.58 PM.png>)

### 4. Select Kernel

In the upper right corner, click 'Select Kernel'.

![click 'Select Kernel](<images/Screenshot 2024-09-06 at 5.20.42 PM.png>)

This will open the kernel selection dialogue at the top. 
Choose 'Python Environments...'

![Choose 'Python Environments...'](<images/Screenshot 2024-09-06 at 5.21.24 PM.png>)

Select the ect environment.

![select ect environment](<images/Screenshot 2024-09-06 at 5.22.54 PM.png>)

### 5. Run Notebook Cells

Run through the notebook cells by clicking the [>] 'arrow' button on each cell, 
or using [shift]+[return].
