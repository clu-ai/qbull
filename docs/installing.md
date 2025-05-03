# Installing QBull Library

This guide explains how to install the `qbull` library directly from its GitHub repository.

## Prerequisites

Before you begin, ensure you have the following installed on your system:

1.  **Python:** Version 3.8 or higher (as specified in `pyproject.toml`).
2.  **pip:** The Python package installer (usually comes with Python). You might want to upgrade it: `python -m pip install --upgrade pip`.
3.  **Git:** The version control system is required by `pip` to clone the repository. ([Install Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)).

## Installation Methods

Choose one of the following methods to install the library:

**1. Install from the Default Branch (Recommended for general use)**

This command installs the latest code from the default branch (usually `main` or `master`) of the repository.

```bash
uv pip install git+https://github.com/clu-ai/qbull.git
```