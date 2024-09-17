#!/bin/bash

# setup.sh

# Set up and activate virtual environment
if [ -d "venv" ]; then
    echo "Virtual environment already exists."
else
    echo "Creating virtual environment..."
    python -m venv venv || { echo "Failed to create virtual environment"; exit 1; }
fi

echo "Checking if virtual environment was created successfully..."
if [ ! -f "venv/Scripts/activate" ]; then
    echo "Virtual environment activation script not found. Please ensure Python is installed correctly."
    exit 1
fi

echo "Activating virtual environment..."

# Detect OS and activate the correct virtual environment
if [[ "$OSTYPE" == "msys" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

# Install dependencies if necessary
echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt

# Check for pip updates
echo "Checking for pip updates..."
CURRENT_PIP_VERSION=$(pip --version | awk '{print $2}')
LATEST_PIP_VERSION=$(python -m pip install --upgrade pip --dry-run 2>&1 | grep -oP 'from \K[\d.]+')

if [ "$CURRENT_PIP_VERSION" != "$LATEST_PIP_VERSION" ]; then
    echo "Updating pip from $CURRENT_PIP_VERSION to $LATEST_PIP_VERSION..."
    python -m pip install --upgrade pip
else
    echo "pip is already up-to-date."
fi

# Set PYTHONPATH to include the project root (where src folder is)
CURRENT_DIR=$(pwd)
export PYTHONPATH="$CURRENT_DIR"
echo "PYTHONPATH set to: $PYTHONPATH"

# Run the data processing script
echo "Running data processing and loading script..."
python src/main.py

# Deactivate the virtual environment
echo "Deactivating virtual environment..."
deactivate || { echo "Deactivation failed"; exit 1; }

echo "setup.sh execution completed successfully."
