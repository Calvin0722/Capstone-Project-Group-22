#!/bin/bash

# Prompt the user for the source path
echo "Enter the data path (The path to the folder that contains data_1 to data_8):"
read source_path

# Check if the source file/directory exists
if [ ! -e "$source_path" ]; then
    echo "Error: Source path does not exist."
    exit 1
fi

# Create target in current path named data
link_path="./data"

# Create the symbolic link
ln -s "$source_path" "$link_path"

# Check the result
if [ $? -eq 0 ]; then
    echo "Symbolic link created successfully!"
else
    echo "Error: Failed to create symbolic link."
    exit 1
fi

# Hardcoded file directory and filename
file_path="./environment.yml"

# Get the name of the environment from the YAML file
env_name=$(grep "name:" $file_path | cut -d ' ' -f 2)

# Check if the environment already exists
conda info --envs | grep -q $env_name
if [ $? -eq 0 ]; then
    echo "Environment '$env_name' already exists."
    exit 1
fi

# Create the environment from the YAML file
conda env create -f $file_path

if [ $? -eq 0 ]; then
    echo "Environment '$env_name' created successfully."
else
    echo "Failed to create environment '$env_name'."
    exit 1
fi
