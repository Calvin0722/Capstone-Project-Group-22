## Capstone Project 22
This is the repository for works done for Capstone Project 22: Data-driven decision making with machine learning tools and focus on customer satisfaction.

### Setup
#### Development Environment Setup
To set up the local environment, please run:
```commandline
# Please make sure to make setup.sh executable before runing the script: chmod +x ./setup.sh
./setup.sh
# Setup package
pip install -e .
```
This script will create a symbolic link to the directory storing the dataset and create a conda environment for the project.

#### Data Cleanup
To perform data cleanup, run the following command:
```
python scripts/cleanup_data.py -r ./data
```

#### Update Config
Before running any code, update `CLEANED_DATA_DIR_ROOT` to be the absolute path to the directory containing the cleaned data (the one containing data_1, ..., data_8). To get the absolute path you can right click on the folder in VS Code and click copy path.
