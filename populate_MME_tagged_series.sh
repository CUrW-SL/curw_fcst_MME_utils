#!/usr/bin/env bash

echo `date`

echo "Changing into ~/curw_fcst_MME_utils"
cd /home/uwcc-admin/curw_fcst_MME_utils
echo "Inside `pwd`"


# If no venv (python3 virtual environment) exists, then create one.
if [ ! -d "venv" ]
then
    echo "Creating venv python3 virtual environment."
    virtualenv -p python3 venv
fi

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

# Install dependencies using pip.
if [ ! -f "MME_utils.log" ]
then
    echo "Installing PyMySQL"
    pip install PyMySQL
    echo "Installing PyYAML"
    pip install PyYAML
    echo "Installing db adapter"
    pip install git+https://github.com/shadhini/curw_db_adapter.git
    touch MME_utils.log
fi


# Populate MME tagged timeseries in curw fcst database
echo "Running populate_MME_tagged_series.py"
python populate_MME_tagged_series.py >> curw_fcst_MME_tagged_series.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
