
# following: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/using-python-libraries.html

# initialize a python virtual environment
python3 -m venv pyspark_venvsource
source pyspark_venvsource/bin/activate

# optionally, ensure pip is up-to-date
pip3 install --upgrade pip

# install the python packages
pip3 install -r ../cc-pyspark/requirements.txt

# package the virtual environment into an archive
pip3 install venv-pack
venv-pack -f -o pyspark_venv.tar.gz