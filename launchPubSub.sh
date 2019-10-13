# script to run on compute engine
# speedFactor can be fixed to 1 to simulate at same speed as data, but for testing,
# use speedFactor=60 so every row of data can be sent in one minute
pip install --user python-dateutil

python ./pubsub/send_meter_data.py --speedFactor 60 --project $PROJECT_ID
