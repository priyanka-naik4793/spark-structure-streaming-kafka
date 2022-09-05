#!/usr/bin/python3.7

# imports
from kafka import KafkaProducer
import random
from sys import argv, exit
from time import sleep
from datetime import datetime, timedelta
import json

# different vaccination "centres" with different
VACCINATION_CENTRES = {
    "Fortis": {'event_time': datetime.now(), 'vaccination_completed': 0, 'male': (10, 40),
               'female': (10, 40)},
    "Akash": {'event_time': datetime.now() - timedelta(days=1), 'vaccination_completed': 0, 'male': (10, 40),
              'female': (10, 40)},
    "Ayushmaan": {'event_time': datetime.now() - timedelta(days=2), 'vaccination_completed': 0, 'male': (10, 40),
                  'female': (10, 40)},
}

# check for arguments, exit if wrong
if len(argv) != 2 or argv[1] not in VACCINATION_CENTRES.keys():
    print("please provide a valid device name:")
    for key in VACCINATION_CENTRES.keys():
        print(f"  * {key}")
    print(f"\nformat: {argv[0]} DEVICE_NAME")
    exit(1)

centre_name = argv[1]
centre = VACCINATION_CENTRES[centre_name]

# set up the producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

count = 1

# until ^C
while True:
    # get random values within a normal distribution of the value
    male = random.randint(centre['male'][0], centre['male'][1])
    female = random.randint(centre['female'][0], centre['female'][1])
    vaccination_completed = male + female
    event_time = centre['event_time'] + timedelta(minutes=random.randint(0, 60))

    data = {'event_time': event_time,
            'center_name': centre_name,
            'vaccination_completed': vaccination_completed,
            'male': male,
            'female': female
            }
    # send to Kafka
    producer.send('covid-vaccine-data', json.dumps(data, default=str).encode('utf-8'))
    print(f'sending data to kafka, #{count}')

    count += 1
    sleep(.5)
