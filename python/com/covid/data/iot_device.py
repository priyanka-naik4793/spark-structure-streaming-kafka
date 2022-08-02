#!/usr/bin/python3.7

# imports
from kafka import KafkaProducer
import random
from sys import argv, exit
from time import time, sleep
from datetime import datetime

# different vaccination "centres" with different
VACCINATION_CENTRES = {
    "Fortis": {'vaccination_completed': 0, 'male': (10000, 40000), 'female': (10000, 40000)},
    "Akash": {'vaccination_completed': 0, 'male': (10000, 40000), 'female': (10000, 40000)},
    "Ayushmaan": {'vaccination_completed': 0, 'male': (10000, 40000), 'female': (10000, 40000)},
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

    # create CSV structure
    # TODO : use  json instead of csv
    # TODO: generate dates for hours later
    msg = f'{datetime.now()},{centre_name},{vaccination_completed},{male},{female}'

    # send to Kafka
    producer.send('covid-vaccine-data', bytes(msg, encoding='utf8'))
    print(f'sending data to kafka, #{count}')

    count += 1
    sleep(.5)
