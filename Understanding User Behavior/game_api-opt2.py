#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')
events_topic = 'events'


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/", methods = ['GET','DELETE'])
def default_response():
    if request.method == 'DELETE':
        default_event = {'event_type': 'delete'}
        log_to_kafka(events_topic, purchase_sword_event)
        return "\n User Deleted!\n"
    else:
        default_event = {'event_type': 'default'}
        log_to_kafka(events_topic, default_event)
        return "\nThis is the default response!\n"


@app.route("/purchase_a_sword", methods = ['GET','DELETE'])
def purchase_a_sword():
    if request.method == 'DELETE':
        purchase_sword_event = {'event_type': 'delete'}
        log_to_kafka(events_topic, purchase_sword_event)
        return "\n User Deleted!\n"
    else:
        purchase_sword_event = {'event_type': 'purchase_sword'}
        log_to_kafka(events_topic, purchase_sword_event)
        return "\n Sword Purchased!\n"

    
