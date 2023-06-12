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
        default_event = {'event_type': 'delete',
                         'item': None,
                         'item_type': None}
        log_to_kafka(events_topic, purchase_sword_event)
        return "\n User Deleted!\n"
    else:
        default_event = {'event_type': 'default',
                         'item': None,
                         'item_type': None}
        log_to_kafka(events_topic, default_event)
        return "\nThis is the default response!\n"

    
@app.route("/purchase_a_wooden_sword", methods = ['GET','DELETE'])
def purchase_a_sword():
    if request.method == 'DELETE':
        delete_purchase_sword_event = {'event_type': 'delete',
                                       'item': 'sword',
                                       'item_type': 'wood'}
        log_to_kafka(events_topic, delete_purchase_sword_event)
        return "\n Wooden Sword Purchase Deleted!\n"        
    else:
        purchase_sword_event = {'event_type': 'purchase',
                                'item': 'sword',
                                'item_type': 'wood'}
        log_to_kafka(events_topic, purchase_sword_event)
        return "\n Wooden Sword Purchased!\n"

    
@app.route("/purchase_a_wooden_shield", methods = ['GET','DELETE'])
def purchase_a_shield():
    if request.method == 'DELETE':
        delete_purchase_shield_event = {'event_type': 'delete',
                                        'item': 'shield',
                                        'item_type': 'wood'}
        log_to_kafka(events_topic, delete_purchase_shield_event)
        return "\n Wooden Shield Purchase Deleted!\n"
    else:
        purchase_shield_event = {'event_type': 'purchase',
                                 'item': 'shield',
                                 'item_type': 'wood'}
        log_to_kafka(events_topic, purchase_shield_event)
        return "\n Wooden Shield Purchased!\n"

    
@app.route("/upgrade_to_metal_sword", methods = ['GET','DELETE'])
def upgrade_a_sword():
    if request.method == 'DELETE':    
        delete_upgrade_sword_event = {'event_type': 'delete',
                                      'item': 'sword',
                                      'item_type': 'metal'}
        log_to_kafka(events_topic, delete_upgrade_sword_event)
        return "\n Metal Sword Upgrade Deleted!\n"
    else:
        upgrade_sword_event = {'event_type': 'upgrade',
                               'item': 'sword',
                               'item_type': 'metal'}
        log_to_kafka(events_topic, upgrade_sword_event)
        return "\n Upgraded to Metal Sword!\n"

    
@app.route("/upgrade_to_metal_shield", methods = ['GET','DELETE'])
def upgrade_a_shield():
    if request.method == 'DELETE':
        delete_upgrade_shield_event = {'event_type': 'delete',
                                       'item': 'shield',
                                       'item_type': 'metal'}
        log_to_kafka(events_topic, delete_upgrade_shield_event)
        return "\n Metal Shield Upgrade Deleted!\n"
    else:
        upgrade_shield_event = {'event_type': 'upgrade',
                                'item': 'shield',
                                'item_type': 'metal'}
        log_to_kafka(events_topic, upgrade_shield_event)
        return "\n Upgraded to Metal Shield!\n"

    
@app.route("/sell_a_metal_sword", methods = ['GET','DELETE'])
def sell_a_sword():
    if request.method == 'DELETE':
        delete_sell_sword_event = {'event_type': 'delete',
                                   'item': 'sword',
                                   'item_type': 'metal'}
        log_to_kafka(events_topic, delete_sell_sword_event)
        return "\n Metal Sword Sale Deleted!\n"
    else:
        sell_sword_event = {'event_type': 'sell',
                            'item': 'sword',
                            'item_type': 'metal'}
        log_to_kafka(events_topic, sell_sword_event)
        return "\n Sold Metal Sword!\n"

    
@app.route("/sell_a_metal_shield", methods = ['GET','DELETE'])
def sell_a_shield():
    if request.method == 'DELETE':
        delete_sell_shield_event = {'event_type': 'delete',
                                    'item': 'shield',
                                    'item_type': 'metal'}
        log_to_kafka(events_topic, delete_sell_shield_event)
        return "\n Metal Shield Sale Deleted!\n"
    else:
        sell_shield_event = {'event_type': 'sell',
                             'item': 'shield',
                             'item_type': 'metal'}
        log_to_kafka(events_topic, sell_shield_event)
        return "\n Sold Metal Shield!\n"

    
@app.route("/ditch_a_wooden_sword", methods = ['GET','DELETE'])
def ditch_a_sword():
    if request.method == 'DELETE':
        delete_ditch_sword_event = {'event_type': 'delete',
                                    'item': 'sword',
                                    'item_type': 'wood'}
        log_to_kafka(events_topic, delete_ditch_sword_event)
        return "\n Wooden Shield Ditch Deleted!\n"
    else:
        ditch_sword_event = {'event_type': 'ditch',
                             'item': 'sword',
                             'item_type': 'wood'}
        log_to_kafka(events_topic, ditch_sword_event)
        return "\n Ditch Wooden Sword!\n"

    
@app.route("/ditch_a_wooden_shield", methods = ['GET','DELETE'])
def ditch_a_shield():
    if request.method == 'DELETE':
        delete_ditch_shield_event = {'event_type': 'delete',
                                     'item': 'shield',
                                     'item_type': 'wood'}
        log_to_kafka(events_topic, delete_ditch_shield_event)
        return "\n Wooden Shield Ditch Deleted!\n"
    else:
        ditch_shield_event = {'event_type': 'ditch',
                              'item': 'shield',
                              'item_type': 'wood'}
        log_to_kafka(events_topic, ditch_shield_event)
        return "\n Ditch Wooden Shield!\n"
