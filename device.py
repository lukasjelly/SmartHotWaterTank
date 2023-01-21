import argparse
from awscrt import auth, io, mqtt, http
from awsiot import iotshadow
from awsiot import mqtt_connection_builder
from concurrent.futures import Future
import sys
import threading
import traceback
import json
from uuid import uuid4
import RPi.GPIO as GPIO
import time
import Adafruit_DHT #temperature sensor library
import csv

##setup gpio pin
green = 17
yellow = 27
red = 22
redHeater = 5
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)
GPIO.setup(green,GPIO.OUT)
GPIO.setup(yellow,GPIO.OUT)
GPIO.setup(red,GPIO.OUT)
GPIO.setup(redHeater,GPIO.OUT)

#setup csv file storage
csvfile = "temp.csv"

# This code sample uses the AWS IoT Device Shadow Service to keep a set of properties
# in sync between the device and server. Imagine a set of vehicle controls that 
# may be changed through an app, or set by a local user.

io.init_logging(getattr(io.LogLevel, "Info"), "stderr")

# Using globals to simplify sample code
thing_name = "hotWaterTank"
client_id = "hotWaterTank-device"
endpoint = "xx"
client_certificate = "xx"
client_private_key = "xx"
root_ca = "xx"

is_sample_done = threading.Event()

mqtt_connection = None
shadow_client = None
shadow_property = ""

SHADOW_VALUE_DEFAULT = {"temperatureCurrent": 20,
                        "temperatureSet": 20}

class LockedData:
    def __init__(self):
        self.lock = threading.Lock()
        self.shadow_value = None
        self.disconnect_called = False
        self.request_tokens = set()

locked_data = LockedData()

# Callback's are the main method to asynchronously process MQTT events
# using the device SDKs.

# Function for gracefully quitting this sample
def exit(msg_or_exception):
    if isinstance(msg_or_exception, Exception):
        print("Exiting sample due to exception.")
        traceback.print_exception(msg_or_exception.__class__, msg_or_exception, sys.exc_info()[2])
    else:
        print("Exiting sample:", msg_or_exception)

    with locked_data.lock:
        if not locked_data.disconnect_called:
            print("Disconnecting...")
            locked_data.disconnect_called = True
            future = mqtt_connection.disconnect()
            future.add_done_callback(on_disconnected)

# Callback for mqtt disconnect
def on_disconnected(disconnect_future):
    print("Disconnected.")

    # Signal that sample is finished
    is_sample_done.set()

#set leds according to currentTemperature
def set_LED_temp_read(temp):
    #temp is hot
    if temp >25:
        GPIO.output(green,GPIO.HIGH)
        GPIO.output(yellow,GPIO.LOW)
        GPIO.output(red,GPIO.LOW)
    #temp is medium
    elif (temp == 25):
        GPIO.output(green,GPIO.LOW)
        GPIO.output(yellow,GPIO.HIGH)
        GPIO.output(red,GPIO.LOW)
    #temp is cold
    elif temp<25:
        GPIO.output(green,GPIO.LOW)
        GPIO.output(yellow,GPIO.LOW)
        GPIO.output(red,GPIO.HIGH)
    #error in reading temperature
    else:
        GPIO.output(green,GPIO.HIGH)
        GPIO.output(yellow,GPIO.HIGH)
        GPIO.output(red,GPIO.HIGH)
        
def set_LED_heater_status(tempCurrent, tempSet):
    try:
        if (tempSet <= tempCurrent):
            GPIO.output(redHeater,GPIO.LOW)
        elif(tempSet > tempCurrent):
            GPIO.output(redHeater,GPIO.HIGH)
    except Exception as e:
        return
    
# Callback for receiving a message from the update accepted topic
def on_get_shadow_accepted(response):
    try:
        with locked_data.lock:
            # check that this is a response to a request from this session
            try:
                locked_data.request_tokens.remove(response.client_token)
            except KeyError:
                print("Ignoring get_shadow_accepted message due to unexpected token.")
                print("""Waiting for next temperature reading... """) 
                return

            print("Finished getting initial shadow state.")
            if locked_data.shadow_value is not None:
                print("  Ignoring initial query because a delta event has already been received.")
                return

        if response.state:
            if response.state.delta:
                value = response.state.delta
                if value:
                    print("  Shadow contains delta value '{}'.".format(value))
                    set_LED_heater_status(value.get('temperatureCurrent'), value.get('temperatureSet'))
                    change_shadow_value(value, update_desired=False)
                    return

            if response.state.reported:
                value = response.state.reported
                if value:
                    print("  Shadow contains reported value '{}'.".format(value))
                    set_LED_heater_status(value.get('temperatureCurrent'), value.get('temperatureSet'))
                    set_local_shadow_value(response.state.reported)
                    return

        print("  Shadow document lacks '{}' property. Setting defaults")
        change_shadow_value(SHADOW_VALUE_DEFAULT)
        return

    except Exception as e:
        exit(e)

# Callback for receiving a message from the update rejected topic
def on_get_shadow_rejected(error):
    try:
        # check that this is a response to a request from this session
        with locked_data.lock:
            try:
                locked_data.request_tokens.remove(error.client_token)
            except KeyError:
                print("Ignoring get_shadow_rejected message due to unexpected token.")
                return

        if error.code == 404:
            print("Thing has no shadow document. Creating with defaults...")
            change_shadow_value(SHADOW_VALUE_DEFAULT)
        else:
            exit("Get request was rejected. code:{} message:'{}'".format(
                error.code, error.message))

    except Exception as e:
        exit(e)

# Callback for receiving a message from the delta updated topic
def on_shadow_delta_updated(delta):
    try:
        print("Received shadow delta event.")
        if delta.state:
            print("  Delta reports that desired value is '{}'. Changing local value...".format(delta.state))
            change_shadow_value(delta.state, update_desired=False)
        else:
            print("  Delta did not report a change")

    except Exception as e:
        exit(e)

# Callback for after the shadow update is published
def on_publish_update_shadow(future):
    try:
        future.result()
        print("Update request published.")
    except Exception as e:
        print("Failed to publish update request.")
        exit(e)

# Callback for if the shadow update is accepted
def on_update_shadow_accepted(response):
    try:
        # check that this is a response to a request from this session
        with locked_data.lock:
            try:
                locked_data.request_tokens.remove(response.client_token)
            except KeyError:
                print("Ignoring update_shadow_accepted message due to unexpected token.")
                return

        try:
            print("Finished updating reported shadow value to '{}'.".format(response.state.reported))
            print("""Waiting for next temperature reading...""")
        except:
            exit("Updated shadow is missing the target property.")

    except Exception as e:
        exit(e)

# Callback for if the shadow update is rejected
def on_update_shadow_rejected(error):
    try:
        # check that this is a response to a request from this session
        with locked_data.lock:
            try:
                locked_data.request_tokens.remove(error.client_token)
            except KeyError:
                print("Ignoring update_shadow_rejected message due to unexpected token.")
                return

        exit("Update request was rejected. code:{} message:'{}'".format(
            error.code, error.message))

    except Exception as e:
        exit(e)

#Sets state to the reported value
def set_local_shadow_value(reported_value):
    with locked_data.lock:
        locked_data.shadow_value = reported_value
    print("""Waiting for next temperature reading... """)

#Change the shadow state and send an update
def change_shadow_value(value, update_desired=True):
    with locked_data.lock:
        if locked_data.shadow_value is None:
            locked_data.shadow_value = {}
            
        for key in value.keys():
            if value[key]:
                locked_data.shadow_value[key] = value[key]
            else:
                locked_data.shadow_value.pop(key, None)
            
        print("Changed local shadow value to '{}'.".format(locked_data.shadow_value))
        #print(locked_data.shadow_value.get('temperatureSet'))
        set_LED_heater_status(locked_data.shadow_value.get('temperatureCurrent'), locked_data.shadow_value.get('temperatureSet'))
        print("Updating reported shadow value")

        # use a unique token so we can correlate this "request" message to
        # any "response" messages received on the /accepted and /rejected topics
        token = str(uuid4())

        if update_desired == True:
            request = iotshadow.UpdateShadowRequest(
                thing_name=thing_name,
                state=iotshadow.ShadowState(
                    reported=value,
                    desired=value
                ),
                client_token=token,
            )
        else:
            request = iotshadow.UpdateShadowRequest(
                thing_name=thing_name,
                state=iotshadow.ShadowState(
                    reported=value
                ),
                client_token=token,
            )
        future = shadow_client.publish_update_shadow(request, mqtt.QoS.AT_LEAST_ONCE)

        locked_data.request_tokens.add(token)

        future.add_done_callback(on_publish_update_shadow)

#Thread to wait for and handle user input
def user_input_thread_fn():
    while True:
        try:
            # Read user input
            new_value = input()

            # If user wants to quit sample, then quit.
            # Otherwise change the shadow value.
            if new_value in ['exit', 'quit']:
                exit("User has quit")
                break
            else:
                change_shadow_value(json.loads(new_value))

        except Exception as e:
            print("Exception on input thread.")
            exit(e)
            break

def send_temp():
    while True:
        try:
            # Read sensor input
            humidity, temperature = Adafruit_DHT.read_retry(11, 4)
            new_value = "{\"temperatureCurrent\":  " + str(temperature) + "}"
            set_LED_temp_read(temperature)
            #store_temp(temperature)

            # If user wants to quit sample, then quit.
            # Otherwise change the shadow value.
            if new_value in ['exit', 'quit']:
                exit("User has quit")
                break
            else:
                change_shadow_value(json.loads(new_value))
            
            time.sleep(3)

        except Exception as e:
            print("Exception on input thread.")
            exit(e)
            break
        
def store_temp(temp):
    t = time.localtime()
    currentTime = time.strftime("%H:%M:%S", t)
    data = [temp, currentTime]

    with open(csvfile, "a")as output:
        writer = csv.writer(output, delimiter=",", lineterminator = '\n')
        writer.writerow(data) 
        
if __name__ == '__main__':
    # Spin up resources
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    # Create native MQTT connection from credentials on path (filesystem)
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=endpoint,
        cert_filepath=client_certificate,
        pri_key_filepath=client_private_key,
        client_bootstrap=client_bootstrap,
        ca_filepath=root_ca,
        client_id=client_id,
        clean_session=True,
        keep_alive_secs=30)

    print("Connecting to {} with client ID '{}'...".format(
        endpoint, thing_name))

    connected_future = mqtt_connection.connect()

    shadow_client = iotshadow.IotShadowClient(mqtt_connection)

    # Wait for connection to be fully established.
    # Note that it's not necessary to wait, commands issued to the
    # mqtt_connection before its fully connected will simply be queued.
    # But this sample waits here so it's obvious when a connection
    # fails or succeeds.
    connected_future.result()
    print("Connected!")

    try:
        # Subscribe to necessary topics.
        # Note that is **is** important to wait for "accepted/rejected" subscriptions
        # to succeed before publishing the corresponding "request".
        print("Subscribing to Update responses...")
        update_accepted_subscribed_future, _ = shadow_client.subscribe_to_update_shadow_accepted(
            request=iotshadow.UpdateShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_update_shadow_accepted)

        update_rejected_subscribed_future, _ = shadow_client.subscribe_to_update_shadow_rejected(
            request=iotshadow.UpdateShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_update_shadow_rejected)

        # Wait for subscriptions to succeed
        update_accepted_subscribed_future.result()
        update_rejected_subscribed_future.result()

        print("Subscribing to Get responses...")
        get_accepted_subscribed_future, _ = shadow_client.subscribe_to_get_shadow_accepted(
            request=iotshadow.GetShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_get_shadow_accepted)

        get_rejected_subscribed_future, _ = shadow_client.subscribe_to_get_shadow_rejected(
            request=iotshadow.GetShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_get_shadow_rejected)

        # Wait for subscriptions to succeed
        get_accepted_subscribed_future.result()
        get_rejected_subscribed_future.result()

        print("Subscribing to Delta events...")
        delta_subscribed_future, _ = shadow_client.subscribe_to_shadow_delta_updated_events(
            request=iotshadow.ShadowDeltaUpdatedSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_shadow_delta_updated)

        # Wait for subscription to succeed
        delta_subscribed_future.result()

        # The rest of the sample runs asynchronously.

        # Issue request for shadow's current state.
        # The response will be received by the on_get_accepted() callback
        print("Requesting current shadow state...")

        with locked_data.lock:
            # use a unique token so we can correlate this "request" message to
            # any "response" messages received on the /accepted and /rejected topics
            token = str(uuid4())

            publish_get_future = shadow_client.publish_get_shadow(
                request=iotshadow.GetShadowRequest(thing_name=thing_name, client_token=token),
                qos=mqtt.QoS.AT_LEAST_ONCE)

            locked_data.request_tokens.add(token)

        # Ensure that publish succeeds
        publish_get_future.result()

        # Launch thread to handle user input.
        # A "daemon" thread won't prevent the program from shutting down.
        #print("Launching thread to read user input...")
        #user_input_thread = threading.Thread(target=user_input_thread_fn, name='user_input_thread')
        #user_input_thread.daemon = True
        #user_input_thread.start()
        send_temp()

    except Exception as e:
        exit(e)

    # Wait for the sample to finish (user types 'quit', or an error occurs)
    is_sample_done.wait()
