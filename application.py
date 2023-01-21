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
import time
from timeit import default_timer as timer
import csv

# This sample uses the AWS IoT Device Shadow Service to keep a set of properties
# in sync between the device and server. Imagine a set of vehicle controls that 
# may be changed through an app, or set by a local user.

io.init_logging(getattr(io.LogLevel, "Info"), "stderr")

#setup csv file storage
csvfile = "metric.csv"

# Using globals to simplify sample code
thing_name = "hotWaterTank"
client_id = "hotWaterTank-app"
endpoint = "xx"
client_certificate = "xx"
client_private_key = "xx"
root_ca = "xx"

is_sample_done = threading.Event()

mqtt_connection = None
shadow_client = None
shadow_property = ""

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

# Callback for receiving a message from the update accepted topic
def on_get_shadow_accepted(response):
    try:
        with locked_data.lock:
            # check that this is a response to a request from this session
            try:
                locked_data.request_tokens.remove(response.client_token)
            except KeyError:
                print("Ignoring get_shadow_accepted message due to unexpected token.")
                return

        if response.state:
            if response.state.delta:
                value = response.state.delta
                if value:
                    print("  Shadow contains delta value '{}'.".format(value))
                    update_local_shadow_value(value)
                    return
            # We only want to update the App's local state if device has
            # updated its reported state. There would be no delta in this case.
            elif response.state.reported:
                value = response.state.reported
                if value:
                    print("  Shadow contains reported value '{}'.".format(value))
                    set_local_shadow_value(value)
                    return

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
            print("Thing has no shadow document.")
        else:
            exit("Get request was rejected. code:{} message:'{}'".format(
                error.code, error.message))

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
                #print("Ignoring update_shadow_accepted message due to unexpected token.")
                #print("""Enter desired hot water temperature ex: 23:  """) 
                return

        try:
            print("Finished updating desired shadow value to '{}'.".format(response.state.desired))
            global end
            end = timer()
            print("\n*******METRIC*******")
            print("The new temperature you have set took {:.2f} seconds to update successfully!".format(end - start))
            print("*******METRIC*******\n")
            store_metric(end-start)
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
        
# Callback for if the shadow updated
def on_update_shadow_documents(response):
    try:
        print("Received shadow document update")
        set_local_shadow_value(response.current.state.reported)
        return

    except Exception as e:
        exit(e)

#Set the local value
def set_local_shadow_value(value):
    with locked_data.lock:
        locked_data.shadow_value = value
            
        print("Changed local shadow value to '{}'.".format(locked_data.shadow_value))
        print("""Enter desired hot water temperature ex: 23:  """) 

#Update the local shadow state
def update_local_shadow_value(value):
    with locked_data.lock:
        for key in value.keys():
            locked_data.shadow_value[key] = value[key]
            
        print("Changed local shadow value to '{}'.".format(locked_data.shadow_value))

#Change the shadow state and send an update
def change_shadow_value(value):
    with locked_data.lock:
        for key in value.keys():
            if value[key]:
                locked_data.shadow_value[key] = value[key]
            else:
                locked_data.shadow_value.pop(key, None)
            
        print("Changed local shadow value to '{}'.".format(locked_data.shadow_value))
        
        print("Updating desired shadow value")
        start = timer()
        # use a unique token so we can correlate this "request" message to
        # any "response" messages received on the /accepted and /rejected topics
        token = str(uuid4())

        #Request to update the shadow with changed values only
        request = iotshadow.UpdateShadowRequest(
            thing_name=thing_name,
            state=iotshadow.ShadowState(
                desired=value
            ),
            client_token=token,
        )
        future = shadow_client.publish_update_shadow(request, mqtt.QoS.AT_LEAST_ONCE)

        locked_data.request_tokens.add(token)

        future.add_done_callback(on_publish_update_shadow)

#Request the current shadow value
def request_shadow_value():
    # use a unique token so we can correlate this "request" message to
    # any "response" messages received on the /accepted and /rejected topics
    token = str(uuid4())

    publish_get_future = shadow_client.publish_get_shadow(
        request=iotshadow.GetShadowRequest(thing_name=thing_name, client_token=token),
        qos=mqtt.QoS.AT_LEAST_ONCE)

    locked_data.request_tokens.add(token)
        
    return publish_get_future

#Thread to wait for and handle user input
def user_input_thread_fn():
    while True:
        try:
            # Read user input
            userInput = input()
            new_value = "{\"temperatureSet\": " + userInput + "}"
            global start
            start = timer()
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
        
def store_metric(metric):
    t = time.localtime()
    currentTime = time.strftime("%H:%M:%S", t)
    data = [metric, currentTime]

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
            
        update_rejected_subscribed_future, _ = shadow_client.subscribe_to_shadow_updated_events(
            request=iotshadow.UpdateShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_update_shadow_documents)

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

        # The rest of the sample runs asynchronously.

        # Issue request for shadow's current state.
        # The response will be received by the on_get_accepted() callback
        print("Requesting current shadow state...")
        with locked_data.lock:
            publish_get_future = request_shadow_value()
        # Ensure that publish succeeds
        publish_get_future.result()

        # Launch thread to handle user input.
        # A "daemon" thread won't prevent the program from shutting down.
        print("Launching thread to read user input...")
        user_input_thread = threading.Thread(target=user_input_thread_fn, name='user_input_thread')
        user_input_thread.daemon = True
        user_input_thread.start()
        

    except Exception as e:
        exit(e)

    # Wait for the sample to finish (user types 'quit', or an error occurs)
    is_sample_done.wait()
