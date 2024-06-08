import pika
import sys
import webbrowser
import csv
import time

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # Create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # Use the connection to create a communication channel
        ch = conn.channel()
        # Use the channel to declare a durable queue
        ch.queue_declare(queue=queue_name, durable=True)
        # Use the channel to publish a message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # Print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # Close the connection to the server
        try:
            conn.close()
        except:
            pass

def read_csv_to_queue(filename: str):
    with open(filename, newline='') as file:
        reader = csv.reader(file)
        header = next(reader)  # Read the header row

        # Map header to column index
        header_map = {header[i]: i for i in range(len(header))}

        for row in reader:
            # Fill missing columns with empty strings
            row = row + [''] * (len(header) - len(row))

            index = row[header_map.get('index', -1)] if 'index' in header_map else ''
            trans_date_trans_time = row[header_map.get('trans_date_trans_time', -1)] if 'trans_date_trans_time' in header_map else ''
            cc_num = row[header_map.get('cc_num', -1)] if 'cc_num' in header_map else ''
            merchant = row[header_map.get('merchant', -1)] if 'merchant' in header_map else ''
            amt = row[header_map.get('amt', -1)] if 'amt' in header_map else ''
            lat = row[header_map.get('lat', -1)] if 'lat' in header_map else ''
            long = row[header_map.get('long', -1)] if 'long' in header_map else ''
            dob = row[header_map.get('dob', -1)] if 'dob' in header_map else ''
            merch_lat = row[header_map.get('merch_lat', -1)] if 'merch_lat' in header_map else ''
            merch_long = row[header_map.get('merch_long', -1)] if 'merch_long' in header_map else ''
            is_fraud = row[header_map.get('is_fraud', -1)] if 'is_fraud' in header_map else ''

            message = (f"Index:{index}, "
                       f"Transaction Timestamp:{trans_date_trans_time}, "
                       f"Credit Card Number:{cc_num}, "
                       f"Merchant:{merchant}, "
                       f"Transaction Amount:{amt}, "
                       f"Latitude:{lat}, "
                       f"Longitude:{long}, "
                       f"Date of Birth:{dob}, "
                       f"Merchant Location Latitude:{merch_lat}, "
                       f"Merchant Location Longitude:{merch_long}, "
                       f"Fraud Determination:{is_fraud}")

            send_message("localhost", "transactions", message)

            time.sleep(1)  # Wait 1 second between messages
            print(f"Sent: {message}. Hit CTRL-C to stop.")

if __name__ == "__main__":  
    # Ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    read_csv_to_queue('fraudTrain.csv')
