import pika
import sys
import time
from multiprocessing import Process

# Define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """Define behavior on getting a message."""
     # Decode the binary message body to a string
    print(f" [x] Received {body.decode()}")
    time.sleep(body.count(b"."))
    print(" [x] Done.")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consumer(hn: str = "localhost", qn: str = "task_queue"):
    """ Continuously listen for task messages on a named queue."""

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        channel = connection.channel()
        channel.queue_declare(queue=qn, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=qn, on_message_callback=callback)
        print(f" [*] Consumer ready for work on queue {qn}. To exit press CTRL+C")
        channel.start_consuming()
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()

def main():
    """ Start multiple consumers."""
    consumers = []
    for i in range(3):  # Start 3 consumers
        consumer_process = Process(target=consumer, args=("localhost", "transactions"))
        consumer_process.start()
        consumers.append(consumer_process)

    # Wait for all consumer processes to complete
    for consumer_process in consumers:
        consumer_process.join()

if __name__ == "__main__":
    main()
