from kafka import KafkaConsumer
import csv
from datetime import datetime
import os

consumer = KafkaConsumer("client_topic",
                         bootstrap_servers=['localhost:9092'],
                         api_version=(0,10))

print('Consumer is ready!')

#Check if exists a file called messages.csv
if os.path.isfile('messages.csv'):

    #If exists, delete it, to use a new one for each execution
    print("messages.csv already exists...\n deleting it...")
    os.remove('messages.csv')
    print("messages.csv deleted!")

#Create and open a new file called messages.csv in append mode
with open('messages.csv', 'a') as f:

    file_writer = csv.writer(f)
    #Write the header of the csv file
    file_writer.writerow(["timestamp", "message"])
    print("CSV initialized!")

    startTime = datetime.now()

    while True:
        #Check if 30 seconds have passed
        if (datetime.now() - startTime).seconds % 60 == 0:
            print("Do you want to continue? (y/n)")
            message = input()
            if message == 'n':
                print('Bye!')
                break

        print("Seconds from start: " + str((datetime.now() - startTime).seconds))
        print("Consumer: ")
        print(consumer)
        print("Consumer metrics: ")
        print(consumer.metrics())
        print("Consumer subscription: ")
        print(consumer.assignment())
        print("Consumer subscribed topics: ")
        print(consumer.bootstrap_connected())
        print("Consumer config: ")
        print(consumer.config)
        consumer.topics().add("client_topic")
        print("Consumer topics: ")
        print(consumer.topics())
        print("Consumer subscription: ")
        print(consumer.subscription())

        if consumer:
            for message in consumer:
                #Extract the message and the timestamp from the kafka message
                msg = str(message.value.decode('utf-8'))
                #Transform the timestamp from milliseconds to a human readable format using the format method
                ts = datetime.fromtimestamp(message.timestamp/1000.0).strftime("%A, %B %d, %Y %I:%M:%S")
                print("Writing message to CSV file...")
                file_writer.writerow([ts, msg])
                print("Message written to CSV file!")
        else:
            print("No messages to read!")


print("Closing CSV file...")