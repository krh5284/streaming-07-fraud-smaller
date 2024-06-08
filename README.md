# streaming-07-fraud
Week 7 NWMSU Streaming Data

Kellie Bernhardt
fraud detection - create a producer and consumer that could be used to stream data into a fraud detection model

## Description
Producer and consumer built to demonstrate a method to stream credit card transactions to a hypothetical fraud detection model. There is one producer that reads the csv however in real life there would likely be many producers. The consumer is actually 3 consumers which simulates the large demand that would be on the system to identify fraudulent transctions as quickly as possible

The original dataset is from Kaggle and can be found here:
(https://www.kaggle.com/datasets/kartik2112/fraud-detection?select=fraudTrain.csv)

The original dataset includes descriptions of the customer, mecherant, time and location and more. Because the origial file was so large, Pandas was used to reduce to the first 500,000 rows so that the repository could be pushed to GitHub with ease

## Visuals

![alt text](https://github.com/krh5284/streaming-07-fraud/blob/main/consumer_producer.png)

## Installation
Pika must be installed before importing. 