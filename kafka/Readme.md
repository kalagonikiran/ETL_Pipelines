Objectives

Start a MySQL Database server.
(#start_mysql

#mysql --host=127.0.0.1 --port=3306 --user=root --password=Mjk0NDQtcnNhbm5h

#create database tolldata;

#use tolldata;

#create table livetolldata(timestamp datetime,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint);

#Create a table to hold the toll data.

#exit)

Start the Kafka server.

Install the Kafka python driver.
#python3 -m pip install kafka-python

Install the MySQL python driver.
#python3 -m pip install mysql-connector-python==8.0.31

Create a topic named toll in kafka.

Download streaming data generator program.

Customize the generator program to steam to toll topic.

Download and customise streaming data consumer.

Customize the consumer program to write into a MySQL database table.

Verify that streamed data is being collected in the database table.
