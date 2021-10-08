CREATE DATABASE my_homework;

USE my_homework;

CREATE TABLE Flights(YEAR int, MONTH int, DAY int, DAY_OF_WEEK int, AIRLINE string, FLIGHT_NUMBER string, TAIL_NUMBER string, ORIGIN_AIRPORT string, 
                     DESTINATION_AIRPORT string, SCHEDULED_DEPARTURE int, DEPARTURE_TIME int, DEPARTURE_DELAY int, 
                     TAXI_OUT int, WHEELS_OFF int, SCHEDULED_TIME int, ELAPSED_TIME int, AIR_TIME int, DISTANCE int, WHEELS_ON int, 
                     TAXI_IN int, SCHEDULED_ARRIVAL int, ARRIVAL_TIME int, ARRIVAL_DELAY int, DIVERTED int, CANCELLED int, 
                     CANCELLATION_REASON int, AIR_SYSTEM_DELAY int, SECURITY_DELAY int, AIRLINE_DELAY int, LATE_AIRCRAFT_DELAY int, 
                     WEATHER_DELAY int) row format delimited fields terminated by ','; 

DESCRIBE Flights;

LOAD DATA INPATH '/hw4/flights.csv' OVERWRITE INTO TABLE Flights;



CREATE TABLE Airlines(IATA_CODE string, AIRLINE string) row format delimited fields terminated by ',';

DESCRIBE Airlines;

LOAD DATA INPATH '/hw4/airlines.csv' OVERWRITE INTO TABLE Airlines;