cat test_flights.csv airlines.csv|./mapper.py|sort|./reducer.py|sort -nr|head -n 5
