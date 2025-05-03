import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime

QUERY_FLIGHT_BY_ID = "SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY FROM flights JOIN airlines ON flights.airline = airlines.id WHERE flights.ID = :id"
QUERY_FLIGHT_BY_DATE = "SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY FROM flights JOIN airlines ON flights.airline = airlines.id WHERE flights.day = :day AND flights.month = :month AND flights.year = :year"
QUERY_DELAYED_FLIGHT_BY_AIRLINE = "SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY, flights.AIRLINE_DELAY FROM flights JOIN airlines ON flights.airline = airlines.id WHERE DELAY IS NOT NULL AND (flights.AIRLINE_DELAY IS NOT NULL OR flights.WEATHER_DELAY IS NOT NULL OR flights.LATE_AIRCRAFT_DELAY IS NOT NULL OR flights.SECURITY_DELAY IS NOT NULL OR flights.AIR_SYSTEM_DELAY IS NOT NULL OR flights.ARRIVAL_DELAY IS NOT NULL) AND LOWER(airlines.AIRLINE) LIKE LOWER(:airline)"
QUERY_DELAYED_FLIGHT_BY_AIRPORT = "SELECT flights.*, airports.airport, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY, flights.AIRLINE_DELAY FROM flights JOIN airports ON flights.ORIGIN_AIRPORT = airports.IATA_CODE WHERE (DELAY IS NOT NULL OR flights.AIRLINE_DELAY IS NOT NULL OR flights.WEATHER_DELAY IS NOT NULL OR flights.LATE_AIRCRAFT_DELAY IS NOT NULL OR flights.SECURITY_DELAY IS NOT NULL OR flights.AIR_SYSTEM_DELAY IS NOT NULL OR flights.ARRIVAL_DELAY IS NOT NULL) AND LOWER(airports.IATA_CODE) LIKE LOWER(:airport)"
QUERY_PERCENTAGE_DELAYED_BY_AIRLINE = "SELECT airlines.AIRLINE, COUNT(CASE WHEN flights.DEPARTURE_DELAY > 0 THEN 1 END) AS delayed_flights, COUNT(*) AS total_flights FROM airlines JOIN flights ON airlines.ID = flights.AIRLINE GROUP BY airlines.AIRLINE"



class FlightData:
    """
    The FlightData class is a Data Access Layer (DAL) object that provides an
    interface to the flight data in the SQLITE database. When the object is created,
    the class forms connection to the sqlite database file, which remains active
    until the object is destroyed.
    """

    def __init__(self, db_uri):
        """
        Initialize a new engine using the given database URI
        """
        self._engine = create_engine(db_uri)

    def _execute_query(self, query, params):
        """
        Execute an SQL query with the params provided in a dictionary,
        and returns a list of records (dictionary-like objects).
        If an exception was raised, print the error, and return an empty list.
        """
        try:
            with self._engine.connect() as connection:
                query_exe = sqlalchemy.text(query)
                result = connection.execute(query_exe, params)
                rows = result.fetchall()
                row_content = []
                for row in rows:
                    row_content.append(row)
                return row_content
        except Exception as e:
            print(f"Error executing query: {e}")
            return []

    def get_flight_by_id(self, flight_id):
        """
        Searches for flight details using flight ID.
        If the flight was found, returns a list with a single record.
        """
        params = {'id': flight_id}
        return self._execute_query(QUERY_FLIGHT_BY_ID, params)


    def get_flights_by_date(self, day, month, year):
        """
        Accepts a date string in 'DD/MM/YYYY' format.
        Returns matching flights.
        """
        try:
            datetime(int(year), int(month), int(day))

            params = {
                'day': int(day),
                'month': int(month),
                'year': int(year)
            }
            return self._execute_query(QUERY_FLIGHT_BY_DATE, params)
        except ValueError:
            print("Invalid date format. Use DD/MM/YYYY.")
            return []

    def get_delayed_flights_by_airline(self, airline):
        """
        Accepts a airline.
        Returns matching flights.
        """

        params = {'airline': airline}
        return self._execute_query(QUERY_DELAYED_FLIGHT_BY_AIRLINE, params)

    def get_delayed_flights_by_airport(self, airport):
        """
        Accepts a airport.
        Returns matching flights.
        """

        params = {'airport': airport}
        return self._execute_query(QUERY_DELAYED_FLIGHT_BY_AIRPORT, params)


    def __del__(self):
        """
        Closes the connection to the databse when the object is about to be destroyed
        """
        self._engine.dispose()
