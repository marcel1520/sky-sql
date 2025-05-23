import data
from datetime import datetime
import sqlalchemy
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium

SQLITE_URI = 'sqlite:///data/flights.sqlite3'
IATA_LENGTH = 3

QUERY_PERCENTAGE_DELAYED_BY_AIRLINE = ("SELECT airlines.AIRLINE, "
                                       "COUNT(CASE WHEN flights.DEPARTURE_DELAY > 0 THEN 1 END) AS delayed_flights, "
                                       "COUNT(*) AS total_flights "
                                       "FROM airlines "
                                       "JOIN flights ON airlines.ID = flights.AIRLINE "
                                       "GROUP BY airlines.AIRLINE")

QUERY_PERCENTAGE_DELAYED_BY_HOUR = ("SELECT DEPARTURE_TIME, DEPARTURE_DELAY "
                                    "FROM flights "
                                    "WHERE DEPARTURE_TIME IS NOT NULL AND DEPARTURE_DELAY IS NOT NULL")

QUERY_HEATMAP_DELAY = ("SELECT ORIGIN_AIRPORT, DESTINATION_AIRPORT, "
                       "COUNT(*) AS total_flights, "
                       "COUNT(CASE WHEN DEPARTURE_DELAY > 0 THEN 1 END) AS  delayed_flights "
                       "FROM flights WHERE ORIGIN_AIRPORT IS NOT NULL AND DESTINATION_AIRPORT IS NOT NULL "
                       "GROUP BY ORIGIN_AIRPORT, DESTINATION_AIRPORT")


def show_delay_lines_on_route_map(data_manager):
    """
        Displays a map showing flight delays between a specific origin and destination.
        Delay severity is color-coded and mapped using Folium.
        """
    origin = input("Enter IATA Code for Origin Airport: ")
    destination = input("Enter IATA Code for Destination Airport: ")

    QUERY_PERCENTAGE_ON_ROUTE_MAP = (f"SELECT f.ORIGIN_AIRPORT, f.DESTINATION_AIRPORT, "
                                     f"COUNT(*) AS total_flights, "
                                     f"COUNT(CASE WHEN f.DEPARTURE_DELAY > 0 THEN 1 END) AS delayed_flights, "
                                     f"a1.LATITUDE AS origin_lat, a1.LONGITUDE AS origin_lon, "
                                     f"a2.LATITUDE AS dest_lat, a2.LONGITUDE AS dest_lon "
                                     f"FROM flights f "
                                     f"JOIN airports a1 ON f.ORIGIN_AIRPORT = a1.IATA_CODE "
                                     f"JOIN airports a2 ON f.DESTINATION_AIRPORT = a2.IATA_CODE "
                                     f"WHERE f.ORIGIN_AIRPORT = '{origin}' AND f.DESTINATION_AIRPORT = '{destination}' "
                                     f"GROUP BY f.ORIGIN_AIRPORT, f.DESTINATION_AIRPORT;")

    results = data_manager._execute_query(QUERY_PERCENTAGE_ON_ROUTE_MAP, {})
    df = pd.DataFrame(results)
    if df.empty:
        print("No route data found.")
        return

    df['delay_percent'] =(df['delayed_flights'] / df['total_flights']) * 10

    us_map = folium.Map(location=[39.8283, -98.5795], zoom_start=4)

    df['origin_lat'] = pd.to_numeric(df['origin_lat'], errors='coerce')
    df['origin_lon'] = pd.to_numeric(df['origin_lon'], errors='coerce')
    df['dest_lat'] = pd.to_numeric(df['dest_lat'], errors='coerce')
    df['dest_lon'] = pd.to_numeric(df['dest_lon'], errors='coerce')

    for _, row in df.iterrows():
        if any(pd.isnull([row['origin_lat'], row['origin_lon'], row['dest_lat'], row['dest_lon']])):
            continue
        delay = row['delay_percent']
        if delay < 10:
            color = 'green'
        elif delay < 30:
            color = 'orange'
        else:
            color = 'red'

        folium.PolyLine(
            locations=[
                (row['origin_lat'], row['origin_lon']),
                (row['dest_lat'], row['dest_lon'])
                ],
            color=color,
            weight=2 + delay / 10,
            opacity=0.6,
            popup = f"{row['ORIGIN_AIRPORT']} -> {row['DESTINATION_AIRPORT']} ({delay:.1f}% delayed)"
        ).add_to(us_map)
    us_map.save('delays_map.html')
    print("Flight paths map saved to flight_paths_delayed_map.html.")


def show_delay_heatmap(data_manager):
    """
        Displays a heatmap of flight delays by route (origin to destination).
        Uses seaborn to visualize delay percentages.
        """
    results = data_manager._execute_query(QUERY_HEATMAP_DELAY, {})
    if not results:
        print("No data found.")
        return

    df = pd.DataFrame(results)

    df['DELAY_PERCENT'] = (df['delayed_flights'] / df['total_flights']) * 100

    pivot_table = df.pivot(index='ORIGIN_AIRPORT', columns='DESTINATION_AIRPORT', values='DELAY_PERCENT')

    plt.figure(figsize=(16, 12))
    sns.heatmap(pivot_table, cmap='YlOrRd', linewidths=0.5, linecolor='gray', square=False, cbar_kws={'label': 'Delay %'})
    plt.title('Percentage of Delayed Flights by route (Origin -> Destination)')
    plt.xlabel('Destination Airport')
    plt.ylabel('Origin Airport')
    plt.tight_layout()
    plt.show()


def show_delay_percent_by_hour(data_manager):
    """
        Displays a bar chart showing percentage of delayed flights by hour of departure.
        Filters invalid hours and uses matplotlib for plotting.
        """
    results = data_manager._execute_query(QUERY_PERCENTAGE_DELAYED_BY_HOUR, {})
    if not results:
        print("No results found.")
        return
    df = pd.DataFrame(results)

    df['DEPARTURE_TIME'] = df['DEPARTURE_TIME'].astype(str).str.zfill(4)
    df['HOUR'] = df['DEPARTURE_TIME'].str[:2].astype(int)

    df = df[df['HOUR'] < 24]

    df['DELAYED'] = df['DEPARTURE_DELAY'] > 0

    grouped = df.groupby('HOUR')['DELAYED'].agg(['sum', 'count'])
    grouped['PERCENT_DELAYED'] =(grouped['sum'] / grouped['count']) * 100

    plt.figure(figsize=(12, 6))
    plt.bar(grouped.index, grouped['PERCENT_DELAYED'], color='skyblue')
    plt.xlabel('Hour of day (0-23)')
    plt.ylabel('Percentage of Delayed Flights')
    plt.title('Percentage of Delayed Flights by Hour of Day')
    plt.xticks(range(0, 24))
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()


def show_delay_percent_by_airline(data_manager):
    """
        Displays a bar chart showing the percentage of delayed flights for each airline.
        """
    results = data_manager._execute_query(QUERY_PERCENTAGE_DELAYED_BY_AIRLINE, {})
    if not results:
        print("No results found.")
        return
    df = pd.DataFrame(results)

    if 'delayed_flights' not in df.columns or 'total_flights' not in df.columns:
        print("Invalid data format.")
        return

    df['delay_percentage'] = (df['delayed_flights'] / df['total_flights']) * 100

    # df.sort_values(by='delay_percentage', ascending=False, inplace=True)

    plt.figure(figsize=(12, 6))
    plt.bar(df['AIRLINE'], df['delay_percentage'], color='orange')
    plt.ylabel('Delay Percentage in %')
    plt.xlabel('Airline')
    plt.title('Percentage of Delayed Flights per Airline')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()


def delayed_flights_by_airline(data_manager):
    """
    Asks the user for a textual airline name (any string will work here).
    Then runs the query using the data object method "get_delayed_flights_by_airline".
    When results are back, calls "print_results" to show them to on the screen.
    """
    airline_input = input("Enter airline name: ")
    results = data_manager.get_delayed_flights_by_airline(airline_input)
    print_results(results, filter_delay_only=True)


def delayed_flights_by_airport(data_manager):
    """
    Asks the user for a textual IATA 3-letter airport code (loops until input is valid).
    Then runs the query using the data object method "get_delayed_flights_by_airport".
    When results are back, calls "print_results" to show them to on the screen.
    """
    valid = False
    while not valid:
        airport_input = input("Enter origin airport IATA code: ")
        # Valide input
        if airport_input.isalpha() and len(airport_input) == IATA_LENGTH:
            valid = True
    results = data_manager.get_delayed_flights_by_airport(airport_input)
    print_results(results, filter_delay_only=True)


def flight_by_id(data_manager):
    """
    Asks the user for a numeric flight ID,
    Then runs the query using the data object method "get_flight_by_id".
    When results are back, calls "print_results" to show them to on the screen.
    """
    valid = False
    while not valid:
        try:
            id_input = int(input("Enter flight ID: "))
        except Exception as e:
            print("Try again...")
        else:
            valid = True
    results = data_manager.get_flight_by_id(id_input)
    print_results(results)


def flights_by_date(data_manager):
    """
    Asks the user for date input (and loops until it's valid),
    Then runs the query using the data object method "get_flights_by_date".
    When results are back, calls "print_results" to show them to on the screen.
    """
    valid = False
    while not valid:
        try:
            date_input = input("Enter date in DD/MM/YYYY format: ")
            date = datetime.strptime(date_input, '%d/%m/%Y')
        except ValueError as e:
            print("Try again...", e)
        else:
            valid = True
    results = data_manager.get_flights_by_date(date.day, date.month, date.year)
    print_results(results)


def print_results(results, filter_delay_only=False):
    """
    Get a list of flight results (List of dictionary-like objects from SQLAachemy).
    Even if there is one result, it should be provided in a list.
    Each object *has* to contain the columns:
    FLIGHT_ID, ORIGIN_AIRPORT, DESTINATION_AIRPORT, AIRLINE, and DELAY.
    """
    print(f"Got {len(results)} results.")
    for result in results:
        # turn result into dictionary
        result = result._mapping

        # Check that all required columns are in place
        try:
            """if result['DELAY'] is None or int(result['DELAY']) <= 0:
                continue"""
            delay = int(result['DELAY'])  if result['DELAY'] is not None else 0   # If delay columns is NULL, set it to 0
            origin = result['ORIGIN_AIRPORT']
            dest = result['DESTINATION_AIRPORT']
            airline = result['AIRLINE']

            if filter_delay_only and delay <= 0:
                continue

        except (ValueError, sqlalchemy.exc.SQLAlchemyError) as e:
            print("Error showing results: ", e)
            return

        # Different prints for delayed and non-delayed flights
        if delay > 0:
            print(f"{result['ID']}. {origin} -> {dest} by {airline}, Delay: {delay} Minutes")
        else:
            print(f"{result['ID']}. {origin} -> {dest} by {airline}")


def show_menu_and_get_input():
    """
    Show the menu and get user input.
    If it's a valid option, return a pointer to the function to execute.
    Otherwise, keep asking the user for input.
    """
    print("Menu:")
    for key, value in FUNCTIONS.items():
        print(f"{key}. {value[1]}")

    # Input loop
    while True:
        try:
            choice = int(input())
            if choice in FUNCTIONS:
                return FUNCTIONS[choice][0]
        except ValueError as e:
            pass
        print("Try again...")

"""
Function Dispatch Dictionary
"""
FUNCTIONS = {   1: (flight_by_id, "Show flight by ID"),
                2: (flights_by_date, "Show flights by date"),
                3: (delayed_flights_by_airline, "Delayed flights by airline"),
                4: (delayed_flights_by_airport, "Delayed flights by origin airport"),
                5: (show_delay_percent_by_airline, "Plot delay percentage by airline"),
                6: (show_delay_percent_by_hour, "Plot delay percentage by hour"),
                7: (show_delay_heatmap, "Plot delay heatmap"),
                8: (show_delay_lines_on_route_map, "Plot delay route map"),
                9: (quit, "Exit")
             }


def main():
    # Create an instance of the Data Object using our SQLite URI
    data_manager = data.FlightData(SQLITE_URI)

    # The Main Menu loop
    while True:
        choice_func = show_menu_and_get_input()
        choice_func(data_manager)


if __name__ == "__main__":
    main()