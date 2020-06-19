
import re
import csv
import datetime as dt
from pymongo import MongoClient



def read_data(csv_file, db):

    """
    Loading data into a database from a CSV-file

    """
    with open(csv_file, encoding='utf-8') as file_csv:
        
        reader = csv.DictReader(file_csv, delimiter=',')
        list_of_artists = list(reader)
        list_for_db = []

        for i, artist in enumerate(list_of_artists, 1):

            each_event = {'_id': i,
                           'Artist': artist['Исполнитель'],
                           'Price': int(artist['Цена']),
                           'Place': artist['Место'],
                           'Date': dt.datetime.strptime('2020 ' + artist['Дата'], '%Y %d.%m')}

            list_for_db.append(each_event)

        db.insert_many(list_for_db)

    return "\nThe data was successfully imported."


def find_cheapest(db):

    """
    Sorting tickets by price ascending
    Documentation: https://docs.mongodb.com/manual/reference/method/cursor.sort/

    """
    tickets = list(db.find().sort('Цена', 1))

    for ticket in tickets:
        print(ticket)

    return "\nOur tickets were sorted by their price."


def find_by_name(name, db):

    """
    Finding tickets by an artist's name (including under- and sublines, e.g. "Seconds to");
    and returning them by prices ascending

    """
    regex = re.compile('\w?\s?'+name+'\w?\s?', re.IGNORECASE)
    result = list(db.find({'Исполнитель': regex}).sort('Цена', 1))

    for line in result:
        print(line)

    return "\nOur artists are sorted accordingly."


if __name__ == '__main__':

    """
    Main program bloque

    """

    client = MongoClient()
    netology_db = client['netology']
    artists_collection = netology_db['artists']
    artists_collection.drop()
    
    read_data('artists.csv', artists_collection)

    print('Sorted by the cheapest option:\n')
    find_cheapest(artists_collection)

    print('Searched by name of artists:\n')
    find_by_name('Seconds to', artists_collection)
