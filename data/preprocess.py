import pandas as pd

from utils.index import remove_columns
from utils.index import rename_columns
from utils.index import reorder_columns
from utils.index import sort_columns
from utils.index import remove_singletons
from utils.index import rename_columns_items


class PreProcess:
    def __init__(self, database):
        match database:
            case 'foursquare':
                self.foursquare()
            case 'gowalla':
                self.gowalla()
            case _:
                return

    def foursquare(self):
        df = pd.read_csv('dataset/foursquare/listening_history.csv')

        df = remove_columns(df, ['latitude', 'longitude'])
        df = rename_columns(df, {"time": "timestamp"})
        df = reorder_columns(df)
        df = sort_columns(df)
        df = remove_singletons(df, 30, 'foursquare')
        df = rename_columns_items(df)

        df.to_csv('dataset/foursquare/result.csv', index=False)

    def gowalla(self):
        df = pd.read_csv('dataset/gowalla/listening_history.txt', sep='\t', names=['user', 'timestamp', 'latitude', 'longitude', 'location'], nrows=200000)

        df = remove_columns(df, ['latitude', 'longitude'])
        df = reorder_columns(df)
        df = sort_columns(df)
        df = remove_singletons(df, 30, 'gowalla')
        df = rename_columns_items(df)

        df.to_csv('dataset/gowalla/result.csv', index=False)
