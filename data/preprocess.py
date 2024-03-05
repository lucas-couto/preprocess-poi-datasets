import pandas as pd

from utils.index import sort_columns
from utils.index import remove_columns
from utils.index import rename_columns
from utils.index import reorder_columns
from utils.index import remove_singletons
from utils.index import rename_columns_items
from utils.index import convert_json_to_dataframe
from utils.index import convert_time_to_datetime64


class PreProcess:
    def __init__(self):
        chosed_option = self.menu()

        match chosed_option:
            case 1:
                self.foursquare()
            case 2:
                self.gowalla()
            case 3:
                self.yelp()
            case _:
                print("Saindo...")
                return

    def menu(self):
        print("Bem vindo ao pre-processamento do RNN-Embeddings!")
        print("Escolha qual base deseja utilizar:")
        print("1 - Foursquare")
        print("2 - Gowalla")
        print("3 - Yelp")
        print("0 - Sair")
        try:
            chosed_option = int(input("Digite o codigo desejado: "))
            return chosed_option
        except ValueError:
            print("Por favor, digite um codigo valido")
            print("\n")
            self.menu()

    def foursquare(self):
        print("\n...INICIANDO PRE-PROCESSAMENTO DA BASE FOURSQUARE...")
        df = pd.read_csv('dataset/foursquare/listening_history.csv')

        df = remove_columns(df, ['latitude', 'longitude'])
        df = rename_columns(df, {"time": "timestamp"})
        df = reorder_columns(df)
        df = sort_columns(df)
        df = remove_singletons(df, 30, 'foursquare')
        df = rename_columns_items(df)

        df.to_csv('dataset/foursquare/result.csv', index=False)

    def gowalla(self):
        print("\n...INICIANDO PRE-PROCESSAMENTO DA BASE GOWALLA...")

        df = pd.read_csv('dataset/gowalla/listening_history.txt', sep='\t',
                         names=['user', 'timestamp', 'latitude', 'longitude', 'location'])

        df = remove_columns(df, ['latitude', 'longitude'])
        df = reorder_columns(df)
        df = sort_columns(df)
        df = remove_singletons(df, 30, 'gowalla')
        df = rename_columns_items(df)

        df.to_csv('dataset/gowalla/result.csv', index=False)

    def yelp(self):
        print("\n...INICIANDO PRE-PROCESSAMENTO DA BASE YELP...")

        d = convert_json_to_dataframe(
            "dataset/yelp/listening_history.json",
            ['review_id', 'stars', 'useful', 'funny', 'cool', 'text'],
            {'user': 'user_id', 'location': 'business_id', 'timestamp': 'date'}
        )

        df = pd.DataFrame(data=d)

        df = sort_columns(df)
        df = convert_time_to_datetime64(df)
        df = remove_singletons(df, 30, 'yelp')

        df.to_csv("dataset/yelp/result.csv", index=False)
