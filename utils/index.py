from datetime import timedelta
import pandas as pd
import os


def reorder_columns(df):
    return df[['user', 'location', 'timestamp']]


def remove_columns(df, chosed_columns):
    return df.drop(columns=chosed_columns)


def sort_columns(df):
    return df.sort_values(by=['user', 'timestamp'])


def rename_columns(df, renamed_columns):
    return df.rename(columns=renamed_columns)


def rename_columns_items(df):
    print('Adicionando string nos identificadores')

    vocab = sorted(set(df.location.tolist()))
    song2ix = {u: i for i, u in enumerate(vocab, 1)}
    df['location'] = df.location.apply(lambda song: song2ix[song])

    for index, row in df.iterrows():
        user_id = row['user']
        location_id = row['location']

        df.loc[index, 'user'] = "u{}".format(user_id)
        df.loc[index, 'location'] = "l{}".format(location_id)

    print('Processo de adicao de string finalizado')

    return df


def sessionize_user(df, session_time, database):
    df['timestamp'] = df['timestamp'].astype('datetime64')
    df['dif'] = df['timestamp'].diff()
    df['session'] = df.apply(
        lambda x: 'NEW_SESSION' if x.dif >= timedelta(minutes=session_time) else 'SAME_SESSION', axis=1)

    # Gera a divisao entre dois usuarios diferentes
    s_no = 0
    l_u = ''
    f = open('dataset/{}/session_listening_history.csv'.format(database), 'w+')
    print(','.join(['user', 'location', 'timestamp', 'session']), file=f)
    print('Sessionized "%s" data file: %s' % (database, 'dataset/{}/session_listening_history.csv'.format(database)))
    for row in df.values:
        if s_no == 0:
            l_u = row[0]
        if (row[4] == 'NEW_SESSION' and l_u == row[0]) or (l_u != row[0]):
            s_no += 1
        row[3] = 's{}'.format(s_no)
        l_u = row[0]
        row[2] = str(row[2])
        print(','.join(map(str, row[:-1])), file=f)

    return pd.read_csv('dataset/{}/session_listening_history.csv'.format(database))


def remove_singletons(df, session_time, database):
    print('Removendo singletons')

    index_to_remove = []

    previous_user = 0
    previous_session_value = 'NEW_SESSION'

    df = sessionize_user(df, session_time, database)

    size = len(df.index)
    for i in range(size):

        actual_user = df['user'][i]
        actual_session_value = df['session'][i]

        if i == range(size)[-1]:
            previous_user = df['user'][i-1]
            previous_session_value = df['session'][i-1]

            if actual_user != previous_user:
                print('Usuario {} no tempo {} foi removido'.format(df['user'][i], df['timestamp'][i]))
                index_to_remove.append(i)

            if actual_user == previous_user and actual_session_value != previous_session_value:
                print('Usuario {} no tempo {} foi removido'.format(df['user'][i], df['timestamp'][i]))
                index_to_remove.append(i)
            break

        next_user = df['user'][i + 1]
        next_session_value = df['session'][i + 1]

        if i == 0:
            if actual_user != next_user:
                print('Usuario {} no tempo {} foi removido'.format(df['user'][i], df['timestamp'][i]))
                index_to_remove.append(i)

            if actual_user == next_user and actual_session_value != next_session_value:
                print('Usuario {} no tempo {} foi removido'.format(df['user'][i], df['timestamp'][i]))
                index_to_remove.append(i)

            previous_user = actual_user
            previous_session_value = actual_session_value
            continue

        if (actual_user != previous_user and actual_user == next_user) and actual_session_value != next_session_value:
            print('Usuario {} no tempo {} foi removido'.format(df['user'][i], df['timestamp'][i]))
            index_to_remove.append(i)

        elif actual_user != previous_user and actual_user != next_user:
            print('Usuario {} no tempo {} foi removido'.format(df['user'][i], df['timestamp'][i]))
            index_to_remove.append(i)

        elif actual_session_value != previous_session_value and actual_session_value != next_session_value:
            print('Usuario {} no tempo {} foi removido'.format(df['user'][i], df['timestamp'][i]))
            index_to_remove.append(i)

        previous_user = actual_user
        previous_session_value = actual_session_value

    df = df.drop(index_to_remove)
    df = df.drop(columns=['session'])
    print('Singletons removidos com sucesso')

    os.remove('dataset/{}/session_listening_history.csv'.format(database))

    return df
