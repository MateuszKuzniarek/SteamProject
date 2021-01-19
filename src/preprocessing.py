import csv
import time

import requests


def get_app_dict(applist_file_name):
    applist_file = open('../resources/' + applist_file_name, encoding="utf-8")
    applist_csv_file = csv.DictReader(applist_file, delimiter=',')
    app_dict = dict()
    for row in applist_csv_file:
        app_dict[row['name']] = row['appid']
    applist_file.close()
    return app_dict


def get_users_genres(file_name, dict_file_name):
    file = open('../resources/' + file_name, encoding="utf-8")
    csv_file = csv.DictReader(file, delimiter=',')
    genres_dict = get_genres_dict(dict_file_name)
    users_genres = dict()
    for row in csv_file:
        if row['game_name'].lower() in genres_dict and genres_dict[row['game_name'].lower()] != ['']:
            if row['user_id'] in users_genres:
                users_genres[row['user_id']] = users_genres[row['user_id']] + (genres_dict[row['game_name'].lower()])
            else:
                users_genres[row['user_id']] = genres_dict[row['game_name'].lower()]
    file.close()
    return users_genres


def write_users_genres_to_file(file_name, users_genres):
    file = open('../resources/' + file_name, "w")
    for user in users_genres:
        unique_genres = list(set(users_genres[user]))
        genres_str = ",".join(unique_genres)
        file.write(genres_str + "\n")
    file.close()


def convert_data(data_file, dict_file, destination_file):
    users_genres = get_users_genres(data_file, dict_file)
    write_users_genres_to_file(destination_file, users_genres)


def get_request_response(url, parameters=None):
    response = requests.get(url=url, params=parameters)

    if response:
        return response.json()
    else:
        # response is none usually means too many requests. Wait and try again
        print('No response, waiting 10 seconds...')
        time.sleep(10)
        print('Retrying.')
        return get_request_response(url, parameters)


def get_applist_spy():
    file = open('../resources/applist_spy.csv', "w", encoding="utf-8")
    file.write('appid,name\n')
    for i in range(0, 42):
        parameters = {"request": "all", "page": i}
        response = get_request_response(url="https://steamspy.com/api.php", parameters=parameters)
        for app in response:
            file.write(str(app) + ',' + response[app]['name'].lower() + '\n')
    file.close()


def get_applist():
    response = get_request_response(url="https://api.steampowered.com/ISteamApps/GetAppList/v2/")
    file = open('../resources/applist.csv', "w", encoding="utf-8")
    file.write('appid,name\n')
    for app in response['applist']['apps']:
        file.write(str(app['appid']) + ',' + app['name'].lower() + '\n')
    file.close()


def get_games_in_data_set(data_set_file):
    file = open('../resources/' + data_set_file, encoding="utf-8")
    csv_file = csv.DictReader(file, delimiter=',')
    games_set = set()
    for row in csv_file:
        if row['action_type'] == 'play':
            games_set.add(row['game_name'].lower())
    file.close()
    return games_set


def get_genres_from_steam(app_id):
    parameters = {"appids": app_id}
    game_data = get_request_response("https://store.steampowered.com/api/appdetails", parameters=parameters)
    genres = game_data[app_id]['data']['genres']
    genres = list(map(lambda x: x['description'], genres))
    return genres


def get_genres_from_steam_spy(app_id):
    parameters = {"request": "appdetails", "appid": app_id}
    game_data = get_request_response("https://steamspy.com/api.php", parameters=parameters)
    genres = game_data['genre'].strip().split(',')
    return genres


def create_genres_dict(data_set_file, dict_file_path):
    games_set = get_games_in_data_set(data_set_file)
    dict_file = open('../resources/' + dict_file_path, "w", encoding="utf-8")
    dict_file.write('game_name,genres')
    app_dict = get_app_dict('applist.csv')
    app_dict_spy = get_app_dict('applist_spy.csv')
    number_of_games = len(games_set)
    i = 0
    for game in games_set:
        if i % 100 == 0:
            print(str(i) + '/' + str(number_of_games))
        try:
            if game in app_dict:
                app_id = app_dict[game]
            else:
                app_id = app_dict_spy[game]
            genres = get_genres_from_steam_spy(app_id)
            dict_file.write(game + '\t' + ','.join(genres) + '\n')
        except:
            print(game + ' not in app list')
        i = i + 1
    dict_file.close()


def get_genres_dict(dict_file_name):
    file = open('../resources/' + dict_file_name, encoding="utf-8")
    csv_file = csv.DictReader(file, delimiter='\t')
    genres_dict = dict()
    for row in csv_file:
        genres_dict[row['game_name']] = list(map(lambda x: x.lstrip(), row['genres'].split(',')))
    file.close()
    return genres_dict


if __name__ == "__main__":
    #get_applist_spy()
    #get_applist()
    #create_genres_dict('steam-200k.csv', 'games_dict3-steamspy_different_applist.txt')
    convert_data('steam-200k.csv', 'games_dict3-steamspy_different_applist.txt', 'users_genres.txt')
