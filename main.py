import pandas as pd
from google.oauth2 import service_account
# pip install pandas_gbq

# Прописываем адрес к файлу с данными по сервисному аккаунту и получаем credentials для доступа к данным
credentials = service_account.Credentials.from_service_account_file(
    'qstn-800bc-9627f0e9646c.json')

# Формируем запрос и получаем количество вопросов с тегом "pandas", сгруппированные по дате создания
query = '''
SELECT event_name, event_params
FROM `qstn-800bc.analytics_231248605.events_20200720`
WHERE event_name="select_item" LIMIT 9000;
'''
# Указываем идентификатор проекта
project_id = 'qstn-800bc'


# Выполняем запрос с помощью функции ((https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_gbq.html read_gbq)) в pandas, записывая результат в dataframe
df = pd.read_gbq(query, project_id=project_id, credentials=credentials)

def correct_key_search (x):
    i = 0
    q = len(x)
    item_id = '' # номер вопроса
    ga_session_id = '' # уникальный идентификатор сессии
    term = '' # ответ пользователя
    ga_session_number = '' # номер сессии у пользователя
    while i != q:
        cheacker = x[i]['key']
        if cheacker == "ga_session_number":
            ga_session_number = x[i]['value']
            ga_session_number = ga_session_number['int_value']
        if cheacker == 'item_id':
            item_id = x[i]['value']
            item_id = item_id['string_value']
        if cheacker == 'ga_session_id':
            ga_session_id = x[i]['value']
            ga_session_id = ga_session_id['int_value']
        if cheacker == 'term':
            term = x[i]['value']
            term = term['string_value']
        if item_id != '' and ga_session_id != '' and term != '':
            return item_id + ';' + str(ga_session_id) + ';' + term + ';' + str(ga_session_number)
        i = i + 1
    return False

i = 0
df_len = len(df.event_params)

while i != df_len:
    my_string = df.event_params[i]
    w = correct_key_search(my_string)
    if w != False:
        print(w)
        f = open("test.txt", "a")
        f.write(w + '\n')
        f.close()
    i = i + 1

print('Kolichestvo strok:', i)


# данные в таком виде:
# {
#      'key': 'firebase_event_origin',
#      'value':
#             {
#              'string_value': 'app',
#              'int_value': None,
#              'float_value': None,
#              'double_value': None
#              }
#     },
#     {
#      'key': 'item_name',
#      'value':
#             {
#              'string_value': 'question_answer',
#              'int_value': None,
#              'float_value': None,
#              'double_value': None
#              }
#     }
