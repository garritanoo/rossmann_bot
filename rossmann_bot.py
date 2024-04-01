import pandas as pd
import json
import requests
from flask import Flask, request, Response


# # Informações sobre o bot
# https://api.telegram.org/bot7131396413:AAFbXA9tqZmpO8AuTJkY0gp9D8LZQCa3vPk/getMe

# # get updates
# https://api.telegram.org/bot7131396413:AAFbXA9tqZmpO8AuTJkY0gp9D8LZQCa3vPk/getUpdate

# # enviando mensagens
# https://api.telegram.org/bot7131396413:AAFbXA9tqZmpO8AuTJkY0gp9D8LZQCa3vPk/sendMessage?chat_id=&text=Hi Jubinha

# # webhook
# https://api.telegram.org/bot7131396413:AAFbXA9tqZmpO8AuTJkY0gp9D8LZQCa3vPk/setWebhook?url=https://02a79c40bed0d8.lhr.life

# constantes
TOKEN = '7131396413:AAFbXA9tqZmpO8AuTJkY0gp9D8LZQCa3vPk'

def send_message(chat_id, text):
    url = f'https://api.telegram.org/bot{TOKEN}'
    url = url + f'sendMessage?chat_id={chat_id}'
    
    r = requests.post(url, json={'text': text})
    print(f'Status Code: {r.status_code}')
    
    return None

def load_dataset(store_id):
    # Carregando dados de teste
    df10 = pd.read_csv('datasets/raw/test.csv')
    df_store_raw = pd.read_csv('datasets/raw/store.csv', low_memory=False)

    # Mesclando dataset de test e store
    df_test = pd.merge(df10, df_store_raw, how='left', on='Store')

    # Escolhendo lojas para predição
    df_test = df_test[df_test['Store'] == store_id]

    if not df_test.empty():
        ## Removendo dias que a loja ficaram fechados
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop('Id', axis=1)

        # Convertendo o dataframe para o formato JSON
        data = json.dumps(df_test.to_dict(orient='records'))
    else:
        data = 'error'
    
    return data

def predict(data):
    # API CALL
    url = 'https://rossmann-telegram-bot-1.onrender.com'
    header = {'Content-type': 'application/json'}
    data = data

    r = requests.post(url, data=data, headers=header)
    print(f'Status Code {r.status_code}')

    d1 = pd.DataFrame(r.json(), columns=r.json()[0].keys())
    
    return d1

def parse_message(message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['chat']['id']
    
    store_id = store_id.replace('/', '')
    
    try:
        store_id = int(store_id)
        
    except ValueError:
        send_message('O Store ID está errado')
        store_id = 'error'
    
    return chat_id, store_id

# Inicialização da API
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])

def index():
    if request.method == 'POST':
        message = request.get_json()
        
        chat_id, store_id = parse_message(message)
        
        if store_id != 'error':
            # load data
            data = load_dataset(store_id)
            
            if data != 'error':
            
                # prediction
                d1 = predict(data)
                
                # calculation
                d2 = d1[['store', 'prediction']].groupby('store').sum().reset_index()
                msg = (f'A Loja { d2.loc["store"].values[0] } venderá { d2.loc["prediction"].values[0] } nas próximas 6 semanas')
                
                # send message
                send_message(chat_id, msg)
                return Response('Ok', status=200)
                
            else:
                send_message(chat_id, 'A loja não está válida')
        else:
            send_message(chat_id, 'Store ID está errado.')
            return Response('Ok', status=200)
    else:
        return '<h1>Rossmann Telegram Bot</h1>'

if __name__ == 'main':
    app.run('0.0.0.0', PORT=5000)

        