import pandas as pd
import json
import os
import requests
from flask import Flask, request, Response

# constantes
TOKEN = os.environ.get('TOKEN')

def send_message(chat_id, text):
    url = f'https://api.telegram.org/bot{TOKEN}/'
    url = url + f'sendMessage?chat_id={chat_id}'
    
    r = requests.post(url, json={'text': text})
    print(f'Status Code: {r.status_code}')
    
    return None

def load_dataset(store_id):
    # Carregando dados de teste
    df10 = pd.read_csv('datasets/raw/test.csv', low_memory=False)
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

def predict(df):
    # API CALL
    url = 'https://rossmann-store-predict.onrender.com/rossmann/predict'
    header = {'Content-type': 'application/json'}
    data = df

    r = requests.post(url, data=data, headers=header)
    print(f'Status Code {r.status_code}')

    d1 = pd.DataFrame(r.json(), columns=r.json()[0].keys())
    
    return d1

def parse_message(message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']
    
    store_id = store_id.replace('/', '')
    
    try:
        store_id = int(store_id)
        
    except ValueError:
        send_message(chat_id, 'O Store ID está errado')
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
            # carrega data
            data = load_dataset(store_id)
            
            if data != 'error':
            
                # prediction
                d1 = predict(data)            
                
                # calculation
                d2 = d1[['store', 'prediction']].groupby('store').sum().reset_index()
                
                # send message
                msg = (f"""A Loja { d2["store"].values[0] }
                          venderá { d2["prediction"].values[0] }
                          nas próximas 6 semanas""")
                
                send_message(chat_id, msg)
                return Response('Ok', status=200)
                
            else:
                send_message(chat_id, f'A loja {store_id} não é válida')
                return Response('Ok', status=200)
        
        else:
            send_message(chat_id, f'O Store ID {store_id} não existe.')
            return Response('Ok', status=200)
    else:
        return '<h1>Rossmann Telegram Bot</h1>'

if __name__ == 'main':
    port = os.environ.get('PORT', 5000)
    app.run(host='0.0.0.0', port=port)
