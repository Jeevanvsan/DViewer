from flask import Flask, render_template, request , send_from_directory, redirect, url_for,flash,jsonify,send_file
from flask_cors import CORS
import os
import yaml
import subprocess
from datetime import datetime,timedelta
import webbrowser
import urllib.parse
from flask_socketio import SocketIO, emit
import json
from main import main
import pandas as pd
import shutil

app = Flask(__name__)
CORS(app)

app.config['SECRET_KEY'] = 'secret'
app.config['TEMPLATES_AUTO_RELOAD'] = True
app._static_folder = "static"
app.jinja_env.variable_start_string = '[['
app.jinja_env.variable_end_string = ']]'

socketio = SocketIO(app,async_mode='eventlet')

table_cache = {}

filters = {}
main_ob = main()

    
@app.route('/')
def index():
    if os.path.exists('fi'):
        shutil.rmtree('fi')
        os.mkdir('fi')

    settings = main_ob.get_source_data()
    return render_template('index.html',settings=settings) 

@app.route('/get_data/<connection_name>/<name>/<source>')
def get_data(connection_name, name,source):
    
        
    data = ''
    table_cache['name'] = name
    table_cache['connection_name'] = connection_name
    table_cache['source'] = source
    fid = f"{connection_name}_{source}_{name}"

    if fid not in filters:
        filters[fid] = {}


    if f'{connection_name}_{name}_{source}' in table_cache:
        data = table_cache[f'{connection_name}_{name}_{source}']

    else:
        df = main_ob.read_data(connection_name,name,source)
        pd.to_pickle(df, f"fi/{name}.pkl")
        data = df.head(100).to_html(classes='table',table_id="data_tbl", index=False)
        table_cache[f'{connection_name}_{name}_{source}'] = data
    
    return jsonify({'html': data,'filters': filters[fid]})

@app.route('/filter/<logic1_op>/<logic1_val>/<column_name>/<id>')
def filter(logic1_op, logic1_val,column_name,id):
    name = table_cache['name'] 
    connection_name = table_cache['connection_name'] 
    source = table_cache['source'] 
    fid = f"{connection_name}_{source}_{name}"
    
    
    filters[fid][id] = {"logic1_op": logic1_op, "logic1_val": logic1_val,"column_name": column_name,"condition": "condition"}

    filtered_df = main_ob.apply_filters(filters,fid,name)

    data = filtered_df.head(100).to_html(classes='table',table_id="data_tbl", index=False)
    table_cache[f'{connection_name}_{name}_{source}'] = data
    return jsonify({'html': data})


@app.route('/filter_remove/<id>')
def filter_remove(id):
    name = table_cache['name'] 
    connection_name = table_cache['connection_name'] 
    source = table_cache['source'] 
    fid = f"{connection_name}_{source}_{name}"

    
    if fid in filters:
        if id in filters[fid]:
            filters[fid].pop(id)
    

    filtered_df = main_ob.apply_filters(filters,fid,name)


    data = filtered_df.head(100).to_html(classes='table',table_id="data_tbl", index=False)
    table_cache[f'{connection_name}_{name}_{source}'] = data

    return jsonify({'html': data})


if __name__ == '__main__':

    PORT = 8082
    webbrowser.open_new_tab(f'http://127.0.0.1:{PORT}/')
    socketio.run(app,debug=False,port = PORT)
    print("server started...")





   