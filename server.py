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
import pandas as pd
import shutil

app = Flask(__name__)
CORS(app)

app.config['SECRET_KEY'] = 'secret'
app.config['TEMPLATES_AUTO_RELOAD'] = True
app._static_folder = "static"
app.jinja_env.variable_start_string = '[['
app.jinja_env.variable_end_string = ']]'

socketio = SocketIO(app)


table_cache = {}

filters = {}
selected_cols = {}



    
@app.route('/')
def index():
      return render_template('index.html') 

@app.route('/main')
def main_():
    global table_cache, filters, selected_cols
    table_cache = {}
    filters = {}
    selected_cols = {}

    if os.path.exists('fi'):
        shutil.rmtree('fi')
        os.mkdir('fi')

    from main import main
    main_ob = main()

    
    settings = main_ob.get_source_data()
    return jsonify(settings=settings)

@app.route('/get_data/<connection_name>/<name>/<source>')
def get_data(connection_name, name,source):
    from main import main
    main_ob = main()
        
    data = ''
    table_cache['name'] = name
    table_cache['connection_name'] = connection_name
    table_cache['source'] = source
    fid = f"{connection_name}_{source}_{name}"


    if fid not in filters:
        filters[fid] = {}

    if fid not in selected_cols:
        selected_cols[fid] = {}


    if fid in table_cache:
        data = table_cache[f'{fid}']

    else:
        df = main_ob.read_data(connection_name,name,source)
        pd.to_pickle(df, f"fi/{name}.pkl")
        data = df.head(100).to_html(classes='table',table_id="data_tbl", index=False)
        table_cache[f'{fid}'] = data
    
    return jsonify({'html': data,'filters': filters[fid],'selected_cols': selected_cols[fid]})
    # return jsonify({'html': data,'filters': filters[fid]})

@app.route('/filter/<logic1_op>/<logic1_val>/<column_name>/<id>')
def filter(logic1_op, logic1_val,column_name,id):
    from main import main
    main_ob = main()
    name = table_cache['name'] 
    connection_name = table_cache['connection_name'] 
    source = table_cache['source'] 
    fid = f"{connection_name}_{source}_{name}"
    
    
    filters[fid][id] = {"logic1_op": logic1_op, "logic1_val": logic1_val,"column_name": column_name,"condition": "condition"}

    filtered_df = main_ob.apply_filters(filters,fid,name)

    data = filtered_df.head(100).to_html(classes='table',table_id="data_tbl", index=False)
    table_cache[f'{fid}'] = data
    return jsonify({'html': data})


@app.route('/filter_remove/<id>')
def filter_remove(id):
    from main import main
    main_ob = main()
    name = table_cache['name'] 
    connection_name = table_cache['connection_name'] 
    source = table_cache['source'] 
    fid = f"{connection_name}_{source}_{name}"

    
    if fid in filters:
        if id in filters[fid]:
            filters[fid].pop(id)
    

    filtered_df = main_ob.apply_filters(filters,fid,name)


    data = filtered_df.head(100).to_html(classes='table',table_id="data_tbl", index=False)
    table_cache[f'{fid}'] = data

    return jsonify({'html': data})

@app.route('/column_filter/<cols>',methods=["GET", "POST"])
def column_filter(cols):
    name = table_cache['name'] 
    connection_name = table_cache['connection_name'] 
    source = table_cache['source'] 
    fid = f"{connection_name}_{source}_{name}"
    if request.method == "POST":
        jsonData = request.get_json()
        print(jsonData)
        selected_cols[fid] = jsonData[fid]

        
    
    # print(jsonify(cols))
     
    data = 'done'

    return jsonify({'html': data})


@app.route('/run_query',methods=["GET", "POST"])
def run_query():
    from main import main
    main_ob = main()
    global rtn_data
    if request.method == "POST":
        jsonData = request.get_json()
        # print(js)
        df = main_ob.query_sheet(jsonData)
        if not isinstance(df,dict):
            rtn_data = df.to_html(classes='table',table_id="data_tbl_query", index=False)
        else:
            rtn_data = df['error']
        return jsonify({'html': rtn_data})


if __name__ == '__main__':

    PORT = 8082
    webbrowser.open_new_tab(f'http://127.0.0.1:{PORT}/')
    socketio.run(app,debug=False,port = PORT)
    print("server started...")





   