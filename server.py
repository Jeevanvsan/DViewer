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

    with open('configs/int_config.json','r') as int_config:
        int_conf = json.load(int_config)


    settings = main_ob.get_source_data()
    return jsonify(settings=settings,int_conf = int_conf)

@app.route('/get_data/<connection_name>/<name>/<source>')
def get_data(connection_name, name,source):
    from main import main
    main_ob = main()
        
    data = ''
    struct = ''
    table_cache['name'] = name
    table_cache['connection_name'] = connection_name
    table_cache['source'] = source
    fid = f"{connection_name}_{source}_{name}"


    if fid not in filters:
        filters[fid] = {}

    if fid not in selected_cols:
        selected_cols[fid] = {}


    if fid in table_cache:
        data = table_cache[f'{fid}']['data']
        struct = table_cache[f'{fid}']['struct']
    
    elif os.path.exists(f"fi/{connection_name}/{name}.parquet"):
        data_pd = pd.read_parquet(f"fi/{connection_name}/{name}.parquet")
        data = data_pd.head(100).to_html(classes='table',table_id="data_tbl", index=False)
        struct = pd.read_parquet(f"fi/{connection_name}/struct/{name}.parquet").head(100).to_html(classes='table',table_id="struct_data_tbl", index=False)
        data_pd.to_excel(f'fi/{connection_name}/{name}.xlsx', index=False)
        table_cache[f'{fid}'] = {'data':data,"struct":struct}

    else:
        df = main_ob.read_data(connection_name,name,source)
        pd.to_pickle(df['data'], f"fi/{name}.pkl")
        df['data'].to_excel(f'fi/{connection_name}/{name}.xlsx', index=False)
        data = df['data'].head(100).to_html(classes='table',table_id="data_tbl", index=False)
        struct = df['struct'].head(100).to_html(classes='table',table_id="struct_data_tbl", index=False)
        table_cache[f'{fid}'] = {'data':data,"struct":struct}
    
    return jsonify({'html': data,'struct':struct,'filters': filters[fid],'selected_cols': selected_cols[fid]})
    # return jsonify({'html': data,'filters': filters[fid]})

@app.route('/filter/<logic1_op>/<logic1_val>/<column_name>/<id>')
def filter(logic1_op, logic1_val,column_name,id):
    #print(logic1_op, logic1_val,column_name,id)
    from main import main
    main_ob = main()
    name = table_cache['name'] 
    connection_name = table_cache['connection_name'] 
    source = table_cache['source'] 
    fid = f"{connection_name}_{source}_{name}"
    
    
    filters[fid][id] = {"logic1_op": logic1_op, "logic1_val": logic1_val,"column_name": column_name,"condition": "condition"}

    filtered_df = main_ob.apply_filters(filters,fid,name,connection_name)

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
    

    filtered_df = main_ob.apply_filters(filters,fid,name,connection_name)


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
        selected_cols[fid] = jsonData[fid]

        
    
    # #print(jsonify(cols))

    data = 'done'

    return jsonify({'html': data})


@app.route('/run_query',methods=["GET", "POST"])
def run_query():
    from main import main
    main_ob = main()
    global rtn_data
    if request.method == "POST":
        jsonData = request.get_json()
        # #print(js)
        df = main_ob.query_sheet(jsonData)
        if not isinstance(df,dict):
            df.to_excel(f'fi/DViewer.xlsx', index=False)
            rtn_data = df.to_html(classes='table',table_id="data_tbl_query", index=False)
        else:
            rtn_data = df['error']
        return jsonify({'html': rtn_data})

@app.route('/xl_download')
def xl_download():
    return send_file('fi/DViewer.xlsx', as_attachment=True)

@app.route('/upload_file',methods=['POST'])
def upload_file():
    files = request.files.getlist("file")

    connection_data = request.form.get('connection')
    connection = json.loads(connection_data)

    conn_name = connection['name']
    src = connection['source']

    with open('configs/settings.yaml', 'r') as f:
        settings = yaml.safe_load(f)

    rtn = False
    file_names = []

    out = 'output'
    file_list = None
   
    # if os.path.exists('uploads'):
    #     shutil.rmtree('uploads')
    # if os.path.exists('uploads') is False:
    #     os.mkdir('uploads')

    
    for file in files:
        file_names.append(file.filename)
        zip_path = os.path.join('inputs', file.filename)
        file.save(zip_path)

    if not settings:
        settings = {}

    settings.update({conn_name:{}})

    if src in ['csv','parquet','xlsx']:
        settings[conn_name]['files'] = file_names

    settings[conn_name]['name'] = conn_name
    
    settings[conn_name]['source'] = src
    

    with open('configs/settings.yaml', 'w') as f:
        yaml.safe_dump(settings, f,sort_keys=False)

    return 'done'

if __name__ == '__main__':

    PORT = 8082
    webbrowser.open_new_tab(f'http://127.0.0.1:{PORT}/')
    socketio.run(app,debug=False,port = PORT)
    #print("server started...")

