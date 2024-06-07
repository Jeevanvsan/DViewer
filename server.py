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


app = Flask(__name__)
CORS(app)

app.config['SECRET_KEY'] = 'secret'
app.config['TEMPLATES_AUTO_RELOAD'] = True
app._static_folder = "static"
app.jinja_env.variable_start_string = '[['
app.jinja_env.variable_end_string = ']]'

socketio = SocketIO(app,async_mode='eventlet')


    
@app.route('/')
def index():
    main_ob = main()
    settings = main_ob.get_source_data()

    return render_template('index.html',settings=settings) 

@app.route('/get_data/<connection_name>/<name>')
def get_data(connection_name, name):
    main_ob = main()
    df = main_ob.read_data(connection_name,name)
    data = df.head(50).to_html(classes='table',table_id="data_tbl", index=False)
    
    return jsonify({'html': data})


if __name__ == '__main__':

    PORT = 8082
    webbrowser.open_new_tab(f'http://127.0.0.1:{PORT}/')
    socketio.run(app,debug=False,port = PORT)
    print("server started...")





   