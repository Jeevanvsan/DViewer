from flask import Flask, render_template, request
import pandas as pd

app = Flask(__name__)

# Load the CSV file into a DataFrame
df = pd.read_csv('inputs/1.csv')

@app.route('/')
def index():
    data = df.to_html(classes='table table-striped', index=False)
    return render_template('table.html', data=data)

@app.route('/filter', methods=['POST'])
def filter_data():
    column = request.form['column']
    value = request.form['value']
    filtered_df = df[df[column] == value]
    data = filtered_df.to_html(classes='table table-striped', index=False)
    return render_template('table.html', data=data)

@app.route('/sort', methods=['POST'])
def sort_data():
    column = request.form['column']
    ascending = request.form['ascending'] == 'True'
    sorted_df = df.sort_values(by=column, ascending=ascending)
    data = sorted_df.to_html(classes='table table-striped', index=False)
    return render_template('table.html', data=data)

@app.route('/aggregate', methods=['POST'])
def aggregate_data():
    column = request.form['column']
    operation = request.form['operation']
    if operation == 'sum':
        result = df[column].sum()
    elif operation == 'mean':
        result = df[column].mean()
    elif operation == 'max':
        result = df[column].max()
    elif operation == 'min':
        result = df[column].min()
    else:
        result = 'Invalid operation'
    return render_template('aggregate.html', result=result, column=column, operation=operation)

if __name__ == '__main__':
    app.run(debug=True)
