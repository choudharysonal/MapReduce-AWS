from flask import Flask,render_template, request
import os
from MRFrequencyCount import MRPeopleFrequencyCount
from mrjob_1 import MRJobCount
from mrjob_2 import MRJobCount
import time
import getpass
import pandas as pd

app = Flask(__name__)

def get_data_1(file_name, col1,col2):
    file_path = os.path.join(app.root_path,'datasets',file_name)
    data = pd.read_csv(file_path)

    data = data[[col1,col2]]

    cur_file_path = os.path.join(app.root_path, 'datasets', 'qdata.csv')

    data.to_csv(cur_file_path, sep=',', index=False, header=False)

    return cur_file_path

def get_data_2(file_name, col1,col2,col3):
    file_path = os.path.join(app.root_path,'datasets',file_name)
    data = pd.read_csv(file_path)

    data = data[[col1,col2,col3]]

    cur_file_path = os.path.join(app.root_path, 'datasets', 'qdata.csv')

    data.to_csv(cur_file_path, sep=',', index=False, header=False)

    return cur_file_path

# To check for Zip Code equal to 19103
@app.route('/',methods=['GET','POST'])
def runmr():
    if request.method == 'GET':
        return render_template('first_page.html')
    elif request.method == 'POST':
        data = {}
        data_path = get_data_1('data.csv','Gender','ZipCode')

        # 5 Mappers and 1 Reducer
        mr_job = MRPeopleFrequencyCount(args=['-r', 'local', '--jobconf', 'mapred.map.tasks={}'.format(5),
                                            '--jobconf',' mapred.reduce.tasks={}'.format(1),'{}'.format(data_path)])
        with mr_job.make_runner() as runner:
            start_time = time.time()
            runner.run()
            end_time =time.time()

            for line in runner.stream_output():
                key,value = mr_job.parse_output_line(line)
                data[key] = value

        max_time = (end_time - start_time)

        proper_data = " <h2> Number of People: {0} <h2>".format(data['People'])
        return render_template('first_page.html', datainfo=proper_data, timeinfo=max_time)

@app.route('/query1',methods=['GET','POST'])
def query_one():
    if request.method == 'GET':
        return render_template('query_1.html')
    elif request.method == 'POST':
        lower = request.form['height1']
        upper = request.form['height2']
        zipcode = request.form['zip']

        data_dict = {}

        data_path = get_data_1('data.csv','ZipCode','Centimeters')


        # Using 8 Mappers and 2 Reducers
        mr_job = MRJobCount(args=['-r', 'local',
                                  '--jobconf', 'mapred.map.tasks={0}'.format(8),
                                  '--jobconf', 'mapred.reduce.tasks={0}'.format(2),
                                  '--jobconf', 'my.job.lower_height={0}'.format(lower),
                                  '--jobconf', 'my.job.upper_height={0}'.format(upper),
                                  '--jobconf', 'my.job.zip_code={0}'.format(zipcode),
                                  '{}'.format(data_path)])

        with mr_job.make_runner() as runner:
            start_time = time.time()
            runner.run()
            end_time = time.time()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                data_dict[key] = value

        max_time = (end_time - start_time)

        proper_data = " <h2> Number of People Between Height - {0} and {1} and ZipCode {2} is ---> {3} <h2>".format(lower,upper,zipcode,data_dict[zipcode])
        return render_template('query_1.html', dataInfo=proper_data, timeinfo=max_time)

# Query Processing
@app.route('/query2', methods=['GET', 'POST'])
def query_two():
    if request.method == 'GET':
        return render_template('query_2.html')
    elif request.method == 'POST':
        lower = request.form['height1']
        upper = request.form['height2']
        zipcode = request.form['zip']
        mapper = int(request.form['Mappers'])
        reducer = int(request.form['Reducers'])

        data_dict = {}

        data_path = get_data_2('data.csv', 'GivenName', 'ZipCode', 'Centimeters')

        # Using 8 Mappers and 2 Reducers
        mr_job = MRJobCount(args=['-r', 'local',
                                  '--jobconf', 'mapred.map.tasks={0}'.format(mapper),
                                  '--jobconf', 'mapred.reduce.tasks={0}'.format(reducer),
                                  '--jobconf', 'my.job.lower_height={0}'.format(lower),
                                  '--jobconf', 'my.job.upper_height={0}'.format(upper),
                                  '--jobconf', 'my.job.zip_code={0}'.format(zipcode),
                                  '{}'.format(data_path)])

        with mr_job.make_runner() as runner:
            start_time = time.time()
            runner.run()
            end_time = time.time()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                data_dict[key] = value

        max_time = (end_time - start_time)

        data_row = data_dict['row']
        data_row = data_row[0]

        proper_data = ""
        for i in range(10):
            proper_data += "<h2> " + str(data_row[i]) + "</h2></br>"
            print(data_row[i])

        return render_template('query_2.html', dataInfo=proper_data, timeinfo=max_time)


if __name__ == '__main__':
    app.run(debug=True)
