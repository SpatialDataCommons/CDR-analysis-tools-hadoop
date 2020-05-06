import json
import matplotlib.pyplot as plt
import csv
import pandas
from matplotlib.widgets import TextBox
from codecs import open


mandatory_columns = [["UID", "IMEI", "IMSI", "CALL_TIME", "DURATION",
                     "CALL_TYPE", "NETWORK_TYPE", "CELL_ID", "LATITUDE", "LONGITUDE"],
                     ['CELL_ID', 'LATITUDE', 'LONGITUDE', 'ADMIN0',
                      'ADMIN1', 'ADMIN2', 'ADMIN3', 'ADMIN4', 'ADMIN5']]


def json_file_to_object(json_file, encoding=''):
    if encoding == '':
        with open(json_file) as jf:
            return json.load(jf)
    else:
        with open(json_file, encoding=encoding) as jf:
            return json.load(jf)


def string_to_json(str_in):
    return json.loads(str_in)


def sql_to_string(filename):
    path = "queries/" + filename
    sql = open(path, mode='r', encoding='utf-8-sig').read()
    return sql


def get_admin_units_from_mapping(cell_tower_mapping):
    admin_units = []
    admins = ['admin0', 'admin1', 'admin2', 'admin3', 'admin4', 'admin5']
    admins.reverse()
    for row in cell_tower_mapping:
        for admin in admins:
            if row['output_no'] != -1 and str.lower(row['name']) == admin:
                admin_units.append(row['name'])

    print('Result admin units = {}'.format(', '.join(admin_units)))
    return admin_units


def format_two_point_time(start, end):
    return round(end - start, 2)


def get_time_from_csv(file_loc):
    with open(file_loc) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 1:
                start_date = row[6]
                end_date = row[7]
                break
            line_count += 1

    start_date = pandas.Timestamp(start_date)
    start_m = start_date.month
    start_y = start_date.year
    end_date = pandas.Timestamp(end_date)
    end_m = end_date.month
    end_y = end_date.year

    print(start_date, start_m, start_y, end_date, end_m, end_y)
    print(start_date, end_date)

    result = dict()
    result['start_date'] = start_date
    result['start_m'] = start_m
    result['start_y'] = start_y
    result['end_date'] = end_date
    result['end_m'] = end_date.month
    result['end_y'] = end_date.year

    return result


def make_graph(xs, x_label, ys, y_label, header, filename, des_pair_1=None,
               des_pair_2=None, des_pair_3=None, des_pair_4=None):
    figure = plt.figure(figsize=(14, 11))

    font_dict = {
        'fontsize': 21,
        'fontweight': 'bold',
     }

    ax = figure.add_subplot(111)
    plt.title(header, fontdict=font_dict)
    plt.subplots_adjust(top=0.75)
    plt.grid(b=True)
    plt.plot(xs, ys)
    plt.ylabel(y_label)
    plt.xticks(rotation=90)
    plt.xlabel(x_label)

    if des_pair_1 is not None:
        plt.text(des_pair_1['text_x'],  des_pair_1['text_y'], des_pair_1['text'], transform=ax.transAxes)
        axbox = plt.axes([0.1, 0.87, 0.2, 0.04])
        offset = 60 - 2*len(des_pair_1['value'])
        text1 = ''
        for i in range(0, offset):
            text1 += ' '
        text_box = TextBox(axbox, '', initial=text1 + des_pair_1['value'], color='orange', label_pad=0.005)
        text_box.disconnect_events()
    if des_pair_2 is not None:
        offset = 60 - 2*len(des_pair_2['value'])
        text2 = ''
        for i in range(0, offset):
            text2 += ' '
        plt.text(des_pair_2['text_x'],  des_pair_2['text_y'], des_pair_2['text'], transform=ax.transAxes)
        # plt.text(0.33, 1.27, des_pair_2['text'], transform=ax.transAxes)
        axbox = plt.axes([0.3, 0.87, 0.2, 0.04])
        text_box = TextBox(axbox, '', initial=text2 + des_pair_2['value'], color='blue')
        text_box.disconnect_events()
    if des_pair_3 is not None:
        offset = 60 - 2*len(des_pair_3['value'])
        text3 = ''
        for i in range(0, offset):
            text3 += ' '
        # plt.text(0.58, 1.27, des_pair_3['text'], transform=ax.transAxes)
        plt.text(des_pair_3['text_x'],  des_pair_3['text_y'], des_pair_3['text'], transform=ax.transAxes)
        axbox = plt.axes([0.5, 0.87, 0.2, 0.04])
        text_box = TextBox(axbox, '', initial=text3 + des_pair_3['value'], color='green')
        text_box.disconnect_events()
    if des_pair_4 is not None:
        offset = 60 - 2*len(des_pair_4['value'])
        text4 = ''
        for i in range(0, offset):
            text4 += ' '
        # plt.text(0.79, 1.27, des_pair_4['text'],  transform=ax.transAxes)
        plt.text(des_pair_4['text_x'],  des_pair_4['text_y'], des_pair_4['text'], transform=ax.transAxes)
        axbox = plt.axes([0.7, 0.87, 0.2, 0.04])
        text_box = TextBox(axbox, '', initial=text4 + des_pair_4['value'], color='red')
        text_box.disconnect_events()

    plt.savefig(filename)


def extract_mapping_data(config, data):
    mappings = [config.cdr_data_layer, config.cdr_cell_tower]
    # Extract arguments
    for i in range(0, len(mappings)):
        arguments_map = []
        arguments_prep = []
        arguments_raw = []
        arguments_con = []
        for argument in mappings[i]:
            if str.upper(argument['name']) in mandatory_columns[i]:
                arguments_prep.append(argument['name'] + ' ' + argument['data_type'])
                arguments_con.append(argument['name'])
            if str.lower(argument['name']) == 'uid' and i == 0:
                arguments_con.append(argument['input_name'])
                arguments_prep.append(argument['input_name'] + ' ' + argument['data_type'])
            if argument['output_no'] != -1:
                if argument['input_no'] != -1:
                    arguments_raw.append(argument['input_name'] + ' ' + argument['data_type'])
                    if 'custom' in argument and argument['custom'] != '':
                        if str.lower(argument['name']) == 'call_time' and config.input_file_time_format != "":
                            arguments_map.append("from_unixtime(unix_timestamp({custom} "
                                                 ",'{time_format}'), 'yyyy-MM-dd hh:mm:ss') as call_time"
                                                 .format(custom=argument['custom'],
                                                         time_format=config.input_file_time_format))
                        else:
                            arguments_map.append(argument['custom'] + ' as ' + argument['name'])
                        if str.lower(argument['name']) == 'uid' and i == 0:
                            arguments_map.append(argument['input_name'] + ' as ' + argument['input_name'])
                    else:
                        if str.lower(argument['name']) == 'call_time' and config.input_file_time_format != "":
                            arguments_map.append("from_unixtime(unix_timestamp({custom} "
                                                 ",'{time_format}'), 'yyyy-MM-dd hh:mm:ss') as call_time"
                                                 .format(custom=argument['input_name'],
                                                         time_format=config.input_file_time_format))
                            print(arguments_map)
                        else:
                            arguments_map.append(argument['input_name'] + ' as ' + argument['name'])
                        if str.lower(argument['name']) == 'uid' and i == 0:
                            arguments_map.append(argument['input_name'] + ' as ' + argument['input_name'])
                else:
                    # input -1 output 1 for custom
                    if 'custom' in argument and argument['custom'] != '':
                        arguments_map.append(argument['custom'] + ' as ' + argument['name'])
                    # else =  cdr without custom or cell tower, this case insert -1 if it is a mandatory column
                    elif str.upper(argument['name']) in mandatory_columns[i]:
                        arguments_map.append('-1' + ' as ' + argument['name'])
                        print('Output ' + argument['name'] + ' ignored ')

            elif argument['input_no'] != -1:
                arguments_raw.append(argument['input_name'] + ' ' + argument['data_type'])
                if str.upper(argument['name']) in mandatory_columns[i]:
                    arguments_map.append('-1' + ' as ' + argument['name'])
                    print('Output ' + argument['name'] + ' ignored ')
            elif str.upper(argument['name']) in mandatory_columns[i]:
                # input -1 output -1 insert -1 if it is a mandatory column
                arguments_map.append('-1' + ' as ' + argument['name'])
                print('Output ' + argument['name'] + ' ignored ')

        if i == 0:
            data.arg_cdr_map, data.arg_cdr_raw, data.arg_cdr_prep, data.arg_cdr_con = \
                arguments_map, arguments_raw, arguments_prep, arguments_con
        else:
            data.arg_cell_map, data.arg_cell_raw, data.arg_cell_create = \
                arguments_map, arguments_raw, arguments_prep
            print(data.arg_cell_map, data.arg_cell_create)


if __name__ == '__main__':
    make_graph([1, 2, 3, 4], 'x', [1, 2, 3, 4], 'y', 'TEST', 'test')
