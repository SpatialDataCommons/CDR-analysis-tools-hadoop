import json
import matplotlib.pyplot as plt
import csv
import pandas
from matplotlib.widgets import TextBox


def json_file_to_object(json_file, encoding=''):
    if encoding == '':
        with open(json_file) as jf:
            return json.load(jf)
    else:
        with open(json_file, encoding=encoding) as jf:
            return json.load(jf)


def string_to_json(str_in):
    return json.loads(str_in)


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


def make_graph(xs, x_label, ys, y_label, header, filename, des_pair_1=None, des_pair_2=None, des_pair_3=None, des_pair_4=None):
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
        offset = 42 - 2*len(des_pair_1['value'])
        text1 = ''
        for i in range(0, offset):
            text1 += ' '
        text_box = TextBox(axbox, '', initial=text1 + des_pair_1['value'], color='orange', label_pad=0.005)
        text_box.disconnect_events()
    if des_pair_2 is not None:
        offset = 42 - 2*len(des_pair_2['value'])
        text2 = ''
        for i in range(0, offset):
            text2 += ' '
        plt.text(des_pair_2['text_x'],  des_pair_2['text_y'], des_pair_2['text'], transform=ax.transAxes)
        # plt.text(0.33, 1.27, des_pair_2['text'], transform=ax.transAxes)
        axbox = plt.axes([0.3, 0.87, 0.2, 0.04])
        text_box = TextBox(axbox, '', initial=text2 + des_pair_2['value'], color='blue')
        text_box.disconnect_events()
    if des_pair_3 is not None:
        offset = 42 - 2*len(des_pair_3['value'])
        text3 = ''
        for i in range(0, offset):
            text3 += ' '
        # plt.text(0.58, 1.27, des_pair_3['text'], transform=ax.transAxes)
        plt.text(des_pair_3['text_x'],  des_pair_3['text_y'], des_pair_3['text'], transform=ax.transAxes)
        axbox = plt.axes([0.5, 0.87, 0.2, 0.04])
        text_box = TextBox(axbox, '', initial=text3 + des_pair_3['value'], color='green')
        text_box.disconnect_events()
    if des_pair_4 is not None:
        offset = 42 - 2*len(des_pair_4['value'])
        text4 = ''
        for i in range(0, offset):
            text4 += ' '
        # plt.text(0.79, 1.27, des_pair_4['text'],  transform=ax.transAxes)
        plt.text(des_pair_4['text_x'],  des_pair_4['text_y'], des_pair_4['text'], transform=ax.transAxes)
        axbox = plt.axes([0.7, 0.87, 0.2, 0.04])
        text_box = TextBox(axbox, '', initial=text4 +des_pair_4['value'], color='red')
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
            if argument['output_no'] != -1:
                arguments_prep.append(argument['name'] + ' ' + argument['data_type'])
                arguments_con.append(argument['name'])
                if str.lower(argument['name']) == 'call_time':
                    arguments_con.append("from_unixtime(unix_timestamp(call_time ,'{time_format}'), 'yyyy-MM-dd hh:mm:ss') as call_time".format(time_format=config.input_file_time_format))
                else:
                    arguments_con.append(argument['name'])
                if argument['input_no'] != -1:
                    arguments_raw.append(argument['input_name'] + ' ' + argument['data_type'])

                    # TODO validate each field in the custom arguments and make sure their input_no is not -1
                    if 'custom' in argument:
                        arguments_map.append(argument['custom'] + ' as ' + argument['name'])
                    else:
                        arguments_map.append(argument['input_name'] + ' as ' + argument['name'])
                else:
                    # TODO validate each field in the custom arguments and make sure their input_no is not -1
                    if 'custom' in argument:
                        arguments_map.append(argument['custom'] + ' as ' + argument['name'])
                    else:
                        arguments_map.append('-1' + ' as ' + argument['name'])
            elif argument['input_no'] != -1:
                arguments_raw.append(argument['input_name'] + ' ' + argument['data_type'])
        if i == 0:
            data.arg_cdr_map, data.arg_cdr_raw, data.arg_cdr_prep, data.arg_cdr_con = \
                arguments_map, arguments_raw, arguments_prep, arguments_con
        else:
            data.arg_cell_map, data.arg_cell_raw, data.arg_cell_create = \
                arguments_map, arguments_raw, arguments_prep


if __name__ == '__main__':
    make_graph([1, 2, 3, 4], 'x', [1, 2, 3, 4], 'y', 'TEST', 'test')