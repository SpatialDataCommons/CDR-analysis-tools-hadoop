import json
import csv
import matplotlib.pyplot as plt
import os
from hive_connector import HiveConnector
import helper as hp
import time
from datetime import datetime

months = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
          7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}


class CDRVisualizer:
    # TODO paramiterize the query
    def __init__(self, config, data):
        self.__dict__ = config.__dict__
        self.hive = HiveConnector(config)
        timer = time.time()
        print('Initilizing Hive')
        self.hive.initialize(config)
        print('Done. Time elapsed: {} seconds'.format(time.time() - timer))
        timer = time.time()
        print('Creating Tables')
        # self.hive.create_tables(config, data)
        print('Done create all tables. Time elapsed: {} seconds'.format(time.time() - timer))

    def calculate_data_statistics(self):
        disable = False
        for item in self.cdr_data_layer:
            if str.lower(item['name']) == 'call_time' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'uid' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'imei' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'imsi' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'cell_id' and item['output_no'] == -1:
                disable = True

        if not disable:
            cursor = self.hive.cursor
            imei = "count(distinct IMEI) as unique_imei, "
            imsi = "count(distinct IMSI) as unique_imsi, "
            for map in self.cdr_data_layer:
                if str.lower(map['input_name']) == 'imei' and str.lower(map['name']) == 'uid':
                    imei = ''
                elif str.lower(map['input_name']) == 'imsi' and str.lower(map['name']) == 'uid':
                    imsi = ''
            query = "select count(*) as total_records, " + \
                    "count(distinct to_date(call_time)) as total_days, " + \
                    "count(distinct uid) as unique_id, " + \
                    imei + \
                    imsi + \
                    "count(distinct cell_id) as unique_location_name, " + \
                    "min(to_date(call_time)) as start_date, " + \
                    "max(to_date(call_time))  as end_date " + \
                    "from {provider_prefix}_preprocess ".format(provider_prefix=self.provider_prefix)

            print('Calculating data statistics')
            timer = time.time()
            cursor.execute(query)
            print('Calculated data statistics. Elasped time: {} seconds'.format(time.time()-timer))
            print('Writing to {}/css_file_data_stat.csv'.format(self.csv_location))
            timer = time.time()
            with open("{}/css_file_data_stat.csv".format(self.csv_location), "w", newline='') as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(col[0] for col in cursor.description)
                for row in cursor:
                    writer.writerow(row)
            print('Successfully wrote to {}/css_file_data_stat.csv'.format(self.csv_location))
            print('Elapsed time: {} seconds'.format(time.time() - timer))
        else:
            print('Mapping for call_time, imsi, imei or uid is not sufficient. Ignored data statistic')

    def calculate_daily_statistic(self):
        disable = False
        for item in self.cdr_data_layer:
            if str.lower(item['name']) == 'network_type' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'call_type' and item['output_no'] == -1:
                disable = True
        if not disable:
            cursor = self.hive.cursor
            cursor.set_arraysize(200)
            results = []
            file_location = '{}/css_file_data_stat.csv'.format(self.csv_location)
            imei = "count(distinct IMEI) as unique_imei, "
            imsi = "count(distinct IMSI) as unique_imsi, "
            replicate = False
            for map in self.cdr_data_layer:
                if str.lower(map['input_name']) == 'imei' and str.lower(map['name']) == 'uid':
                    imei = ''
                    replicate = True
                elif str.lower(map['input_name']) == 'imsi' and str.lower(map['name']) == 'uid':
                    imsi = ''
                    replicate = True

            time_dict = hp.get_time_from_csv(file_location, replicate)
            start_date, end_date = time_dict['start_date'], time_dict['end_date']
            timer = time.time()
            print('Calculating Daily Statistics')
            # FOR CASE ALL
            query = "SELECT to_date(call_time) as date, 'ALL' as call_type, 'ALL' as network_type, COUNT(*) as total_records, " + \
                    "COUNT(DISTINCT TO_DATE(call_time)) as total_days, " + \
                    'COUNT(DISTINCT uid) as unique_id, ' + \
                    imei + imsi + \
                    'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                    "FROM {provider_prefix}_consolidate_data_all where to_date(pdt) between to_date('{start_date}') and to_date('{end_date}') " \
                        .format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date) + \
                    "GROUP BY to_date(call_time)"

            query += ' UNION '

            query += "SELECT to_date(call_time) as date, call_type, 'ALL' as network_type, COUNT(*) as total_records, " + \
                     "COUNT(DISTINCT TO_DATE(call_time)) as total_days, " + \
                     'COUNT(DISTINCT uid) as unique_id, ' + \
                     imei + imsi + \
                     'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                     "FROM {provider_prefix}_consolidate_data_all where to_date(pdt) between to_date('{start_date}') and to_date('{end_date}') " \
                         .format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date) + \
                     "GROUP BY to_date(call_time), call_type"

            query += ' UNION '

            query += "SELECT to_date(call_time) as date, 'ALL' as call_type,  network_type, COUNT(*) as total_records, " + \
                     "COUNT(DISTINCT TO_DATE(call_time)) as total_days, " + \
                     'COUNT(DISTINCT uid) as unique_id, ' + \
                     imei + imsi + \
                     'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                     "FROM {provider_prefix}_consolidate_data_all where to_date(pdt) between to_date('{start_date}') and to_date('{end_date}') " \
                         .format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date) + \
                     "GROUP BY to_date(call_time), network_type"

            query += ' UNION '

            query += "SELECT to_date(call_time) as date, call_type, network_type, COUNT(*) as total_records, " + \
                     "COUNT(DISTINCT TO_DATE(call_time)) as total_days, " + \
                     'COUNT(DISTINCT uid) as unique_id, ' + \
                     imei + imsi + \
                     'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                     "FROM {provider_prefix}_consolidate_data_all where to_date(pdt) between to_date('{start_date}') and to_date('{end_date}') " \
                         .format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date) + \
                     "GROUP BY to_date(call_time), call_type, network_type ORDER BY date ASC, call_type ASC, network_type DESC"

            cursor.execute(query)
            print('Query completed. Time elapsed: {} seconds.'.format(time.time() - timer))
            timer = time.time()
            description = cursor.description
            rows = []
            for row in cursor:
                rows.append(row)
            results += rows
            print('Writing into the graph for daily statistics')
            file_path = '{}/css_provider_data_stat_daily.csv'.format(self.csv_location)
            if os.path.exists(file_path):
                os.remove(file_path)

            with open(file_path, "w", newline='') as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(col[0][4:] for col in description)
                for row in results:
                    writer.writerow(row)
            print('Successfully wrote to file css_provider_data_stat_daily.csv')
        else:
            print('Mapping for network_type or call_type is not sufficient. Ignored daily statistics')

        # TODO output to a graph -> put in daily_cdr multiple lines
        print('Querying daily cdr by call_type')
        timer = time.time()
        query = "SELECT to_date(call_time) as date, 'ALL' as call_type, 'ALL' as network_type, COUNT(*) as total_records, " + \
                "COUNT(DISTINCT TO_DATE(call_time)) as total_days, " + \
                'COUNT(DISTINCT uid) as unique_id, ' + \
                imei + imsi + \
                'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                "FROM {provider_prefix}_consolidate_data_all where to_date(pdt) between to_date('{start_date}') and to_date('{end_date}') " \
                    .format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date) + \
                "GROUP BY to_date(call_time)"
        query += ' UNION '

        query += "SELECT to_date(call_time) as date, call_type, 'ALL' as network_type, COUNT(*) as total_records, " + \
                 "COUNT(DISTINCT TO_DATE(call_time)) as total_days, " + \
                 'COUNT(DISTINCT uid) as unique_id, ' + \
                 imei + imsi + \
                 'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                 "FROM {provider_prefix}_consolidate_data_all where to_date(pdt) between to_date('{start_date}') and to_date('{end_date}') " \
                     .format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date) + \
                 "GROUP BY to_date(call_time), call_type ORDER BY  call_type ASC, network_type DESC"
        cursor.execute(query)
        print('Query completed. Time elapsed: {} seconds.'.format(time.time() - timer))
        rows = []
        xs_all = set([])
        ys_all = []
        ys_data = []
        ys_voice_or_sms = []
        for row in cursor:
            rows.append(row)
            xs_all.add(row[0])
        xs_all = list(xs_all)
        xs_all.sort(key=lambda date: datetime.strptime(date, '%Y-%m-%d'))
        # find the day in rows and match, then extract ALL, DATA and VOICE/SMS
        print('Writing into the graph for daily cdr by call type')
        for day in xs_all:
            c_all = 0
            c_data = 0
            c_sms_voice = 0
            for row in rows:
                if row[0] == day:
                    if row[1] == 'ALL':
                        c_all += row[3]
                    elif row[1] == 'DATA':
                        c_data += row[3]
                    elif row[1] in ['VOICE', 'SMS']:
                        c_sms_voice += row[3]
            ys_all.append(c_all)
            ys_data.append(c_data)
            ys_voice_or_sms.append(c_sms_voice)

        figure = plt.figure(figsize=(14, 11))
        font_dict = {
            'fontsize': 21,
            'fontweight': 'bold',
        }

        ax = figure.add_subplot(111)
        plt.subplots_adjust(top=0.95)
        plt.grid(b=True)
        plt.plot(xs_all, ys_all)
        plt.plot(xs_all, ys_data)
        plt.plot(xs_all, ys_voice_or_sms)
        plt.ylabel('Total Records')
        plt.xticks(rotation=90)
        plt.xlabel('Date')
        plt.title('Daily CDR by call type', fontdict=font_dict)
        plt.legend(['ALL', 'DATA', 'VOICE and SMS'], loc='upper left')
        plt.savefig('{}/daily_cdr_by_call_type'.format(self.graph_location))
        print('Graph created successfully in {}/daily_cdr_by_call_type'.format(self.graph_location))

    def calculate_monthly_statistic(self):
        disable = False
        for item in self.cdr_data_layer:
            if str.lower(item['name']) == 'network_type' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'call_type' and item['output_no'] == -1:
                disable = True
        if not disable:
            cursor = self.hive.cursor
            results = []
            file_location = '{}/css_file_data_stat.csv'.format(self.csv_location)

            imei = "count(distinct IMEI) as unique_imei, "
            imsi = "count(distinct IMSI) as unique_imsi, "
            replicate = False

            for map in self.cdr_data_layer:
                if str.lower(map['input_name']) == 'imei' and str.lower(map['name']) == 'uid':
                    imei = ''
                    replicate = True
                elif str.lower(map['input_name']) == 'imsi' and str.lower(map['name']) == 'uid':
                    imsi = ''
                    replicate = True

            time = hp.get_time_from_csv(file_location, replicate)
            start_y, start_m, end_y, end_m = time['start_y'], time['start_m'], time['end_y'], time['end_m']
            print('### Calculating Monthly Statistics ###')
            # FOR CASE ALL
            query = "SELECT YEAR(call_time) as year, MONTH(call_time) as month  , 'ALL' as call_type, 'ALL' as network_type, COUNT(*) as total_records, " + \
                    "COUNT(DISTINCT TO_DATE(call_time)) as total_days, " + \
                    'COUNT(DISTINCT uid) as unique_id, ' + \
                    imei + imsi + \
                    'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                    "FROM {provider_prefix}_consolidate_data_all where (year(pdt) between {start_year} and {end_year}) " \
                        .format(provider_prefix=self.provider_prefix, start_year=start_y, end_year=end_y) + \
                    "and (MONTH(pdt) between {start_month} and {end_month}) GROUP BY YEAR(call_time), MONTH(call_time)" \
                        .format(start_month=start_m, end_month=end_m)

            query += ' UNION '

            query += "SELECT YEAR(call_time) as year, MONTH(call_time) as month, call_type, 'ALL' as network_type, COUNT(*) as total_records, " + \
                     "COUNT(DISTINCT TO_DATE(call_time)) as total_days, " + \
                     'COUNT(DISTINCT uid) as unique_id, ' + \
                     'COUNT(DISTINCT imei) as unique_imei, COUNT(DISTINCT imsi) unique_imsi, ' + \
                     'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                     "FROM {provider_prefix}_consolidate_data_all where (year(pdt) between {start_year} and {end_year}) " \
                         .format(provider_prefix=self.provider_prefix, start_year=start_y, end_year=end_y) + \
                     "and (MONTH(pdt) between {start_month} and {end_month}) GROUP BY YEAR(call_time), MONTH(call_time), call_type" \
                         .format(start_month=start_m, end_month=end_m)

            query += ' UNION '

            query += "SELECT YEAR(call_time) as year, MONTH(call_time) as month, 'ALL' as call_type,  network_type, COUNT(*) as total_records, " + \
                     "COUNT(DISTINCT TO_DATE(call_time)) as total_days, " + \
                     'COUNT(DISTINCT uid) as unique_id, ' + \
                     imei + imsi + \
                     'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                     "FROM {provider_prefix}_consolidate_data_all where (year(pdt) between {start_year} and {end_year}) " \
                         .format(provider_prefix=self.provider_prefix, start_year=start_y, end_year=end_y) + \
                     "and (MONTH(pdt) between {start_month} and {end_month}) GROUP BY YEAR(call_time), MONTH(call_time), network_type" \
                         .format(start_month=start_m, end_month=end_m)

            query += ' UNION '
            query += "SELECT YEAR(call_time) as year, MONTH(call_time) as month , call_type, network_type, COUNT(*) as total_records, " + \
                     "COUNT(DISTINCT TO_DATE(call_time)) as total_days, " + \
                     'COUNT(DISTINCT uid) as unique_id, ' + \
                     imei + imsi + \
                     'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                     "FROM {provider_prefix}_consolidate_data_all where (year(pdt) between {start_year} and {end_year}) " \
                         .format(provider_prefix=self.provider_prefix, start_year=start_y, end_year=end_y) + \
                     "and (MONTH(pdt) between {start_month} and {end_month}) GROUP BY YEAR(call_time), MONTH(call_time), call_type, network_type ".format(
                         start_month=start_m, end_month=end_m) + \
                     "ORDER BY year ASC, month ASC, call_type ASC, network_type DESC"

            cursor.execute(query)
            description = cursor.description
            results += cursor.fetchall()

            file_path = '{}/css_provider_data_stat_monthly.csv'.format(self.csv_location)
            if os.path.exists(file_path):
                os.remove(file_path)

            with open(file_path, "w", newline='') as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(col[0][4:] for col in description)
                for row in results:
                    writer.writerow(row)

            print('### Successfully wrote to file css_provider_data_stat_monthly.csv###')
            # TODO output to a graph
        else:
            print('Mapping for network_type or call_type is not sufficient. Ignored monthly statistics')

    # def calculate_frequent_locations(self):
    #     cursor = self.hive.cursor
    #     # join by cell_id and get its admin unit
    #     query = "SELECT * from {provider_prefix}_frequent_location order by uid, trank ASC".format(provider_prefix=self.provider_prefix)
    #
    #     cursor.execute(query)
    #     print(query)
    #
    #     accumulate = 0
    #     active_id = 0
    #     if self.input_file_have_header == 1:
    #         row_i = 1
    #     else:
    #         row_i = 0
    #     description = cursor.description
    #     print(description)
    #     rows = []
    #     for row in cursor:
    #         rows.append(row)
    #     print('FETCHED!')
    #     while row_i < len(rows):
    #         if rows[row_i][0] == active_id:
    #             if accumulate < self.frequent_location_percentage:
    #                 accumulate += rows[row_i][2]
    #                 row_i += 1
    #             else:
    #                 del rows[row_i]
    #         else:
    #             accumulate = rows[row_i][2]
    #             active_id = rows[row_i][0]
    #             row_i += 1
    #
    #     file_path = '{}/frequent_locations.csv'.format(self.csv_location)
    #     if os.path.exists(file_path):
    #         os.remove(file_path)
    #
    #     with open(file_path, "w", newline='') as outfile:
    #         writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
    #         writer.writerow(col[0] for col in description)
    #         for row in rows:
    #             writer.writerow(row)
    #
    # def calculate_frequent_locations_night(self):
    #     cursor = self.hive.cursor
    #     # join by cell_id and get its admin unit
    #     query = "select a1.uid, count(a1.uid) as count, " + \
    #             "count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid) * 100 as percentage, " + \
    #             "ROW_NUMBER() OVER(PARTITION BY a1.uid order by count(a1.uid) DESC) as rank" + \
    #             ", concat(a2.latitude, ' : ', a2.longitude) as unique_location " + \
    #             "from {provider_prefix}_consolidate_data_all a1 ".format(provider_prefix=self.provider_prefix) + \
    #             "JOIN {provider_prefix}_cell_tower_data_preprocess a2 ".format(
    #                 provider_prefix=self.provider_prefix) + \
    #             "ON(a1.cell_id = a2.cell_id) group by a1.uid, " + \
    #             "concat(a2.latitude, ' : ', a2.longitude) " + \
    #             "order by a1.uid, count DESC "
    #
    #     cursor.execute(query)
    #     accumulate = 0
    #     active_id = 0
    #     row_i = 1
    #     description = cursor.description
    #     rows = cursor.fetchall()
    #     while row_i < len(rows):
    #         if rows[row_i][0] == active_id:
    #             if accumulate < self.frequent_location_percentage:
    #                 accumulate += rows[row_i][2]
    #                 row_i += 1
    #             else:
    #                 if rows[row_i][2] != rows[row_i - 1]:
    #                     del rows[row_i]
    #                 else:
    #                     row_i += 1
    #         else:
    #             accumulate = rows[row_i][2]
    #             active_id = rows[row_i][0]
    #             row_i += 1
    #
    #     file_path = '{}/frequent_locations.csv'.format(self.csv_location)
    #     if os.path.exists(file_path):
    #         os.remove(file_path)
    #
    #     with open(file_path, "w", newline='') as outfile:
    #         writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
    #         writer.writerow(col[0] for col in description)
    #         for row in rows:
    #             writer.writerow(row)
    #
    #
    # def calculate_frequent_locations_day(self):
    #     cursor = self.hive.cursor
    #     query = "select a1.uid, count(a1.uid) as count, " + \
    #             "count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid) * 100 as percentage, " + \
    #             "ROW_NUMBER() OVER(PARTITION BY a1.uid order by count(a1.uid) DESC) as rank" + \
    #             ", concat(a2.latitude, ' : ', a2.longitude) as unique_location " + \
    #             "from {provider_prefix}_consolidate_data_all a1 ".format(provider_prefix=self.provider_prefix) + \
    #             "JOIN {provider_prefix}_cell_tower_data_preprocess a2 ".format(
    #                 provider_prefix=self.provider_prefix) + \
    #             "ON(a1.cell_id = a2.cell_id) group by a1.uid, " + \
    #             "concat(a2.latitude, ' : ', a2.longitude) " + \
    #             "order by a1.uid, count DESC "
    #
    #     cursor.execute(query)
    #     accumulate = 0
    #     active_id = 0
    #     row_i = 1
    #     description = cursor.description
    #     rows = cursor.fetchall()
    #     while row_i < len(rows):
    #         if rows[row_i][0] == active_id:
    #             if accumulate < self.frequent_location_percentage:
    #                 accumulate += rows[row_i][2]
    #                 row_i += 1
    #             else:
    #                 if rows[row_i][2] != rows[row_i - 1]:
    #                     del rows[row_i]
    #                 else:
    #                     row_i += 1
    #         else:
    #             accumulate = rows[row_i][2]
    #             active_id = rows[row_i][0]
    #             row_i += 1
    #
    #     file_path = '{}/frequent_locations.csv'.format(self.csv_location)
    #     if os.path.exists(file_path):
    #         os.remove(file_path)
    #
    #     with open(file_path, "w", newline='') as outfile:
    #         writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
    #         writer.writerow(col[0] for col in description)
    #         for row in rows:
    #             writer.writerow(row)

    def calculate_zone_population(self):
        cursor = self.hive.cursor
        admin_units = ['ADMIN0', 'ADMIN1', 'ADMIN2', 'ADMIN3', 'ADMIN4', 'ADMIN5']
        admin_units_active = []
        geo_jsons_active = []
        name_columns = []
        geo_json_filename = []
        for col in self.cdr_cell_tower:
            if col['name'] in admin_units:
                admin_units_active.append(col['name'])
                if col['geojson_filename'] == '':
                    geo_jsons_active.append('')
                else:
                    geo_jsons_active.append(hp.json_file_to_object(col['geojson_filename'], encoding="utf-8"))
                name_columns.append(col['geojson_col_name'])
                geo_json_filename.append(col['geojson_filename'])
        geo_i = 0
        for admin_unit in admin_units_active:
            timer = time.time()
            print('Calculating zone population for {admin}'.format(admin=admin_unit))
            query = (
                    "select lv as {level}, sum(td.count) as count_activities, count(td.uid) as count_unique_ids from "
                    "(select a1.{level} as lv, " +
                    "count(a1.{level}) as count, a2.uid as uid " +
                    "from {provider_prefix}_cell_tower_data_preprocess a1 ".format(
                        provider_prefix=self.provider_prefix) +
                    "JOIN {provider_prefix}_consolidate_data_all a2 ".format(
                        provider_prefix=self.provider_prefix) +
                    "on (a1.cell_id = a2.cell_id) " +
                    "group by a1.{level}, a2.uid) td group by lv").format(level=admin_unit)

            cursor.execute(query)
            description = cursor.description
            print('Successfully zone population for {admin}. Elapsed time: {time} seconds'.format(admin=admin_unit, time=time.time()-timer))
            timer = time.time()
            rows = []
            cursor.set_arraysize(200)
            for row in cursor:
                rows.append(row)

            file_path = '{csv_location}/zone_based_aggregations_level_{level}.csv'.format(
                csv_location=self.csv_location, level=admin_unit)
            if geo_jsons_active[geo_i] != '':
                print('Merging dictionary object to geojson')
                for f in range(0, len(geo_jsons_active[geo_i]['features'])):

                    if geo_jsons_active[geo_i]['features'][f]['properties'][name_columns[geo_i]] == 'Kochi Ken':
                        geo_jsons_active[geo_i]['features'][f]['properties']['num_population'] = 'Kochi Ken'
                print('Merging completed. Time elapsed: {} seconds'.format(time.time() - timer))
                timer = time.time()
            else:
                print('No geojson file input')

            if os.path.exists(file_path):
                os.remove(file_path)

            print('Writing result zone population to {}'.format(file_path))
            with open(file_path, "w", newline='') as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(col[0] for col in description)
                for row in rows:
                    writer.writerow(row)
            print('Writing completed. Time elapsed: {} seconds'.format(time.time() - timer))
            timer = time.time()
            print('Writing into geojson file ' + geo_json_filename[geo_i][:-4] + '_joined_' + admin_unit + '.json')
            if geo_json_filename[geo_i] != '':
                with open(geo_json_filename[geo_i][:-4] + '_joined_' + admin_unit + '.json', "w",
                          newline='') as outfile:
                    json.dump(geo_jsons_active[geo_i], outfile)
                print('Writing completed. Time elapsed: {} seconds'.format(time.time() - timer))
            geo_i += 1

    def calculate_user_date_histogram(self):
        cursor = self.hive.cursor

        query = ("select explode(histogram_numeric(active_days, 10)) as active_day_bins from "
                 "(select count(*) as active_days, td.uid from "
                 "(select year(to_date(call_time)) as year, "
                 "month(to_date(call_time)) as month, "
                 "day(to_date(call_time)) as day, uid "
                 "from {provider_prefix}_consolidate_data_all group by uid, year(to_date(call_time)), "
                 "month(to_date(call_time)), "
                 "day(to_date(call_time)) order by year, month, day, uid) td "
                 "group by td.uid) td2".format(provider_prefix=self.provider_prefix))
        timer = time.time()
        print('Calculating data histogram')
        cursor.execute(query)
        description = cursor.description
        rows = cursor.fetchall()
        print('Calculating completed. Time elapsed: {} seconds'.format(time.time() - timer))

        file_path = '{}/histogram.csv'.format(self.csv_location)
        print('Writing into {}'.format(file_path))
        if os.path.exists(file_path):
            os.remove(file_path)

        with open(file_path, "w", newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(col[0] for col in description)
            for row in rows:
                writer.writerow(row)
        print('Writing completed.')

        xs = []
        ys = []
        for row in rows:
            json_data = hp.string_to_json(row[0])
            xs.append(json_data['x'])
            ys.append(json_data['y'])

        plt.subplots_adjust(left=0.15)
        plt.bar(xs, ys, align='center')  # A bar chart
        plt.xlabel('Active Day Bins')
        plt.ylabel('Count No. Unique Ids')
        print('Plotting graph and writing into {}/user_data_histogram.png'.format(self.graph_location))
        plt.savefig('{}/user_data_histogram.png'.format(self.graph_location))
        print('Done.')

    def calculate_summary(self):
        cursor = self.hive.cursor
        tb_1_description = ('All Data', 'Value')
        tb_2_description = ('Statistics',)
        output_1_rows = []
        print('Calculating total records')
        timer = time.time()
        q_total_records = 'select count(*) as total_records from {provider_prefix}_consolidate_data_all'.format(
            provider_prefix=self.provider_prefix)
        cursor.execute(q_total_records)
        des = cursor.description
        row_total_records = cursor.fetchall()
        row_total_records = (des[0][0], row_total_records[0][0])
        output_1_rows.append(row_total_records)
        total_records = row_total_records[1]
        print('Successfully calculated total records. Total records: {recs} records \nElapsed time: {time} seconds'
              .format(recs=total_records, time=time.time()-timer))
        timer = time.time()
        print('Calculating total unique uids')
        q_total_uids = 'select count(*) as total_uids from (select distinct uid from {provider_prefix}_consolidate_data_all) td'.format(
            provider_prefix=self.provider_prefix)
        cursor.execute(q_total_uids)
        des = cursor.description
        row_total_uids = cursor.fetchall()
        row_total_uids = (des[0][0], row_total_uids[0][0])
        output_1_rows.append(row_total_uids)
        total_uids = row_total_uids[1]
        print('Successfully calculated total unique uids. Total unique ids: {ids} ids  \nElapsed time: {time} seconds'.format(
            ids=total_uids,time=time.time()-timer))
        timer = time.time()

        print('Calculating total days')
        q_total_days = " select count(*) as total_days, min(dates) as start_date, max(dates) as end_date from (select  to_date(" \
                       "call_time) as dates " \
                       "from {provider_prefix}_consolidate_data_all " \
                       "group by to_date(call_time)) td" \
            .format(provider_prefix=self.provider_prefix)

        cursor.execute(q_total_days)
        des = cursor.description
        row_total_days = cursor.fetchall()

        total_days = row_total_days[0][0]
        start_yyyy_mm_dd = row_total_days[0][1].split('-')
        end_yyyy_mm_dd = row_total_days[0][2].split('-')

        start_day = start_yyyy_mm_dd[2]
        start_month = start_yyyy_mm_dd[1]
        start_year = start_yyyy_mm_dd[0]

        end_day = end_yyyy_mm_dd[2]
        end_month = end_yyyy_mm_dd[1]
        end_year = end_yyyy_mm_dd[0]

        if int(total_days) == 0:
            row_total_days = (des[0][0], row_total_days[0][0])
        elif int(total_days) == 1:
            row_total_days = (des[0][0],
                              str(row_total_days[0][0]) + ' ({} {} {})'.format(int(start_day), months[int(start_month)],
                                                                               start_year))
        elif int(total_days) >= 2:
            if start_year == end_year:
                # same year
                if start_month == end_month:
                    # no same day because it is gonna be total_days 1, which is done above
                    row_total_days = (des[0][0], str(row_total_days[0][0]) +
                                      ' ({}-{} {} {})'.format(int(start_day), int(end_day),
                                                              months[int(start_month)], start_year))
                else:
                    # for different months, same or different day will also be outputted
                    row_total_days = (des[0][0], str(row_total_days[0][0]) +
                                      ' ({} {}-{} {} {})'.format(int(start_day), months[int(start_month)],
                                                                 int(end_day), months[int(end_month)], start_year))

            else:
                # for the more-than-one-year case, everything is displayed
                row_total_days = (des[0][0], str(row_total_days[0][0]) +
                                  ' ({} {} {}-{} {} {})'.format(int(start_day), months[int(start_month)],
                                                                start_year, int(end_day), months[int(end_month)],
                                                                end_year))

        output_1_rows.append(row_total_days)
        print('Successfully calculated total days. Total days: {days} \nElapsed time: {time} seconds'
              .format(days=row_total_days[1], time=time.time()-timer))

        print('Calculating daily average location name')

        q_total_locations = "select count(*) as count_unique_locations from (select distinct a2.latitude, a2.longitude from {provider_prefix}_consolidate_data_all a1 " \
                            "join {provider_prefix}_cell_tower_data_preprocess a2 " \
                            "on(a1.cell_id = a2.cell_id)) td".format(provider_prefix=self.provider_prefix)

        cursor.execute(q_total_locations)
        des = cursor.description
        row_total_locations = cursor.fetchall()
        row_total_locations = (des[0][0], row_total_locations[0][0])
        output_1_rows.append(row_total_locations)
        total_unique_locations = row_total_locations[1]
        print('Successfully calculated daily average location name. Daily average location names : {locs} '
              '\nElapsed time: {time} seconds'
              .format(locs=row_total_locations[1], time=time.time() - timer))
        timer = time.time()
        # average usage per day
        print('Calculating average daily usage')
        output_2_rows = []
        row_avg_daily_usage = ('average_usage_per_day', float(total_records / total_days))
        output_2_rows.append(row_avg_daily_usage)
        print('Successfully calculated average daily usage. Daily average usages : {uses} '
              '\nElapsed time: {time} seconds'
              .format(uses=row_avg_daily_usage[1], time=time.time() - timer))
        timer = time.time()
        # avg voice call per day

        disable = False
        for item in self.cdr_data_layer:
            if str.lower(item['name']) == 'network_type' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'call_type' and item['output_no'] == -1:
                disable = True

        if not disable:
            print('Calculating average daily voice call usage')
            q_avg_daily_voice = "select count(*)/{total_records} as average_daily_voice from {provider_prefix}_consolidate_data_all where call_type = 'VOICE'".format(
                provider_prefix=self.provider_prefix, total_records=total_records)
            cursor.execute(q_avg_daily_voice)
            des = cursor.description
            row_avg_daily_voice = cursor.fetchall()
            row_avg_daily_voice = (des[0][0], row_avg_daily_voice[0][0])
            output_2_rows.append(row_avg_daily_voice)
            print('Successfully calculated average daily voice call usage. Daily average sms usages : {uses} '
                  '\nElapsed time: {time} seconds'
                  .format(uses=row_avg_daily_voice[1], time=time.time() - timer))
            timer = time.time()
            # avg sms per day
            print('Calculating average daily sms usage')
            q_avg_daily_sms = "select count(*)/{total_records} as average_daily_sms from {provider_prefix}_consolidate_data_all where call_type = 'SMS'".format(
                provider_prefix=self.provider_prefix, total_records=total_records)
            cursor.execute(q_avg_daily_sms)
            des = cursor.description
            row_avg_daily_sms = cursor.fetchall()
            row_avg_daily_sms = (des[0][0], row_avg_daily_sms[0][0])
            output_2_rows.append(row_avg_daily_sms)
            print('Successfully calculated average daily sms usage. Daily average sms usages : {uses} '
                  '\nElapsed time: {time} seconds'
                  .format(uses=row_avg_daily_sms[1], time=time.time() - timer))
            timer = time.time()
        else:
            print('call_type or network_type not completed. Ignored daily usage of sms and voice call')

        # avg unique cell id

        disable = False
        for item in self.cdr_data_layer:
            if str.lower(item['name']) == 'cell_id' and item['output_no'] == -1:
                disable = True

        if not disable:
            print('Calculating average daily unique cell id')
            q_avg_daily_unique_cell_id = "select count(*)/{total_records} as average_daily_unique_cell_id from (select distinct cell_id from {provider_prefix}_consolidate_data_all) td" \
                .format(provider_prefix=self.provider_prefix, total_records=total_records)

            cursor.execute(q_avg_daily_unique_cell_id)
            des = cursor.description
            row_avg_daily_unique_cell_id = cursor.fetchall()
            row_avg_daily_unique_cell_id = (des[0][0], row_avg_daily_unique_cell_id[0][0])
            output_2_rows.append(row_avg_daily_unique_cell_id)
            print('Successfully calculated average daily unique cell id')
            print('Successfully calculated average daily unique cel id.'
                  '\nElapsed time: {time} seconds'
                  .format(time=time.time() - timer))
            timer = time.time()

            print('Calculating average daily district')
            q_avg_daily_district = (
                "select count(distinct a1.{level})/{total_records} as average_district_per_day " \
                "from {provider_prefix}_cell_tower_data_preprocess a1 " \
                "JOIN {provider_prefix}_consolidate_data_all a2 " \
                "on (a1.cell_id = a2.cell_id)") \
                .format(provider_prefix=self.provider_prefix, level='ADMIN1', total_records=total_records)

            cursor.execute(q_avg_daily_district)
            des = cursor.description
            row_avg_daily_district = cursor.fetchall()
            row_avg_daily_district = (des[0][0], row_avg_daily_district[0][0])
            output_2_rows.append(row_avg_daily_district)
            print('Successfully calculated average daily district. Daily average districts : {dists} '
                  '\nElapsed time: {time} seconds'
                  .format(dists=row_avg_daily_district[1], time=time.time() - timer))
            timer = time.time()

        else:
            print('Skipped due to incomplete cell_id data')

        print('Recording to summary_stats')
        with open("{}/summary_stats.csv".format(self.csv_location), "w", newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(tb_1_description)
            for row in output_1_rows:
                writer.writerow(row)

            writer.writerow('\n')

            writer.writerow(tb_2_description)
            for row in output_2_rows:
                writer.writerow(row)

        print('Successfully wrote to summary_stats.csv'
              '\nElapsed time: {time} seconds'
              .format(time=time.time() - timer))

        timer = time.time()

        print('Daily cdrs')
        print('Selecting date and total records')
        q_total_daily_cdr = "select to_date(call_time) as date, " \
                            "count(*) as total_records from {provider_prefix}_consolidate_data_all group by " \
                            "to_date(call_time)".format(provider_prefix=self.provider_prefix)
        cursor.execute(q_total_daily_cdr)
        row_total_daily_cdr = cursor.fetchall()
        print('Query done'
              '\nElapsed time: {time} seconds'
              .format(time=time.time() - timer))
        timer = time.time()
        total_daily_cdr_x = []
        total_daily_cdr_y = []
        for row in row_total_daily_cdr:
            total_daily_cdr_x.append(row[0])
            total_daily_cdr_y.append(row[1])

        print('Querying min, max and avg of total records')
        q_total_daily_cdr_all = "select min(total_records), max(total_records), avg(total_records) from ({}) td".format(
            q_total_daily_cdr)
        cursor.execute(q_total_daily_cdr_all)

        row_total_daily_cdr_all = cursor.fetchall()

        daily_cdr_min, daily_cdr_max, daily_cdr_avg = row_total_daily_cdr_all[0][0], row_total_daily_cdr_all[0][1], \
                                                      row_total_daily_cdr_all[0][2]
        print('Done.'
              '\nElapsed time: {time} seconds'
              .format(time=time.time() - timer))
        print('Writing into the graph for daily cdrs')
        hp.make_graph(total_daily_cdr_x, 'Day', total_daily_cdr_y, 'Total Records', 'Daily CDRs',
                      '{}/daily_cdrs'.format(self.graph_location),
                      des_pair_1={'text_x': 0.090, 'text_y': 1.27, 'text': 'MIN', 'value': f"{daily_cdr_min:,.2f}"},
                      des_pair_2={'text_x': 0.345, 'text_y': 1.27, 'text': 'MAX', 'value': f"{daily_cdr_max:,.2f}"},
                      des_pair_3={'text_x': 0.595, 'text_y': 1.27, 'text': 'AVG', 'value': f"{daily_cdr_avg:,.2f}"},
                      des_pair_4={'text_x': 0.83, 'text_y': 1.27, 'text': 'Total Records',
                                  'value': f"{total_records:,.2f}"})
        print('Writing completed. File located in {}/daily_cdrs'.format(self.graph_location))

        print('Daily unique users')
        print('Quering date and unique users')
        timer = time.time()
        q_total_daily_uid = "select to_date(call_time) as date, " \
                            "count(distinct uid) as total_users from {provider_prefix}_consolidate_data_all group by " \
                            "to_date(call_time)" \
            .format(provider_prefix=self.provider_prefix)

        cursor.execute(q_total_daily_uid)

        row_total_daily_uid = cursor.fetchall()
        print('Query completed. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()
        total_daily_uid_x = []
        total_daily_uid_y = []
        for row in row_total_daily_uid:
            total_daily_uid_x.append(row[0])
            total_daily_uid_y.append(row[1])
        print('Selecing min, max and avg of total users')
        q_total_daily_uid_all = "select min(total_users), max(total_users), avg(total_users) from ({}) td".format(
            q_total_daily_uid)
        cursor.execute(q_total_daily_uid_all)

        row_total_daily_uid_all = cursor.fetchall()
        daily_uid_min, daily_uid_max, daily_uid_avg = row_total_daily_uid_all[0][0], row_total_daily_uid_all[0][1], \
                                                      row_total_daily_uid_all[0][2]
        print('Query completed. Elapsed time: {} seconds'.format(time.time() - timer))


        print('Writing into the graph for daily unique users')
        hp.make_graph(total_daily_uid_x, 'Date', total_daily_uid_y, 'Total Users', 'Daily Unique Users',
                      '{}/daily_unique_users'.format(self.graph_location),
                      des_pair_1={'text_x': 0.090, 'text_y': 1.27, 'text': 'MIN', 'value': f"{daily_uid_min:,.2f}"},
                      des_pair_2={'text_x': 0.345, 'text_y': 1.27, 'text': 'MAX', 'value': f"{daily_uid_max:,.2f}"},
                      des_pair_3={'text_x': 0.595, 'text_y': 1.27, 'text': 'AVG', 'value': f"{daily_uid_avg:,.2f}"},
                      des_pair_4={'text_x': 0.805, 'text_y': 1.27, 'text': 'Total Unique IDs',
                                  'value': f"{total_uids:,.2f}"})
        print('Writing completed. File located in {}/daily_unique_users'.format(self.graph_location))
        timer = time.time()

        print('Daily unique locations')
        print('Querying call_time and unique locations')
        q_total_daily_locations = "select to_date(call_time) as date, " \
                                  "count(distinct a2.latitude, a2.longitude) as unique_locations from {provider_prefix}_consolidate_data_all a1 " \
                                  "join {provider_prefix}_cell_tower_data_preprocess a2 on(a1.cell_id = a2.cell_id) " \
                                  "group by to_date(call_time)".format(provider_prefix=self.provider_prefix)
        cursor.execute(q_total_daily_locations)
        row_total_daily_locations = cursor.fetchall()
        print('Query completed. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()

        total_daily_location_x = []
        total_daily_location_y = []
        for row in row_total_daily_locations:
            total_daily_location_x.append(row[0])
            total_daily_location_y.append(row[1])
        print('Selecing min, max and avg of unique locations')
        q_total_daily_location_all = "select min(unique_locations), max(unique_locations), avg(unique_locations) from ({}) td".format(
            q_total_daily_locations)
        cursor.execute(q_total_daily_location_all)

        row_total_daily_location_all = cursor.fetchall()
        daily_location_min, daily_location_max, daily_location_avg = row_total_daily_location_all[0][0], \
                                                                     row_total_daily_location_all[0][1], \
                                                                     row_total_daily_location_all[0][2]
        print('Query completed. Elapsed time: {} seconds'.format(time.time() - timer))

        print('Writing into the graph for daily unique locations')
        hp.make_graph(total_daily_location_x, 'Date', total_daily_location_y, 'Total Locations',
                      'Daily Unique Locations', '{}/daily_unique_locations'.format(self.graph_location),
                      des_pair_1={'text_x': 0.090, 'text_y': 1.27, 'text': 'MIN',
                                  'value': f"{daily_location_min:,.2f}"},
                      des_pair_2={'text_x': 0.345, 'text_y': 1.27, 'text': 'MAX', 'value': f"{daily_location_max:,.2f}"},
                      des_pair_3={'text_x': 0.595, 'text_y': 1.27, 'text': 'AVG', 'value': f"{daily_location_avg:,.2f}"},
                      des_pair_4={'text_x': 0.805, 'text_y': 1.27, 'text': 'Total Unique Locations',
                                  'value': f"{total_unique_locations:,.2f}"})
        print('Writing completed. File located in {}/daily_unique_locations'.format(self.graph_location))

        timer = time.time()
        print('Daily Average CDRs')
        print('Querying for average cdr and total unique users')
        q_total_daily_avg_cdr = "select date, total_records/total_uids as daily_average_cdr from(select to_date(call_time) as date, " \
                                "count(distinct uid) as total_uids, count(*) as total_records from {provider_prefix}_consolidate_data_all a1 " \
                                "group by to_date(call_time))td1".format(
            provider_prefix=self.provider_prefix)

        cursor.execute(q_total_daily_avg_cdr)
        row_total_daily_avg_cdr = cursor.fetchall()
        print('Query completed. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()

        total_daily_avg_cdr_x = []
        total_daily_avg_cdr_y = []
        for row in row_total_daily_avg_cdr:
            total_daily_avg_cdr_x.append(row[0])
            total_daily_avg_cdr_y.append(row[1])

        print('Querying for average daily cdrs')

        q_total_daily_location_all = "select avg(daily_average_cdr) from ({}) td".format(
            q_total_daily_avg_cdr)
        cursor.execute(q_total_daily_location_all)

        row_total_daily_avg_cdr_all = cursor.fetchall()
        print('Query completed. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()
        daily_avg_cdr = row_total_daily_avg_cdr_all[0][0]
        print('Writing into the graph for daily average CDRs')
        hp.make_graph(total_daily_avg_cdr_x, 'Date', total_daily_avg_cdr_y, 'Total Daily Average CDRs',
                      'Daily Average CDRs', '{}/daily_avg_cdr'.format(self.graph_location),
                      des_pair_1={'text_x': 0.035, 'text_y': 1.27, 'text': 'Total Daily Avg CDRs',
                                  'value': f"{daily_avg_cdr:,.2f}"})

        print('Daily unique average locations')

        disable = False
        for item in self.cdr_data_layer:
            if str.lower(item['name']) == 'cell_id' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'call_time' and item['output_no'] == -1:
                disable = True
        if not disable:
            print('Querying daily average cell ids and daily average locations')
            timer = time.time()
            q_total_daily_avg_locations = "select date, unique_locations/unique_users as daily_avg_locations, unique_cell_ids/unique_users as daily_avg_cell_ids " \
                                          "from (select to_date(call_time) as date, " \
                                          "count(distinct a2.latitude, a2.longitude)  as unique_locations , count(distinct a1.uid) as unique_users, " \
                                          "count(distinct a1.cell_id) as unique_cell_ids from {provider_prefix}_consolidate_data_all a1 " \
                                          "join {provider_prefix}_cell_tower_data_preprocess a2 on(a1.cell_id = a2.cell_id) " \
                                          "group by to_date(call_time)) td1".format(
                provider_prefix=self.provider_prefix)

            cursor.execute(q_total_daily_avg_locations)
            row_total_daily_avg_locations = cursor.fetchall()
            print('Query completed. Elapsed time: {} seconds'.format(time.time() - timer))
            timer = time.time()
            total_daily_avg_location_x = []
            total_daily_avg_location_y = []
            for row in row_total_daily_avg_locations:
                total_daily_avg_location_x.append(row[0])
                total_daily_avg_location_y.append(row[1])
            print('Querying for average daily avg cell_id and locations')
            q_total_daily_avg_location_all = "select avg(td.daily_avg_cell_ids), avg(td.daily_avg_locations) from ({}) td".format(
                q_total_daily_avg_locations)
            cursor.execute(q_total_daily_avg_location_all)

            row_total_daily_location_all = cursor.fetchall()
            print('Query completed. Elapsed time: {} seconds'.format(time.time() - timer))
            timer = time.time()
            daily_avg_location_cell_ids, daily_avg_location = row_total_daily_location_all[0][0], \
                                                              row_total_daily_location_all[0][1]
            print('Writing into the graph for daily unique average locations')
            hp.make_graph(total_daily_avg_location_x, 'Date', total_daily_avg_location_y, 'Total Unique Locations',
                          'Daily Unique Average Locations',
                          '{}/daily_unique_avg_locations'.format(self.graph_location),
                          des_pair_1={'text_x': 0.00, 'text_y': 1.27, 'text': 'Avg Daily Unique Cell IDs ',
                                      'value': f"{daily_avg_location_cell_ids:,.2f}"},
                          des_pair_2={'text_x': 0.28, 'text_y': 1.27, 'text': 'Avg Daily Unique Locations',
                                      'value': f"{daily_avg_location:,.2f}"})
            print('Writing completed. File located in {}/daily_unique_avg_locations'.format(self.graph_location))
        else:
            print('call_time or cell_id is in incorrect form. Ignored output.')

    def calculate_od(self):
        cursor = self.hive.cursor
        query = "CREATE TABLE la_cdr_all_with_ant_zone_by_uid (uid string, arr ARRAY<ARRAY<string>>)" \
                "PARTITIONED BY (pdt string)" \
                "ROW FORMAT DELIMITED" \
                "FIELDS TERMINATED BY '\t'" \
                "COLLECTION ITEMS TERMINATED BY ','" \
                "MAP KEYS TERMINATED BY '!'" \
                "LINES TERMINATED BY '\n'" \
                "STORED AS SEQUENCEFILE"

        cursor.execute(query)

        # query = "INSERT OVERWRITE TABLE  la_cdr_all_with_ant_zone_by_uid  PARTITION (pdt)" \
        #         "select uid,  CreateTrajectoriesJICAWithZone(uid,call_time,duration,a2.longitude,a2.latitude,a1.cell_id,district_id)" \
        #         "as arr,pdt from la_cdr_all_with_pro_dis a1 JOIN {provider_prefix}_consolidate_data_all a2" \
        #         "on(a1.cell_id = a2.cell_id) where pdt) = " \
        #         "'2016-05-01' group by uid, pdt"
