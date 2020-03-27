import json
import csv
import matplotlib.pyplot as plt
import os
from hive_create_tables import HiveConnector
import helper as hp
import time
from datetime import datetime

months = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
          7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}


class CDRVisualizer:
    def __init__(self, config, data):
        self.__dict__ = config.__dict__
        self.hive = HiveConnector(config)
        timer = time.time()
        print('########## Initilizing Hive ##########')
        self.hive.initialize(config)
        print('########## Done. Time elapsed: {} seconds ##########'.format(hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('########## Creating Tables ##########')
        self.hive.create_tables(config, data)
        print('########## Done create all tables. Time elapsed: {} seconds ##########'.format(hp.format_two_point_time(timer, time.time())))

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
            print('########## CALCULATING DATA STATISTICS ##########')
            cursor = self.hive.cursor
            imei = "count(distinct IMEI) as unique_imei, "
            imsi = "count(distinct IMSI) as unique_imsi, "
            for map in self.cdr_data_layer:
                if str.lower(map['input_name']) == 'imei' and str.lower(map['name']) == 'uid':
                    imei = ''
                elif str.lower(map['input_name']) == 'imsi' and str.lower(map['name']) == 'uid':
                    imsi = ''
            raw_sql = hp.sql_to_string('statistics/reports/all_statistics/data_statistics.sql')
            query = raw_sql.format(provider_prefix=self.provider_prefix, imei=imei, imsi=imsi)
            print('Calculating data statistics')
            timer = time.time()
            cursor.execute(query)
            print('Calculated data statistics. Elasped time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
            print('Writing to {}/css_file_data_stat.csv'.format(self.output_report_location))
            timer = time.time()

            with open("{}/css_file_data_stat.csv".format(self.output_report_location), "w", newline='') as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(col[0] for col in cursor.description)
                for row in cursor:
                    writer.writerow(row)
            print('Successfully wrote to {}/css_file_data_stat.csv'.format(self.output_report_location))
            print('Elapsed time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
            print('########## FINISHED CALCULATING DATA STATISTICS ##########')
        else:
            print('Mapping for call_time, imsi, imei or uid is not sufficient. Ignored data statistic')

    def calculate_daily_statistics(self):
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
        file_location = '{}/css_file_data_stat.csv'.format(self.output_report_location)
        time_dict = hp.get_time_from_csv(file_location, replicate)
        start_date, end_date = time_dict['start_date'], time_dict['end_date']

        disable = False
        for item in self.cdr_data_layer:
            if str.lower(item['name']) == 'network_type' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'call_type' and item['output_no'] == -1:
                disable = True
        if not disable:
            print('########## CALCULATING DAILY STATISTICS ##########')
            cursor = self.hive.cursor
            cursor.set_arraysize(1)
            results = []

            timer = time.time()
            print('Calculating Daily Statistics')
            # FOR CASE ALL
            raw_query = hp.sql_to_string('statistics/reports/daily_statistics/daily_statistics.sql')
            query = raw_query.format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date, imei=imei, imsi=imsi)
            cursor.execute(query)
            print('Query completed. Time elapsed: {} seconds.'.format(hp.format_two_point_time(timer, time.time())))
            description = cursor.description
            rows = []

            for row in cursor:
                rows.append(row)
            results += rows
            print('Writing into the graph for daily statistics')
            file_path = '{}/css_provider_data_stat_daily.csv'.format(self.output_report_location)
            if os.path.exists(file_path):
                os.remove(file_path)

            with open(file_path, "w", newline='') as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(col[0][4:] for col in description)
                for row in results:
                    writer.writerow(row)
            print('Successfully wrote to file css_provider_data_stat_daily.csv')
            print('########## FINISHED CALCULATING DAILY STATISTICS ##########')

            print('########## Querying daily cdr by call_type ##########')
            timer = time.time()
            raw_sql = hp.sql_to_string('statistics/graphs/daily_cdrs_by_call_type/daily_cdrs_by_call_type.sql')
            query = raw_sql.format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date, imei=imei, imsi=imsi)
            cursor.execute(query)
            print('Query completed. Time elapsed: {} seconds.'.format(hp.format_two_point_time(timer, time.time())))
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
            plt.savefig('{}/daily_cdr_by_call_type'.format(self.output_graph_location))
            plt.clf()
            print('Graph created successfully in {}/daily_cdr_by_call_type'.format(self.output_graph_location))
        else:
            print('Mapping for network_type or call_type is not sufficient. Ignored daily statistics')

    def calculate_monthly_statistics(self):
        disable = False
        for item in self.cdr_data_layer:
            if str.lower(item['name']) == 'network_type' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'call_type' and item['output_no'] == -1:
                disable = True
        if not disable:
            print('########## CALCULATING MONTHLY STATISTICS ##########')
            cursor = self.hive.cursor
            cursor.set_arraysize(1)
            results = []
            file_location = '{}/css_file_data_stat.csv'.format(self.output_report_location)

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
            raw_sql = hp.sql_to_string('statistics/reports/monthly_statistics/monthly_statistics.sql')
            query = raw_sql.format(provider_prefix=self.provider_prefix,
                                   start_year=start_y,
                                   end_year=end_y,
                                   start_month=start_m,
                                   end_month=end_m,
                                   imei=imei,
                                   imsi=imsi)
            cursor.execute(query)
            description = cursor.description
            rows = []
            for row in cursor:
                rows.append(row)

            results += cursor.fetchall()

            file_path = '{}/css_provider_data_stat_monthly.csv'.format(self.output_report_location)
            if os.path.exists(file_path):
                os.remove(file_path)

            with open(file_path, "w", newline='') as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(col[0][4:] for col in description)
                for row in results:
                    writer.writerow(row)

            print('### Successfully wrote to file css_provider_data_stat_monthly.csv###')
            print('########## CALCULATING MONTHLY STATISTICS ##########')
        else:
            print('Mapping for network_type or call_type is not sufficient. Ignored monthly statistics')

    def calculate_zone_population(self):
        print('########## CALCULATING ZONE POPULATION STATISTICS ##########')
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
            raw_sql = hp.sql_to_string('statistics/reports/zone_population/zone_population.sql')
            query = raw_sql.format(provider_prefix=self.provider_prefix, level=admin_unit)
            cursor.execute(query)

            description = cursor.description
            print('Successfully zone population for {admin}. Elapsed time: {time} seconds'.format(admin=admin_unit, time=hp.format_two_point_time(timer, time.time())))
            timer = time.time()
            rows = []
            cursor.set_arraysize(1)
            for row in cursor:
                rows.append(row)

            file_path = '{output_report_location}/zone_based_aggregations_level_{level}.csv'.format(
                output_report_location=self.output_report_location, level=admin_unit)
            if geo_jsons_active[geo_i] != '':
                print('Merging dictionary object to geojson')
                for f in range(0, len(geo_jsons_active[geo_i]['features'])):

                    if geo_jsons_active[geo_i]['features'][f]['properties'][name_columns[geo_i]] == 'Kochi Ken':
                        geo_jsons_active[geo_i]['features'][f]['properties']['num_population'] = 'Kochi Ken'
                print('Merging completed. Time elapsed: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
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
            print('Writing completed. Time elapsed: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
            timer = time.time()

            if geo_json_filename[geo_i] != '':
                print('Writing into geojson file ' + geo_json_filename[geo_i][:-4] + '_joined_' + admin_unit + '.json')
                with open('{}/'.format(self.output_report_location) + geo_json_filename[geo_i][:-4] + '_joined_' + admin_unit + '.json', "w",
                          newline='') as outfile:
                    json.dump(geo_jsons_active[geo_i], outfile)
                print('Writing completed. Time elapsed: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
            geo_i += 1
        print('########## FINISHED CALCULATING ZONE POPULATION STATISTICS ##########')

    def calculate_user_date_histogram(self):
        print('########## CALCULATING USER DATE HISTOGRAM ##########')
        cursor = self.hive.cursor
        raw_sql = hp.sql_to_string('statistics/graphs/date_histogram/histogram.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix)

        timer = time.time()
        print('Calculating data histogram')
        cursor.execute(query)
        description = cursor.description
        rows = cursor.fetchall()
        print('Calculating completed. Time elapsed: {} seconds'.format(hp.format_two_point_time(timer, time.time())))

        file_path = '{}/histogram.csv'.format(self.output_report_location)
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
        print('Plotting graph and writing into {}/user_data_histogram.png'.format(self.output_graph_location))
        plt.savefig('{}/user_data_histogram.png'.format(self.output_graph_location))
        print('Done.')
        print('########## CALCULATING USER DATE HISTOGRAM ##########')

    def calculate_summary(self):
        print('########## CALCULATING SUMMARY ##########')
        cursor = self.hive.cursor
        tb_1_description = ('All Data', 'Value')
        tb_2_description = ('Statistics',)
        output_1_rows = []

        print('Calculating total records')
        timer = time.time()
        raw_sql = hp.sql_to_string('statistics/total_records.sql')
        q_total_records = raw_sql.format(provider_prefix=self.provider_prefix)
        cursor.execute(q_total_records)

        des = cursor.description
        row_total_records = cursor.fetchall()
        row_total_records = (des[0][0], row_total_records[0][0])
        output_1_rows.append(row_total_records)
        total_records = row_total_records[1]
        print('Successfully calculated total records. Total records: {recs} records \nElapsed time: {time} seconds'
              .format(recs=total_records, time=hp.format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Calculating total unique uids')
        raw_sql = hp.sql_to_string('statistics/total_unique_uids.sql')
        q_total_uids = raw_sql.format(provider_prefix=self.provider_prefix)
        cursor.execute(q_total_uids)

        des = cursor.description
        row_total_uids = cursor.fetchall()
        row_total_uids = (des[0][0], row_total_uids[0][0])
        output_1_rows.append(row_total_uids)
        total_uids = row_total_uids[1]
        print('Successfully calculated total unique uids. Total unique ids: {ids} ids  \nElapsed time: {time} seconds'.format(
            ids=total_uids,time=hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        raw_sql = hp.sql_to_string('statistics/reports/summary/total_days.sql')
        print('Calculating total days')
        query = raw_sql.format(provider_prefix=self.provider_prefix)
        cursor.execute(query)
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
              .format(days=row_total_days[1], time=hp.format_two_point_time(timer, time.time())))

        # average usage per day
        print('Calculating average daily usage')
        output_2_rows = []
        row_avg_daily_usage = ('average_usage_per_day', round(float(total_records / total_days), 3))
        output_2_rows.append(row_avg_daily_usage)
        print('Successfully calculated average daily usage. Daily average usages : {uses} '
              '\nElapsed time: {time} seconds'
              .format(uses=row_avg_daily_usage[1], time=hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        # avg voice call per day

        disable = False
        for item in self.cdr_data_layer:
            if str.lower(item['name']) == 'network_type' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'call_type' and item['output_no'] == -1:
                disable = True

        if not disable:
            print('########## Calculating average daily voice call usage ##########')
            raw_sql = hp.sql_to_string('statistics/reports/summary/average_daily_voice.sql')
            q_avg_daily_voice = raw_sql.format(
                provider_prefix=self.provider_prefix, total_days=total_days)
            cursor.execute(q_avg_daily_voice)
            des = cursor.description
            row_avg_daily_voice = cursor.fetchall()
            row_avg_daily_voice = (des[0][0], round(row_avg_daily_voice[0][0], 3))
            output_2_rows.append(row_avg_daily_voice)
            print('Successfully calculated average daily voice call usage. Daily average sms usages : {uses} '
                  '\nElapsed time: {time} seconds'
                  .format(uses=row_avg_daily_voice[1], time=hp.format_two_point_time(timer, time.time())))
            timer = time.time()
            # avg sms per day
            print('Calculating average daily sms usage')
            raw_sql = hp.sql_to_string('statistics/reports/summary/average_daily_sms.sql')
            q_avg_daily_sms = raw_sql.format(provider_prefix=self.provider_prefix, total_days=total_days)
            cursor.execute(q_avg_daily_sms)
            des = cursor.description
            row_avg_daily_sms = cursor.fetchall()
            row_avg_daily_sms = (des[0][0], round(row_avg_daily_sms[0][0], 3))
            output_2_rows.append(row_avg_daily_sms)
            print('########## Successfully calculated average daily sms usage. Daily average sms usages : {uses} ##########'
                  '\n########## Elapsed time: {time} seconds ##########'
                  .format(uses=row_avg_daily_sms[1], time=hp.format_two_point_time(timer, time.time())))
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
            raw_sql = hp.sql_to_string('statistics/reports/summary/average_unique_cell_ids.sql')
            query = raw_sql.format(provider_prefix=self.provider_prefix, total_days=total_days)
            cursor.execute(query)

            des = cursor.description
            row_avg_daily_unique_cell_id = cursor.fetchall()
            row_avg_daily_unique_cell_id = (des[0][0], round(row_avg_daily_unique_cell_id[0][0], 3))
            output_2_rows.append(row_avg_daily_unique_cell_id)
            print('Successfully calculated average daily unique cell id')
            print('Successfully calculated average daily unique cel id.'
                  '\nElapsed time: {time} seconds'
                  .format(time=hp.format_two_point_time(timer, time.time())))
            timer = time.time()
            have_district = False
            for col in self.cdr_cell_tower:
                if str.lower(col['name']) == 'admin1':
                    have_district = True
            if have_district:
                print('Calculating average daily administration level 1')
                raw_sql = hp.sql_to_string('statistics/reports/summary/average_daily_admin1.sql')
                query = raw_sql.format(provider_prefix=self.provider_prefix, level='ADMIN1', total_days=total_days)
                cursor.execute(query)

                des = cursor.description
                row_avg_daily_district = cursor.fetchall()
                row_avg_daily_district = (des[0][0], round(row_avg_daily_district[0][0], 3))
                output_2_rows.append(row_avg_daily_district)
                print('Successfully calculated average daily administration level 1. Daily average value : {dists} '
                      '\nElapsed time: {time} seconds'
                      .format(dists=row_avg_daily_district[1], time=hp.format_two_point_time(timer, time.time())))
                timer = time.time()

        else:
            print('Skipped due to incomplete cell_id data')

        print('Recording to summary_stats')
        with open("{}/summary_stats.csv".format(self.output_report_location), "w", newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(tb_1_description)
            for row in output_1_rows:
                writer.writerow(row)

            writer.writerow('\n')

            writer.writerow(tb_2_description)
            for row in output_2_rows:
                writer.writerow(row)

        print('Successfully wrote to summary_stats.csv\nElapsed time: {time} seconds'
              .format(time=hp.format_two_point_time(timer, time.time())))

        print('########## FINISHED CALCULATING SUMMARY ##########')

    def daily_cdrs(self):
        timer = time.time()
        cursor = self.hive.cursor
        print('########## Daily cdrs ##########')
        print('Selecting total records')
        raw_sql = hp.sql_to_string('statistics/total_records.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix)
        cursor.execute(query)

        des = cursor.description
        row_total_records = cursor.fetchall()
        row_total_records = (des[0][0], row_total_records[0][0])
        total_records = row_total_records[1]
        print('Successfully calculated total records. Total records: {recs} records \nElapsed time: {time} seconds'
              .format(recs=total_records, time=hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        raw_sql = hp.sql_to_string('statistics/graphs/daily_cdrs/total_daily_cdrs.sql')
        q_total_daily_cdr = raw_sql.format(provider_prefix=self.provider_prefix)
        cursor.execute(q_total_daily_cdr)
        row_total_daily_cdr = cursor.fetchall()
        print('Query done'
              '\nElapsed time: {time} seconds'
              .format(time=hp.format_two_point_time(timer, time.time())))
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

        daily_cdr_min, daily_cdr_max, daily_cdr_avg = row_total_daily_cdr_all[0][0], row_total_daily_cdr_all[0][1],\
                                                      row_total_daily_cdr_all[0][2]
        print('Done.'
              '\nElapsed time: {time} seconds'
              .format(time=hp.format_two_point_time(timer, time.time())))
        print('Writing into the graph for daily cdrs')
        hp.make_graph(total_daily_cdr_x, 'Day', total_daily_cdr_y, 'Total Records', 'Daily CDRs',
                      '{}/daily_cdrs'.format(self.output_graph_location),
                      des_pair_1={'text_x': 0.090, 'text_y': 1.27, 'text': 'MIN', 'value': f"{daily_cdr_min:,.2f}"},
                      des_pair_2={'text_x': 0.345, 'text_y': 1.27, 'text': 'MAX', 'value': f"{daily_cdr_max:,.2f}"},
                      des_pair_3={'text_x': 0.595, 'text_y': 1.27, 'text': 'AVG', 'value': f"{daily_cdr_avg:,.2f}"},
                      des_pair_4={'text_x': 0.83, 'text_y': 1.27, 'text': 'Total Records',
                                  'value': f"{total_records:,.2f}"})
        print(
            '########## Writing completed. File located in {}/daily_cdrs ##########'.format(self.output_graph_location))

    def daily_unique_users(self):
        cursor = self.hive.cursor
        print('########## Daily unique users ###########')
        print('Calculating total unique uids')
        raw_sql = hp.sql_to_string('statistics/total_unique_uids.sql')
        q_total_uids = raw_sql.format(provider_prefix=self.provider_prefix)
        timer = time.time()
        cursor.execute(q_total_uids)
        des = cursor.description
        row_total_uids = cursor.fetchall()
        row_total_uids = (des[0][0], row_total_uids[0][0])
        total_uids = row_total_uids[1]
        print(
            'Successfully calculated total unique uids. Total unique ids: {ids} ids  \nElapsed time: {time} seconds'.format(
                ids=total_uids, time=hp.format_two_point_time(timer, time.time())))
        print('Quering date and unique users')
        timer = time.time()
        raw_sql = hp.sql_to_string('statistics/graphs/daily_unique_users/total_daily_uids.sql')
        q_total_daily_uid = raw_sql.format(provider_prefix=self.provider_prefix)
        cursor.execute(q_total_daily_uid)
        row_total_daily_uid = cursor.fetchall()
        print('Query completed. Elapsed time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
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
        print('Query completed. Elapsed time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))

        print('Writing into the graph for daily unique users')
        hp.make_graph(total_daily_uid_x, 'Date', total_daily_uid_y, 'Total Users', 'Daily Unique Users',
                      '{}/daily_unique_users'.format(self.output_graph_location),
                      des_pair_1={'text_x': 0.090, 'text_y': 1.27, 'text': 'MIN', 'value': f"{daily_uid_min:,.2f}"},
                      des_pair_2={'text_x': 0.345, 'text_y': 1.27, 'text': 'MAX', 'value': f"{daily_uid_max:,.2f}"},
                      des_pair_3={'text_x': 0.595, 'text_y': 1.27, 'text': 'AVG', 'value': f"{daily_uid_avg:,.2f}"},
                      des_pair_4={'text_x': 0.805, 'text_y': 1.27, 'text': 'Total Unique IDs',
                                  'value': f"{total_uids:,.2f}"})
        print('########## Writing completed. File located in {}/daily_unique_users ##########'.format(self.output_graph_location))

    def daily_unique_locations(self):
        timer = time.time()
        cursor = self.hive.cursor
        print('########## Daily unique locations ##########')
        print('Calculating daily average location name')
        raw_sql = hp.sql_to_string('statistics/graphs/daily_unique_locations/total_unique_locations.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix)

        cursor.execute(query)
        des = cursor.description
        row_total_locations = cursor.fetchall()
        row_total_locations = (des[0][0], row_total_locations[0][0])
        total_unique_locations = row_total_locations[1]
        print('Successfully calculated daily average location name. Daily average location names : {locs} '
              '\nElapsed time: {time} seconds'
              .format(locs=row_total_locations[1], time=hp.format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Querying daily unique locations')
        raw_sql = hp.sql_to_string('statistics/graphs/daily_unique_locations/daily_unique_locations.sql')
        q_total_daily_locations = raw_sql.format(
            provider_prefix=self.provider_prefix)
        cursor.execute(q_total_daily_locations)
        row_total_daily_locations = cursor.fetchall()
        print('Query completed. Elapsed time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
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
        daily_location_min, daily_location_max, daily_location_avg = row_total_daily_location_all[0][0],\
                                                                     row_total_daily_location_all[0][1],\
                                                                     row_total_daily_location_all[0][2]
        print('Query completed. Elapsed time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))

        print('Writing into the graph for daily unique locations')
        hp.make_graph(total_daily_location_x, 'Date', total_daily_location_y, 'Total Locations',
                      'Daily Unique Locations', '{}/daily_unique_locations'.format(self.output_graph_location),
                      des_pair_1={'text_x': 0.090, 'text_y': 1.27, 'text': 'MIN',
                                  'value': f"{daily_location_min:,.2f}"},
                      des_pair_2={'text_x': 0.345, 'text_y': 1.27, 'text': 'MAX',
                                  'value': f"{daily_location_max:,.2f}"},
                      des_pair_3={'text_x': 0.595, 'text_y': 1.27, 'text': 'AVG',
                                  'value': f"{daily_location_avg:,.2f}"},
                      des_pair_4={'text_x': 0.805, 'text_y': 1.27, 'text': 'Total Unique Locations',
                                  'value': f"{total_unique_locations:,.2f}"})
        print('########## Writing completed. File located in {}/daily_unique_locations ###########'.format(self.output_graph_location))

    def daily_average_cdrs(self):
        cursor = self.hive.cursor
        timer = time.time()
        print('########## Daily Average CDRs ##########')
        print('Querying for average cdr and total unique users')
        raw_sql = hp.sql_to_string('statistics/graphs/daily_average_cdrs/daily_average_cdrs.sql')
        q_total_daily_avg_cdr = raw_sql.format(provider_prefix=self.provider_prefix)

        cursor.execute(q_total_daily_avg_cdr)
        row_total_daily_avg_cdr = cursor.fetchall()
        print('Query completed. Elapsed time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
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
        print('Query completed. Elapsed time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
        daily_avg_cdr = row_total_daily_avg_cdr_all[0][0]
        print('########## Writing into the graph for daily average CDRs ##########')
        hp.make_graph(total_daily_avg_cdr_x, 'Date', total_daily_avg_cdr_y, 'Total Daily Average CDRs',
                      'Daily Average CDRs', '{}/daily_avg_cdr'.format(self.output_graph_location),
                      des_pair_1={'text_x': 0.035, 'text_y': 1.27, 'text': 'Total Daily Avg CDRs',
                                  'value': f"{daily_avg_cdr:,.2f}"})

    def daily_unique_average_locations(self):
        print('########## Daily unique average locations ##########')
        cursor = self.hive.cursor
        disable = False
        for item in self.cdr_data_layer:
            if str.lower(item['name']) == 'cell_id' and item['output_no'] == -1 \
                    or str.lower(item['name']) == 'call_time' and item['output_no'] == -1:
                disable = True
        if not disable:
            print('Querying daily average cell ids and daily average locations')
            timer = time.time()
            raw_sql = hp.sql_to_string('statistics/graphs/daily_average_unique_locations/daily_average_unique_locations.sql')
            q_total_daily_avg_locations = raw_sql.format(provider_prefix=self.provider_prefix)
            cursor.execute(q_total_daily_avg_locations)
            row_total_daily_avg_locations = cursor.fetchall()
            print('Query completed. Elapsed time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
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
            print('Query completed. Elapsed time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
            timer = time.time()

            daily_avg_location_cell_ids, daily_avg_location = row_total_daily_location_all[0][0],\
                                                              row_total_daily_location_all[0][1]
            print('Writing into the graph for daily unique average locations')
            hp.make_graph(total_daily_avg_location_x, 'Date', total_daily_avg_location_y, 'Total Unique Locations',
                          'Daily Unique Average Locations',
                          '{}/daily_unique_avg_locations'.format(self.output_graph_location),
                          des_pair_1={'text_x': 0.00, 'text_y': 1.27, 'text': 'Avg Daily Unique Cell IDs ',
                                      'value': f"{daily_avg_location_cell_ids:,.2f}"},
                          des_pair_2={'text_x': 0.28, 'text_y': 1.27, 'text': 'Avg Daily Unique Locations',
                                      'value': f"{daily_avg_location:,.2f}"})
            print('########## Writing completed. File located in {}/daily_unique_avg_locations ##########'
                  .format(self.output_graph_location))
        else:
            print('call_time or cell_id is in incorrect form. Ignored output.')

    def frequent_location(self):
        cursor = self.hive.cursor
        print('########## CREATE FREQUENT LOCATION TABLE ##########')
        print('Checking and dropping frequent location table if existing.')
        timer = time.time()
        admin = self.od_admin_unit
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_frequent_location'.format(provider_prefix=self.provider_prefix))
        print('Checked and dropped frequent location table if existing. Elapsed time: {} seconds'.format(
            hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating frequent location table')
        raw_sql = hp.sql_to_string('statistics/reports/frequent_locations/create_frequent_locations.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix, admin_params=admin + '_id string')

        cursor.execute(query)
        print('Created frequent location table. Elapsed time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Inserting into frequent location table')
        raw_sql = hp.sql_to_string('statistics/reports/frequent_locations/frequent_locations.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix, admin_params=admin + '_id', admin=admin)

        cursor.execute(query)
        print('Inserted into frequent location table.\nResult are in the table named {provider_prefix}_frequent_location\nElapsed time: {time} seconds. '
              .format(provider_prefix=self.provider_prefix, time=hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Dropping freq location with accumulated percentage')
        cursor.execute(
            'DROP TABLE IF EXISTS {provider_prefix}_freq_with_acc_wsum'.format(provider_prefix=self.provider_prefix))
        print('Checked and dropped frequent location table with accumulated percentage if existing. Elapsed time: {} seconds'.format(
            hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating and insert freq with acc wsum Table (Frequent Location) with accumulated percentage')
        raw_sql = hp.sql_to_string('statistics/reports/frequent_locations/frequent_locations_wsum.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix, admin=admin)
        cursor.execute(query)
        print(
            'Inserted into frequent location table with accumulated percentage. \nElapsed time: {time} seconds. '
            .format(time=hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Dropping frequent location thresholded table')
        cursor.execute(
            'DROP TABLE IF EXISTS {provider_prefix}_frequent_location_thresholded'.format(provider_prefix=self.provider_prefix))
        print(
            'Checked and dropped frequent location table with accumulated percentage if existing. Elapsed time: {} seconds'.format(
                hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating and insert frequent location thresholded table ')
        raw_sql = hp.sql_to_string('statistics/reports/frequent_locations/frequent_locations_thresholded.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix, admin=admin, threshold=self.frequent_location_percentage)
        cursor.execute(query)
        print(
            'Inserted into frequent location thresholded table. \nElapsed time: {time} seconds. '
                .format(time=hp.format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING FREQUENT LOCATION TABLE ##########')

    def frequent_location_night(self):
        cursor = self.hive.cursor
        print('########## CREATE FREQUENT LOCATION NIGHT TABLE ##########')
        print('Checking and dropping frequent location night table if existing.')
        timer = time.time()
        admin = self.od_admin_unit
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_frequent_location_night'.format(provider_prefix=self.provider_prefix))
        print('Checked and dropped frequent location night table if existing. Elapsed time: {} seconds'.format(
            hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating frequent location night table')
        raw_sql = hp.sql_to_string('statistics/reports/frequent_locations/create_frequent_locations_night.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix, admin_params=admin + '_id string')
        cursor.execute(query)

        print('Created frequent location night table. Elapsed time: {} seconds'.format(hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Inserting into frequent location night table')
        raw_sql = hp.sql_to_string('statistics/reports/frequent_locations/frequent_locations_night.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix, admin_params=admin + '_id', admin=admin)
        cursor.execute(query)
        print('Inserted into frequent location night table.\nResult are in the table named {provider_prefix}_frequent_location_night\nElapsed time: {time} seconds. '
              .format(provider_prefix=self.provider_prefix, time=hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Dropping freq location night with accumulated percentage')
        cursor.execute(
            'DROP TABLE IF EXISTS {provider_prefix}_freq_with_acc_wsum_night'.format(provider_prefix=self.provider_prefix))
        print(
            'Checked and dropped frequent location night table with accumulated percentage if existing. Elapsed time: {} seconds'.format(
                hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating and insert freq night with acc wsum Table (Frequent Location Night) with accumulated percentage')
        raw_sql = hp.sql_to_string('statistics/reports/frequent_locations/frequent_locations_wsum_night.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix, admin=admin)
        cursor.execute(query)
        print(
            'Inserted into frequent location night table with accumulated percentage. \nElapsed time: {time} seconds. '
                .format(time=hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Dropping frequent location thresholded night table')
        cursor.execute(
            'DROP TABLE IF EXISTS {provider_prefix}_frequent_location_thresholded_night'.format(provider_prefix=self.provider_prefix))
        print(
            'Checked and dropped frequent location night table with accumulated percentage if existing. Elapsed time: {} seconds'.format(
                hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating and insert frequent location thresholded night table ')
        raw_sql = hp.sql_to_string('statistics/reports/frequent_locations/frequent_locations_thresholded_night.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix, admin=admin, threshold=self.frequent_location_percentage)
        cursor.execute(query)
        print(
            'Inserted into frequent location thresholded night table. \nElapsed time: {time} seconds. '
                .format(time=hp.format_two_point_time(timer, time.time())))

        print('########## FINISHED CREATING FREQUENT LOCATION NIGHT TABLE ##########')

    def rank1_frequent_location(self):
        cursor = self.hive.cursor
        print('########## CREATE RANK 1 FREQUENT LOCATION TABLE ##########')
        admin = self.od_admin_unit
        create_param = admin + '_id string'
        timer = time.time()
        print('Checking and dropping rank 1 frequent location table if existing.')
        cursor.execute(
            'DROP TABLE IF EXISTS {provider_prefix}_la_cdr_uid_home'.format(provider_prefix=self.provider_prefix))
        print('Checked and dropped rank 1 frequent location table if existing. Elapsed time: {} seconds'.format(
            hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating rank 1 frequent location table')
        raw_sql = hp.sql_to_string('origin_destination/create_la_cdr_uid_home.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix, admin_params=create_param)
        cursor.execute(query)
        print('Created rank 1 frequent location table. Elapsed time: {} seconds'.format(
            hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Inserting into rank 1 frequent location table')
        raw_sql = hp.sql_to_string('origin_destination/insert_la_cdr_uid_home.sql')
        query = raw_sql.format(provider_prefix=self.provider_prefix)
        cursor.execute(query)
        print('Inserted into rank 1 frequent location table (located in {provider_prefix}_la_cdr_uid_home). Elapsed time: {time} seconds'
            .format(provider_prefix=self.provider_prefix, time=
            hp.format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING RANK 1 FREQUENT LOCATION TABLE ##########')