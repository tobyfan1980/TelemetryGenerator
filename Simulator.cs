using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace TelemetryGenerator
{
    class Simulator
    {
        public List<TelemetryInspect> telemetry_inspectors = new List<TelemetryInspect>();

        public SQLProcessor sql_process = new SQLProcessor();
        public Simulator(float yellow_alert_rate = 1.0f/288, float red_alert_rate=1.0f/288/7)
        {
            //hardcode or inspectors
            //we sample once per 5 minutes, so 288 time a day
            float yellow_rate = yellow_alert_rate; 
            float red_rate = red_alert_rate;

            telemetry_inspectors.Add(new TelemetryInspect(1, 1, "冰箱", "温度", 1, "00", "度", -80f, -83f, -77f, -86f, -74f, yellow_rate, red_rate));
            telemetry_inspectors.Add(new TelemetryInspect(2, 2, "培养箱", "温度", 1, "00", "度", 37, 36, 38, 35, 39, yellow_rate * 1.2f, red_rate * 1.2f));
            telemetry_inspectors.Add(new TelemetryInspect(3, 2, "培养箱", "湿度", 2, "01", "%", 95, 94, 96, 93, 97, yellow_rate*1.4f, red_rate));
            telemetry_inspectors.Add(new TelemetryInspect(4, 2, "培养箱", "二氧化碳", 3, "02", "ppm", 10000, 9500, 10500, 9000, 11000, yellow_rate*0.9f, red_rate*1.1f));

            telemetry_inspectors.Add(new TelemetryInspect(5, 3, "洁净室", "温度", 1, "00", "度", 22, 20, 24, 18, 26, yellow_rate * 1.8f, red_rate*1.4f));
            telemetry_inspectors.Add(new TelemetryInspect(6, 3, "洁净室", "湿度", 2, "01", "%", 55, 50, 60, 45, 65, yellow_rate * 1.2f, red_rate * 0.7f));
            telemetry_inspectors.Add(new TelemetryInspect(7, 3, "洁净室", "房间压差", 3, "02", "pa", 40, 35, 45, 30, 50, yellow_rate, red_rate));

            TelemetryInspect inspect = new TelemetryInspect(8, 4, "通风柜", "甲烷含量", 4, "03", "%", 1, 3, 3, 5, 5, yellow_rate * 1.3f, red_rate * 0.9f);
            inspect.lower_bound = 0;

            telemetry_inspectors.Add(inspect);

        }

        public List<DimDevice> generateDevices()
        {
            DeviceGenerator dg = new DeviceGenerator();
            dg.initialize();

            List<DimDevice> device_list = dg.generateDevices();

            /*
            foreach(DimDevice dev in device_list)
            {
                Console.Out.WriteLine(dev.toString());
            }
            */

            return device_list;
        }

        public void GetDeviceFromDB()
        {
            List<DimDevice> dl = sql_process.readDevices(0, 5);
            foreach(DimDevice dev in dl)
            {
                Console.WriteLine(dev.toString());
            }
        }

        public void SaveTelemetryInspectors()
        {
            sql_process.WriteDeviceTelemetries(this.telemetry_inspectors);
        }

        public void GenerateAndSaveDevices()
        {

            sql_process.WriteDevices(generateDevices());
        }

        public void GenerateDeviceMonitorResultByDeviceType(int device_type_id, DateTime start, DateTime end, int sampling_interval)
        {
            foreach(TelemetryInspect tele in this.telemetry_inspectors)
            {
                if(tele.device_type_id == device_type_id)
                {
                    GenerateDeviceMonitorResultByInspectType(device_type_id, tele, start, end, sampling_interval);
                }
            }
        }

        public ReportData GenerateDeviceMonitorResultByInspectType(int device_type_id, TelemetryInspect tele, DateTime start, DateTime end, int sampling_interval_minutes)
        {
            TimeSpan diff = end - start;
            int count = (int)diff.TotalMinutes / sampling_interval_minutes;
            ReportData telemetry_report = tele.generateTelemetry(count);

            return telemetry_report;
        }

        public void GenerateMonitorResultPerDevice(DimDevice device, DateTime start, DateTime end, int sample_interval_minutes, int recent_days)
        {
            Console.WriteLine("generate simulator results for device {0}", device.id);
            foreach (TelemetryInspect teleInspect in telemetry_inspectors)
            {
                if (teleInspect.device_type_id == device.type_id)
                {
                    ReportData reportData = GenerateDeviceMonitorResultByInspectType(device.type_id, teleInspect, start, end, sample_interval_minutes);
                    GetAlertDataRowsFromReportData(reportData, device, teleInspect, start, sample_interval_minutes);
                    GetTelemetryAverageDataRowsFromReportData(reportData, device, teleInspect, start, sample_interval_minutes);

                    TimeSpan recent_days_gap = new TimeSpan(recent_days, 0, 0, 0);
                    DateTime recent_date = end - recent_days_gap;

                    if(recent_date < start)
                    {
                        recent_date = start;
                    }

                    int data_offset = (int)((recent_date - start).TotalMinutes) / sample_interval_minutes;

                    GetRecentTelemetryDataRowsFromReportData(reportData, device, teleInspect, recent_date, data_offset, sample_interval_minutes);

                }
            }
        }

        public void GenerateMonitorResultForAllDevice(DateTime start, DateTime end, int sample_interval_minutes)
        {
            // get all device from db
            List<DimDevice> device_list = sql_process.readDevices(0, 10);
            // get device type, and for all corresponding inspect type generate report data
            foreach (DimDevice device in device_list)
            {
                GenerateMonitorResultPerDevice(device, start, end, sample_interval_minutes, 2);
            }
            // add datetime and device info to report data and write to db

        }

        private DataTable MakeMonitorResultTable()
        {
            DataTable monitor_result_tbl = new DataTable("monitor_result");

            DataColumn result_id_col = new DataColumn();
            result_id_col.ColumnName = "id";
            result_id_col.AutoIncrement = true;
            result_id_col.DataType = System.Type.GetType("System.Int64");
            monitor_result_tbl.Columns.Add(result_id_col);

            DataColumn device_id_col = new DataColumn();
            device_id_col.ColumnName = "device_id";
            device_id_col.AutoIncrement = false;
            device_id_col.DataType = System.Type.GetType("System.Int64");
            monitor_result_tbl.Columns.Add(device_id_col);

            
            DataColumn device_type_id_col = new DataColumn();
            device_type_id_col.ColumnName = "device_type_id";
            device_type_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_result_tbl.Columns.Add(device_type_id_col);

            DataColumn telemetry_type_id_col = new DataColumn();
            telemetry_type_id_col.ColumnName = "device_telemetry_id";
            telemetry_type_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_result_tbl.Columns.Add(telemetry_type_id_col);


            DataColumn time_col = new DataColumn();
            time_col.ColumnName = "create_time";
            time_col.DataType = System.Type.GetType("System.DateTime");
            monitor_result_tbl.Columns.Add(time_col);


            DataColumn monitor_result = new DataColumn();
            monitor_result.ColumnName = "result";
            monitor_result.DataType = System.Type.GetType("System.Double");// ? how to get float datatype
            monitor_result_tbl.Columns.Add(monitor_result);

            DataColumn company_id_col = new DataColumn();
            company_id_col.ColumnName = "company_id";
            company_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_result_tbl.Columns.Add(company_id_col);

            return monitor_result_tbl;
        }

        public void GetTelemetryDataRowsFromReportData(ReportData report, DimDevice device, TelemetryInspect telemetry_inspect, DateTime start, int sampling_interval_minutes)
        {
            TimeSpan interval = new TimeSpan(0, 5, 0);
            DataTable monitor_result_tbl = MakeMonitorResultTable();

            DateTime result_time = start;
            for (int i = 0; i < report.data.Length; i++)
            {
                DataRow dataRow = monitor_result_tbl.NewRow();
                dataRow["id"] = 10 + i;
                dataRow["device_id"] = device.id;
                dataRow["company_id"] = device.company_id;
                dataRow["create_time"] = result_time;
                dataRow["device_type_id"] = device.type_id;
                dataRow["device_telemetry_id"] = telemetry_inspect.id;
                dataRow["result"] = report.data[i];

                monitor_result_tbl.Rows.Add(dataRow);

                result_time += interval;
                
            }
            monitor_result_tbl.AcceptChanges();

            sql_process.WriteDataRowsToSQLTable("fact_monitor_result", monitor_result_tbl.Select());
        
        }

        public void GetRecentTelemetryDataRowsFromReportData(ReportData report, DimDevice device, TelemetryInspect telemetry_inspect, DateTime start, int offset, int sampling_interval_minutes)
        {
            TimeSpan interval = new TimeSpan(0, sampling_interval_minutes, 0);

            DataTable monitor_result_tbl = MakeMonitorResultTable();

            DateTime result_time = start;
            for (int i = offset; i < report.data.Length; i++)
            {
                DataRow dataRow = monitor_result_tbl.NewRow();
                dataRow["device_id"] = device.id;
                dataRow["create_time"] = result_time;
                dataRow["device_type_id"] = device.type_id;
                dataRow["device_telemetry_id"] = telemetry_inspect.id;
                dataRow["result"] = report.data[i];
                dataRow["company_id"] = device.company_id;

                monitor_result_tbl.Rows.Add(dataRow);

                result_time += interval;

            }
            monitor_result_tbl.AcceptChanges();

            sql_process.WriteDataRowsToSQLTable("fact_monitor_result", monitor_result_tbl.Select());

        }

        private DataTable MakeAlertTable()
        {
            DataTable monitor_alert_tbl = new DataTable("monitor_alert");

            DataColumn alert_id_col = new DataColumn();
            alert_id_col.ColumnName = "id";
            alert_id_col.AutoIncrement = true;
            alert_id_col.DataType = System.Type.GetType("System.Int64");
            monitor_alert_tbl.Columns.Add(alert_id_col);

            DataColumn device_id_col = new DataColumn();
            device_id_col.ColumnName = "device_id";
            device_id_col.DataType = System.Type.GetType("System.Int64");
            monitor_alert_tbl.Columns.Add(device_id_col);


            DataColumn device_type_id_col = new DataColumn();
            device_type_id_col.ColumnName = "device_type_id";
            device_type_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_tbl.Columns.Add(device_type_id_col);

            DataColumn telemetry_type_id_col = new DataColumn();
            telemetry_type_id_col.ColumnName = "device_telemetry_id";
            telemetry_type_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_tbl.Columns.Add(telemetry_type_id_col);

            DataColumn alert_type_col = new DataColumn();
            alert_type_col.ColumnName = "alert_type";
            alert_type_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_tbl.Columns.Add(alert_type_col);

            DataColumn alert_count_col = new DataColumn();
            alert_count_col.ColumnName = "consecutive_alert_count";
            alert_count_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_tbl.Columns.Add(alert_count_col);

            DataColumn time_col = new DataColumn();
            time_col.ColumnName = "alert_time";
            time_col.DataType = System.Type.GetType("System.DateTime");
            monitor_alert_tbl.Columns.Add(time_col);

            DataColumn alert_result_col = new DataColumn();
            alert_result_col.ColumnName = "result";
            alert_result_col.DataType = System.Type.GetType("System.Double");// ? how to get float datatype
            monitor_alert_tbl.Columns.Add(alert_result_col);

            DataColumn company_id_col = new DataColumn();
            company_id_col.ColumnName = "company_id";
            company_id_col.AutoIncrement = false;
            company_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_tbl.Columns.Add(company_id_col);

            return monitor_alert_tbl;
        }

        public void GetAlertDataRowsFromReportData(ReportData report, DimDevice device, TelemetryInspect tele_inspect, DateTime start, int sampling_interval_minutes)
        {
            TimeSpan interval = new TimeSpan(0, 5, 0);
            
            DataTable monitor_alert_tbl = MakeAlertTable();
            DateTime result_time = start;
            for (int i = 0; i < report.yellow_alert_index.Length; i++)
            {
                result_time = start + new TimeSpan(0, report.yellow_alert_index[i] * sampling_interval_minutes, 0);

                DataRow dataRow = monitor_alert_tbl.NewRow();

                dataRow["id"] = 10 + i;
                dataRow["device_id"] = device.id;
                dataRow["device_type_id"] = device.type_id;
                dataRow["device_telemetry_id"] = tele_inspect.id ;
                dataRow["alert_time"] = result_time;

                dataRow["result"] = report.data[i];
                dataRow["alert_type"] = 1;
                
                dataRow["consecutive_alert_count"] = alert_count(report.yellow_alert_index, i);
                dataRow["company_id"] = device.company_id;

                monitor_alert_tbl.Rows.Add(dataRow);

            }

            result_time = start;
            for (int i = 0; i < report.red_alert_index.Length; i++)
            {
                result_time = start + new TimeSpan(0, report.red_alert_index[i] * sampling_interval_minutes, 0);

                DataRow dataRow = monitor_alert_tbl.NewRow();
                dataRow["device_id"] = device.id;
                dataRow["company_id"] = device.company_id;
                dataRow["device_type_id"] = device.type_id;
                dataRow["alert_time"] = result_time;
                dataRow["device_telemetry_id"] = tele_inspect.id;
                dataRow["result"] = report.data[i];
                dataRow["alert_type"] = 2;

                dataRow["consecutive_alert_count"] = alert_count(report.red_alert_index, i);
                monitor_alert_tbl.Rows.Add(dataRow);

            }
            monitor_alert_tbl.AcceptChanges();

            sql_process.WriteDataRowsToSQLTable("fact_alert", monitor_alert_tbl.Select());

        }

        private int alert_count(int[] alerts, int cur_index)
        {
            int count = 1;
            while(cur_index > 0)
            {
                if(alerts[cur_index-1] == alerts[cur_index] - 1)
                {
                    count++;
                    cur_index--;
                }
                else
                {
                    break;
                }
            }

            return count;
        }

        private DataTable MakeTelemetryAverageTable()
        {
            DataTable monitor_result_avg_tbl = new DataTable("monitor_result");

            DataColumn result_id_col = new DataColumn();
            result_id_col.ColumnName = "id";
            result_id_col.AutoIncrement = true;
            result_id_col.DataType = System.Type.GetType("System.Int64");
            monitor_result_avg_tbl.Columns.Add(result_id_col);

            DataColumn device_id_col = new DataColumn();
            device_id_col.ColumnName = "device_id";
            device_id_col.AutoIncrement = false;
            device_id_col.DataType = System.Type.GetType("System.Int64");
            monitor_result_avg_tbl.Columns.Add(device_id_col);

            DataColumn device_type_id_col = new DataColumn();
            device_type_id_col.ColumnName = "device_type_id";
            device_type_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_result_avg_tbl.Columns.Add(device_type_id_col);

            DataColumn telemetry_type_id_col = new DataColumn();
            telemetry_type_id_col.ColumnName = "device_telemetry_id";
            telemetry_type_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_result_avg_tbl.Columns.Add(telemetry_type_id_col);


            DataColumn time_col = new DataColumn();
            time_col.ColumnName = "create_date";
            time_col.DataType = System.Type.GetType("System.DateTime");
            monitor_result_avg_tbl.Columns.Add(time_col);


            DataColumn monitor_result = new DataColumn();
            monitor_result.ColumnName = "result";
            monitor_result.DataType = System.Type.GetType("System.Double");// ? how to get float datatype
            monitor_result_avg_tbl.Columns.Add(monitor_result);


            DataColumn company_id_col = new DataColumn();
            company_id_col.ColumnName = "company_id";
            company_id_col.AutoIncrement = false;
            company_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_result_avg_tbl.Columns.Add(company_id_col);

            return monitor_result_avg_tbl;
        }

        public void GetTelemetryAverageDataRowsFromReportData(ReportData report, DimDevice device, TelemetryInspect tele_inspect, DateTime start, int sampling_interval_minutes)
        {

            DataTable monitor_result_avg_tbl = MakeTelemetryAverageTable();
            DateTime result_time = start;

            TimeSpan one_day = new TimeSpan(24, 0, 0);
            TimeSpan interval = new TimeSpan(0, 5, 0);

            int daily_sample_count = (int)one_day.TotalMinutes / sampling_interval_minutes;

            double agg_result = 0;
            DateTime cur_date = start.Date;
            DateTime cur_time = cur_date;
            int daily_samples = 0;
            
            for (int i = 0; i < report.data.Length; i++)
            {
                if (cur_time.Date != cur_date)
                {
                    // get average and create row
                    double average = agg_result / daily_samples;
                    DataRow dataRow = monitor_result_avg_tbl.NewRow();
                    dataRow["device_id"] = device.id;
                    dataRow["company_id"] = device.company_id;
                    dataRow["device_type_id"] = device.type_id;
                    dataRow["device_telemetry_id"] = tele_inspect.id;
                    dataRow["create_date"] = cur_date;

                    dataRow["result"] = average;
                    monitor_result_avg_tbl.Rows.Add(dataRow);

                    cur_date = cur_time.Date;
                    agg_result = 0;
                    daily_samples = 0;

                }
                else
                {
                    agg_result += report.data[i];
                    cur_time += interval;
                    daily_samples++;
                }
                
            }
            // write last day's result
            if(daily_samples != 0)
            {
                // get average and create row
                double average = agg_result / daily_samples;
                DataRow dataRow = monitor_result_avg_tbl.NewRow();
                dataRow["device_id"] = device.id;
                dataRow["company_id"] = device.company_id;
                dataRow["device_type_id"] = device.type_id;
                dataRow["device_telemetry_id"] = tele_inspect.id;
                dataRow["create_date"] = cur_date;

                dataRow["result"] = average;
                monitor_result_avg_tbl.Rows.Add(dataRow);
            }

            monitor_result_avg_tbl.AcceptChanges();

            sql_process.WriteDataRowsToSQLTable("fact_monitor_result_daily_average", monitor_result_avg_tbl.Select());

        }
    }
}
