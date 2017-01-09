using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace TelemetryGenerator
{

    class SqlDataTransfer
    {
        SQLProcessor proc_sqlserver = new SQLProcessor("intelab-db", "windows.net", "intelab-vm-production", "superadmin", "intelab-2016");

        MySqlProcessor proc_mysql = new MySqlProcessor();
        void AddResultsToDailyAverage(
            Dictionary<Tuple<long, int>, DataRow> monitor_result_daily_average,
            Dictionary<Tuple<long, int>, int> monitor_result_count,
            DataRow[] monitor_results
            )
        {
            
            DataTable mr_daily_tbl = DataHelper.MakeTelemetryAverageTable();
            foreach (DataRow mr in monitor_results)
            {
                long device_id = (long)mr["device_id"];
                int telemetry_id = (int)mr["device_telemetry_id"];
                Tuple<long, int> key = new Tuple<long, int>(device_id, telemetry_id);

                if (monitor_result_daily_average.ContainsKey(key))
                {
                    monitor_result_count[key] = monitor_result_count[key] + 1;
                    double cur_sum = (double)(monitor_result_daily_average[key]["result"]);
                    cur_sum += (double)mr["result"];
                    monitor_result_daily_average[key]["result"] =  cur_sum ;
                }
                else
                {
                    DataRow dr = mr_daily_tbl.NewRow();
                    dr["id"] = 0;
                    dr["device_id"] = device_id;
                    dr["device_type_id"] = mr["device_type_id"];
                    dr["device_type_name"] = mr["device_type_name"];
                    dr["device_telemetry_id"] = mr["device_telemetry_id"];
                    dr["create_date"] = ((DateTime)mr["create_time"]).Date;
                    dr["result"] = mr["result"];
                    dr["company_id"] = mr["company_id"];
                    dr["company_name"] = mr["company_name"];

                    monitor_result_daily_average.Add(key, dr);
                    monitor_result_count.Add(key, 1);
                }
            }
        }

        void AddAlertsToDailySum(
            Dictionary<long, DataRow> yellow_alert_daily_sum,
            Dictionary<long, DataRow> red_alert_daily_sum,
            DataRow[] alerts
            )
        {
            DataTable alert_daily_tbl = DataHelper.MakeAlertDailySumTable();
            foreach (DataRow alert in alerts)
            {
                long device_id = (long)alert["device_id"];
                int alert_type = (int)alert["alert_type"];
                if (alert_type == 1)
                {
                    if (yellow_alert_daily_sum.ContainsKey(device_id))
                    {
                        yellow_alert_daily_sum[device_id]["alert_count"] = (int)yellow_alert_daily_sum[device_id]["alert_count"] + 1;
                        yellow_alert_daily_sum[device_id]["result"] = (float)yellow_alert_daily_sum[device_id]["result"] + (float)alert["result"];
                    }
                    else
                    {
                        DataRow dr = alert_daily_tbl.NewRow();
                        dr["id"] = 0;
                        dr["device_id"] = device_id;
                        dr["device_type_id"] = alert["device_type_id"];
                        dr["device_type_name"] = alert["device_type_name"];
                        dr["device_telemetry_id"] = alert["device_telemetry_id"];
                        dr["alert_type"] = 1;
                        dr["alert_count"] = 1;
                        dr["create_date"] = ((DateTime)alert["start_time"]).Date;
                        dr["result"] = alert["result"];
                        dr["company_id"] = alert["company_id"];
                        dr["company_name"] = alert["company_name"];

                        yellow_alert_daily_sum.Add(device_id, dr);

                    }
                }
                else
                {
                    if (red_alert_daily_sum.ContainsKey(device_id))
                    {
                        red_alert_daily_sum[device_id]["alert_count"] = (int)red_alert_daily_sum[device_id]["alert_count"] + 1;
                        red_alert_daily_sum[device_id]["result"] = (float)red_alert_daily_sum[device_id]["result"] + (float)alert["result"];
                    }
                    else
                    {
                        DataRow dr = alert_daily_tbl.NewRow();
                        dr["id"] = 0;
                        dr["device_id"] = device_id;
                        dr["device_type_id"] = alert["device_type_id"];
                        dr["device_type_name"] = alert["device_type_name"];
                        dr["device_telemetry_id"] = alert["device_telemetry_id"];
                        dr["alert_type"] = 2;
                        dr["alert_count"] = 1;
                        dr["create_date"] = ((DateTime)alert["start_time"]).Date;
                        dr["result"] = alert["result"];
                        dr["company_id"] = alert["company_id"];
                        dr["company_name"] = alert["company_name"];

                        red_alert_daily_sum.Add(device_id, dr);

                    }
                }

            }
        }

        void AddAlertResultsToDailySum(
            Dictionary<Tuple<long, int>, DataRow> alert_daily_sum,
            DataRow[] alerts
            )
        {
            DataTable alert_daily_tbl = DataHelper.MakeAlertDailySumTable();
            foreach (DataRow alert in alerts)
            {
                long device_id = (long)alert["device_id"];
                int alert_type = (int)alert["alert_type"];
                Tuple<long, int> key = new Tuple<long, int>(device_id, alert_type);
                
                if (alert_daily_sum.ContainsKey(key))
                {
                    alert_daily_sum[key]["alert_count"] = (int)alert_daily_sum[key]["alert_count"] + 1;
                    //alert_daily_sum[key]["result"] = (double)alert_daily_sum[key]["result"] + (double)alert["result"];
                }
                else
                {
                    DataRow dr = alert_daily_tbl.NewRow();
                    dr["id"] = 0;
                    dr["device_id"] = device_id;
                    dr["device_type_id"] = alert["device_type_id"];
                    dr["device_type_name"] = alert["device_type_name"];
                    dr["device_telemetry_id"] = alert["device_telemetry_id"];
                    dr["alert_type"] = alert_type;
                    dr["alert_count"] = 1;
                    dr["create_date"] = ((DateTime)alert["start_time"]).Date;
                    dr["result"] = alert["result"];
                    dr["company_id"] = alert["company_id"];
                    dr["company_name"] = alert["company_name"];

                    alert_daily_sum.Add(key, dr);

                }
            }
                
            
        }

        void FinalizeMonitorResultDailyAvg(
            Dictionary<Tuple<long, int>, DataRow> monitor_result_daily_average,
            Dictionary<Tuple<long, int>, int> monitor_result_count)
        {
            foreach (Tuple<long, int> key in monitor_result_daily_average.Keys)
            {
                if (monitor_result_count[key] != 0)
                {
                    monitor_result_daily_average[key]["result"] = (double)monitor_result_daily_average[key]["result"] / (int)monitor_result_count[key];
                }
                else
                {
                    Console.WriteLine("result count is 0 for device {0}, something is wrong", key);
                }
            }

            // write to sql server
            proc_sqlserver.WriteDataRowsToSQLTable("fact_monitor_result_daily_average", monitor_result_daily_average.Values.ToArray());
        }

        void FinalizeAlertDailySum(
            Dictionary<Tuple<long, int>, DataRow> alert_sum
            )
        {

            
            // write to sql server
            proc_sqlserver.WriteDataRowsToSQLTable("fact_alert_daily_sum", alert_sum.Values.ToArray());
            
            
        }


        /// <summary>
        /// read data from mysql and save to sql server
        /// </summary>
        public void TransferFromMysqlToSqlserver()
        {
            // dim_device
            proc_sqlserver.MergeDevicesToDB(proc_mysql.ReadDimDevice());

            // dim_device_telemetry
            proc_sqlserver.MergeDeviceTelemetryToDB(proc_mysql.ReadDeviceTelemetry());

            // fact_monitor_result
            Dictionary<Tuple<long, int>, DataRow> monitor_result_daily_average = new Dictionary<Tuple<long, int>, DataRow>();
            Dictionary<Tuple<long, int>, int> monitor_result_count = new Dictionary<Tuple<long, int>, int>();


            long mr_count = 0;
            int limit = 1000;
            DateTime yesterday = DateTime.Now.Date - new TimeSpan(3, 0, 0, 0);

            while (true)
            {
                string cur_date_str = DateTime.Now.Date.ToShortDateString();
                DataRow[] monitor_results = proc_mysql.ReadMonitorResult(yesterday, mr_count, limit);
                proc_sqlserver.WriteDataRowsToSQLTable("fact_monitor_result", monitor_results);
                mr_count += monitor_results.Length;

                AddResultsToDailyAverage(monitor_result_daily_average, monitor_result_count, monitor_results);
                if (monitor_results.Length < limit)
                {
                    FinalizeMonitorResultDailyAvg(monitor_result_daily_average, monitor_result_count);

                    break;
                }
            }

            // fact_alert
            //Dictionary<long, DataRow> yellow_alert_daily_sum = new Dictionary<long, DataRow>();
            //Dictionary<long, DataRow> red_alert_daily_sum = new Dictionary<long, DataRow>();

            Dictionary<Tuple<long, int>, DataRow> alert_daily_sum = new Dictionary<Tuple<long, int>, DataRow>();
            long alert_count = 0;
            while (true)
            {

                DataRow[] alert_results = proc_mysql.ReadAlert(yesterday, alert_count, limit);
                proc_sqlserver.WriteDataRowsToSQLTable("fact_alert", alert_results);
                alert_count += alert_results.Length;

                AddAlertResultsToDailySum(alert_daily_sum, alert_results);
                if (alert_results.Length < limit)
                {
                    FinalizeAlertDailySum(alert_daily_sum);
                    break;
                }
            }

        }
    }
}
