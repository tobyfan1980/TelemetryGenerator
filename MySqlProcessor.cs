using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Data;

namespace TelemetryGenerator
{
    class MySqlProcessor
    {
        private string connectionString;

        public MySqlProcessor()
        {
            connectionString = "server=139.196.240.232;user=lixie;password=123456;port=3306;database=inspect";
        }
        public MySqlProcessor(string server_ip, string username, string password, string database)
        {

            connectionString = string.Format("server={0};user={1};password={2};port=3306;database={3}",
                server_ip, username, password, database);
        }
        public DataRow[] ReadDevices()
        {
            DataTable device_table = DataHelper.MakeDeviceTable();
            try
            {
                MySqlConnection conn = new MySqlConnection(connectionString);
                conn.Open();
                string command = "select * from device";
                MySqlCommand sqlCommand = new MySqlCommand(command, conn);
                MySqlDataReader mysqlReader = sqlCommand.ExecuteReader();
                while (mysqlReader.Read())
                {
                    Console.WriteLine("Get device {0} - {1} - {2} - {3}",
                        mysqlReader["id"], mysqlReader["type_id"],
                        mysqlReader["create_date"], mysqlReader["creator"]);

                }
                mysqlReader.Close();

                device_table.AcceptChanges();
                return device_table.Select();
            }
            catch (Exception sql_excp)
            {
                Console.WriteLine("My sql exception: {0}", sql_excp.ToString());
                return null;
            }
        }

        /// <summary>
        /// read one day alert result
        /// </summary>
        /// <param name="cut_date"></param>
        /// <param name="skip"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        public DataRow[] ReadAlert(DateTime cut_date, long skip=0, int limit=1000)
        {
            DataTable alert_tble = DataHelper.MakeAlertTable();
            DateTime next_day = cut_date + new TimeSpan(1, 0, 0, 0);
            string cut_date_str = string.Format("{0}-{1}-{2}", cut_date.Year, cut_date.Month, cut_date.Day);
            string next_day_str = string.Format("{0}-{1}-{2}", next_day.Year, next_day.Month, next_day.Day);
            try
            {
                MySqlConnection conn = new MySqlConnection(connectionString);
                conn.Open();
                DateTime before_sql_command = DateTime.Now;
                string command = string.Format( @"SELECT a.id, a.device_id, d.type_id 'device_type_id',
dt.name 'device_type_name',
c.id 'company_id', c.name 'company_name',
a.inspect_type_id 'device_telemetry_id', a.alert_type,
a.alert_num 'consecutive_alert_count', a.create_date 'start_time', a.finish_date 'end_time'
FROM alert_count a
left JOIN device as d on a.device_id=d.id
left JOIN device_type as dt on d.type_id=dt.id
left JOIN company as c on dt.company_id=c.id
where (a.create_date BETWEEN '{0}' AND '{1}') limit {2},{3};", cut_date_str, next_day_str, skip, limit);
                MySqlCommand sqlCommand = new MySqlCommand(command, conn);
                MySqlDataReader mysqlReader = sqlCommand.ExecuteReader();
                DateTime after_sql_command = DateTime.Now;
                var columns = new List<string>();
                for (int i = 0; i < mysqlReader.FieldCount; i++)
                {
                    columns.Add(mysqlReader.GetName(i));
                }
                while (mysqlReader.Read())
                {
                    Console.WriteLine("Get device {0} - {1} - {2} - {3}",
                        mysqlReader["id"], mysqlReader["device_type_id"],
                        mysqlReader["start_time"], mysqlReader["end_time"]);

                    DataRow alert_row = alert_tble.NewRow();
                    foreach(string column_name in columns)
                    {
                        alert_row[column_name] = mysqlReader[column_name];
                    }
                    alert_tble.Rows.Add(alert_row);
                }
                mysqlReader.Close();
                alert_tble.AcceptChanges();

                DateTime after_read_to_daterows = DateTime.Now;

                Console.WriteLine("Executing sql query takes: {0}", (after_sql_command - before_sql_command).TotalSeconds);
                Console.WriteLine("Reading sql query data to alert rows takes: {0}", (after_read_to_daterows - after_sql_command).TotalSeconds);


                return alert_tble.Select();
            }
            catch (Exception sql_excp)
            {
                Console.WriteLine("Read alert failed. My sql exception: {0}", sql_excp.ToString());
                return null;
            }
        }

        /// <summary>
        /// read one day monitor result
        /// </summary>
        /// <param name="cut_date"></param>
        /// <param name="skip"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        public DataRow[] ReadMonitorResult(DateTime cut_date, long skip=0, int limit=1000)
        {
            DataTable monitor_result_tbl = DataHelper.MakeMonitorResultTable();
            DateTime next_day = cut_date + new TimeSpan(1, 0, 0, 0);
            string cut_date_str = string.Format("{0}-{1}-{2}", cut_date.Year, cut_date.Month, cut_date.Day);
            string next_day_str = string.Format("{0}-{1}-{2}", next_day.Year, next_day.Month, next_day.Day);
            try
            {
                MySqlConnection conn = new MySqlConnection(connectionString);
                conn.Open();

                DateTime before_sql_command = DateTime.Now;

                string command = string.Format(@"select id.id, id.device_id, d.type_id 'device_type_id',
dt.name 'device_type_name',
id.device_inspect_id 'device_telemetry_id', c.id 'company_id', c.name 'company_name',
id.create_date 'create_time', id.result
from inspect_data as id
left JOIN device as d on id.device_id=d.id
left JOIN device_type as dt on d.type_id=dt.id
left JOIN company as c on dt.company_id=c.id
where (id.create_date BETWEEN '{0}' AND '{1}') limit {2},{3};", cut_date_str, next_day_str, skip, limit);
                MySqlCommand sqlCommand = new MySqlCommand(command, conn);
                MySqlDataReader mysqlReader = sqlCommand.ExecuteReader();

                DateTime after_sql_command = DateTime.Now;

                var columns = new List<string>();
                for (int i = 0; i < mysqlReader.FieldCount; i++)
                {
                    columns.Add(mysqlReader.GetName(i));
                }
                while (mysqlReader.Read())
                {
                    Console.WriteLine("Get device {0} - {1} - {2} - {3}",
                        mysqlReader["id"], mysqlReader["device_type_id"],
                        mysqlReader["create_time"], mysqlReader["result"]);

                    DataRow mr_row = monitor_result_tbl.NewRow();
                    foreach(string column_name in columns)
                    {
                        mr_row[column_name] = mysqlReader[column_name];
                    }

                    monitor_result_tbl.Rows.Add(mr_row);
                }
                mysqlReader.Close();
                monitor_result_tbl.AcceptChanges();

                DateTime after_read_to_datarows = DateTime.Now;

                Console.WriteLine("Executing sql query takes: {0}", (after_sql_command - before_sql_command).TotalSeconds);
                Console.WriteLine("Read query data to monitor results rows is: {0}", (after_read_to_datarows - after_sql_command).TotalSeconds);


                return monitor_result_tbl.Select();
            }
            catch (Exception sql_excp)
            {
                Console.WriteLine("Read monitor result failed. My sql exception: {0}", sql_excp.ToString());
                return null;
            }
        }

        public DataRow[] ReadDimDevice()
        {
            DataTable device_tbl = DataHelper.MakeDeviceTable();
            try
            {
                MySqlConnection conn = new MySqlConnection(connectionString);
                conn.Open();
                string command = @"select d.id 'device_id', d.name 'device_name', d.code 'serial_number',
d.model 'model', d.create_date, d.purchase_date,
dt.id 'type_id', dt.name 'type_name',
c.id 'company_id', c.name 'company_name', c.id 'company_id', b.id 'building_id', b.name 'building_name',
f.id 'floor_id', f.name 'floor_name', r.id 'room_id', r.name 'room_name',
m.number 'control', m.online_status 'control_online_status', m.battery_status 'control_battery_status'
from device d
left
join device_type as dt on d.type_id = dt.id
left
join buildings as b on d.build_id = b.id
left
join company as c on b.company_id = c.id
left
join floors as f on d.floor_id = f.id
left
join room as r on d.room_id = r.id
left
join monitor_device as m on d.id = m.device_id; ";
                MySqlCommand sqlCommand = new MySqlCommand(command, conn);
                MySqlDataReader mysqlReader = sqlCommand.ExecuteReader();
                var columns = new List<string>();
                for(int i=0; i<mysqlReader.FieldCount; i++)
                {
                    columns.Add(mysqlReader.GetName(i));
                }
                while (mysqlReader.Read())
                {
                    Console.WriteLine("Get device {0} - {1} - {2} - {3}",
                        mysqlReader["device_id"], mysqlReader["device_name"],
                        mysqlReader["serial_number"], mysqlReader["model"]);

                    DataRow device_row = device_tbl.NewRow();
                    foreach(string column_name in columns)
                    {
                        device_row[column_name] = mysqlReader[column_name];
                    }

                    device_tbl.Rows.Add(device_row);
                }
                mysqlReader.Close();

                device_tbl.AcceptChanges();
                return device_tbl.Select();
            }
            catch (Exception sql_excp)
            {
                Console.WriteLine("Read device from mysql failed. My sql exception: {0}", sql_excp.ToString());
                return null;
            }
        }

        public DataRow[] ReadDeviceTelemetry()
        {
            DataTable device_telemetry_tbl = DataHelper.MakeDeviceTelemetryTable();
            try
            {
                MySqlConnection conn = new MySqlConnection(connectionString);
                conn.Open();
                string command = @"select dti.id, 
dt.id 'device_type_id', dt.name 'device_type_name',
it.id 'inspect_type_id', it.code 'inspect_type_code',
it.name 'inspect_type_name', it.unit 'inspect_type_unit',
dti.standard,
dti.low_down_alert 'yellow_lower_bound', dti.low_up_alert 'yellow_upper_bound',
dti.high_down_alert 'red_lower_bound', dti.high_up_alert 'red_upper_bound'
from device_type_inspect dti
left join device_type as dt on dti.device_type_id=dt.id
left join inspect_type as it on dti.inspect_type_id=it.id;
";
                MySqlCommand sqlCommand = new MySqlCommand(command, conn);
                MySqlDataReader mysqlReader = sqlCommand.ExecuteReader();
                var columns = new List<string>();
                for (int i = 0; i < mysqlReader.FieldCount; i++)
                {
                    columns.Add(mysqlReader.GetName(i));
                }
                while (mysqlReader.Read())
                {
                    Console.WriteLine("Get device telemetry {0} - {1} - {2} - {3}",
                        mysqlReader["id"], mysqlReader["device_type_id"],
                        mysqlReader["inspect_type_id"], mysqlReader["inspect_type_code"]);

                    DataRow dt_row = device_telemetry_tbl.NewRow();

                    foreach(string column_name in columns)
                    {
                        dt_row[column_name] = mysqlReader[column_name];
                    }

                    device_telemetry_tbl.Rows.Add(dt_row);

                }

                mysqlReader.Close();
                device_telemetry_tbl.AcceptChanges();

                return device_telemetry_tbl.Select();
            }
            catch (Exception sql_excp)
            {
                Console.WriteLine("Read device telemetry type from mysql failed. My sql exception: {0}", sql_excp.ToString());
                return null;
            }
        }


    }
}
