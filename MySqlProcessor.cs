using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace TelemetryGenerator
{
    class MySqlProcessor
    {
        public string connectionString = "server=139.196.240.232;user=lixie;password=123456;port=3306;database=inspect";

        public void ReadDevices()
        {
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
            }
            catch (Exception sql_excp)
            {
                Console.WriteLine("My sql exception: {0}", sql_excp.ToString());
            }
        }

        public void ReadAlert()
        {
            try
            {
                MySqlConnection conn = new MySqlConnection(connectionString);
                conn.Open();
                string command = @"SELECT a.id, a.device_id, d.type_id 'device_type_id',
dt.name 'device_type_name',
c.id 'company_id', c.name 'company_name',
a.inspect_type_id 'device_telemetry_id', a.alert_type,
a.alert_num 'consecutive_alert_count', a.create_date 'create_time', a.finish_date 'end_time'
FROM alert_count a
left join device as d on a.device_id=d.id
left join device_type as dt on d.type_id=dt.id
left join company as c on dt.company_id=c.id
order by a.device_id, a.inspect_Type_id, a.create_date;";
                MySqlCommand sqlCommand = new MySqlCommand(command, conn);
                MySqlDataReader mysqlReader = sqlCommand.ExecuteReader();
                while (mysqlReader.Read())
                {
                    Console.WriteLine("Get device {0} - {1} - {2} - {3}",
                        mysqlReader["id"], mysqlReader["device_type_id"],
                        mysqlReader["create_time"], mysqlReader["end_time"]);

                }
                mysqlReader.Close();
            }
            catch (Exception sql_excp)
            {
                Console.WriteLine("My sql exception: {0}", sql_excp.ToString());
            }
        }
        public void ReadMonitorResult()
        {
            try
            {
                MySqlConnection conn = new MySqlConnection(connectionString);
                conn.Open();
                string command = @"select id.id, id.device_id, d.type_id 'device_type_id',
dt.name 'device_type_name',
id.device_inspect_id 'device_telemetry_id', c.id 'company_id', c.name 'company_name',
id.create_date 'create_time', id.result
from inspect_data as id
left join device as d on id.device_id=d.id
left join device_type as dt on d.type_id=dt.id
left join company as c on dt.company_id=c.id
where id.create_date>='2016-10-20' limit 5,10;";
                MySqlCommand sqlCommand = new MySqlCommand(command, conn);
                MySqlDataReader mysqlReader = sqlCommand.ExecuteReader();
                while (mysqlReader.Read())
                {
                    Console.WriteLine("Get device {0} - {1} - {2} - {3}",
                        mysqlReader["id"], mysqlReader["device_type_id"],
                        mysqlReader["create_time"], mysqlReader["result"]);

                }
                mysqlReader.Close();
            }
            catch (Exception sql_excp)
            {
                Console.WriteLine("My sql exception: {0}", sql_excp.ToString());
            }
        }

        public void ReadDimDevice()
        {
            try
            {
                MySqlConnection conn = new MySqlConnection(connectionString);
                conn.Open();
                string command = @"select d.id 'device_id', d.name 'device_name', d.code 'device_sn',
d.model 'device_model', d.create_date, d.purchase_date,
dt.id 'device_type_id', dt.name 'device_type_name',
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
                while (mysqlReader.Read())
                {
                    Console.WriteLine("Get device {0} - {1} - {2} - {3}",
                        mysqlReader["device_id"], mysqlReader["device_name"],
                        mysqlReader["device_sn"], mysqlReader["device_model"]);

                }
                mysqlReader.Close();
            }
            catch (Exception sql_excp)
            {
                Console.WriteLine("My sql exception: {0}", sql_excp.ToString());
            }
        }

        public void ReadDeviceTelemetry()
        {
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
                while (mysqlReader.Read())
                {
                    Console.WriteLine("Get device telemetry {0} - {1} - {2} - {3}",
                        mysqlReader["id"], mysqlReader["device_type_id"],
                        mysqlReader["inspect_type_id"], mysqlReader["inspect_type_code"]);

                }
                mysqlReader.Close();
            }
            catch (Exception sql_excp)
            {
                Console.WriteLine("My sql exception: {0}", sql_excp.ToString());
            }
        }


    }
}
