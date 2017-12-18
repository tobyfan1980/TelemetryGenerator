using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QC = System.Data.SqlClient;  // System.Data.dll  
using System.Data;

namespace TelemetryGenerator
{
    class SQLProcessor
    {
        string db_server;
        string db_name;
        string connection_string;
        public List<AlertType> alert_types = new List<AlertType>();

        Dictionary<string, string> create_table_cmds = new Dictionary<string, string>();

        public SQLProcessor(string sql_server, string domain, string sql_db, string username, string password)
        {
            db_server = sql_server;
            db_name = sql_db;
            connection_string = string.Format("Server=tcp:{0}.database.{4},1433;Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;",
                db_server, db_name, username, password, domain);

            InitCreateCommands();
        }

        public void InitCreateCommands()
        {   
            string create_table_webjob_run_record =
                @"create table webjob_run_record(
id bigint not null identity(1, 1) primary key,
monitor_data_date datetime NOT NULL,
timezone int not null default(8),
job_start_time datetime,
job_end_time datetime,
monitor_data_added bigint,
alert_data_added bigint,
utilization_data_added bigint,
retry int,
CONSTRAINT UC_date UNIQUE (monitor_data_date,timezone));";

            string create_table_dim_monitor_type =
                @"create table dim_monitor_type(
id int not null primary key,
name nvarchar(64),
code varchar(64),
unit nvarchar(64));";

            string create_table_dim_alert =
                @"create table dim_alert(
alert_type int not null primary key,
alert_name nvarchar(50));";

            // create table 
            string create_table_dim_device =
                @"create table dim_device(
device_id bigint not null primary key,
serial_number nvarchar(50),
device_name nvarchar(255),
model nvarchar(50),
type_name nvarchar(50),
type_id int,
create_date datetime,
purchase_date datetime,
maintain_date datetime,
maintain_rule nvarchar(255),
maintain_alert_days int,
push_type nvarchar(50),
push_internval int,
building_id int,
building_name nvarchar(255),
floor_id int,
floor_name nvarchar(255),
room_id int,
room_name nvarchar(255),
control varchar(50),
control_online_status int,
control_battery_status int,
company_id int,
company_name nvarchar(50)); ";

            string create_table_dim_device_telemetry =
                @"create table dim_device_telemetry(
id int not null primary key,
device_type_name nvarchar(50),
device_type_id int,
inspect_type_id int,
inspect_type_code varchar(10),
inspect_type_name nvarchar(50),
inspect_type_unit nvarchar(20),
standard float,
yellow_lower_bound float,
yellow_upper_bound float,
red_lower_bound float,
red_upper_bound float); ";

            string create_table_fact_monitor_result =
                @"create table fact_monitor_result(
id bigint not null identity(1,1) primary key,
device_id bigint not null foreign key references dim_device(device_id),
device_type_id int not null,
device_type_name nvarchar(50),
device_telemetry_id int not null foreign key references dim_device_telemetry(id),
create_time datetime,
result float,
company_id int,
company_name nvarchar(50)); ";

            /*
            // with compnay
            string create_table_fact_alert =
                @"create table fact_alert(
id bigint not null identity(1,1) primary key,
device_id bigint not null foreign key references dim_device(device_id),
device_type_id int not null,
device_type_name nvarchar(50),
device_telemetry_id int not null foreign key references dim_device_telemetry(id),
alert_type int not null foreign key references dim_alert(alert_type),
consecutive_alert_count int,
start_time datetime,
end_time datetime,
result float,
company_id int,
company_name nvarchar(50)); ";

*/
            string create_table_fact_alert =
                            @"create table fact_alert(
id bigint not null identity(1,1) primary key,
device_id bigint not null foreign key references dim_device(device_id),
device_type_id int not null,
device_type_name nvarchar(50),
monitor_type_id int not null foreign key references dim_monitor_type(id),
monitor_type_name nvarchar(50),
alert_type int not null foreign key references dim_alert(alert_type),
consecutive_alert_count int,
start_time datetime,
end_time datetime,
result float); ";

            //without company
            /* 
            //with company 
            string create_table_fact_alert_daily_sum =
                @"create table fact_alert_daily_sum(
id bigint not null identity(1,1) primary key,
device_id bigint not null foreign key references dim_device(device_id),
device_type_id int not null,
device_type_name nvarchar(50),
device_telemetry_id int not null foreign key references dim_device_telemetry(id),
alert_type int not null foreign key references dim_alert(alert_type),
alert_count int,
create_date date NOT NULL,
result float,
company_id int,
company_name nvarchar(50)); ";
*/

            // without company
            string create_table_fact_alert_daily_sum =
                            @"create table fact_alert_daily_sum(
id bigint not null identity(1,1) primary key,
device_id bigint not null foreign key references dim_device(device_id),
device_type_id int not null,
device_type_name nvarchar(50),
monitor_type_id int not null foreign key references dim_monitor_type(id),
monitor_type_name nvarchar(50),
alert_type int not null foreign key references dim_alert(alert_type),
alert_count int,
create_date date NOT NULL,
result float,
CONSTRAINT device_alert_per_day UNIQUE NONCLUSTERED (device_id, monitor_type_id, alert_type, create_date)); ";

            /*
           //with company
            string create_table_fact_monitor_result_daily_average =
               @"create table fact_monitor_result_daily_average(
id bigint not null identity(1,1) primary key,
device_id bigint not null foreign key references dim_device(device_id),
device_type_id int not null,
device_type_name nvarchar(50),
device_telemetry_id int not null foreign key references dim_device_telemetry(id),
create_date date NOT NULL,
result float,
company_id int,
company_name nvarchar(50)); ";
*/

            //without company
            string create_table_fact_monitor_result_daily_average =
   @"create table fact_monitor_result_daily_average(
id bigint not null identity(1,1) primary key,
device_id bigint not null foreign key references dim_device(device_id),
device_type_id int not null,
device_type_name nvarchar(50),
monitor_type_id int not null foreign key references dim_monitor_type(id),
monitor_type_name nvarchar(50),
create_date date NOT NULL,
result float,
CONSTRAINT device_monitor_per_day UNIQUE NONCLUSTERED (device_id, monitor_type_id, create_date)); ";

            string create_table_fact_utilization_daily =
    @"create table fact_utilization_daily (
id bigint not null identity(1,1) primary key,
device_id bigint not null foreign key references dim_device(device_id),
device_name nvarchar(50),
device_type nvarchar(50), 
create_date datetime not null,
running_time float,
idle_time float,
poweroff_time float,
total_hours int,
utilization float,
power_lower_bound float,
power_upper_bound float,
consumed_energy float,
CONSTRAINT AK_Device_Date UNIQUE(device_id, create_date));";

            create_table_cmds.Add("fact_utilization_daily", create_table_fact_utilization_daily);
            create_table_cmds.Add("fact_alert", create_table_fact_alert);
            create_table_cmds.Add("fact_alert_daily_sum", create_table_fact_alert_daily_sum);
            create_table_cmds.Add("fact_monitor_result_daily_average", create_table_fact_monitor_result_daily_average);
            create_table_cmds.Add("dim_device", create_table_dim_device);
            create_table_cmds.Add("dim_alert", create_table_dim_alert);
            create_table_cmds.Add("dim_device_telemetry", create_table_dim_device_telemetry);
            create_table_cmds.Add("dim_monitor_type", create_table_dim_monitor_type);
            create_table_cmds.Add("fact_monitor_results", create_table_fact_monitor_result);
            create_table_cmds.Add("webjob_run_record", create_table_webjob_run_record);
            create_table_cmds.Add("fact_daily_utilization", create_table_fact_utilization_daily);
        }

        public void createTable(string tableName)
        {
            ExecuteSqlCommandNonQuery(create_table_cmds[tableName]);
        }

        public void CleanupIntelabDB()
        {
            ExecuteSqlCommandNonQuery("delete from fact_monitor_result_daily_average");
            ExecuteSqlCommandNonQuery("delete from fact_monitor_result");
            ExecuteSqlCommandNonQuery("delete from fact_alert");
            ExecuteSqlCommandNonQuery("delete from dim_device_telemetry");
            ExecuteSqlCommandNonQuery("delete from dim_device");
            ExecuteSqlCommandNonQuery("delete from fact_alert_daily_sum");

        }

        public void CleanupFactTable()
        {
            ExecuteSqlCommandNonQuery("delete from fact_monitor_result_daily_average");
            ExecuteSqlCommandNonQuery("delete from fact_monitor_result");
            ExecuteSqlCommandNonQuery("delete from fact_alert");
            ExecuteSqlCommandNonQuery("delete from fact_alert_daily_sum");
            ExecuteSqlCommandNonQuery("delete from fact_utilization_daily");

            ExecuteSqlCommandNonQuery("drop table  fact_monitor_result_daily_average");
            ExecuteSqlCommandNonQuery("drop table  fact_monitor_result");
            ExecuteSqlCommandNonQuery("drop table  fact_alert");
            ExecuteSqlCommandNonQuery("drop table  fact_alert_daily_sum");
            ExecuteSqlCommandNonQuery("drop table  fact_utilization_daily");
        }

        public void SaveAlertTypes()
        {
            alert_types.Add(new AlertType(1, "黄色警报"));
            alert_types.Add(new AlertType(2, "红色警报"));

            WriteAlertTypes(alert_types);
        }


        public void InitilizeTable(string tableName)
        {
            string drop_cmd = "drop table " + tableName;

            ExecuteSqlCommandNonQuery(drop_cmd);

            createTable(tableName);
        }

        
        public void CleanupDimTable()
        {
            ExecuteSqlCommandNonQuery("drop table dim_device_telemetry");
            ExecuteSqlCommandNonQuery("drop table dim_device");
            ExecuteSqlCommandNonQuery("drop table dim_alert");
            ExecuteSqlCommandNonQuery("drop table dim_monitor_type");
            
        }
        public void InitializeIntelabDB(bool addAllMonitorResult)
        {
            CleanupFactTable();
            CleanupDimTable();

            ExecuteSqlCommandNonQuery("drop table webjob_run_record");

            // create table
            createTable("dim_monitor_type");
            createTable("dim_device");
            createTable("dim_alert");
            createTable("dim_device_telemetry");

            if (addAllMonitorResult)
            {
                createTable("fact_monitor_result");
            }
            
            createTable("fact_alert");
            createTable("fact_monitor_result_daily_average");
            createTable("fact_alert_daily_sum");
            createTable("fact_daily_utilization");
            createTable("webjob_run_record");
        }

        public void ExecuteSqlCommandQuery(string cmdStr)
        {
            Console.WriteLine("Executing SQL command {0}", cmdStr);
            using (var connection = new QC.SqlConnection(connection_string))
            {
                try
                {
                    connection.Open();
                    using (var command = new QC.SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandType = System.Data.CommandType.Text;
                        command.CommandText = cmdStr;
                        try
                        {
                            
                            QC.SqlDataReader reader = command.ExecuteReader();
                            while (reader.Read())
                            {
                                Console.WriteLine("\t{0}\t{1}\t{2}",
                                    reader[0], reader[1], reader[2]);
                            }
                            reader.Close();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }

                    }

                }
                catch (Exception expt)
                {
                    Console.WriteLine("executing sql cmd failed: {0}", expt.ToString());
                }
            }
        }
        
        public void ExecuteSqlCommandNonQuery(string cmdStr)
        {
            Console.WriteLine("Executing SQL command {0}", cmdStr);
            using (var connection = new QC.SqlConnection(connection_string))
            {
                try
                {
                    connection.Open();
                    using (var command = new QC.SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandType = System.Data.CommandType.Text;
                        command.CommandText = cmdStr;
                        command.ExecuteNonQuery();
                    }

                    }catch(Exception expt)
                {
                    Console.WriteLine("executing sql cmd failed: {0}", expt.ToString());
                }
            }
        }


        public Int64 ExecuteSqlCommandCountQuery(string cmdStr)
        {
            Console.WriteLine("Executing SQL command {0}", cmdStr);
            using (var connection = new QC.SqlConnection(connection_string))
            {
                try
                {
                    connection.Open();
                    using (var command = new QC.SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandType = System.Data.CommandType.Text;
                        command.CommandText = cmdStr;
                        string returnValue = command.ExecuteScalar().ToString();
                        Console.WriteLine("sql command count result is {0}", returnValue);
                        return Int64.Parse(returnValue);
                    }

                }
                catch (Exception expt)
                {
                    Console.WriteLine("executing sql cmd failed: {0}", expt.ToString());
                    return -1;
                }
            }
        }
        public void ExecuteSqlCommandCountByGroup(string cmdStr)
        {
            Console.WriteLine("Executing SQL command {0}", cmdStr);
            using (var connection = new QC.SqlConnection(connection_string))
            {
                try
                {
                    connection.Open();

                    using (var command = new QC.SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandType = System.Data.CommandType.Text;
                        command.CommandText = cmdStr;

                        QC.SqlDataReader reader = command.ExecuteReader();

                        while (reader.Read())
                        {
                            Console.WriteLine("sql command result is {0} - {1}", reader[0], reader[1]);
                        }
                    }

                }
                catch (Exception expt)
                {
                    Console.WriteLine("executing sql cmd failed: {0}", expt.ToString());
                }
            }
        }
        public void WriteDevices(List<DimDevice> device_list)
        {
           
            using (var connection = new QC.SqlConnection(connection_string))
            {
                connection.Open();
                Console.Out.WriteLine("Connected to SQL server");

                foreach(DimDevice device in device_list)
                {
                    try
                    {
                        WriteOneDevice(connection, device);
                    }catch(Exception e)
                    {
                        Console.WriteLine("Failed to write device. {0}", e.ToString());
                    }
                }
            }
        }

        public void MergeDevicesToDB(DataRow[] devices_to_merge)
        {
            
            List<DataRow> devices_to_insert = new List<DataRow>();
            using (var connection = new QC.SqlConnection(connection_string))
            {
                connection.Open();
                Console.Out.WriteLine("Connected to SQL server");
                
                foreach(DataRow device in devices_to_merge)
                {
                    //filter out the existing ones
                    Int64 count = ExecuteSqlCommandCountQuery(string.Format("select count(*) from dim_device where device_id={0}", device["device_id"]));
                    if (count == 0)
                    {
                        devices_to_insert.Add(device);
                    }
                }

            }
            WriteDataRowsToSQLTable("dim_device", devices_to_insert.ToArray());
        }

        public void MergeDeviceTelemetryToDB(DataRow[] telemetry_to_merge)
        {

            List<DataRow> inspect_telemetry_to_insert = new List<DataRow>();
            using (var connection = new QC.SqlConnection(connection_string))
            {
                connection.Open();
                Console.Out.WriteLine("Connected to SQL server");

                foreach (DataRow telemetry in telemetry_to_merge)
                {
                    //filter out the existing ones
                    Int64 count = ExecuteSqlCommandCountQuery(string.Format("select count(*) from dim_device_telemetry where id={0}", telemetry["id"]));
                    if (count == 0)
                    {
                        inspect_telemetry_to_insert.Add(telemetry);
                    }
                }

            }
            WriteDataRowsToSQLTable("dim_device_telemetry", inspect_telemetry_to_insert.ToArray());
        }

        public void WriteOneDevice(QC.SqlConnection connection, DimDevice device)
        {
            QC.SqlParameter parameter;
            using (var command = new QC.SqlCommand())
            {
                command.Connection = connection;
                command.CommandType = System.Data.CommandType.Text;
                command.CommandText = @"
                    Insert into dim_device
                        (device_id, company_id, company_name, serial_number, device_name, model, type_name, type_id, create_date, purchase_date, 
                         building_id, building_name, floor_id, floor_name, control, control_online_status, control_battery_status)
                    Values
                        (@device_id, @company_id, @company_name, @serial_number, @device_name, @model, @type_name, @type_id, @create_date, @purchase_date, 
                         @building_id, @building_name, @floor_id, @floor_name, @control, @control_online_status, @control_battery_status
                         );";

                parameter = new QC.SqlParameter("@device_id", System.Data.SqlDbType.BigInt);
                parameter.Value = device.id;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@company_id", System.Data.SqlDbType.Int);
                parameter.Value = device.company_id;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@company_name", System.Data.SqlDbType.NVarChar, 255);
                parameter.Value = device.company_name;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@serial_number", System.Data.SqlDbType.VarChar, 50);
                parameter.Value = device.serial_number;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@device_name", System.Data.SqlDbType.NVarChar, 255);
                parameter.Value = device.device_name;
                command.Parameters.Add(parameter);
                parameter = new QC.SqlParameter("@model", System.Data.SqlDbType.NVarChar, 50);
                parameter.Value = device.model;
                command.Parameters.Add(parameter);
                parameter = new QC.SqlParameter("@type_name", System.Data.SqlDbType.NVarChar, 50);
                parameter.Value = device.type_name;
                command.Parameters.Add(parameter);
                parameter = new QC.SqlParameter("@type_id", System.Data.SqlDbType.Int);
                parameter.Value = device.type_id;
                command.Parameters.Add(parameter);
                parameter = new QC.SqlParameter("@create_date", System.Data.SqlDbType.DateTime);
                parameter.Value = device.create_date;
                command.Parameters.Add(parameter);
                parameter = new QC.SqlParameter("@purchase_date", System.Data.SqlDbType.DateTime);
                parameter.Value = device.purchase_date;
                command.Parameters.Add(parameter);
                parameter = new QC.SqlParameter("@building_id", System.Data.SqlDbType.Int);
                parameter.Value = device.building_id;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@building_name", System.Data.SqlDbType.NVarChar, 255);
                parameter.Value = device.building_name;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@floor_id", System.Data.SqlDbType.Int);
                parameter.Value = device.floor_id;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@floor_name", System.Data.SqlDbType.NVarChar, 255);
                parameter.Value = device.floor_name;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@control", System.Data.SqlDbType.VarChar, 50);
                parameter.Value = device.control;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@control_online_status", System.Data.SqlDbType.Int);
                parameter.Value = device.control_online_status;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@control_battery_status", System.Data.SqlDbType.Int);
                parameter.Value = device.control_battery_status;
                command.Parameters.Add(parameter);

                command.ExecuteScalar();

                Console.WriteLine("Insert device to DB: {0}", device.toString());
            }

        }


        public void WriteAlertTypes(List<AlertType> alert_list)
        {
            using (var connection = new QC.SqlConnection(connection_string))
            {
                connection.Open();
                Console.Out.WriteLine("Connected to SQL server");

                foreach (AlertType alert in alert_list)
                {
                    try
                    {
                        WriteOneAlertType(connection, alert);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Failed to write alert type {1}. {0}", e.ToString(), alert.alert_name);
                    }
                }
            }
        }

        public void WriteDeviceTelemetries(List<TelemetryInspect> tele_list)
        {

            using (var connection = new QC.SqlConnection(connection_string))
            {
                connection.Open();
                Console.Out.WriteLine("Connected to SQL server");

                foreach (TelemetryInspect tele in tele_list)
                {
                    try
                    {
                        WriteOneTelemetryInspector(connection, tele);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Failed to write device telemetry. {0}", e.ToString());
                    }
                }
            }
        }

        public void WriteOneTelemetryInspector(QC.SqlConnection connection, TelemetryInspect tele)
        {
            QC.SqlParameter parameter;
            using (var command = new QC.SqlCommand())
            {
                command.Connection = connection;
                command.CommandType = System.Data.CommandType.Text;
                command.CommandText = @"
                    Insert into dim_device_telemetry
                        (id, device_type_name, device_type_id, inspect_type_id, inspect_type_code, inspect_type_name, inspect_type_unit, 
                         standard, yellow_lower_bound, yellow_upper_bound, red_lower_bound, red_upper_bound)
                    Values
                        (@id, @device_type_name, @device_type_id, @inspect_type_id, @inspect_type_code, @inspect_type_name, @inspect_type_unit, 
                         @standard, @yellow_lower_bound, @yellow_upper_bound, @red_lower_bound, @red_upper_bound
                         );";
                
                parameter = new QC.SqlParameter("@id", System.Data.SqlDbType.Int);
                parameter.Value = tele.id;
                command.Parameters.Add(parameter);
                

                parameter = new QC.SqlParameter("@device_type_name", System.Data.SqlDbType.NVarChar, 50);
                parameter.Value = tele.device_type_name;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@device_type_id", System.Data.SqlDbType.Int);
                parameter.Value = tele.device_type_id;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@inspect_type_id", System.Data.SqlDbType.Int);
                parameter.Value = tele.inspect_type_id;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@inspect_type_code", System.Data.SqlDbType.VarChar, 10);
                parameter.Value = tele.inspect_type_code;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@inspect_type_name", System.Data.SqlDbType.NVarChar, 50);
                parameter.Value = tele.inspect_type_name;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@inspect_type_unit", System.Data.SqlDbType.NVarChar, 20);
                parameter.Value = tele.inspect_type_unit;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@standard", System.Data.SqlDbType.Float);
                parameter.Value = tele.standard;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@yellow_lower_bound", System.Data.SqlDbType.Float);
                parameter.Value = tele.yellow_lower_bound;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@yellow_upper_bound", System.Data.SqlDbType.Float);
                parameter.Value = tele.yellow_upper_bound;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@red_lower_bound", System.Data.SqlDbType.Float);
                parameter.Value = tele.red_lower_bound;
                command.Parameters.Add(parameter);

                parameter = new QC.SqlParameter("@red_upper_bound", System.Data.SqlDbType.Float);
                parameter.Value = tele.red_upper_bound;
                command.Parameters.Add(parameter);

                command.ExecuteScalar();

                Console.WriteLine("Insert telemetry inspect to DB: {0}", tele.toString());
            }

        }

        public void WriteOneAlertType(QC.SqlConnection connection, AlertType alert)
        {
            QC.SqlParameter parameter;
            using (var command = new QC.SqlCommand())
            {
                command.Connection = connection;
                command.CommandType = System.Data.CommandType.Text;
                command.CommandText = @"
                    Insert into dim_alert
                        (alert_type,alert_name)
                    Values
                        (@alert_type, @alert_name
                         );";

                parameter = new QC.SqlParameter("@alert_type", System.Data.SqlDbType.Int);
                parameter.Value = alert.alert_type;
                command.Parameters.Add(parameter);


                parameter = new QC.SqlParameter("@alert_name", System.Data.SqlDbType.NVarChar, 50);
                parameter.Value = alert.alert_name;
                command.Parameters.Add(parameter);

               

                command.ExecuteScalar();

                Console.WriteLine("Insert alert to DB: {0}", alert.toString());
            }

        }


        public List<DimDevice> readDevices(int skip, int limit)
        {
            List<DimDevice> device_list = new List<DimDevice>();

            using (var connection = new QC.SqlConnection(connection_string))
            {
                string queryString = string.Format("select device_id, device_name, type_name, type_id, company_id from dim_device order by device_id offset {0} rows fetch next {1} rows only;", skip, limit);
                QC.SqlCommand command = new System.Data.SqlClient.SqlCommand(queryString, connection);

                try
                {
                    connection.Open();
                    Console.Out.WriteLine("Connected to SQL server");
                    QC.SqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        DimDevice device = new DimDevice();

                        
                        device.id = (int)reader.GetInt64(0);
                        
                        device.device_name = reader.GetString(1);
                        device.type_name = reader.GetString(2);
                        device.type_id = reader.GetInt32(3);
                        device.company_id = reader.GetInt32(4);

                        device_list.Add(device);
                    }
                    
                }catch(Exception except)
                {
                    Console.WriteLine("read device table error: {0}", except.ToString());
                }
            }


            return device_list;
        }

        public void WriteOneMonitorResult(long device_id, int company_id, int device_type_id, int telemetry_id, DateTime event_time, float result)
        {
            using (var connection = new QC.SqlConnection(connection_string))
            {
                connection.Open();
                Console.Out.WriteLine("Connected to SQL server");

                QC.SqlParameter parameter;
                using (var command = new QC.SqlCommand())
                {
                    command.Connection = connection;
                    command.CommandType = System.Data.CommandType.Text;
                    command.CommandText = @"
                        Insert into fact_monitor_result
                            ( device_id, company_id, device_type_id, device_telemetry_id, create_time, result)
                        Values
                            ( @device_id, @company_id, @device_type_id, @device_telemetry_id, @create_time, @result
                             );";
                    
                    parameter = new QC.SqlParameter("@device_id", System.Data.SqlDbType.BigInt);
                    parameter.Value = device_id;
                    command.Parameters.Add(parameter);

                    parameter = new QC.SqlParameter("@company_id", System.Data.SqlDbType.Int);
                    parameter.Value = company_id;
                    command.Parameters.Add(parameter);

                    parameter = new QC.SqlParameter("@device_type_id", System.Data.SqlDbType.Int);
                    parameter.Value = device_type_id;
                    command.Parameters.Add(parameter);

                    parameter = new QC.SqlParameter("@device_telemetry_id", System.Data.SqlDbType.Int);
                    parameter.Value = telemetry_id;
                    command.Parameters.Add(parameter);

                    parameter = new QC.SqlParameter("@create_time", System.Data.SqlDbType.DateTime);
                    parameter.Value = event_time;
                    command.Parameters.Add(parameter);

                    parameter = new QC.SqlParameter("@result", System.Data.SqlDbType.Float);
                    parameter.Value = result;
                    command.Parameters.Add(parameter);

                    try
                    {
                        command.ExecuteScalar();
                        Console.WriteLine("Insert {1} result inspect to DB: {0}", device_id, result.ToString());

                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Failed to write. err: {0}", e.ToString());
                    }
                }
            }
        
        }

        public void WriteReportDateToDB(ReportData report, DateTime start, TimeSpan interval, long device_id, int inspect_type_id)
        {
            // use alert index to write all alert data in report data to fact_alert table

            // calculate daily average and write to fact_monitor_result_daily_average

            // if it include today's data, write data to fact_monitor_result table
        }

        public void WriteDataRowsToSQLTable(string tableName, DataRow[] resultRows)
        {
            using (var connection = new QC.SqlConnection(connection_string))
            {
                connection.Open();

                using (QC.SqlBulkCopy bulkCopy = new QC.SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = tableName;

                    try
                    {
                        bulkCopy.WriteToServer(resultRows);

                        Console.WriteLine("---Wrote {0} rows to table {1}", resultRows.Length, tableName);
                    }catch(Exception e)
                    {
                        Console.WriteLine("======!!!failed to write bulk data to table {0}. Error: {1}", 
                            tableName, e.ToString());
                    }
                }
            }
               
        }
    }
}
