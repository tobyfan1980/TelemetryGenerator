using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QC = System.Data.SqlClient;  // System.Data.dll  

namespace TelemetryGenerator
{
    class SQLProcessor
    {
        public string connection_string = string.Format("Server=tcp:intelab-db.database.windows.net,1433;Initial Catalog=intelab-db;Persist Security Info=False;User ID={0};Password={1};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;",
                "superadmin", "intelab-2016");
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

        public void WriteOneDevice(QC.SqlConnection connection, DimDevice device)
        {
            QC.SqlParameter parameter;
            using (var command = new QC.SqlCommand())
            {
                command.Connection = connection;
                command.CommandType = System.Data.CommandType.Text;
                command.CommandText = @"
                    Insert into dim_device
                        (device_id, serial_number, device_name, model, type_name, type_id, create_date, purchase_date, 
                         building_id, building_name, floor_id, floor_name, control, control_online_status, control_battery_status)
                    Values
                        (@device_id, @serial_number, @device_name, @model, @type_name, @type_id, @create_date, @purchase_date, 
                         @building_id, @building_name, @floor_id, @floor_name, @control, @control_online_status, @control_battery_status
                         );";

                parameter = new QC.SqlParameter("@device_id", System.Data.SqlDbType.BigInt);
                parameter.Value = device.id;
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
                        Console.WriteLine("Failed to write device. {0}", e.ToString());
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
                        ( device_type_name, device_type_id, inspect_type_id, inspect_type_code, inspect_type_name, inspect_type_unit, 
                         standard, yellow_lower_bound, yellow_upper_bound, red_lower_bound, red_upper_bound)
                    Values
                        ( @device_type_name, @device_type_id, @inspect_type_id, @inspect_type_code, @inspect_type_name, @inspect_type_unit, 
                         @standard, @yellow_lower_bound, @yellow_upper_bound, @red_lower_bound, @red_upper_bound
                         );";
                /*
                parameter = new QC.SqlParameter("@id", System.Data.SqlDbType.Int);
                parameter.Value = tele.id;
                command.Parameters.Add(parameter);
                */

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
         

        public List<DimDevice> readDevices(int skip, int limit)
        {
            List<DimDevice> device_list = new List<DimDevice>();

            using (var connection = new QC.SqlConnection(connection_string))
            {
                string queryString = string.Format("select device_id, device_name, type_name, type_id from dim_device order by device_id offset {0} rows fetch next {1} rows only;", skip, limit);
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

                        device_list.Add(device);
                    }
                    
                }catch(Exception except)
                {
                    Console.WriteLine("read device table error: {0}", except.ToString());
                }
            }


            return device_list;
        }
    }
}
