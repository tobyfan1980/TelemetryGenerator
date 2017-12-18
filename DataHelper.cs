using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace TelemetryGenerator
{
    class DataHelper
    {
        public static DataTable MakeDeviceTable()
        {
            DataTable device_tbl = new DataTable("devices");

            DataColumn device_id_col = new DataColumn();
            device_id_col.ColumnName = "device_id";
            device_id_col.AutoIncrement = false;
            device_id_col.DataType = System.Type.GetType("System.Int64");
            device_tbl.Columns.Add(device_id_col);


            DataColumn sn_col = new DataColumn();
            sn_col.ColumnName = "serial_number";
            sn_col.DataType = System.Type.GetType("System.String");
            device_tbl.Columns.Add(sn_col);

            DataColumn device_name_col = new DataColumn();
            device_name_col.ColumnName = "device_name";
            device_name_col.DataType = System.Type.GetType("System.String");
            device_tbl.Columns.Add(device_name_col);

            DataColumn model_col = new DataColumn();
            model_col.ColumnName = "model";
            model_col.DataType = System.Type.GetType("System.String");
            device_tbl.Columns.Add(model_col);

            DataColumn type_name_col = new DataColumn();
            type_name_col.ColumnName = "type_name";
            type_name_col.DataType = System.Type.GetType("System.String");
            device_tbl.Columns.Add(type_name_col);

            DataColumn type_id_col = new DataColumn();
            type_id_col.ColumnName = "type_id";
            type_id_col.DataType = System.Type.GetType("System.Int32");
            device_tbl.Columns.Add(type_id_col);

            DataColumn create_date_col = new DataColumn();
            create_date_col.ColumnName = "create_date";
            create_date_col.DataType = System.Type.GetType("System.DateTime");
            device_tbl.Columns.Add(create_date_col);

            DataColumn purchase_date_col = new DataColumn();
            purchase_date_col.ColumnName = "purchase_date";
            purchase_date_col.DataType = System.Type.GetType("System.DateTime");
            device_tbl.Columns.Add(purchase_date_col);

            DataColumn maintain_date_col = new DataColumn();
            maintain_date_col.ColumnName = "maintain_date";
            maintain_date_col.DataType = System.Type.GetType("System.DateTime");
            device_tbl.Columns.Add(maintain_date_col);

            DataColumn maintain_rule_col = new DataColumn();
            maintain_rule_col.ColumnName = "maintain_rule";
            maintain_rule_col.DataType = System.Type.GetType("System.String");
            device_tbl.Columns.Add(maintain_rule_col);

            DataColumn maintain_alert_days_col = new DataColumn();
            maintain_alert_days_col.ColumnName = "maintain_alert_days";
            maintain_alert_days_col.DataType = System.Type.GetType("System.Int32");
            device_tbl.Columns.Add(maintain_alert_days_col);

            DataColumn push_type_col = new DataColumn();
            push_type_col.ColumnName = "push_type";
            push_type_col.DataType = System.Type.GetType("System.String");
            device_tbl.Columns.Add(push_type_col);

            DataColumn push_interval_col = new DataColumn();
            push_interval_col.ColumnName = "push_interval";
            push_interval_col.DataType = System.Type.GetType("System.Int32");
            device_tbl.Columns.Add(push_interval_col);

            DataColumn building_id_col = new DataColumn();
            building_id_col.ColumnName = "building_id";
            building_id_col.DataType = System.Type.GetType("System.Int32");
            device_tbl.Columns.Add(building_id_col);

            DataColumn building_name_col = new DataColumn();
            building_name_col.ColumnName = "building_name";
            building_name_col.DataType = System.Type.GetType("System.String");
            device_tbl.Columns.Add(building_name_col);

            DataColumn floor_id_col = new DataColumn();
            floor_id_col.ColumnName = "floor_id";
            floor_id_col.DataType = System.Type.GetType("System.Int32");
            device_tbl.Columns.Add(floor_id_col);

            DataColumn floor_name_col = new DataColumn();
            floor_name_col.ColumnName = "floor_name";
            floor_name_col.DataType = System.Type.GetType("System.String");
            device_tbl.Columns.Add(floor_name_col);

            DataColumn room_id_col = new DataColumn();
            room_id_col.ColumnName = "room_id";
            room_id_col.DataType = System.Type.GetType("System.Int32");
            device_tbl.Columns.Add(room_id_col);

            DataColumn room_name_col = new DataColumn();
            room_name_col.ColumnName = "room_name";
            room_name_col.DataType = System.Type.GetType("System.String");
            device_tbl.Columns.Add(room_name_col);

            DataColumn control_col = new DataColumn();
            control_col.ColumnName = "control";
            control_col.DataType = System.Type.GetType("System.String");
            device_tbl.Columns.Add(control_col);

            DataColumn control_online_status_col = new DataColumn();
            control_online_status_col.ColumnName = "control_online_status";
            control_online_status_col.DataType = System.Type.GetType("System.Int32");
            device_tbl.Columns.Add(control_online_status_col);

            DataColumn control_battery_status_col = new DataColumn();
            control_battery_status_col.ColumnName = "control_battery_status";
            control_battery_status_col.DataType = System.Type.GetType("System.Double");
            device_tbl.Columns.Add(control_battery_status_col);

            DataColumn company_id_col = new DataColumn();
            company_id_col.ColumnName = "company_id";
            company_id_col.DataType = System.Type.GetType("System.Int32");
            device_tbl.Columns.Add(company_id_col);

            DataColumn company_name_col = new DataColumn();
            company_name_col.ColumnName = "company_name";
            company_name_col.DataType = System.Type.GetType("System.String");
            device_tbl.Columns.Add(company_name_col);

            return device_tbl;
        }
        public static DataTable MakeMonitorResultTable()
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

            DataColumn device_type_name_col = new DataColumn();
            device_type_name_col.ColumnName = "device_type_name";
            device_type_name_col.DataType = System.Type.GetType("System.String");
            monitor_result_tbl.Columns.Add(device_type_name_col);

            DataColumn telemetry_type_id_col = new DataColumn();
            telemetry_type_id_col.ColumnName = "monitor_type_id";
            telemetry_type_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_result_tbl.Columns.Add(telemetry_type_id_col);

            DataColumn monitor_type_name_col = new DataColumn();
            monitor_type_name_col.ColumnName = "monitor_type_name";
            monitor_type_name_col.DataType = System.Type.GetType("System.String");
            monitor_result_tbl.Columns.Add(monitor_type_name_col);


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

            DataColumn company_name_col = new DataColumn();
            company_name_col.ColumnName = "company_name";
            company_name_col.DataType = System.Type.GetType("System.String");
            monitor_result_tbl.Columns.Add(company_name_col);

            return monitor_result_tbl;
        }

        public static DataTable MakeDeviceTelemetryTable()
        {
            DataTable device_telemetry_tbl = new DataTable("device_telemetries");

            DataColumn id_col = new DataColumn();
            id_col.ColumnName = "id";
            id_col.AutoIncrement = true;
            id_col.DataType = System.Type.GetType("System.Int64");
            device_telemetry_tbl.Columns.Add(id_col);

            DataColumn device_type_name_col = new DataColumn();
            device_type_name_col.ColumnName = "device_type_name";
            device_type_name_col.DataType = System.Type.GetType("System.String");
            device_telemetry_tbl.Columns.Add(device_type_name_col);


            DataColumn device_type_id_col = new DataColumn();
            device_type_id_col.ColumnName = "device_type_id";
            device_type_id_col.DataType = System.Type.GetType("System.Int32");
            device_telemetry_tbl.Columns.Add(device_type_id_col);


            DataColumn inspect_type_id_col = new DataColumn();
            inspect_type_id_col.ColumnName = "inspect_type_id";
            inspect_type_id_col.DataType = System.Type.GetType("System.Int32");
            device_telemetry_tbl.Columns.Add(inspect_type_id_col);

            DataColumn inspect_type_code_col = new DataColumn();
            inspect_type_code_col.ColumnName = "inspect_type_code";
            inspect_type_code_col.DataType = System.Type.GetType("System.String");
            device_telemetry_tbl.Columns.Add(inspect_type_code_col);

            DataColumn inspect_type_name_col = new DataColumn();
            inspect_type_name_col.ColumnName = "inspect_type_name";
            inspect_type_name_col.DataType = System.Type.GetType("System.String");
            device_telemetry_tbl.Columns.Add(inspect_type_name_col);

            DataColumn inspect_type_unit_col = new DataColumn();
            inspect_type_unit_col.ColumnName = "inspect_type_unit";
            inspect_type_unit_col.DataType = System.Type.GetType("System.String");
            device_telemetry_tbl.Columns.Add(inspect_type_unit_col);

            DataColumn standard_col = new DataColumn();
            standard_col.ColumnName = "standard";
            standard_col.DataType = System.Type.GetType("System.Double");
            device_telemetry_tbl.Columns.Add(standard_col);

            DataColumn yellow_lower_bound_col = new DataColumn();
            yellow_lower_bound_col.ColumnName = "yellow_lower_bound";
            yellow_lower_bound_col.DataType = System.Type.GetType("System.Double");
            device_telemetry_tbl.Columns.Add(yellow_lower_bound_col);

            DataColumn yellow_upper_bound_col = new DataColumn();
            yellow_upper_bound_col.ColumnName = "yellow_upper_bound";
            yellow_upper_bound_col.DataType = System.Type.GetType("System.Double");
            device_telemetry_tbl.Columns.Add(yellow_upper_bound_col);

            DataColumn red_lower_bound_col = new DataColumn();
            red_lower_bound_col.ColumnName = "red_lower_bound";
            red_lower_bound_col.DataType = System.Type.GetType("System.Double");
            device_telemetry_tbl.Columns.Add(red_lower_bound_col);

            DataColumn red_upper_bound_col = new DataColumn();
            red_upper_bound_col.ColumnName = "red_upper_bound";
            red_upper_bound_col.DataType = System.Type.GetType("System.Double");
            device_telemetry_tbl.Columns.Add(red_upper_bound_col);


            return device_telemetry_tbl;
        }

        public static DataTable MakeAlertDailySumTable()
        {
            DataTable monitor_alert_daily_sum_tbl = new DataTable("monitor_alert");

            DataColumn alert_id_col = new DataColumn();
            alert_id_col.ColumnName = "id";
            alert_id_col.AutoIncrement = true;
            alert_id_col.DataType = System.Type.GetType("System.Int64");
            monitor_alert_daily_sum_tbl.Columns.Add(alert_id_col);

            DataColumn device_id_col = new DataColumn();
            device_id_col.ColumnName = "device_id";
            device_id_col.DataType = System.Type.GetType("System.Int64");
            monitor_alert_daily_sum_tbl.Columns.Add(device_id_col);


            DataColumn device_type_id_col = new DataColumn();
            device_type_id_col.ColumnName = "device_type_id";
            device_type_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_daily_sum_tbl.Columns.Add(device_type_id_col);

            DataColumn device_type_name_col = new DataColumn();
            device_type_name_col.ColumnName = "device_type_name";
            device_type_name_col.DataType = System.Type.GetType("System.String");
            monitor_alert_daily_sum_tbl.Columns.Add(device_type_name_col);

            DataColumn monitor_type_id_col = new DataColumn();
            monitor_type_id_col.ColumnName = "monitor_type_id";
            monitor_type_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_daily_sum_tbl.Columns.Add(monitor_type_id_col);


            DataColumn monitor_type_name_col = new DataColumn();
            monitor_type_name_col.ColumnName = "monitor_type_name";
            monitor_type_name_col.DataType = System.Type.GetType("System.String");
            monitor_alert_daily_sum_tbl.Columns.Add(monitor_type_name_col);

            DataColumn alert_type_col = new DataColumn();
            alert_type_col.ColumnName = "alert_type";
            alert_type_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_daily_sum_tbl.Columns.Add(alert_type_col);

            DataColumn alert_count_col = new DataColumn();
            alert_count_col.ColumnName = "alert_count";
            alert_count_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_daily_sum_tbl.Columns.Add(alert_count_col);

            DataColumn date_col = new DataColumn();
            date_col.ColumnName = "create_date";
            date_col.DataType = System.Type.GetType("System.DateTime");
            monitor_alert_daily_sum_tbl.Columns.Add(date_col);

            DataColumn alert_result_col = new DataColumn();
            alert_result_col.ColumnName = "result";
            alert_result_col.DataType = System.Type.GetType("System.Double");// ? how to get float datatype
            monitor_alert_daily_sum_tbl.Columns.Add(alert_result_col);
            /*
            DataColumn company_id_col = new DataColumn();
            company_id_col.ColumnName = "company_id";
            company_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_daily_sum_tbl.Columns.Add(company_id_col);

            DataColumn company_name_col = new DataColumn();
            company_name_col.ColumnName = "company_name";
            company_name_col.DataType = System.Type.GetType("System.String");
            monitor_alert_daily_sum_tbl.Columns.Add(company_name_col);
            */
            return monitor_alert_daily_sum_tbl;
        }
        public static DataTable MakeAlertTable()
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

            DataColumn device_type_name_col = new DataColumn();
            device_type_name_col.ColumnName = "device_type_name";
            device_type_name_col.DataType = System.Type.GetType("System.String");
            monitor_alert_tbl.Columns.Add(device_type_name_col);

            DataColumn monitor_type_id_col = new DataColumn();
            monitor_type_id_col.ColumnName = "monitor_type_id";
            monitor_type_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_tbl.Columns.Add(monitor_type_id_col);

            DataColumn monitor_type_name_col = new DataColumn();
            monitor_type_name_col.ColumnName = "monitor_type_name";
            monitor_type_name_col.DataType = System.Type.GetType("System.String");
            monitor_alert_tbl.Columns.Add(monitor_type_name_col);

            DataColumn alert_type_col = new DataColumn();
            alert_type_col.ColumnName = "alert_type";
            alert_type_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_tbl.Columns.Add(alert_type_col);

            DataColumn alert_count_col = new DataColumn();
            alert_count_col.ColumnName = "consecutive_alert_count";
            alert_count_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_tbl.Columns.Add(alert_count_col);

            DataColumn start_time_col = new DataColumn();
            start_time_col.ColumnName = "start_time";
            start_time_col.DataType = System.Type.GetType("System.DateTime");
            monitor_alert_tbl.Columns.Add(start_time_col);

            DataColumn stop_time_col = new DataColumn();
            stop_time_col.ColumnName = "end_time";
            stop_time_col.DataType = System.Type.GetType("System.DateTime");
            monitor_alert_tbl.Columns.Add(stop_time_col);

            DataColumn alert_result_col = new DataColumn();
            alert_result_col.ColumnName = "result";
            alert_result_col.DataType = System.Type.GetType("System.Double");// ? how to get float datatype
            monitor_alert_tbl.Columns.Add(alert_result_col);

            /*
            DataColumn company_id_col = new DataColumn();
            company_id_col.ColumnName = "company_id";
            company_id_col.AutoIncrement = false;
            company_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_alert_tbl.Columns.Add(company_id_col);

            DataColumn company_name_col = new DataColumn();
            company_name_col.ColumnName = "company_name";
            company_name_col.AutoIncrement = false;
            company_name_col.DataType = System.Type.GetType("System.String");
            monitor_alert_tbl.Columns.Add(company_name_col);
            */
            return monitor_alert_tbl;
        }

        public static DataTable MakeMonitorTypeTable()
        {
            DataTable monitor_type_tbl = new DataTable("monitor_type");

            DataColumn type_id_col = new DataColumn();
            type_id_col.ColumnName = "id";
            type_id_col.DataType = System.Type.GetType("System.Int64");
            monitor_type_tbl.Columns.Add(type_id_col);

            DataColumn type_name_col = new DataColumn();
            type_name_col.ColumnName = "name";
            type_name_col.DataType = System.Type.GetType("System.String");
            monitor_type_tbl.Columns.Add(type_name_col);


            DataColumn type_code_col = new DataColumn();
            type_code_col.ColumnName = "code";
            type_code_col.DataType = System.Type.GetType("System.String");
            monitor_type_tbl.Columns.Add(type_code_col);

            DataColumn type_unit_col = new DataColumn();
            type_unit_col.ColumnName = "unit";
            type_unit_col.DataType = System.Type.GetType("System.String");
            monitor_type_tbl.Columns.Add(type_unit_col);


            return monitor_type_tbl;
        }

        public static DataTable MakeTelemetryAverageTable()
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

            DataColumn device_type_name_col = new DataColumn();
            device_type_name_col.ColumnName = "device_type_name";
            device_type_name_col.DataType = System.Type.GetType("System.String");
            monitor_result_avg_tbl.Columns.Add(device_type_name_col);

            DataColumn monitor_type_id_col = new DataColumn();
            monitor_type_id_col.ColumnName = "monitor_type_id";
            monitor_type_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_result_avg_tbl.Columns.Add(monitor_type_id_col);

            DataColumn monitor_type_name_col = new DataColumn();
            monitor_type_name_col.ColumnName = "monitor_type_name";
            monitor_type_name_col.DataType = System.Type.GetType("System.String");
            monitor_result_avg_tbl.Columns.Add(monitor_type_name_col);

            DataColumn time_col = new DataColumn();
            time_col.ColumnName = "create_date";
            time_col.DataType = System.Type.GetType("System.DateTime");
            monitor_result_avg_tbl.Columns.Add(time_col);


            DataColumn monitor_result = new DataColumn();
            monitor_result.ColumnName = "result";
            monitor_result.DataType = System.Type.GetType("System.Double");// ? how to get float datatype
            monitor_result_avg_tbl.Columns.Add(monitor_result);

            /*
            DataColumn company_id_col = new DataColumn();
            company_id_col.ColumnName = "company_id";
            company_id_col.DataType = System.Type.GetType("System.Int32");
            monitor_result_avg_tbl.Columns.Add(company_id_col);

            DataColumn company_name_col = new DataColumn();
            company_name_col.ColumnName = "company_name";
            company_name_col.DataType = System.Type.GetType("System.String");
            monitor_result_avg_tbl.Columns.Add(company_name_col);
            */
            return monitor_result_avg_tbl;
        }

        public static DataTable MakeDailyUtilizationTable()
        {
            DataTable daily_utilization_table = new DataTable("daily_utilization");

            DataColumn result_id_col = new DataColumn();
            result_id_col.ColumnName = "id";
            result_id_col.AutoIncrement = true;
            result_id_col.DataType = System.Type.GetType("System.Int64");
            daily_utilization_table.Columns.Add(result_id_col);

            DataColumn device_id_col = new DataColumn();
            device_id_col.ColumnName = "device_id";
            device_id_col.AutoIncrement = false;
            device_id_col.DataType = System.Type.GetType("System.Int64");
            daily_utilization_table.Columns.Add(device_id_col);

            DataColumn device_type_col = new DataColumn();
            device_type_col.ColumnName = "device_type";
            device_type_col.AutoIncrement = false;
            device_type_col.DataType = System.Type.GetType("System.String");
            daily_utilization_table.Columns.Add(device_type_col);

            DataColumn device_name_col = new DataColumn();
            device_name_col.ColumnName = "device_name";
            device_name_col.AutoIncrement = false;
            device_name_col.DataType = System.Type.GetType("System.String");
            daily_utilization_table.Columns.Add(device_name_col);

            DataColumn time_col = new DataColumn();
            time_col.ColumnName = "create_date";
            time_col.DataType = System.Type.GetType("System.DateTime");
            daily_utilization_table.Columns.Add(time_col);


            DataColumn running_time_col = new DataColumn();
            running_time_col.ColumnName = "running_time";
            running_time_col.DataType = System.Type.GetType("System.Double");
            daily_utilization_table.Columns.Add(running_time_col);

            DataColumn idle_time_col = new DataColumn();
            idle_time_col.ColumnName = "idle_time";
            idle_time_col.DataType = System.Type.GetType("System.Double");
            daily_utilization_table.Columns.Add(idle_time_col);

            DataColumn poweroff_time_col = new DataColumn();
            poweroff_time_col.ColumnName = "poweroff_time";
            poweroff_time_col.AutoIncrement = false;
            poweroff_time_col.DataType = System.Type.GetType("System.Double");
            daily_utilization_table.Columns.Add(poweroff_time_col);

            DataColumn total_hours_col = new DataColumn();
            total_hours_col.ColumnName = "total_hours";
            total_hours_col.AutoIncrement = false;
            total_hours_col.DataType = System.Type.GetType("System.Int32");
            daily_utilization_table.Columns.Add(total_hours_col);

            DataColumn utilization_col = new DataColumn();
            utilization_col.ColumnName = "utilization";
            utilization_col.DataType = System.Type.GetType("System.Double");
            daily_utilization_table.Columns.Add(utilization_col);

            DataColumn power_lower_bound_col = new DataColumn();
            power_lower_bound_col.ColumnName = "power_lower_bound";
            power_lower_bound_col.DataType = System.Type.GetType("System.Double");
            daily_utilization_table.Columns.Add(power_lower_bound_col);

            DataColumn power_upper_bound_col = new DataColumn();
            power_upper_bound_col.ColumnName = "power_upper_bound";
            power_upper_bound_col.DataType = System.Type.GetType("System.Double");
            daily_utilization_table.Columns.Add(power_upper_bound_col);

            DataColumn consumed_energy_col = new DataColumn();
            consumed_energy_col.ColumnName = "consumed_energy";
            consumed_energy_col.DataType = System.Type.GetType("System.Double");
            daily_utilization_table.Columns.Add(consumed_energy_col);

            return daily_utilization_table;
        }
    }
}
