using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TelemetryGenerator
{
    struct DimDevice
    {
        public int id;
        public String serial_number;
        public string device_name;
        public string model;
        public string type_name;
        public int type_id;
        public string building_name;
        public int building_id;
        public int floor_id;
        public string floor_name;

        public string control;
        public int control_online_status;
        public int control_battery_status;

        public DateTime create_date;
        public DateTime purchase_date;

        public string toString()
        {
            return string.Format(@"Dev -- {0}, {1}, {2}, Model:{3}, type:{4}, building:{5}, floor:{6}, online:{7}, battery:{8}, purchase: {9} ",
                id, serial_number, device_name, model, type_name, building_name, floor_name, control_online_status, control_battery_status, purchase_date);
        }
    }

}
