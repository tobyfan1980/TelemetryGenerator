using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TelemetryGenerator
{

    struct DeviceType
    {
        public string name;
        public string[] models;
        public string[] sn_prefix_by_model;
        public int type_id;
        public int[] number_by_model;
    }
    struct Floor
    {
        public string name;
        public int id;
    }

    struct Building
    {
        public string name;
        public int id;
    }

    struct Company
    {
        public string name;
        public int id;
    }
    class DeviceGenerator
    {
        private string control_prefix = "intelab-";
        private float control_online_rate = 0.95f;
        private int control_battery_low = 80;
        private int control_battery_high = 100;
        public List<DeviceType> device_types = new List<DeviceType>();

        private List<Floor> floors = new List<Floor>();
        private List<Building> buildings = new List<Building>();
        private List<Company> companies = new List<Company>();

        private List<DateTime> create_dates = new List<DateTime>();
        private List<DateTime> purchase_dates = new List<DateTime>();
        public void initialize()
        {
            DeviceType fridge = new DeviceType();
            fridge.name = "冰箱";
            fridge.models = new string[2]{ "Thermo", "Haier"};
            fridge.sn_prefix_by_model = new string[2] { "Thermo-1-", "Haier-1-" };
            fridge.type_id = 1;
            //fridge.number_by_model = new int[2] { 60, 40};
            fridge.number_by_model = new int[2] { 6, 4 };
            device_types.Add(fridge);

            DeviceType peiyang = new DeviceType();
            peiyang.name = "培养箱";
            peiyang.models =new string[2] { "Thermo", "Binder" };
            peiyang.sn_prefix_by_model = new string[2] { "Thermo-2-", "Binder-2-" };
            peiyang.type_id = 2;
            //peiyang.number_by_model = new int[2] { 25, 30 };
            peiyang.number_by_model = new int[2] { 2, 3 };
            device_types.Add(peiyang);

            DeviceType jiejing = new DeviceType();
            jiejing.name = "洁净室";
            jiejing.models = new string[1] { "Newshine" };
            jiejing.sn_prefix_by_model = new string[1] { "Newshine-3-"};
            jiejing.type_id = 3;
           // jiejing.number_by_model = new int[1] {20 };
            jiejing.number_by_model = new int[1] { 2 };
            device_types.Add(jiejing);

            DeviceType tongfeng = new DeviceType();
            tongfeng.name = "通风柜";
            tongfeng.models = new string[1] { "Newshine" };
            tongfeng.sn_prefix_by_model = new string[1] { "Newshine-4-" };
            tongfeng.type_id = 4;
            //tongfeng.number_by_model = new int[1] { 40 };
            tongfeng.number_by_model = new int[1] { 4 };
            device_types.Add(tongfeng);

            Company c_a = new Company();
            c_a.name = "newshine";
            c_a.id = 1;
            Company c_b = new Company();
            c_b.name = "ils";
            c_b.id = 2;

            companies.Add(c_a);
            companies.Add(c_b);

            Building b_a = new Building();
            b_a.name = "A楼";
            b_a.id = 1;

            Building b_b = new Building();
            b_b.name = "B楼";
            b_b.id = 2;

            Building b_c = new Building();
            b_c.name = "C楼";
            b_c.id = 3;

            buildings.Add(b_a);
            buildings.Add(b_b);
            buildings.Add(b_c);

            Floor f_test = new Floor();
            f_test.name = "实验层";
            f_test.id = 2;

            Floor f_peiyang = new Floor();
            f_peiyang.name = "培养层";
            f_peiyang.id = 3;

            Floor f_sample = new Floor();
            f_sample.name = "样品层";
            f_sample.id = 1;
            floors.Add(f_test);
            floors.Add(f_peiyang);
            floors.Add(f_sample);

            create_dates.Add(new DateTime(2016, 7, 16));
            create_dates.Add(new DateTime(2016, 8, 21));
            create_dates.Add(new DateTime(2016, 8, 6));

            purchase_dates.Add(new DateTime(2016, 5, 1));
            purchase_dates.Add(new DateTime(2016, 6, 1));
            purchase_dates.Add(new DateTime(2016, 7, 1));
        }

        private int get_online_status(Random rd)
        {
            if(rd.NextDouble() > this.control_online_rate)
            {
                return 0;
            }
            else
            {
                return 1;
            }
        }

        public List<DimDevice> generateDevices()
        {
            List<DimDevice> devices = new List<DimDevice>();
            Random rd = new Random((int)DateTime.Now.Ticks);
            int device_count = 1;
            foreach (DeviceType dt in device_types)
            {
                
                for(int i=0; i<dt.models.Length; i++)
                {
                    int num_of_devices = dt.number_by_model[i];
                    string prefix = dt.sn_prefix_by_model[i];
                    string model_name = dt.models[i];

                    for(int j=1; j<=num_of_devices; j++)
                    {
                        string sn = string.Format("{0}{1}", prefix, j);
                        int building_index = rd.Next(buildings.Count);
                        int floor_index = rd.Next(floors.Count);
                        int company_index = rd.Next(companies.Count);

                        int control_online = get_online_status(rd);
                        int battery = this.control_battery_low + rd.Next(control_battery_high - control_battery_low + 1);

                        DimDevice device = new DimDevice();
                        device.id = device_count;
                        device.device_name = string.Format("{0}-{1}", dt.name, device_count);
                        device.serial_number = sn;
                        device.model = model_name;
                        device.type_name = dt.name;
                        device.type_id = dt.type_id;
                        device.company_id = companies[company_index].id;
                        device.company_name = companies[company_index].name;

                        device.building_name = buildings[building_index].name;
                        device.building_id = buildings[building_index].id;
                        device.floor_id = floors[floor_index].id;
                        device.floor_name = floors[floor_index].name;
                        device.control = string.Format("{0}{1}", this.control_prefix, device_count);
                        device.control_online_status = control_online;
                        device.control_battery_status = battery;

                        device.create_date = create_dates[rd.Next(create_dates.Count)];
                        device.purchase_date = purchase_dates[rd.Next(purchase_dates.Count)];

                        devices.Add(device);
                        device_count++;
                    }
                }
            }
            return devices;
        }
    }
}
