using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TelemetryGenerator
{
    class Simulator
    {
        public List<TelemetryInspect> telemetry_inspectors = new List<TelemetryInspect>();

        public SQLProcessor sql_process = new SQLProcessor();
        public Simulator()
        {
            //hardcode or inspectors
            //we sample once per 5 minutes, so 288 time a day
            float yellow_rate = 1.0f / 288; 
            float red_rate = 1.0f / 288 / 7;

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

        public void GenerateDeviceMonitorResultByInspectType(int device_type_id, TelemetryInspect tele, DateTime start, DateTime end, int sampling_interval)
        {
            //TimeSpan interval = new TimeSpan(0, 0, 5);
            TimeSpan diff = end - start;
            int count = (int)diff.TotalMinutes / sampling_interval;
            tele.generateTelemetry(count);
        }
    }
}
