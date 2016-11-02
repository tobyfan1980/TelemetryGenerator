using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TelemetryGenerator
{
    class Program
    {
        static void testDataGenerator()
        {
            DataGenerator dg = new DataGenerator(50, 45, 55, 40, 60, 45, 55);
            dg.generate(1000, 50, 0.01f, 0.002f, 0);

            
        }

        static void testDeviceGenerator()
        {
            Simulator sim = new Simulator();
            sim.generateDevices();
        }

        static void testDeviceGeneratorSavor()
        {
            Simulator sim = new Simulator();
            sim.GenerateAndSaveDevices();
        }

        static void testDeviceRead()
        {
            Simulator sim = new Simulator();
            sim.GetDeviceFromDB();
        }

        static void testSaveTelemetries()
        {
            Simulator sim = new Simulator();
            sim.SaveTelemetryInspectors();
        }

        static void testGenerateResults()
        {
            Simulator sim = new Simulator();
            sim.GenerateDeviceMonitorResultByDeviceType(1, new DateTime(2000, 1, 1), new DateTime(2000, 3, 1), 5);
        }

        static void testGenerateResultsDB()
        {
            Simulator sim = new Simulator(1.0f/100, 1.0f/500);
            List<DimDevice> device_list = sim.sql_process.readDevices(0, 1);
            // get device type, and for all corresponding inspect type generate report data
            foreach (DimDevice device in device_list)
            {
                sim.GenerateMonitorResultPerDevice(device, new DateTime(2016, 10, 1), new DateTime(2016, 11, 1), 5, 2);
            }
            // sim.GenerateMonitorResultForAllDevice()
        }

        static void GenerateResultsDB()
        {
            // create simulator with yellow alert rate 0.01, red alert rate 0.002
            Simulator sim = new Simulator(1.0f / 100, 1.0f / 500);
            int skip = 0;

            while (true)
            {
                List<DimDevice> device_list = sim.sql_process.readDevices(skip, 10);
                // get device type, and for all corresponding inspect type generate report data
                foreach (DimDevice device in device_list)
                {
                    // generate data will 5 minutes interval and save only latest 2 days telemetry data in fact_monitor_result table
                    sim.GenerateMonitorResultPerDevice(device, new DateTime(2016, 9, 1), new DateTime(2016, 11, 1), 5, 2);
                }

                if(device_list.Count < 10)
                {
                    break;
                }

                skip += 10;
            }
        }
            // sim.GenerateMonitorResultForAllDevice()
        


        static void TestWriteMonitorResult()
        {
            Simulator sim = new Simulator();
            sim.sql_process.WriteOneMonitorResult(1, 1, 1, new DateTime(2016, 10, 1, 5, 6, 7), 24.5f);

        }

        static void Main(string[] args)
        {
            //testDeviceGeneratorSavor();
            //testDeviceRead();
            //testSaveTelemetries();
            //testGenerateResults();

            //testGenerateResultsDB();
            GenerateResultsDB();
            //TestWriteMonitorResult();

            Console.Out.WriteLine("finished==============");
            Console.In.ReadLine();
        }
    }
}
