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
        static void Main(string[] args)
        {
            //testDeviceGeneratorSavor();
            //testDeviceRead();
            //testSaveTelemetries();
            testGenerateResults();
            Console.Out.WriteLine("finished==============");
            Console.In.ReadLine();
        }
    }
}
