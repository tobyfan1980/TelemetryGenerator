using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

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

        static void GenerateDeviceAndTelemetry()
        {
            Simulator sim = new Simulator();
            sim.sql_process = new SQLProcessor("intelab-db", "intelab-db", "superadmin", "intelab-2016");
            sim.sql_process.CleanupIntelabDB();
            sim.GenerateAndSaveDevices();
            sim.SaveTelemetryInspectors();
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
            sim.sql_process = new SQLProcessor("intelab-db", "intelab-db", "superadmin", "intelab-2016");
            List<DimDevice> device_list = sim.sql_process.readDevices(0, 2);
            // get device type, and for all corresponding inspect type generate report data
            foreach (DimDevice device in device_list)
            {
                sim.GenerateMonitorResultPerDevice(device, new DateTime(2016, 10, 1), new DateTime(2016, 11, 1), 5, 2);
            }
            // sim.GenerateMonitorResultForAllDevice()
        }

        static void GenerateResultsDB()
        {
            // create simulator with yellow alert rate 0.01 (1 alert per 500 min), red alert rate 0.002
            Simulator sim = new Simulator(1.0f / 150, 1.0f / 700);
            sim.sql_process = new SQLProcessor("intelab-db", "intelab-db", "superadmin", "intelab-2016");
            int skip = 0;

            while (true)
            {
                List<DimDevice> device_list = sim.sql_process.readDevices(skip, 10);
                // get device type, and for all corresponding inspect type generate report data
                foreach (DimDevice device in device_list)
                {
                    // generate data will 5 minutes interval and save only latest 2 days telemetry data in fact_monitor_result table
                    sim.GenerateMonitorResultPerDevice(device, new DateTime(2016, 8, 1), new DateTime(2016, 11, 20,23,0,0), 5, 2);
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
            sim.sql_process.WriteOneMonitorResult(1, 1, 1, 1, new DateTime(2016, 10, 1, 5, 6, 7), 24.5f);

        }

        static void TestMysql()
        {
            MySqlProcessor mysqlProc = new MySqlProcessor();
            //mysqlProc.ReadDevices();
            mysqlProc.ReadDimDevice();
            mysqlProc.ReadDeviceTelemetry();
            mysqlProc.ReadMonitorResult(DateTime.Now.Date);
            mysqlProc.ReadAlert(DateTime.Now.Date);

        }


        static void InitializeDB()
        {
            SQLProcessor proc = new SQLProcessor("intelab-db", "intelab-vm-production", "superadmin", "intelab-2016");
            //SQLProcessor proc = new SQLProcessor("intelab-db", "intelab-db", "superadmin", "intelab-2016");

            proc.InitializeIntelabDB();
        }

        static void CleanupDB()
        {
            //SQLProcessor proc = new SQLProcessor("intelab-db", "intelab-db", "superadmin", "intelab-2016");
            SQLProcessor proc = new SQLProcessor("intelab-db", "intelab-vm-production", "superadmin", "intelab-2016");
            proc.CleanupIntelabDB();
        }

        static void CleanupFactTables()
        {
            //SQLProcessor proc = new SQLProcessor("intelab-db", "intelab-db", "superadmin", "intelab-2016");
            SQLProcessor proc = new SQLProcessor("intelab-db", "intelab-vm-production", "superadmin", "intelab-2016");
            proc.CleanupFactTable();
        }

        

        static void TestRunSQLcommand()
        {
            SQLProcessor proc = new SQLProcessor("intelab-db", "intelab-vm-production", "superadmin", "intelab-2016");
            //SQLProcessor proc = new SQLProcessor("intelab-db", "intelab-db", "superadmin", "intelab-2016");
            string cmd1 = "Alter table fact_monitor_result add company_id int null";
           
            //proc.ExecuteSqlCommandNonQuery(cmdsetcompanyname2);

            string cmdcount = "select count(*) from fact_alert";
            string cmdcount2 = "select count(*) as count, device_id from fact_monitor_result group by device_id";
            string cmdcount22 = "select count(*) as count, company_id from fact_monitor_result group by company_id";
            string cmdcount23 = "select count(*) as count, company_id from fact_monitor_result_daily_average group by company_id";
            string cmdcount3 = "select count(*) from dim_device where company_id=1";

            proc.ExecuteSqlCommandCountQuery(cmdcount);
            proc.ExecuteSqlCommandCountQuery(cmdcount2);

            //proc.ExecuteSqlCommandCountByGroup(cmdcount);


        }

        static void DailySqlTransfer()
        {
            SqlDataTransfer sqlTrans = new SqlDataTransfer();
            sqlTrans.TransferFromMysqlToSqlserver();
        }

        static void Main(string[] args)
        {
            //GenerateDeviceAndTelemetry();
            //testDeviceRead();
            //testSaveTelemetries();
            //testGenerateResults();

            //testGenerateResultsDB();
            //TestWriteMonitorResult();

            //CleanupFactTables();
            //GenerateResultsDB();

            //TestMysql();
            //  TestRunSQLcommand();
            //InitializeDB();
            //CleanupDB();

            Tuple<long, int> a = new Tuple<long, int>(1, 2);
            Tuple<long, int> b = new Tuple<long, int>(1, 2);

            if (a.Equals(b))
            {
                Console.WriteLine("guessing tuple succeessfully");
            }

            double c = 2.5555d;
            float d = (float)c;


            CleanupFactTables();
            DailySqlTransfer();
            Console.Out.WriteLine("finished==============");
            Console.In.ReadLine();
        }
    }
}
