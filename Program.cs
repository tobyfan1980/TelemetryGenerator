using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TelemetryGenerator
{
    class Program
    {
        static void Main(string[] args)
        {
            DataGenerator dg = new DataGenerator(50, 45, 55, 40, 60, 30, 2);
            dg.generate(1000, 50, 0.01, 0.002);

            Console.Out.WriteLine("finished==============");
            Console.In.ReadLine();
        }
    }
}
