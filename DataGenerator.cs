using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MathNet.Numerics.Distributions;

namespace TelemetryGenerator
{
    struct ReportData
    {
        public float[] data;
        public int[] yellow_alert_index;
        public int[] red_alert_index;
    }
    class DataGenerator
    {
        private float _standard;
        private float _yellow_low;
        private float _yellow_high;
        private float _red_low;
        private float _red_high;

        private int _yellow_alert_count = 0;
        private int _red_alert_count = 0;

        private float _lower_bound;
        private float _upper_bound;

        private int _total_event = 0;
        public DataGenerator(float standard, float yellow_low, float yellow_high, 
            float red_low, float red_high, float lower_bound, float upper_bound)
        {
            this._standard = standard;
            this._yellow_low = yellow_low;
            this._yellow_high = yellow_high;
            this._red_low = red_low;
            this._red_high = red_high;
            this._lower_bound = Math.Min(yellow_low, lower_bound);
            this._upper_bound = Math.Max(yellow_high, upper_bound);
        }

        public void generate(DateTime start_time, DateTime end_time, int interval_sec)
        {
            
            

        }

        public ReportData generate(int count, float start_value, float yellow_prob, float red_prob, int alert_direction)
        {
            int seed = (int)DateTime.Now.Ticks;
            Random rd = new Random(seed);
            float v = start_value;

            // assuming yellow and red alert rate of devices follows a normal distribution with mean = given rate and sd = give_rate/4

            double yellow_rate = Math.Max(yellow_prob/4, Normal.Sample(yellow_prob, yellow_prob / 4));
            double red_rate = Math.Max(red_prob/4, Normal.Sample(red_prob, red_prob / 4));
           
            // red_rate must be at most 1/2 yellow rate
            red_rate = Math.Min(yellow_rate / 2, red_rate);

            int total_yellow_count = (int)(count * yellow_rate);
            int total_red_count = (int)(count * red_rate);

            int[] yellow_alert_time = new int[total_yellow_count];
            int[] red_alert_time = new int[total_red_count];

            // first generate events following brown movement with in range
            float[] telemetries = new float[count];
            for(_total_event=0; _total_event<count; _total_event++)
            {
                v = generateOneStep(v, rd);
                telemetries[_total_event] = v;
                
            }

            // insert alert events
            // generate yellow alert time

            // plan A, divide total time to time slots whose number is the yellow alert count, 
            // and randomly generate an alert inside each time slot
            /*
            int yellow_interval = count / total_yellow_count;
            for (int i=0; i< total_yellow_count; i++)
            {
                yellow_alert_time[i] = yellow_interval * i + (int)(rd.NextDouble() * yellow_interval);
                if (yellow_alert_time[i] > yellow_interval * (i + 1) - 2)
                    yellow_alert_time[i] = yellow_interval * (i + 1) - 2;
            }
            */

            // plan B, randomly put yellow alerts in the total time period
            // ensure each yellow alerts not next to each other
            List<int> yellow_alert_index = new List<int>();
            for (int i=0; i<total_yellow_count; i++)
            {
                while (true)
                {
                    int index = rd.Next(count-2);
                    if (!(yellow_alert_index.Contains(index)
                       || yellow_alert_index.Contains(index-1) 
                       || yellow_alert_index.Contains(index+1)))
                    {
                        yellow_alert_index.Add(index);
                        break;
                    }
                }
            }


            //yellow_alert_index.Sort();

            yellow_alert_time = yellow_alert_index.ToArray();
            

            // generate red alert time

            // get the yellow alert which red alert will associate with
            List<int> yellow_index = new List<int>();
            for (int i=0; i<total_red_count; i++)
            {
                while (true)
                {
                    int index = rd.Next(total_yellow_count);
                    if (yellow_index.Contains(index))
                    {
                        continue;
                    }
                    else
                    {
                        yellow_index.Add(index);
                        break;
                    }
                }

            }

            int[] red_alert_index = new int[total_red_count];
            for(int i=0; i<total_red_count; i++)
            {
                red_alert_index[i] = yellow_alert_time[yellow_index[i]] + 1;
            }

            //insert yellow alert events
            for(int i=0; i<total_yellow_count; i++)
            {
                float yellow_alert_value;
                if (alert_direction == 0) //both low and high
                {
                    if (telemetries[yellow_alert_time[i]] <= _standard)
                    {
                        yellow_alert_value = this._yellow_low + (_red_low - _yellow_low) * (float)rd.NextDouble();
                    }
                    else
                    {
                        yellow_alert_value = this._yellow_high + (_red_high - _yellow_high) * (float)rd.NextDouble();
                    }
                }
                else if(alert_direction < 0) // only low alert
                {
                    yellow_alert_value = this._yellow_low + (_red_low - _yellow_low) * (float)rd.NextDouble();
                }
                else // only high alert
                {
                    yellow_alert_value = this._yellow_high + (_red_high - _yellow_high) * (float)rd.NextDouble();
                }
                telemetries[yellow_alert_time[i]] = yellow_alert_value;
                Console.Out.WriteLine("---insert yellow alert {0} at {1}", yellow_alert_value, yellow_alert_time[i]);
                //insert red alert
                if (yellow_index.Contains(i))
                {
                    float red_alert_value;
                    if (yellow_alert_value <= _standard)
                    {
                        red_alert_value = _red_low + (_red_low - _yellow_low) * (float)rd.NextDouble();
                    }
                    else
                    {
                        red_alert_value = _red_high + (_red_high - _yellow_high) * (float)rd.NextDouble();
                    }
                    telemetries[yellow_alert_time[i] + 1] = red_alert_value;
                    Console.Out.WriteLine("---insert red alert {0} at {1}", red_alert_value, yellow_alert_time[i] + 1);

                }
            }
            
            /*
            foreach(float tel in telemetries)
            {
                Console.Out.WriteLine("--- {0} ---", tel);
            }
           */

            ReportData rdata = new ReportData();
            rdata.data = telemetries;
            rdata.yellow_alert_index = yellow_alert_time;
            rdata.red_alert_index = red_alert_index;

            return rdata;
        }
      
        private float generateOneStep(float cur_value, Random rd)
        {
            float max_step_size = (this._standard - this._lower_bound) / 3;

            int sign = 1;
            if (cur_value > _standard)
            {
                if(rd.Next() % 5 <= 2)
                {
                    sign = -1;
                }
            }
            else if(cur_value < _standard)
            {
                if(rd.Next() % 5 > 2)
                {
                    sign = -1;
                }
            }
            else
            {
                if(rd.Next() % 2 == 1)
                {
                    sign = -1;
                }
            }

            float step_size = (float)rd.NextDouble() * max_step_size * sign;

            float new_value = cur_value + step_size;


            //if out of normal range, force to go back

            
            if (new_value <= _lower_bound)
            {
                new_value = _lower_bound + (_standard - _lower_bound) * 0.2f;
            }
            else if(new_value >= _upper_bound)
            {
                new_value = _upper_bound - (_upper_bound - _standard) * 0.2f;
            }
            

            return new_value;

        }
    }
}
