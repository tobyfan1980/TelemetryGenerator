using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        private int _total_event = 0;
        public DataGenerator(float standard, float yellow_low, float yellow_high, 
            float red_low, float red_high)
        {
            this._standard = standard;
            this._yellow_low = yellow_low;
            this._yellow_high = yellow_high;
            this._red_low = red_low;
            this._red_high = red_high;

        }

        public void generate(DateTime start_time, DateTime end_time, int interval_sec)
        {
            
            

        }

        public ReportData generate(int count, float start_value, float yellow_prob, float red_prob)
        {
            Random rd = new Random();
            float v = start_value;

            int total_yellow_count = (int)(count * yellow_prob);
            int total_red_count = (int)(count * red_prob);

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
            int yellow_interval = count / total_yellow_count;
            for (int i=0; i< total_yellow_count; i++)
            {
                yellow_alert_time[i] = yellow_interval * i + (int)(rd.NextDouble() * yellow_interval);
                if (yellow_alert_time[i] > yellow_interval * (i + 1) - 2)
                    yellow_alert_time[i] = yellow_interval * (i + 1) - 2;
            }
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
                if (telemetries[yellow_alert_time[i]] <= _standard)
                {
                    yellow_alert_value = this._yellow_low + (_red_low - _yellow_low) * (float)rd.NextDouble();
                }
                else
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
        /*

        private float filter_result_by_yellow_alert_count(float new_value)
        {
            
            if (_yellow_alert_count >= _max_yellow_alert_count)
            {
                if (new_value <= _yellow_low)
                {
                    new_value = _yellow_low + (_standard - _yellow_low) * 0.2;
                }
                else
                {
                    new_value = _yellow_high - (_yellow_high - _standard) * 0.2;
                }
                Console.Out.WriteLine("already have {0} yellow alert, reset value to normal at {1}", _yellow_alert_count, new_value);

            }
            else
            {
                _yellow_alert_count++;
                Console.Out.WriteLine("got an yellow alert {0} at {2}, total {1} alerts", new_value, _yellow_alert_count, _total_event);
            }
            

            return new_value;

        }

        private float filter_result_by_red_alert_count(float new_value)
        {
            
            if (_red_alert_count >= _max_red_alert_count)
            {
                if (new_value <= _red_low)
                {
                    new_value = _red_low + (_standard - _red_low) * 0.2;
                }
                else
                {
                    new_value = _red_high - (_red_high - _standard) * 0.2;
                }
                Console.Out.WriteLine("already have {0} red alert at {2}, reset value to no red at {1}", _red_alert_count, new_value, _total_event);

                new_value = filter_result_by_yellow_alert_count(new_value);
            }
            else
            {
                _red_alert_count++;
                Console.Out.WriteLine("got an red alert {0}, total {1} alerts", new_value, _red_alert_count);
            }
            
            return new_value;
        }
        */
        private float generateOneStep(float cur_value, Random rd)
        {
            float max_step_size = (this._standard - this._yellow_low) / 3;

            int sign = 1;
            if (cur_value > _standard)
            {
                if(rd.Next() % 5 <= 2)
                {
                    sign = -1;
                }
            }
            else
            {
                if(rd.Next() % 5 > 2)
                {
                    sign = -1;
                }
            }

            float step_size = (float)rd.NextDouble() * max_step_size * sign;

            float new_value = cur_value + step_size;


            //if out of normal range, force to go back

            if (new_value >= this._yellow_high || new_value <= this._yellow_low)
            {
                if (new_value <= _yellow_low)
                {
                    new_value = _yellow_low + (_standard - _yellow_low) * 0.2f;
                }
                else
                {
                    new_value = _yellow_high - (_yellow_high - _standard) * 0.2f;
                }
            }

            return new_value;

        }
    }
}
