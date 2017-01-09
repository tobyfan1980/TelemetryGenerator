using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TelemetryGenerator
{
    struct AlertType
    {
        public int alert_type;
        public string alert_name;

        public AlertType(int type, string name)
        {
            this.alert_type = type;
            this.alert_name = name;
        }

        public string toString()
        {
            return string.Format("alert--name: {0}, type {1}", alert_name, alert_type);
        }
    }
    class TelemetryInspect
    {
        private int _id;
        private int _device_type;
        private String _inspect_type_name;
        private int _inspect_type_id;
        private String _device_type_name;
        private String _inspect_type_code;
        private String _inspect_type_unit;

        // inspect values
        private float _standard;
        private float _yellow_low;
        private float _yellow_high;
        private float _red_low;
        private float _red_high;

        public float lower_bound;
        public float upper_bound;

        // alert 
        private float _yellow_alert_rate;
        private float _red_alert_rate;

        // alert directions, -1 means only low, 0 means both, 1 means only high
        private int _alert_direction;

        public TelemetryInspect(int id, int d_type, String d_type_name,
            String i_type_name, int i_type_id, String i_type_code, String i_type_unit,
            float st, float yl, float yh, float rl, float rh,
            float yellow_rate, float red_rate)
        {
            _id = id;
            _device_type = d_type;
            _device_type_name = d_type_name;
            _inspect_type_name = i_type_name;
            _inspect_type_id = i_type_id;
            _inspect_type_code = i_type_code;
            _inspect_type_unit = i_type_unit;
            _standard = st;
            _yellow_low = yl;
            _yellow_high = yh;
            _red_low = rl;
            _red_high = rh;
            _yellow_alert_rate = yellow_rate;
            _red_alert_rate = red_rate;
            _alert_direction = 0;
            if(_yellow_low > standard)
            {
                _alert_direction = 1;
            }
            else if(_yellow_high < standard)
            {
                _alert_direction = -1;
            }

            lower_bound = yellow_lower_bound;
            upper_bound = yellow_upper_bound;

            
        }

        public int id
        {
            get { return _id; }
        }

        public int device_type_id
        {
            get { return _device_type; }
        }

        public string device_type_name
        {
            get { return _device_type_name; }
        }

        public int inspect_type_id
        {
            get { return _inspect_type_id; }
        }

        public string inspect_type_code
        {
            get { return _inspect_type_code; }
        }
        public string inspect_type_name
        {
            get { return _inspect_type_name; }
        }

        public string inspect_type_unit
        {
            get { return _inspect_type_unit; }
        }

        public float standard
        {
            get { return _standard; }
        }

        public float yellow_lower_bound
        {
            get { return _yellow_low; }
        }

        public float yellow_upper_bound
        {
            get { return _yellow_high; }
        }

        public float red_lower_bound
        {
            get { return _red_low; }
        }
        public float red_upper_bound
        {
            get { return _red_high; }
        }

        public string toString()
        {
            return string.Format("Telemetry -- {0}, device: {1}, inspect_type: {2}, name: {3}, unit: {4}, standard: {5}, yellow_low: {6}, yellow_high: {7}, red_low: {8}, red_high: {9} ",
                id, device_type_id, inspect_type_code, inspect_type_name, inspect_type_unit, standard, yellow_lower_bound, yellow_upper_bound, red_lower_bound, red_upper_bound);
        }
        public ReportData generateTelemetry(int count)
        {
            DataGenerator  data_generator = new DataGenerator(_standard, _yellow_low, _yellow_high,
                _red_low, _red_high, lower_bound, upper_bound);

            ReportData rdata = data_generator.generate(count, _standard, _yellow_alert_rate, _red_alert_rate, _alert_direction);

            return rdata;
        }
    }


}
