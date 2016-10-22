using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TelemetryGenerator
{
    class TelemetryInspect
    {
        private int _device_id;
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

        // alert 
        private float _yellow_alert_rate;
        private float _red_alert_rate;

        private DataGenerator _data_generator;
        public TelemetryInspect(int d_id, int d_type, String d_type_name,
            String i_type_name, int i_type_id, String i_type_code, String i_type_unit,
            float st, float yl, float yh, float rl, float rh,
            float yellow_rate, float red_rate)
        {
            _device_id = d_id;
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

            _data_generator = new DataGenerator(_standard, _yellow_low, _yellow_high,
                _red_low, _red_high);

        }

        public ReportData generateTelemetry(int count)
        {
            ReportData rdata = _data_generator.generate(count, _standard, _yellow_alert_rate, _red_alert_rate);

            return rdata;
        }
    }


}
