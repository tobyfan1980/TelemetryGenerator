using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TelemetryGenerator
{
    struct InspectResult
    {
        public int device_id;
        public int device_type_id;
        public int device_telemetry_id;
        public float result;
        public DateTime time;
    }

    struct InspectAlert
    {
        public int device_id;
        public int device_telemetry_id;
        public int alert_type;
        public int consecutive_alert_count;
        public DateTime time;
    }
}
