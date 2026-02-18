import express from "express";
import mqtt from "mqtt";
import { InfluxDB, Point } from "@influxdata/influxdb-client";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

/* ================== INFLUX ================== */

const influxDB = new InfluxDB({
  url: process.env.INFLUX_URL,
  token: process.env.INFLUX_TOKEN,
});

const org = process.env.INFLUX_ORG;
const bucket = process.env.INFLUX_BUCKET;

const queryApi = influxDB.getQueryApi(org);
const writeApi = influxDB.getWriteApi(org, bucket);

/* ================== MQTT ================== */

const mqttClient = mqtt.connect(process.env.MQTT_BROKER, {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASS,
});

let deviceLastSeen = {}; // online/offline için

mqttClient.on("connect", () => {
  console.log("MQTT Connected");
  mqttClient.subscribe("tarla/+/data");
});

mqttClient.on("message", (topic, message) => {
  const parts = topic.split("/");
  if (parts.length < 3) return;

  const device = parts[1];
  const type = parts[2];

  if (type === "data") {
    try {
      const data = JSON.parse(message.toString());

      const point = new Point(device)
        .floatField("temperature", Number(data.temperature))
        .floatField("humidity", Number(data.humidity))
        .floatField("soil_moisture", Number(data.soil_moisture))
        .floatField("battery", Number(data.battery));

      writeApi.writePoint(point);
      writeApi.flush();

      deviceLastSeen[device] = Date.now();

      console.log("Influx yazıldı:", device);
    } catch (err) {
      console.error("Parse error:", err.message);
    }
  }
});

/* ================== ONLINE / OFFLINE ================== */

app.get("/api/status/:device", (req, res) => {
  const device = req.params.device;
  const lastSeen = deviceLastSeen[device];

  if (!lastSeen) {
    return res.json({ status: 0 });
  }

  const diff = Date.now() - lastSeen;

  // 5 dakika = offline
  const online = diff < 5 * 60 * 1000 ? 1 : 0;

  res.json({ status: online });
});

/* ================== LAST DATA ================== */

app.get("/api/last-data/:device", (req, res) => {
  const device = req.params.device;

  const query = `
    from(bucket: "${bucket}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "${device}")
      |> last()
  `;

  const rows = [];

  queryApi.queryRows(query, {
    next(row, tableMeta) {
      rows.push(tableMeta.toObject(row));
    },
    complete() {
      let result = {};
      rows.forEach(r => {
        result[r._field] = r._value;
      });
      res.json(result);
    },
    error(err) {
      res.status(500).json({ error: err.message });
    }
  });
});

/* ================== HISTORY PRO ================== */

app.get("/api/history/:device", (req, res) => {
  const device = req.params.device;
  const field = req.query.field;
  const start = req.query.start || "-24h";

  if (!field) {
    return res.status(400).json({ error: "field required" });
  }

  let window = "5m";
  if (start === "-7d") window = "30m";
  if (start === "-30d") window = "2h";
  if (start === "-365d") window = "1d";

  const seriesQuery = `
    from(bucket: "${bucket}")
      |> range(start: ${start})
      |> filter(fn: (r) => r._measurement == "${device}")
      |> filter(fn: (r) => r._field == "${field}")
      |> aggregateWindow(every: ${window}, fn: mean, createEmpty: false)
      |> sort(columns: ["_time"])
  `;

  const statsQuery = `
    data = from(bucket: "${bucket}")
      |> range(start: ${start})
      |> filter(fn: (r) => r._measurement == "${device}")
      |> filter(fn: (r) => r._field == "${field}")

    minVal = data |> min()
    maxVal = data |> max()
    meanVal = data |> mean()
    lastVal = data |> last()

    union(tables: [minVal, maxVal, meanVal, lastVal])
  `;

  const series = [];
  let stats = { min: null, max: null, mean: null, last: null };

  // SERIES
  queryApi.queryRows(seriesQuery, {
    next(row, tableMeta) {
      const o = tableMeta.toObject(row);
      series.push({
        time: o._time,
        value: o._value
      });
    },
    complete() {

      // STATS
      queryApi.queryRows(statsQuery, {
        next(row, tableMeta) {
          const o = tableMeta.toObject(row);

          if (o._value === undefined) return;

          if (o._measurement === device) {
            if (o._field === field) {
              // Flux fonksiyon tipine göre ayırıyoruz
              if (o._value !== undefined) {
                if (!stats.min || o._value < stats.min) stats.min = o._value;
                if (!stats.max || o._value > stats.max) stats.max = o._value;
              }
            }
          }
        },
        complete() {
          res.json({ series, stats });
        },
        error(err) {
          res.status(500).json({ error: err.message });
        }
      });

    },
    error(err) {
      res.status(500).json({ error: err.message });
    }
  });
});

/* ================== START ================== */

app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});
