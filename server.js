import express from "express";
import mqtt from "mqtt";
import { InfluxDB } from "@influxdata/influxdb-client";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

/* ================== INFLUX ================== */

const influxDB = new InfluxDB({
  url: process.env.INFLUX_URL,
  token: process.env.INFLUX_TOKEN,
});

const queryApi = influxDB.getQueryApi(process.env.INFLUX_ORG);
const bucket = process.env.INFLUX_BUCKET;

/* ================== MQTT ================== */

const mqttClient = mqtt.connect(process.env.MQTT_URL);

let deviceStatus = { istasyon1: 0 };

mqttClient.on("connect", () => {
  console.log("MQTT Connected");
  mqttClient.subscribe("tarla/istasyon1/status");
});

mqttClient.on("message", (topic, message) => {
  if (topic === "tarla/istasyon1/status") {
    deviceStatus.istasyon1 = parseInt(message.toString());
  }
});

/* ================== STATUS ================== */

app.get("/api/status/:device", (req, res) => {
  res.json({ status: deviceStatus[req.params.device] || 0 });
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

/* ================== HISTORY (GRAFANA STYLE) ================== */

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

  const query = `
    from(bucket: "${bucket}")
      |> range(start: ${start})
      |> filter(fn: (r) => r._measurement == "${device}")
      |> filter(fn: (r) => r._field == "${field}")
      |> aggregateWindow(every: ${window}, fn: mean, createEmpty: false)
      |> sort(columns: ["_time"])
  `;

  const result = [];

  queryApi.queryRows(query, {
    next(row, tableMeta) {
      const o = tableMeta.toObject(row);
      result.push({
        time: o._time,
        value: o._value
      });
    },
    complete() {
      res.json(result);
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
