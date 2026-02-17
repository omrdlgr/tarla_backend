const express = require("express");
const mqtt = require("mqtt");
const { InfluxDB } = require("@influxdata/influxdb-client");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

/* ============================
   INFLUX CONFIG
============================ */
const token = process.env.INFLUX_TOKEN;
const org = process.env.INFLUX_ORG;
const bucket = process.env.INFLUX_BUCKET;
const url = process.env.INFLUX_URL;

const influxDB = new InfluxDB({ url, token });
const queryApi = influxDB.getQueryApi(org);

/* ============================
   MQTT CONFIG
============================ */
const mqttClient = mqtt.connect(process.env.MQTT_URL);

let deviceStatus = {
  istasyon1: 0,
};

mqttClient.on("connect", () => {
  console.log("MQTT Connected");
  mqttClient.subscribe("tarla/istasyon1/status");
});

mqttClient.on("message", (topic, message) => {
  if (topic === "tarla/istasyon1/status") {
    deviceStatus.istasyon1 = parseInt(message.toString());
  }
});

/* ============================
   STATUS ENDPOINT
============================ */
app.get("/api/status/:device", (req, res) => {
  const device = req.params.device;
  res.json({ status: deviceStatus[device] || 0 });
});

/* ============================
   LAST DATA ENDPOINT
============================ */
app.get("/api/last-data/:device", async (req, res) => {
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
      const o = tableMeta.toObject(row);
      rows.push(o);
    },
    complete() {
      let result = {};
      rows.forEach(r => {
        result[r._field] = r._value;
      });
      res.json(result);
    },
    error(error) {
      res.status(500).json({ error: error.message });
    }
  });
});

/* ============================
   HISTORY ENDPOINT (NEW)
============================ */
app.get("/api/history/:device", async (req, res) => {
  const device = req.params.device;
  const field = req.query.field;
  const start = req.query.start || "-24h";

  if (!field) {
    return res.status(400).json({ error: "field param required" });
  }

  const query = `
    from(bucket: "${bucket}")
      |> range(start: ${start})
