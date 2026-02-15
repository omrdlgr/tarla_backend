import mqtt from 'mqtt';
import dotenv from 'dotenv';
import express from 'express';
import { InfluxDB, Point } from '@influxdata/influxdb-client';

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

/* =========================
   InfluxDB Setup
========================= */
const influxDB = new InfluxDB({
  url: process.env.INFLUX_URL,
  token: process.env.INFLUX_TOKEN
});

const writeApi = influxDB.getWriteApi(
  process.env.INFLUX_ORG,
  process.env.INFLUX_BUCKET
);
const queryApi = influxDB.getQueryApi(process.env.INFLUX_ORG);

/* =========================
   Device Tracking
========================= */
const deviceLastSeen = {};
const deviceStates = {};

/* =========================
   MQTT Setup
========================= */
const client = mqtt.connect(process.env.MQTT_BROKER, {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASS,
  rejectUnauthorized: false
});

client.on('error', (err) => console.error('❌ MQTT Error:', err));

client.on('connect', () => {
  console.log('🟢 MQTT Connected');
  client.subscribe('tarla/+/data');
});

client.on('message', async (topic, message) => {
  try {
    const data = JSON.parse(message.toString());
    const deviceId = topic.split('/')[1];
    deviceLastSeen[deviceId] = Date.now();

    console.log(`📩 Data from ${deviceId}`);

    // ===== SENSOR DATA =====
    const dataPoint = new Point('tarla_data')
      .tag('device', deviceId)
      .floatField('temperature', data.temperature)
      .floatField('humidity', data.humidity)
      .floatField('soil_moisture', data.soil_moisture)
      .floatField('battery', data.battery);
    writeApi.writePoint(dataPoint);

    // ===== ONLINE STATUS =====
    if (deviceStates[deviceId] !== 1) {
      const statusPoint = new Point('tarla_status')
        .tag('device', deviceId)
        .intField('status', 1);
      writeApi.writePoint(statusPoint);
      deviceStates[deviceId] = 1;
      console.log(`🟢 ${deviceId} ONLINE yazıldı`);
    }

    await writeApi.flush();
    console.log(`✅ ${deviceId} verisi Influx'a yazıldı`);
  } catch (err) {
    console.error('❌ Veri işleme hatası:', err);
  }
});

/* =========================
   OFFLINE CHECK (3 dk)
========================= */
setInterval(async () => {
  const now = Date.now();
  const offlineThreshold = 3 * 60 * 1000; // 5 dakika

  for (const deviceId in deviceLastSeen) {
    if (now - deviceLastSeen[deviceId] > offlineThreshold && deviceStates[deviceId] !== 0) {
      const statusPoint = new Point('tarla_status')
        .tag('device', deviceId)
        .intField('status', 0);
      writeApi.writePoint(statusPoint);
      await writeApi.flush();
      deviceStates[deviceId] = 0;
      console.log(`🔴 ${deviceId} OFFLINE yazıldı`);
    }
  }
}, 60000);

/* =========================
   API ENDPOINTS
========================= */

// Health check
app.get('/', (req, res) => res.send('Backend çalışıyor 🚀'));

// Cihaz Status
app.get('/api/status/:deviceId', async (req, res) => {
  const { deviceId } = req.params;
  const fluxQuery = `
    from(bucket: "${process.env.INFLUX_BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "tarla_status")
      |> filter(fn: (r) => r.device == "${deviceId}")
      |> last()
  `;
  try {
    const rows = await queryApi.collectRows(fluxQuery);
    res.json({ status: rows.length ? rows[0]._value : 0 });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Son sensör verisi
app.get('/api/last-data/:deviceId', async (req, res) => {
  const { deviceId } = req.params;
  const fluxQuery = `
    from(bucket: "${process.env.INFLUX_BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "tarla_data")
      |> filter(fn: (r) => r.device == "${deviceId}")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  `;
  try {
    const rows = await queryApi.collectRows(fluxQuery);
    res.json(rows.length ? rows[0] : {});
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

/* =========================
   Server Start
========================= */
app.listen(port, () => console.log(`🚀 Server listening on port ${port}`));
