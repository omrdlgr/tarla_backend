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

/* =========================
   MQTT Setup
========================= */
const client = mqtt.connect(process.env.MQTT_BROKER, {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASSWORD
});

client.on('connect', () => {
  console.log('🟢 MQTT Connected');
  client.subscribe('tarla/istasyon1/data');
});

client.on('message', async (topic, message) => {
  try {
    const data = JSON.parse(message.toString());

    console.log('📩 MQTT Data:', data);

    const point = new Point('sensor_data')
      .tag('device', data.device_id)
      .floatField('temperature', data.temperature)
      .floatField('humidity', data.humidity)
      .floatField('soil_moisture', data.soil_moisture)
      .floatField('battery', data.battery)
      .intField('timestamp', data.timestamp);

    writeApi.writePoint(point);
    await writeApi.flush();

    console.log('✅ Influx yazıldı');
  } catch (err) {
    console.error('❌ Veri işleme hatası:', err);
  }
});

/* =========================
   Health Check
========================= */
app.get('/', (req, res) => {
  res.send('Backend çalışıyor 🚀');
});

app.listen(port, () => {
  console.log(`🚀 Server listening on port ${port}`);
});

client.on('connect', () => {
  console.log('🟢 MQTT Connected');
  client.subscribe('#');
});

