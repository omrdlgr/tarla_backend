import mqtt from 'mqtt';
import dotenv from 'dotenv';
import express from 'express';
import { InfluxDB, Point } from '@influxdata/influxdb-client';

dotenv.config();
const app = express();
const port = process.env.PORT || 3000;

// --- InfluxDB Setup ---
const influxDB = new InfluxDB({
  url: process.env.INFLUX_URL,
  token: process.env.INFLUX_TOKEN
});
const writeApi = influxDB.getWriteApi(process.env.INFLUX_ORG, process.env.INFLUX_BUCKET);
writeApi.useDefaultTags({ location: 'istasyon1' });

// --- MQTT Setup ---
const client = mqtt.connect(process.env.MQTT_BROKER, {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASSWORD
});

client.on('connect', () => {
  console.log("🟢 MQTT Connected");
  client.subscribe('tarla/istasyon1/data');
});

client.on('message', (topic, message) => {
  console.log("🔥 TOPIC:", topic);
  console.log("🔥 RAW:", message.toString());
});



setInterval(async () => {
  const point = new Point('sensor_data')
    .tag('device', 'test_device')
    .floatField('temperature', 99.9);

  writeApi.writePoint(point);

  try {
    await writeApi.flush();
    console.log("✅ TEST VERİ YAZILDI");
  } catch (err) {
    console.error("❌ Influx hata:", err);
  }
}, 10000);

client.on('message', async (topic, message) => {
  console.log("🔥 MQTT RAW:", message.toString());
});

client.on('connect', () => {
  console.log("🟢 MQTT Connected");
  client.subscribe('#');   // geçici test için
});

client.on('message', (topic, message) => {
  console.log("🔥 GELEN TOPIC:", topic);
  console.log("🔥 GELEN MESAJ:", message.toString());
});




client.on('message', async (topic, message) => {
  const data = JSON.parse(message.toString());
  const point = new Point('sensor_data')
    .tag('device', data.device_id)
    .floatField('temperature', data.temperature)
    .floatField('humidity', data.humidity)
    .floatField('soil_moisture', data.soil_moisture)
    .floatField('battery', data.battery)
    .intField('timestamp', data.timestamp);

  writeApi.writePoint(point);

  try {
    await writeApi.flush();
    console.log('📩 InfluxDB’ye yazıldı:', data);
  } catch (err) {
    console.error('❌ InfluxDB flush hatası:', err);
  }
});


// --- Express Route ---
app.get('/', (req, res) => {
  res.send('Backend çalışıyor 🚀');
});

// --- Server Start ---
app.listen(port, () => console.log(`🚀 Server listening on port ${port}`));
