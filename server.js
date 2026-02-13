import('dotenv').config();

import { InfluxDB, Point } from '@influxdata/influxdb-client';

const influxDB = new InfluxDB({ 
  url: process.env.INFLUX_URL, 
  token: process.env.INFLUX_TOKEN 
});
const writeApi = influxDB.getWriteApi(process.env.INFLUX_ORG, process.env.INFLUX_BUCKET);
writeApi.useDefaultTags({ location: 'istasyon1' });




const mqtt = require('mqtt');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const express = require('express');
const app = express();
const port = process.env.PORT || 3000;

// InfluxDB setup
const influx = new InfluxDB({ url: process.env.INFLUX_URL, token: process.env.INFLUX_TOKEN });

// MQTT setup
const client = mqtt.connect(process.env.MQTT_HOST, {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASS
});

client.on('connect', () => {
  console.log('✅ MQTT Connected');
  client.subscribe('tarla/istasyon1/data', (err) => {
    if (!err) console.log('📩 Subscribed to tarla topics');
  });
});

client.on('message', (topic, message) => {
  const data = JSON.parse(message.toString());
  console.log('📩 Data received:', data);

  // InfluxDB’ye yaz
  const point = new Point('sensor_data')
    .tag('device', data.device_id)
    .floatField('temperature', data.temperature)
    .floatField('humidity', data.humidity)
    .floatField('soil_moisture', data.soil_moisture)
    .floatField('battery', data.battery);

  writeApi.writePoint(point);
  writeApi.flush()
    .then(() => console.log('✅ Data yazıldı'))
    .catch(err => console.error('❌ InfluxDB Hatası:', err));
});


// Express API
app.get('/api/data', async (req, res) => {
  const queryApi = influx.getQueryApi(process.env.INFLUX_ORG);
  const fluxQuery = `
    from(bucket:"${process.env.INFLUX_BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "tarla_data")
  `;
  let results = [];
  queryApi.queryRows(fluxQuery, {
    next(row, tableMeta) { results.push(tableMeta.toObject(row)); },
    error(err) { console.error(err); res.status(500).send('Query error'); },
    complete() { res.json(results); }
  });
});

// Manual pump endpoint
app.post('/api/pump', (req, res) => {
  client.publish('tarla/pump', JSON.stringify({ action: 'start' }));
  res.send({ status: 'Pump triggered' });
});

app.listen(port, () => console.log(`🚀 API running on port ${port}`));
