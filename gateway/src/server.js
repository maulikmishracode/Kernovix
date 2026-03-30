const express = require('express');

const app = express();
app.use(express.json());

const events = []; // stores events in memory

// Health check
app.get('/', (req, res) => {
  res.json({ 
    status: 'Kernovix gateway running', 
    version: '1.0.0',
    total_events: events.length
  });
});

// Ingest a new event
app.post('/events', (req, res) => {
  const event = {
    id: Date.now().toString(),
    timestamp: new Date().toISOString(),
    source: req.body.source || 'unknown',
    type: req.body.type || 'generic',
    payload: req.body.payload || {}
  };

  events.push(event);
  console.log('Event received:', event);
  res.status(201).json({ message: 'Event ingested successfully', event });
});

// Get all events
app.get('/events', (req, res) => {
  res.json({ 
    count: events.length, 
    events: events.slice(-10) // last 10
  });
});

// Delete all events
app.delete('/events', (req, res) => {
  events.length = 0;
  res.json({ message: 'All events cleared' });
});

app.listen(3000, () => {
  console.log('Kernovix gateway running on http://localhost:3000');
});