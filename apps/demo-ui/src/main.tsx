import React from 'react';
import { createRoot } from 'react-dom/client';
import { io } from 'socket.io-client';
import './styles.css';

const API = import.meta.env.VITE_API_URL || 'http://localhost:3000';

type TimelineEvent = {
  eventId: string;
  eventType: string;
  occurredAt: string;
  correlationId: string;
  customerId: string;
  source: string;
};

function App() {
  const [customerId, setCustomerId] = React.useState('C-1234');
  const [planId, setPlanId] = React.useState('Plan Digital');
  const [simulateFailure, setFailure] = React.useState('none');
  const [activationId, setActivationId] = React.useState('');
  const [events, setEvents] = React.useState<TimelineEvent[]>([]);

  React.useEffect(() => {
    const socket = io(API);
    socket.on('activation-event', (event: TimelineEvent) => {
      setEvents(previous => event.correlationId === activationId || !activationId ? [...previous, event] : previous);
    });
    return () => { socket.disconnect(); };
  }, [activationId]);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setEvents([]);
    const response = await fetch(`${API}/activations`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ customerId, planId, simulateFailure })
    });
    const data = await response.json();
    setActivationId(data.activationId);
  }

  return (
    <main>
      <section className="card">
        <p className="eyebrow">TP6 · Sistemas Distribuidos</p>
        <h1>Activación de servicios con Kafka</h1>
        <form onSubmit={submit}>
          <label>Cliente<input value={customerId} onChange={e => setCustomerId(e.target.value)} required /></label>
          <label>Plan<input value={planId} onChange={e => setPlanId(e.target.value)} required /></label>
          <label>Simular fallo<select value={simulateFailure} onChange={e => setFailure(e.target.value)}><option value="none">Ninguno</option><option value="billing">Billing</option><option value="provisioning">Provisioning</option></select></label>
          <button>Contratar plan</button>
        </form>
        {activationId && <div className="activation">Activation ID: <strong>{activationId}</strong></div>}
      </section>

      <section className="card">
        <h2>Timeline</h2>
        <div className="timeline">
          {events.length === 0 ? <p className="muted">Todavía no hay eventos.</p> : events.filter(e => !activationId || e.correlationId === activationId).map(e => (
            <article key={e.eventId} className={e.eventType.includes('Failed') ? 'error' : 'ok'}>
              <span>{new Date(e.occurredAt).toLocaleTimeString()}</span>
              <strong>{e.eventType}</strong>
              <small>{e.source}</small>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}

createRoot(document.getElementById('root')!).render(<App />);
