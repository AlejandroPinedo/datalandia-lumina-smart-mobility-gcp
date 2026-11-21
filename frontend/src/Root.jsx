import React, { useEffect, useState } from 'react';
import App from './App.jsx';
import PassengerApp from './PassengerApp.jsx';

const analytics = new Set(['overview', 'explore', 'compare', 'questions', 'source', 'operations']);
export default function Root() {
  const [hash, setHash] = useState(() => location.hash.slice(1));
  useEffect(() => {
    const changed = () => {
      const next = location.hash.slice(1);
      if (!['main-content', 'passenger-content'].includes(next)) setHash(next);
    };
    window.addEventListener('hashchange', changed);
    return () => window.removeEventListener('hashchange', changed);
  }, []);
  return analytics.has(hash) ? <App /> : <PassengerApp />;
}
