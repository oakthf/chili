upd: {[table; data] table upsert data; tick[this.h; 1]; };

.sub.init: {[tickSocket; topics]
  .sub.topics: topics;
  h: .handle.open tickSocket;
  .handle.onDisconnected[h; `.sub.recover];
  info: h (`.tick.subscribe; topics);
  (set) each info[2];
  // .log.info ("broker info"; info);
  // D-3 (ADR-0006 §4): replay from the persisted resume cursor
  // (min over subscribed topics; 0 ⇒ full replay) instead of a
  // hardcoded 0. Empty resume map ⇒ 0 ⇒ identical to the old behavior.
  replay[info[0]; resume_cursor[topics]; info[1]; (); 1b; h];
  .handle.subscribing[h];
};

// this function will be called when the connection is lost, retry every minute until no error
.sub.recover: {[handle]
  .handle.connect[handle];
  info: handle (`.tick.subscribe; .sub.topics);
  // D-3 (ADR-0006 §4): replay from the persisted resume cursor for
  // the reconnecting handle's own topics, replacing the latent
  // hardcoded `tick[0]` (handle-0 tick count). Empty resume map ⇒ 0
  // ⇒ safe full replay (mdata's own `seq` dedups — Q1 Path-1).
  replay[info[0]; resume_cursor[.sub.topics]; info[1]; (); 1b; handle];
  .handle.subscribing[handle];
};
