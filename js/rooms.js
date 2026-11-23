// Real-time multiplayer rooms on Cloud Firestore.
//
// Data layout (must match firestore.rules):
//   rooms/{roomId}/players/{uid}  ->  { name, spot, joinedAt, lastSeen, character? }
//
// Firestore can't tell when a browser closes, so every player refreshes `lastSeen` on a
// heartbeat and everyone ignores players who haven't been seen for a while.
//
// Usage:
//   import { joinRoom, watchPlayers } from './js/rooms.js';
//   const me = await joinRoom('campfire', 'Jvhan', { max: 20, character: 'soldier-1' }); // may throw RoomFullError
//   const stopWatching = watchPlayers('campfire', players => draw(players));
//   ...
//   stopWatching();
//   await me.leave();      // intentional: no automatic rejoin after this

import { db, auth, ensureSignedIn } from './firebase.js';
import {
  collection, doc, setDoc, updateDoc, deleteDoc, getDocFromServer, getDocsFromServer,
  onSnapshot, serverTimestamp,
} from 'https://www.gstatic.com/firebasejs/13.4.0/firebase-firestore.js';

export const HEARTBEAT_MS = 15000;  // how often each player says "still here"
export const STALE_MS = 90000;      // players not seen for this long are treated as gone
const MIN_BEAT_GAP_MS = 6000;       // firestore.rules rejects heartbeats < 5s apart

const playersOf = roomId => collection(db, 'rooms', roomId, 'players');

export class RoomFullError extends Error {
  constructor(max) { super(`Room is full (${max} players)`); this.name = 'RoomFullError'; this.full = true; }
}

// ---- server clock -------------------------------------------------------------------
// `lastSeen` is written by Firestore's clock, so ages must be measured against Firestore's
// clock too, never the device's (a phone set 5 minutes wrong would otherwise see everyone
// as gone, or nobody ever leaving).
//
// Every time a server timestamp reaches us we learn  serverTime >= value, at this moment.
// It's tied to performance.now(), which doesn't jump when the device clock is changed.
// Delivery delay only makes an estimate too low, so the largest one seen is the best.
let serverMinusPerf = null;

export function observeServerTime(serverMillis) {
  const est = serverMillis - performance.now();
  if (serverMinusPerf === null || est > serverMinusPerf) serverMinusPerf = est;
}

// best guess of Firestore's current time; falls back to the device clock until a server
// timestamp has been seen
export function serverNow() {
  return serverMinusPerf === null ? Date.now() : serverMinusPerf + performance.now();
}

// ---- reading player docs ------------------------------------------------------------
function readPlayer(snap) {
  const pending = snap.metadata.hasPendingWrites;
  const d = snap.data({ serverTimestamps: 'estimate' });
  return {
    uid: snap.id,
    name: d.name,
    spot: d.spot,
    character: typeof d.character === 'string' ? d.character : null,
    joinedAt: d.joinedAt ? d.joinedAt.toMillis() : serverNow(),
    // a write of ours that the server hasn't confirmed yet counts as "just now"
    lastSeen: !pending && d.lastSeen ? d.lastSeen.toMillis() : serverNow(),
  };
}

const isFresh = p => serverNow() - p.lastSeen < STALE_MS;
const byJoinOrder = (a, b) => a.joinedAt - b.joinedAt || (a.uid < b.uid ? -1 : 1);

// ---- joining ------------------------------------------------------------------------
// Adds the signed-in player to the room and keeps them there until leave().
//
// Capacity: rules can't count documents, so the check happens after writing. Everyone in
// the room is ranked by server join time and if you're past `max` you're removed again
// and this rejects with RoomFullError. Two people joining in the same instant almost
// always resolve correctly, but a strict limit would need a server; this is best effort.
//
// While joined:
//   - heartbeat every 15s, plus one straight away when the tab comes back into view or
//     the network comes back (browsers slow timers in background tabs);
//   - if the player doc has gone (closed in another tab, tab restored from the back/forward
//     cache, deleted while offline) it's recreated with the same name and spot;
//   - Firestore itself reconnects and resends after a network drop.
// leave() is intentional: it stops all of that and deletes the doc for good.
export async function joinRoom(roomId, name, { max = 20, spot = Math.random(), character = null } = {}) {
  const user = await ensureSignedIn();
  name = String(name).trim().slice(0, 20);
  if (!name) throw new Error('A name is required');
  if (!(spot >= 0 && spot < 1)) spot = Math.random();

  const ref = doc(playersOf(roomId), user.uid);
  const joinData = { name, spot };
  if (character) joinData.character = String(character);
  const writeJoin = () => setDoc(ref, { ...joinData, joinedAt: serverTimestamp(), lastSeen: serverTimestamp() });

  await writeJoin();
  // read our own doc back to learn the server's clock before judging anyone else's age
  const mine = await getDocFromServer(ref);
  const myData = mine.data();
  if (myData && myData.lastSeen) observeServerTime(myData.lastSeen.toMillis());

  const everyone = (await getDocsFromServer(playersOf(roomId))).docs
    .map(readPlayer)
    .filter(p => p.uid === user.uid || isFresh(p))
    .sort(byJoinOrder);
  if (everyone.findIndex(p => p.uid === user.uid) >= max) {
    await deleteDoc(ref).catch(() => {});
    throw new RoomFullError(max);
  }

  let left = false;
  let busy = false;           // one heartbeat/rejoin at a time, so offline beats don't pile up
  let lastBeat = performance.now();

  async function beat() {
    if (left || busy) return;
    if (performance.now() - lastBeat < MIN_BEAT_GAP_MS) return;
    busy = true;
    lastBeat = performance.now();
    try {
      await updateDoc(ref, { lastSeen: serverTimestamp() });
    } catch (err) {
      // If our doc is gone, the heartbeat fails: firestore.rules can't check a heartbeat
      // against a missing doc, so it comes back as permission-denied (not-found without
      // rules). The same error also means "too soon", so look before rejoining.
      // Offline errors are skipped: Firestore resends by itself when the network is back.
      if (!left && err && (err.code === 'not-found' || err.code === 'permission-denied')) {
        try {
          const snap = await getDocFromServer(ref);
          // we never chose to leave, so put our doc back
          if (!snap.exists() && !left) await writeJoin();
        } catch (e) { /* try again on the next beat */ }
        if (left) deleteDoc(ref).catch(() => {}); // leave() happened while we were rejoining
      }
    } finally {
      busy = false;
      // count the gap from when the server actually got it (a write held back while
      // offline lands late), so the next beat isn't rejected for being too soon
      lastBeat = performance.now();
    }
  }

  const timer = setInterval(beat, HEARTBEAT_MS);
  const onVisible = () => { if (document.visibilityState === 'visible') beat(); };
  const onOnline = () => beat();
  // closing the tab removes us straight away instead of after 90s
  const onPageHide = () => { if (!left) deleteDoc(ref).catch(() => {}); };
  // but if the page comes back from the back/forward cache we're still meant to be here
  const onPageShow = e => {
    if (e.persisted && !left) { lastBeat = 0; beat(); }
  };
  document.addEventListener('visibilitychange', onVisible);
  window.addEventListener('online', onOnline);
  window.addEventListener('pagehide', onPageHide);
  window.addEventListener('pageshow', onPageShow);

  return {
    uid: user.uid,
    name,
    spot,
    character: joinData.character || null,
    get left() { return left; },
    async leave() {
      if (left) return;
      left = true;
      clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisible);
      window.removeEventListener('online', onOnline);
      window.removeEventListener('pagehide', onPageHide);
      window.removeEventListener('pageshow', onPageShow);
      await deleteDoc(ref).catch(() => {});
    },
  };
}

// ---- watching -----------------------------------------------------------------------
// Calls `callback(players)` with everyone currently in the room, sorted by join time,
// whenever someone joins, leaves, renames or goes quiet for 90s. You always count as
// present to yourself, even while your own connection is down.
// Returns a function that stops watching; call it exactly once per watch.
export function watchPlayers(roomId, callback, { onError } = {}) {
  let latest = [];
  let lastKey = null;
  let stopped = false;
  let stopSnapshot = () => {};

  const emit = () => {
    if (stopped) return;
    const myUid = auth.currentUser && auth.currentUser.uid;
    const present = latest.filter(p => p.uid === myUid || isFresh(p)).sort(byJoinOrder);
    const key = present.map(p => `${p.uid}:${p.name}:${p.spot}:${p.character}`).join('|');
    if (key !== lastKey) { lastKey = key; callback(present); }
  };

  ensureSignedIn().then(() => {
    if (stopped) return;
    let synced = false;
    stopSnapshot = onSnapshot(playersOf(roomId), snap => {
      // a heartbeat that just arrived from the server tells us roughly what time it is there.
      // The first server snapshot is skipped: it holds whatever is stored, including players
      // who left long ago, and their old times would make it look like it's still back then
      // (a watcher who never joined, like admin, would then see them as present).
      if (!snap.metadata.fromCache && !synced) synced = true;
      else if (!snap.metadata.fromCache) {
        snap.docChanges().forEach(ch => {
          if (ch.type === 'removed' || ch.doc.metadata.hasPendingWrites) return;
          const t = ch.doc.data().lastSeen;
          if (t) observeServerTime(t.toMillis());
        });
      }
      latest = snap.docs.map(readPlayer);
      emit();
    }, err => { if (onError) onError(err); else console.error('watchPlayers:', err); });
  }).catch(err => { if (onError) onError(err); else console.error('watchPlayers sign-in:', err); });

  const staleCheck = setInterval(emit, 5000); // drops players whose heartbeat stopped
  return () => {
    if (stopped) return;
    stopped = true;
    clearInterval(staleCheck);
    stopSnapshot();
  };
}
