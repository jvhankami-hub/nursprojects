// Shared wolf-battle state for a room, on Cloud Firestore.
//
// Data layout (must match firestore.rules):
//   rooms/{roomId}/battle/state   ->  the whole battle, written only by the host (admin's screen)
//   rooms/{roomId}/answers/{uid}  ->  { turn, choice, at }  each player's latest answer
//
// The host decides everything (whose turn, right or wrong, lives, rounds); players only write
// their own answer. Every screen watches the battle doc and plays the same animations.

import { db, ensureSignedIn } from './firebase.js';
import { observeServerTime, serverNow } from './rooms.js';
import {
  collection, doc, setDoc, onSnapshot, serverTimestamp,
} from 'https://www.gstatic.com/firebasejs/13.4.0/firebase-firestore.js';

export { serverNow };

const battleRef = roomId => doc(db, 'rooms', roomId, 'battle', 'state');
const answersOf = roomId => collection(db, 'rooms', roomId, 'answers');

export async function myUid() {
  return (await ensureSignedIn()).uid;
}

// Calls `callback(state)` whenever the battle changes (null if there has never been one).
// Timestamps come back as milliseconds of server time; `pending` is true while the host's own
// write hasn't reached the server yet (its times are then only estimates).
export function watchBattle(roomId, callback, { onError } = {}) {
  let stopped = false;
  let stop = () => {};
  ensureSignedIn().then(() => {
    if (stopped) return;
    let synced = false;
    stop = onSnapshot(battleRef(roomId), { includeMetadataChanges: true }, snap => {
      const pending = snap.metadata.hasPendingWrites;
      // a fresh change from the server tells us what time it is there (the first snapshot
      // may be an old battle, so it's skipped, same as in rooms.js)
      if (!snap.metadata.fromCache && !pending) {
        if (synced && snap.exists()) {
          const t = snap.data().updatedAt;
          if (t) observeServerTime(t.toMillis());
        }
        synced = true;
      }
      if (!snap.exists()) { callback(null); return; }
      const d = snap.data({ serverTimestamps: 'estimate' });
      callback({
        ...d,
        phaseAt: d.phaseAt ? d.phaseAt.toMillis() : serverNow(),
        updatedAt: d.updatedAt ? d.updatedAt.toMillis() : serverNow(),
        pending,
      });
    }, err => { if (onError) onError(err); else console.error('watchBattle:', err); });
  }).catch(err => { if (onError) onError(err); else console.error('watchBattle sign-in:', err); });
  return () => {
    if (stopped) return;
    stopped = true;
    stop();
  };
}

// Host only: replace the battle doc. `newPhase: true` stamps phaseAt with the server time.
export function writeBattle(roomId, state, { newPhase = false } = {}) {
  const { pending, phaseAt, updatedAt, ...data } = state;
  return setDoc(battleRef(roomId), {
    ...data,
    phaseAt: newPhase || !phaseAt ? serverTimestamp() : new Date(phaseAt),
    updatedAt: serverTimestamp(),
  });
}

// Player: answer the current turn (choice is 0-3).
export async function submitAnswer(roomId, turn, choice) {
  const uid = await myUid();
  return setDoc(doc(answersOf(roomId), uid), { turn, choice, at: serverTimestamp() });
}

// Host: every player's latest answer, as a Map uid -> { turn, choice }.
export function watchAnswers(roomId, callback) {
  let stopped = false;
  let stop = () => {};
  ensureSignedIn().then(() => {
    if (stopped) return;
    stop = onSnapshot(answersOf(roomId), snap => {
      callback(new Map(snap.docs.map(d => [d.id, { turn: d.data().turn, choice: d.data().choice }])));
    }, err => console.error('watchAnswers:', err));
  });
  return () => {
    if (stopped) return;
    stopped = true;
    stop();
  };
}
