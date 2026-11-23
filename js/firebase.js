// Firebase setup shared by every page that needs it.
//
// The site has no build step, so the SDK is loaded straight from Firebase's CDN, pinned
// to the same version as package.json (13.4.0). Use it from a module script:
//   <script type="module"> import { db, auth, ensureSignedIn } from './js/firebase.js'; </script>
//
// The apiKey below is not a secret; Firebase web keys are meant to be public.
// Access is controlled by Firestore Security Rules (see firestore.rules).

import { initializeApp } from 'https://www.gstatic.com/firebasejs/13.4.0/firebase-app.js';
import { getFirestore } from 'https://www.gstatic.com/firebasejs/13.4.0/firebase-firestore.js';
import { getAuth, signInAnonymously, onAuthStateChanged } from 'https://www.gstatic.com/firebasejs/13.4.0/firebase-auth.js';

const firebaseConfig = {
  apiKey: 'AIzaSyDcl9ZpYlPskbSVnxR3tJFjknrwTgUhUNQ',
  authDomain: 'nursing-project-8be5b.firebaseapp.com',
  projectId: 'nursing-project-8be5b',
  storageBucket: 'nursing-project-8be5b.firebasestorage.app',
  messagingSenderId: '304057735226',
  appId: '1:304057735226:web:e5e59ccd9aecfd145ffd03',
};

export const app = initializeApp(firebaseConfig);
export const db = getFirestore(app);
export const auth = getAuth(app);

// Students don't make accounts: each browser gets an anonymous Firebase user, which gives
// it a stable uid that the security rules can check. Resolves with that user.
let signingIn = null;
export function ensureSignedIn() {
  if (auth.currentUser) return Promise.resolve(auth.currentUser);
  if (!signingIn) {
    signingIn = new Promise((resolve, reject) => {
      const stop = onAuthStateChanged(auth, user => {
        if (user) { stop(); resolve(user); }
      });
      signInAnonymously(auth).catch(err => { stop(); signingIn = null; reject(err); });
    });
  }
  return signingIn;
}
