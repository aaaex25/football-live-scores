const admin = require("firebase-admin");
const axios = require("axios");

// ─── Firebase Init ────────────────────────────────────────────────────────────
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
}

const db = admin.firestore();

// ─── Config ───────────────────────────────────────────────────────────────────
const FOOTBALL_API_KEY = process.env.FOOTBALL_API_KEY;
const API_BASE = "https://api.football-data.org/v4";
const COLLECTION = "live_matches";

// ─── Free tier competitions ───────────────────────────────────────────────────
const FREE_COMPETITIONS = [
  "PL",   // Premier League
  "BL1",  // Bundesliga
  "SA",   // Serie A
  "PD",   // La Liga
  "FL1",  // Ligue 1
  "DED",  // Eredivisie
  "PPL",  // Primeira Liga
  "ELC",  // Championship
  "CL",   // Champions League
  "WC",   // World Cup
  "EC",   // European Championship
];

// ─── Get date string ─────────────────────────────────────────────────────────
function getDateString(offsetDays) {
  const date = new Date();
  date.setDate(date.getDate() + offsetDays);
  return date.toISOString().split("T")[0];
}

// ─── Fetch Matches from one competition ──────────────────────────────────────
async function fetchCompetitionMatches(competitionCode, dateFrom, dateTo) {
  try {
    const response = await axios.get(
      `${API_BASE}/competitions/${competitionCode}/matches`,
      {
        headers: { "X-Auth-Token": FOOTBALL_API_KEY },
        params: { dateFrom, dateTo },
        timeout: 10000,
      }
    );
    const matches = response.data.matches || [];
    console.log(`${competitionCode}: ${matches.length} matches`);
    return matches;
  } catch (err) {
    if (err.response?.status === 404 || err.response?.status === 403) {
      console.log(`${competitionCode}: skipped (${err.response.status})`);
      return [];
    }
    console.warn(`${competitionCode}: error - ${err.message}`);
    return [];
  }
}

// ─── Fetch all competitions ───────────────────────────────────────────────────
async function fetchMatches() {
  const yesterday = getDateString(-1);
  const tomorrow  = getDateString(+1);

  console.log(`Fetching matches from ${yesterday} to ${tomorrow}`);

  const allMatches = [];

  for (const code of FREE_COMPETITIONS) {
    const matches = await fetchCompetitionMatches(code, yesterday, tomorrow);
    allMatches.push(...matches);

    // 1 second delay to respect rate limit (10 req/min on free tier)
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  // Remove duplicates by match ID
  const unique = allMatches.filter(
    (match, index, self) =>
      index === self.findIndex((m) => m.id === match.id)
  );

  console.log(`Total unique matches: ${unique.length}`);
  return unique;
}

// ─── Map API match to Firestore document ─────────────────────────────────────
function mapMatch(match) {
  return {
    matchId: match.id,
    competition: match.competition?.name || "Unknown",
    competitionEmblem: match.competition?.emblem || null,

    homeTeam: {
      id: match.homeTeam?.id || null,
      name: match.homeTeam?.name || "TBD",
      shortName: match.homeTeam?.shortName || "TBD",
      crest: match.homeTeam?.crest || null,
    },
    awayTeam: {
      id: match.awayTeam?.id || null,
      name: match.awayTeam?.name || "TBD",
      shortName: match.awayTeam?.shortName || "TBD",
      crest: match.awayTeam?.crest || null,
    },

    score: {
      home: match.score?.fullTime?.home ?? match.score?.halfTime?.home ?? null,
      away: match.score?.fullTime?.away ?? match.score?.halfTime?.away ?? null,
    },

    status: match.status || "TIMED",
    minute: match.minute || null,
    utcDate: match.utcDate || null,
    lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
  };
}

// ─── Sync to Firestore (batch write) ─────────────────────────────────────────
async function syncToFirestore(matches) {
  if (matches.length === 0) {
    console.log("No matches found.");
    return;
  }

  const BATCH_SIZE = 499;
  let processed = 0;

  for (let i = 0; i < matches.length; i += BATCH_SIZE) {
    const batch = db.batch();
    const chunk = matches.slice(i, i + BATCH_SIZE);

    for (const match of chunk) {
      const mapped = mapMatch(match);
      const docRef = db.collection(COLLECTION).doc(String(mapped.matchId));
      batch.set(docRef, mapped, { merge: true });
    }

    await batch.commit();
    processed += chunk.length;
    console.log(`Committed batch: ${processed}/${matches.length} matches`);
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`[${new Date().toISOString()}] Starting sync...`);

  try {
    const matches = await fetchMatches();
    await syncToFirestore(matches);
    console.log("Sync complete ✓");
  } catch (err) {
    console.error("Sync failed:", err.message);
    if (err.response) {
      console.error("API Response:", err.response.status, err.response.data);
    }
    process.exit(1);
  }
}

main();