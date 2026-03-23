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
const FOOTBALL_DATA_API_KEY = process.env.FOOTBALL_API_KEY;
const API_FOOTBALL_KEY      = process.env.API_FOOTBALL_KEY; // x-rapidapi-key
const API_BASE_FD           = "https://api.football-data.org/v4";
const API_BASE_AF           = "https://v3.football.api-sports.io"; // or RapidAPI host
const COLLECTION            = "live_matches";

// ─── Free tier competitions (football-data.org) ───────────────────────────────
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

// ═══════════════════════════════════════════════════════════════════════════════
// PRIMARY SOURCE — football-data.org
// ═══════════════════════════════════════════════════════════════════════════════

async function fetchCompetitionMatches(competitionCode, dateFrom, dateTo) {
  try {
    const response = await axios.get(
      `${API_BASE_FD}/competitions/${competitionCode}/matches`,
      {
        headers: { "X-Auth-Token": FOOTBALL_DATA_API_KEY },
        params: { dateFrom, dateTo },
        timeout: 10000,
      }
    );
    const matches = response.data.matches || [];
    console.log(`[football-data] ${competitionCode}: ${matches.length} matches`);
    return matches;
  } catch (err) {
    if (err.response?.status === 404 || err.response?.status === 403) {
      console.log(`[football-data] ${competitionCode}: skipped (${err.response.status})`);
      return [];
    }
    console.warn(`[football-data] ${competitionCode}: error - ${err.message}`);
    return [];
  }
}

async function fetchFromFootballData() {
  const yesterday = getDateString(-1);
  const tomorrow  = getDateString(+1);

  console.log(`[football-data] Fetching matches from ${yesterday} to ${tomorrow}`);

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

  console.log(`[football-data] Total unique matches: ${unique.length}`);
  return unique;
}

// ─── Map football-data.org match → Firestore doc ──────────────────────────────
function mapMatchFD(match) {
  return {
    matchId:          match.id,
    source:           "football-data",
    competition:      match.competition?.name || "Unknown",
    competitionEmblem: match.competition?.emblem || null,

    homeTeam: {
      id:        match.homeTeam?.id || null,
      name:      match.homeTeam?.name || "TBD",
      shortName: match.homeTeam?.shortName || "TBD",
      crest:     match.homeTeam?.crest || null,
    },
    awayTeam: {
      id:        match.awayTeam?.id || null,
      name:      match.awayTeam?.name || "TBD",
      shortName: match.awayTeam?.shortName || "TBD",
      crest:     match.awayTeam?.crest || null,
    },

    score: {
      home: match.score?.fullTime?.home ?? match.score?.halfTime?.home ?? null,
      away: match.score?.fullTime?.away ?? match.score?.halfTime?.away ?? null,
    },

    status:      match.status || "TIMED",
    minute:      match.minute || null,
    utcDate:     match.utcDate || null,
    dateOnly:    match.utcDate ? match.utcDate.split("T")[0] : null, // e.g. "2026-03-23"
    lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// FALLBACK SOURCE — API-Football (api-sports.io / RapidAPI)
// ═══════════════════════════════════════════════════════════════════════════════

async function fetchFromApiFootball() {
  if (!API_FOOTBALL_KEY) {
    console.warn("[api-football] API_FOOTBALL_KEY not set — skipping fallback.");
    return [];
  }

  const yesterday = getDateString(-1);
  const tomorrow  = getDateString(+1);
  const today     = getDateString(0);

  console.log(`[api-football] Fetching matches for ${yesterday}, ${today}, ${tomorrow}`);

  const allMatches = [];
  const dates = [yesterday, today, tomorrow];

  for (const date of dates) {
    try {
      const response = await axios.get(`${API_BASE_AF}/fixtures`, {
        headers: {
          "x-apisports-key": API_FOOTBALL_KEY,
          // If using RapidAPI instead, replace the two lines above with:
          // "x-rapidapi-key": API_FOOTBALL_KEY,
          // "x-rapidapi-host": "v3.football.api-sports.io",
        },
        params: { date },
        timeout: 10000,
      });

      const fixtures = response.data.response || [];
      console.log(`[api-football] ${date}: ${fixtures.length} fixtures`);
      allMatches.push(...fixtures);
    } catch (err) {
      console.warn(`[api-football] ${date}: error - ${err.message}`);
    }

    // Small delay between requests
    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  // Remove duplicates by fixture ID
  const unique = allMatches.filter(
    (f, index, self) =>
      index === self.findIndex((x) => x.fixture?.id === f.fixture?.id)
  );

  console.log(`[api-football] Total unique fixtures: ${unique.length}`);
  return unique;
}

// ─── Map API-Football fixture → Firestore doc ─────────────────────────────────
function mapMatchAF(fixture) {
  const f = fixture.fixture   || {};
  const l = fixture.league    || {};
  const t = fixture.teams     || {};
  const g = fixture.goals     || {};

  // Normalise status to football-data.org conventions for consistency
  const statusMap = {
    "TBD":  "TIMED",
    "NS":   "TIMED",       // Not Started
    "1H":   "IN_PLAY",
    "HT":   "PAUSED",
    "2H":   "IN_PLAY",
    "ET":   "IN_PLAY",
    "BT":   "IN_PLAY",
    "P":    "IN_PLAY",
    "SUSP": "SUSPENDED",
    "INT":  "IN_PLAY",
    "FT":   "FINISHED",
    "AET":  "FINISHED",
    "PEN":  "FINISHED",
    "PST":  "POSTPONED",
    "CANC": "CANCELLED",
    "ABD":  "CANCELLED",
    "AWD":  "FINISHED",
    "WO":   "FINISHED",
    "LIVE": "IN_PLAY",
  };

  return {
    matchId:          f.id,
    source:           "api-football",
    competition:      l.name  || "Unknown",
    competitionEmblem: l.logo || null,

    homeTeam: {
      id:        t.home?.id   || null,
      name:      t.home?.name || "TBD",
      shortName: t.home?.name || "TBD",
      crest:     t.home?.logo || null,
    },
    awayTeam: {
      id:        t.away?.id   || null,
      name:      t.away?.name || "TBD",
      shortName: t.away?.name || "TBD",
      crest:     t.away?.logo || null,
    },

    score: {
      home: g.home ?? null,
      away: g.away ?? null,
    },

    status:      statusMap[f.status?.short] || f.status?.short || "TIMED",
    minute:      f.status?.elapsed || null,
    utcDate:     f.date || null,
    dateOnly:    f.date ? f.date.split("T")[0] : null, // e.g. "2026-03-23"
    lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Sync to Firestore (batch write)
// ═══════════════════════════════════════════════════════════════════════════════

async function syncToFirestore(mappedMatches) {
  if (mappedMatches.length === 0) {
    console.log("No matches to sync.");
    return;
  }

  const BATCH_SIZE = 499;
  let processed = 0;

  for (let i = 0; i < mappedMatches.length; i += BATCH_SIZE) {
    const batch = db.batch();
    const chunk = mappedMatches.slice(i, i + BATCH_SIZE);

    for (const match of chunk) {
      const docRef = db.collection(COLLECTION).doc(String(match.matchId));
      batch.set(docRef, match, { merge: true });
    }

    await batch.commit();
    processed += chunk.length;
    console.log(`Committed batch: ${processed}/${mappedMatches.length} matches`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main — primary fetch → fallback if empty
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  console.log(`[${new Date().toISOString()}] Starting sync...`);

  const today = getDateString(0);

  try {
    // ── 1. Always fetch from football-data.org (yesterday + today + tomorrow) ─
    const fdMatches = await fetchFromFootballData();

    if (fdMatches.length > 0) {
      console.log(`[primary] Syncing football-data.org (${fdMatches.length} matches)`);
      const mapped = fdMatches.map(mapMatchFD);
      await syncToFirestore(mapped);
    }

    // ── 2. Check if TODAY specifically has matches from football-data.org ─────
    const todayFD = fdMatches.filter((m) => m.utcDate && m.utcDate.startsWith(today));
    console.log(`[primary] Today (${today}): ${todayFD.length} matches from football-data.org`);

    if (todayFD.length === 0) {
      // ── 3. Fallback to API-Football for today only ──────────────────────────
      console.log(`[fallback] No matches today from football-data.org → trying API-Football...`);
      const afFixtures = await fetchFromApiFootball();

      const afToday = afFixtures.filter((f) => f.fixture?.date && f.fixture.date.startsWith(today));
      console.log(`[fallback] API-Football today (${today}): ${afToday.length} fixtures`);

      if (afToday.length > 0) {
        console.log(`[fallback] Syncing ${afToday.length} fixtures from API-Football`);
        const mapped = afToday.map(mapMatchAF);
        await syncToFirestore(mapped);
      } else {
        console.log(`[fallback] API-Football also returned 0 matches for today.`);
      }
    }

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