const admin = require("firebase-admin");
const axios = require("axios");

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
if (!admin.apps.length) {
  admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
}

const db = admin.firestore();

const FOOTBALL_API_KEY = process.env.FOOTBALL_API_KEY;
const NEWS_API_KEY     = process.env.NEWS_API_KEY;
const API_BASE         = "https://api.football-data.org/v4";
const MATCHES_COL      = "live_matches";
const NEWS_COL         = "football_news";

const FREE_COMPETITIONS = [
  "PL", "BL1", "SA", "PD", "FL1", "DED", "PPL", "ELC", "CL", "WC", "EC",
];

function getDateString(offsetDays) {
  const date = new Date();
  date.setDate(date.getDate() + offsetDays);
  return date.toISOString().split("T")[0];
}

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

/**
 * Fetch matches strategy:
 * 1. Try to get today's live/scheduled matches first
 * 2. Also fetch yesterday + tomorrow for context
 * 3. If today has zero matches, go back up to 7 days to find the latest day with matches
 */
async function fetchMatches() {
  // Always fetch: 2 days back (recent results) + today + 2 days ahead (upcoming)
  const dateFrom = getDateString(-3);
  const dateTo   = getDateString(+3);
  console.log(`Fetching matches from ${dateFrom} to ${dateTo}`);

  let allMatches = [];
  for (const code of FREE_COMPETITIONS) {
    const matches = await fetchCompetitionMatches(code, dateFrom, dateTo);
    allMatches.push(...matches);
    await new Promise((r) => setTimeout(r, 1000));
  }

  allMatches = dedupe(allMatches);
  console.log(`Total unique matches: ${allMatches.length}`);
  return allMatches;
}

function dedupe(matches) {
  return matches.filter(
    (match, index, self) =>
      index === self.findIndex((m) => m.id === match.id)
  );
}

function mapMatch(match) {
  return {
    matchId:           match.id,
    competition:       match.competition?.name || "Unknown",
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
    dateOnly:    match.utcDate ? match.utcDate.substring(0, 10) : null,
    lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
  };
}

async function syncMatches(matches) {
  if (matches.length === 0) { console.log("No matches to sync."); return; }

  // First delete old docs that are not in current fetch window
  // (keep only what we fetched this run)
  const fetchedIds = new Set(matches.map(m => String(m.id)));
  const existingSnap = await db.collection(MATCHES_COL).get();
  const toDelete = existingSnap.docs.filter(d => !fetchedIds.has(d.id));
  if (toDelete.length > 0) {
    const deleteBatch = db.batch();
    toDelete.forEach(d => deleteBatch.delete(d.ref));
    await deleteBatch.commit();
    console.log(`Deleted ${toDelete.length} stale match docs`);
  }

  const BATCH_SIZE = 499;
  let processed = 0;
  for (let i = 0; i < matches.length; i += BATCH_SIZE) {
    const batch = db.batch();
    const chunk = matches.slice(i, i + BATCH_SIZE);
    for (const match of chunk) {
      const mapped = mapMatch(match);
      const docRef = db.collection(MATCHES_COL).doc(String(mapped.matchId));
      batch.set(docRef, mapped, { merge: true });
    }
    await batch.commit();
    processed += chunk.length;
    console.log(`Matches committed: ${processed}/${matches.length}`);
  }
}

async function fetchNews() {
  try {
    const response = await axios.get("https://newsapi.org/v2/everything", {
      params: {
        q: '("Premier League" OR "La Liga" OR "Serie A" OR "Bundesliga" OR "Ligue 1" OR "Champions League" OR "Europa League" OR "UEFA") AND (football OR soccer) AND NOT (basketball OR tennis OR golf OR cricket OR rugby OR baseball OR "American football" OR NFL)',
        language: "en",
        sortBy: "publishedAt",
        pageSize: 50,
        apiKey: NEWS_API_KEY,
      },
      timeout: 10000,
    });
    const articles = response.data.articles || [];
    return articles.filter(a => a.title && a.title !== "[Removed]" && a.url);
  } catch (err) {
    console.warn("News fetch failed:", err.message);
    return [];
  }
}

async function syncNews(articles) {
  if (articles.length === 0) { console.log("No news articles."); return; }
  const batch = db.batch();
  for (const article of articles) {
    const docId  = Buffer.from(article.url).toString("base64").slice(0, 50);
    const docRef = db.collection(NEWS_COL).doc(docId);
    batch.set(docRef, {
      title:       article.title || "",
      description: article.description || "",
      urlToImage:  article.urlToImage || null,
      url:         article.url || "",
      publishedAt: article.publishedAt || "",
      sourceName:  article.source?.name || "Unknown",
      syncedAt:    admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  }
  await batch.commit();
  console.log(`News committed: ${articles.length} articles`);
}

async function cleanOldNews() {
  try {
    const snapshot = await db.collection(NEWS_COL)
      .orderBy("publishedAt", "desc")
      .offset(100)
      .get();
    if (snapshot.empty) return;
    const batch = db.batch();
    snapshot.docs.forEach(doc => batch.delete(doc.ref));
    await batch.commit();
    console.log(`Deleted ${snapshot.docs.length} old articles`);
  } catch (err) {
    console.warn("Cleanup failed:", err.message);
  }
}

async function main() {
  console.log(`[${new Date().toISOString()}] Starting sync...`);
  try {
    const matches = await fetchMatches();
    await syncMatches(matches);

    const articles = await fetchNews();
    await syncNews(articles);
    await cleanOldNews();

    console.log("Sync complete ✓");
  } catch (err) {
    console.error("Sync failed:", err.message);
    process.exit(1);
  }
}

main();