// Prints SQL that fills ema_results and crossover_events with synthetic data,
// for working on the frontend without replaying the real tick data set.
//
//   node web/frontend/scripts/seed-demo.mjs | docker exec -i timescaledb psql -q -U trading -d trading
//
// Development only. The SQL aborts if the tables already hold rows; pass
// --force to replace them. EMAs and crossovers follow the same rules as the
// Flink job (EMA starts at 0, 5-minute windows).

const SYMBOLS = [
  ["RDSA.NL", 19.4], ["ASML.NL", 742], ["INGA.NL", 12.6], ["PHIA.NL", 38.2], ["ADYEN.NL", 2610],
  ["MC.FR", 688], ["OR.FR", 402], ["AIR.FR", 112.5], ["BNP.FR", 59.3], ["SAN.FR", 88.1],
  ["TTE.FR", 43.7], ["ALMIL.FR", 0.842], ["SAP.ETR", 124.9], ["SIE.ETR", 148.6], ["SHL.ETR", 61.2],
  ["VOW3.ETR", 187.4], ["BMW.ETR", 91.8], ["DBK.ETR", 11.3], ["ALV.ETR", 204.5], ["BAS.ETR", 63.9],
];
const DAYS = ["2021-11-08", "2021-11-09", "2021-11-10", "2021-11-11", "2021-11-12"];
const OPEN_MINUTE = 9 * 60;
const CLOSE_MINUTE = 17 * 60 + 30;

// Small seeded generator so every run produces the same market.
let state = 20211108;
function random() {
  state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
  return state / 4294967296;
}
function gaussian() {
  return Math.sqrt(-2 * Math.log(1 - random())) * Math.cos(2 * Math.PI * random());
}

const emaRows = [];
const signalRows = [];

for (const [symbol, start] of SYMBOLS) {
  let price = start;
  let drift = 0;
  let ema38 = 0;
  let ema100 = 0;
  let previous = null;

  for (const day of DAYS) {
    for (let minute = OPEN_MINUTE + 5; minute <= CLOSE_MINUTE; minute += 5) {
      // Illiquid symbols skip windows, as in the real data.
      if (symbol === "ALMIL.FR" && random() < 0.35) continue;

      drift = drift * 0.97 + gaussian() * 0.00012;
      price = Math.max(0.01, price * (1 + drift + gaussian() * 0.0011));
      ema38 = price * (2 / 39) + ema38 * (1 - 2 / 39);
      ema100 = price * (2 / 101) + ema100 * (1 - 2 / 101);

      const hh = String(Math.floor(minute / 60)).padStart(2, "0");
      const mm = String(minute % 60).padStart(2, "0");
      const windowEnd = `${day} ${hh}:${mm}:00 Europe/Paris`;
      emaRows.push(`('${windowEnd}','${symbol}',${price.toFixed(4)},${ema38.toFixed(6)},${ema100.toFixed(6)})`);

      if (previous) {
        const type =
          previous.ema38 <= previous.ema100 && ema38 > ema100
            ? "BUY"
            : previous.ema38 >= previous.ema100 && ema38 < ema100
              ? "SELL"
              : null;
        if (type) {
          signalRows.push(`('${windowEnd}','${symbol}','${type}',${ema38.toFixed(6)},${ema100.toFixed(6)})`);
        }
      }
      previous = { ema38, ema100 };
    }
  }
}

console.log("BEGIN;");
if (process.argv.includes("--force")) {
  console.log("TRUNCATE ema_results, crossover_events;");
} else {
  console.log(`DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM ema_results) OR EXISTS (SELECT 1 FROM crossover_events) THEN
    RAISE EXCEPTION 'ema_results or crossover_events already has rows; rerun the seed script with --force to replace them';
  END IF;
END $$;`);
}
console.log(`INSERT INTO ema_results (window_end, symbol, last_price, ema38, ema100) VALUES\n${emaRows.join(",\n")};`);
if (signalRows.length > 0) {
  console.log(
    `INSERT INTO crossover_events (window_end, symbol, signal_type, ema38, ema100) VALUES\n${signalRows.join(",\n")};`,
  );
}
console.log("COMMIT;");
