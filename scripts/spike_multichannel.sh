#!/usr/bin/env bash
# ============================================================================
# spike_multichannel.sh — ספייק חומרה ל-Phase 0 של מיזוג DMR ⨯ DMR-DECREP.
#
# מטרה: לענות על *השאלה היחידה* שמכריעה את כל המיזוג —
#   "כמה ערוצי Cap+ ה-Pi 5 + RSP1B באמת מפענח בו-זמנית בזמן-אמת?"
#
# הסקריפט מריץ את מצב --channel-plan של DMR-DECREP על חומרה אמיתית, מודד:
#   • CPU כולל + CPU לכל תהליך dsd-fme (מ-/proc, לא ממוצע-חיים של ps)
#   • זיכרון (RSS) של עץ התהליכים
#   • כמה אירועים באמת פוענחו (עדות ל-sync-lock, לא רק "התהליך רץ")
#   • ניצול לעומת מספר הליבות (Pi 5 = 4 → 400% = רוויה מלאה)
# ואז מדפיס verdict: האם ריבוי-הערוצים בר-קיימא, וכמה ערוצים.
#
# ⚠ חייב לרוץ על ה-Pi עם ה-RSP1B מחובר ומול אתר Cap+ חי (אחרת dsd-fme
#   לא ינעל ולא יהיו אירועים — וזה כשלעצמו תוצאה: "אין קליטה").
#
# הרצה:
#   bash scripts/spike_multichannel.sh                 # תוכנית לדוגמה, 90ש'
#   bash scripts/spike_multichannel.sh site.json 120   # התוכנית שלך, 120ש'
#   FOLLOW=1 bash scripts/spike_multichannel.sh site.json   # גם מעבר follow-traffic
# ============================================================================
set -uo pipefail

# ── פרמטרים ─────────────────────────────────────────────────────────────────
PLAN="${1:-}"                       # קובץ channel-plan (JSON). ריק → נייצר לדוגמה
DURATION="${2:-90}"                 # משך מדידה בשניות
PORT="${PORT:-8081}"               # פורט ל-UI (8081 כדי לא להתנגש ב-DMR על 8080)
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-1}"
FOLLOW="${FOLLOW:-0}"              # 1 → הרצה שנייה עם --follow-traffic להשוואה
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$REPO_ROOT/.venv/bin/python"
OUT_DIR="$(mktemp -d /tmp/dmr_spike.XXXXXX)"
NPROC="$(nproc 2>/dev/null || echo 4)"

cd "$REPO_ROOT" || { echo "לא נמצא שורש הריפו"; exit 1; }
[ -x "$VENV_PY" ] || VENV_PY="python3"   # נפילה חיננית ל-python3 אם אין venv

echo "════════════════════════════════════════════════════════════════"
echo "  ספייק ריבוי-ערוצים — DMR-DECREP על חומרה אמיתית"
echo "  ריפו:    $REPO_ROOT"
echo "  ליבות:   $NPROC   (רוויה מלאה = $((NPROC*100))% CPU)"
echo "  משך:     ${DURATION}s   פורט UI: $PORT   פלט: $OUT_DIR"
echo "════════════════════════════════════════════════════════════════"

# ── 1. Preflight — בלי אלה אין טעם להתחיל ──────────────────────────────────
fail=0
echo; echo "── בדיקות מקדימות ──"
check() { printf "  %-28s " "$1"; if eval "$2" >/dev/null 2>&1; then echo "✓"; else echo "✗  ($3)"; fail=1; fi; }
check "python + backend"     "$VENV_PY -c 'import backend'" "הרץ מתוך שורש הריפו, ודא .venv"
check "numpy"                "$VENV_PY -c 'import numpy'"    "pip install numpy"
check "SoapySDR (python)"    "$VENV_PY -c 'import SoapySDR'" "צריך SoapySDR — ר' scripts/setup-sdrplay.sh"
check "dsd-fme בנתיב"        "command -v dsd-fme"           "בנה מ-lwvmobile/dsd-fme"
check "SoapySDRUtil"         "command -v SoapySDRUtil"      "התקנת SoapySDR חסרה"
echo "  ── מכשירי SDR שנראים ל-Soapy: ──"
SoapySDRUtil --find 2>/dev/null | sed 's/^/    /' | grep -iE "driver|serial|label" || echo "    (לא נראה RSP1B! ודא חיבור USB ו-sdrplay.service)"
if [ "$fail" -ne 0 ]; then
  echo; echo "❌ preflight נכשל — תקן את ה-✗ למעלה לפני שממשיכים."; exit 1
fi

# ── 2. תוכנית ערוצים — נייצר לדוגמה אם לא סופקה ─────────────────────────────
if [ -z "$PLAN" ]; then
  PLAN="$OUT_DIR/sample_plan.json"
  cat > "$PLAN" <<'JSON'
{
  "channels": [
    {"label": "cc",  "frequency_hz": 168500000, "lsn": 1, "control": true},
    {"label": "ch2", "frequency_hz": 168512500, "lsn": 2},
    {"label": "ch3", "frequency_hz": 168525000, "lsn": 3},
    {"label": "ch4", "frequency_hz": 168537500, "lsn": 4}
  ]
}
JSON
  echo; echo "⚠ לא סופקה תוכנית — נוצרה תוכנית-דוגמה ($PLAN)."
  echo "  ה-4 תדרים למעלה הם דמה! ערוך אותם לתדרי האתר האמיתי שלך והרץ שוב:"
  echo "     bash scripts/spike_multichannel.sh $PLAN $DURATION"
  echo "  (ממשיך בכל זאת — יימדד CPU של ה-DSP, אך dsd-fme לא ינעל בלי תדרים אמיתיים.)"
fi
NCH="$($VENV_PY -c "import json,sys; print(len(json.load(open('$PLAN'))['channels']))" 2>/dev/null || echo '?')"
echo; echo "תוכנית: $PLAN  ($NCH ערוצים)"

# ── 3. מודד CPU/זיכרון של עץ-התהליכים (jiffies מ-/proc, מדויק) ──────────────
# מחזיר: סכום utime+stime jiffies של PID + כל צאצאיו
tree_jiffies() {
  local root=$1 total=0 pids p st
  pids=$(pgrep -P "$root" 2>/dev/null; echo "$root"
         # שני דורות (backend → dsd-fme הם ילדים ישירים; ליתר ביטחון סורקים גם נכדים)
         for c in $(pgrep -P "$root" 2>/dev/null); do pgrep -P "$c" 2>/dev/null; done)
  for p in $pids; do
    st=$(cut -d' ' -f14,15 "/proc/$p/stat" 2>/dev/null) || continue
    total=$(( total + ${st% *} + ${st#* } ))
  done
  echo "$total"
}
tree_rss_kb() {   # סכום RSS (KB) של העץ
  local root=$1 total=0 p pids
  pids=$(echo "$root"; pgrep -P "$root" 2>/dev/null
         for c in $(pgrep -P "$root" 2>/dev/null); do pgrep -P "$c" 2>/dev/null; done)
  for p in $pids; do
    r=$(awk '/^VmRSS/{print $2}' "/proc/$p/status" 2>/dev/null) && total=$((total+r))
  done
  echo "$total"
}
count_dsdfme() { pgrep -c -x dsd-fme 2>/dev/null || echo 0; }

# ── 4. פונקציית הרצה+מדידה (משמשת גם ל-follow-traffic) ──────────────────────
run_pass() {
  local label="$1"; shift
  local extra=("$@")
  local evlog="$OUT_DIR/events_${label}.jsonl"
  local snap="$OUT_DIR/snap_${label}.json"
  echo; echo "══════ הרצה: $label ══════"
  echo "פקודה: $VENV_PY -m backend.cli --live --channel-plan $PLAN ${extra[*]} --serve --port $PORT ..."

  "$VENV_PY" -m backend.cli --live --rf-backend soapy \
      --channel-plan "$PLAN" "${extra[@]}" \
      --serve --port "$PORT" \
      --event-log "$evlog" --snapshot "$snap" \
      --calls-dir "$OUT_DIR/calls_${label}" \
      > "$OUT_DIR/backend_${label}.log" 2>&1 &
  local BE=$!
  sleep 4   # bring-up: פתיחת SDR + שרתי אודיו + עליית dsd-fme
  if ! kill -0 "$BE" 2>/dev/null; then
    echo "❌ ה-backend קרס תוך שניות. 20 שורות אחרונות:"; tail -n 20 "$OUT_DIR/backend_${label}.log" | sed 's/^/    /'
    return 1
  fi

  local HZ; HZ=$(getconf CLK_TCK 2>/dev/null || echo 100)
  local samples=0 sum_cpu=0 peak_cpu=0 peak_rss=0 peak_dsd=0
  local prev; prev=$(tree_jiffies "$BE")
  printf "  %-6s %-9s %-8s %-9s %s\n" "t(s)" "CPU%" "dsd-fme" "RSS(MB)" "אירועים"
  for ((t=SAMPLE_INTERVAL; t<=DURATION; t+=SAMPLE_INTERVAL)); do
    sleep "$SAMPLE_INTERVAL"
    kill -0 "$BE" 2>/dev/null || { echo "  ❌ ה-backend מת ב-t=${t}s"; break; }
    local now; now=$(tree_jiffies "$BE")
    local dj=$(( now - prev )); prev=$now
    # CPU% = jiffies-delta / (interval * HZ) * 100
    local cpu=$(( dj * 100 / (SAMPLE_INTERVAL * HZ) ))
    local rss_mb=$(( $(tree_rss_kb "$BE") / 1024 ))
    local ndsd; ndsd=$(count_dsdfme)
    local nev=0; [ -f "$evlog" ] && nev=$(wc -l < "$evlog" 2>/dev/null || echo 0)
    printf "  %-6s %-9s %-8s %-9s %s\n" "$t" "${cpu}%" "$ndsd" "$rss_mb" "$nev"
    samples=$((samples+1)); sum_cpu=$((sum_cpu+cpu))
    [ "$cpu" -gt "$peak_cpu" ] && peak_cpu=$cpu
    [ "$rss_mb" -gt "$peak_rss" ] && peak_rss=$rss_mb
    [ "$ndsd" -gt "$peak_dsd" ] && peak_dsd=$ndsd
  done

  # עצירה נקייה
  kill -INT "$BE" 2>/dev/null; sleep 2; kill -KILL "$BE" 2>/dev/null; wait "$BE" 2>/dev/null
  pkill -x dsd-fme 2>/dev/null

  local avg_cpu=0; [ "$samples" -gt 0 ] && avg_cpu=$(( sum_cpu / samples ))
  local total_ev=0; [ -f "$evlog" ] && total_ev=$(wc -l < "$evlog" 2>/dev/null || echo 0)
  local voice=0; [ -f "$evlog" ] && voice=$(grep -c '"voice' "$evlog" 2>/dev/null || echo 0)
  local per_ch="?"; [ "$peak_dsd" -gt 0 ] && per_ch=$(( avg_cpu / peak_dsd ))
  local headroom=$(( NPROC*100 - peak_cpu ))

  echo "  ─────────────────────────────────────────"
  echo "  סיכום [$label]:"
  echo "    ערוצי dsd-fme חיים (שיא):   $peak_dsd / $NCH"
  echo "    CPU ממוצע:                  ${avg_cpu}%   (שיא ${peak_cpu}% מתוך $((NPROC*100))%)"
  echo "    CPU לכל ערוץ (משוער):       ${per_ch}%"
  echo "    מרווח-CPU פנוי בשיא:        ${headroom}%"
  echo "    זיכרון (שיא RSS):           ${peak_rss}MB"
  echo "    אירועים פוענחו:             $total_ev   (מתוכם voice: $voice)"
  # שומרים למסקנה הכוללת
  echo "$label $peak_dsd $avg_cpu $peak_cpu $per_ch $total_ev $voice $peak_rss" >> "$OUT_DIR/summary.tsv"
}

# ── 5. הרצות ────────────────────────────────────────────────────────────────
run_pass "all-channels"
if [ "$FOLLOW" = "1" ]; then
  run_pass "follow-traffic" --follow-traffic
fi

# ── 6. Verdict ──────────────────────────────────────────────────────────────
echo; echo "════════════════════════════════════════════════════════════════"
echo "  VERDICT — האם ריבוי-ערוצים בר-קיימא על החומרה הזו?"
echo "════════════════════════════════════════════════════════════════"
if [ ! -s "$OUT_DIR/summary.tsv" ]; then
  echo "  אין נתונים — כל ההרצות קרסו. בדוק $OUT_DIR/backend_*.log"
else
  # שורת all-channels
  read -r _l pdsd acpu pcpu perch tev voi prss < <(grep '^all-channels' "$OUT_DIR/summary.tsv")
  echo "  ערוצים שהוזנו:      $NCH,   נועלו/רצו:  $pdsd"
  echo "  אירועים מפוענחים:   $tev  (voice: $voi)"
  echo "  CPU שיא:            ${pcpu}% / $((NPROC*100))%"
  echo
  if [ "${tev:-0}" -eq 0 ]; then
    echo "  ⚠ אפס אירועים — dsd-fme רץ אך לא נעל sync."
    echo "    סיבות אפשריות: תדרים לא-אמיתיים בתוכנית / gain / אין תעבורה כרגע /"
    echo "    ה-channelizer לא מספק אודיו תקין. זו תוצאה חשובה: הליבה עוד לא הוכחה."
  elif [ "${pcpu:-999}" -ge "$((NPROC*100 - 30))" ]; then
    echo "  🔴 CPU רווי (${pcpu}%). ה-Pi בקושי עומד ב-$pdsd ערוצים — 'המון תדרים'"
    echo "     בו-זמנית לא ריאלי כאן. המסקנה: --follow-traffic חובה, או פחות ערוצים."
    echo "     הרץ שוב עם FOLLOW=1 להשוואה."
  else
    echo "  🟢 פענוח מרובה-ערוצים עובד עם מרווח-CPU. $pdsd ערוצים ב-${pcpu}% CPU."
    echo "     המנוע הוכח על חומרה → Phase 0 עבר, אפשר להתקדם למיזוג (Phase 2)."
  fi
fi
echo
echo "  לוגים ונתונים גולמיים: $OUT_DIR"
echo "  (backend_*.log, events_*.jsonl, summary.tsv — צרף אותם כשתדווח לי תוצאות)"
echo "════════════════════════════════════════════════════════════════"
