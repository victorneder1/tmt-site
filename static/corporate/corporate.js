/* Insider Tracker — corporate.js */

// ── State ──────────────────────────────────────────────────────────────────
let cache = { buybacks_executed: [], insiders_buying: [], insiders_selling: [] };
let allCompanies = [];
let allSectors   = [];
let bbSortMode = "mcap";    // "volume" | "mcap"  — applies to both monthly and accumulated
let inSortMode     = "pct";  // "pct" | "volume"
let syncInterval = null;
let topSearchQuery = "";
let bbHistorySearchQuery = "";

const TOP_N = 10;

// ── Month names ─────────────────────────────────────────────────────────────
const MON     = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const MONFULL = ["January","February","March","April","May","June","July","August","September","October","November","December"];

// ── DOM refs ────────────────────────────────────────────────────────────────
const $sector  = () => document.getElementById("sectorFilter");
const $company = () => document.getElementById("companyFilter");
const $topSearch = () => document.getElementById("tickerSearch");
const $year    = () => document.getElementById("analyticsYearFilter");
const $month   = () => document.getElementById("analyticsMonthFilter");
const $bbFrom  = () => document.getElementById("buybacksRollingFrom");
const $bbTo    = () => document.getElementById("buybacksRollingTo");
const $inFrom  = () => document.getElementById("insidersRollingFrom");
const $inTo    = () => document.getElementById("insidersRollingTo");
const $bbHistSearch  = () => document.getElementById("bbHistoryTickerSearch");
const $bbHistSector  = () => document.getElementById("bbHistorySectorFilter");
const $bbHistCompany = () => document.getElementById("bbHistoryCompanyFilter");

// ── Format helpers ──────────────────────────────────────────────────────────
function fmt(n, d = 2) {
    return Number(n || 0).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
}

function fmtVol(vol) {
    const v = Number(vol || 0);
    if (v === 0) return "—";
    if (v < 1e6) return "R$ " + fmt(v / 1e3, 0) + "k";
    if (v < 1e9) return "R$ " + fmt(v / 1e6, 1) + "mn";
    return "R$ " + fmt(v / 1e9, 2) + "bn";
}

// Like fmtVol but without "R$ " prefix — used in table cells to reduce line-breaks
function fmtVolNoPrefix(vol) {
    const v = Number(vol || 0);
    if (v === 0) return "—";
    if (v < 1e6) return fmt(v / 1e3, 0) + "k";
    if (v < 1e9) return fmt(v / 1e6, 1) + "mn";
    return fmt(v / 1e9, 2) + "bn";
}

function fmtMcap(vol) {
    const v = Number(vol || 0);
    if (v === 0) return "—";
    if (v < 1e6) return fmt(v / 1e3, 1) + "k";
    if (v < 1e9) return fmt(v / 1e6, 1) + "mn";
    return fmt(v / 1e9, 1) + "bn";
}

function fmtPrice(price) {
    if (!price) return "—";
    return "R$ " + fmt(price, 2);
}

function ymIndex(ym) {
    if (!ym || ym.length < 7) return -1;
    return Number(ym.slice(0,4)) * 12 + Number(ym.slice(5,7)) - 1;
}

function ymLabel(ym) {
    if (!ym || ym.length < 7) return "—";
    return MON[Number(ym.slice(5,7)) - 1] + "/" + ym.slice(0,4);
}

function ymIndexToStr(idx) {
    const y = Math.floor(idx / 12), m = (idx % 12) + 1;
    return `${y}-${String(m).padStart(2,"0")}`;
}

function normalizeSearchQuery(value) {
    return String(value || "").trim().toLowerCase();
}

function getCompanyMeta(name) {
    return allCompanies.find(c => (c.name || "") === (name || "")) || null;
}

function matchesCompanySearch(name, ticker, query) {
    const q = normalizeSearchQuery(query);
    if (!q) return true;
    return String(name || "").toLowerCase().includes(q) || String(ticker || "").toLowerCase().includes(q);
}

function filterCompanyNamesBySearch(names, query) {
    return names.filter(name => {
        const meta = getCompanyMeta(name);
        return matchesCompanySearch(name, meta?.ticker || "", query);
    });
}

function resolveSearchCompany(names, query) {
    const matches = filterCompanyNamesBySearch(names, query);
    if (!matches.length) return "";

    const q = normalizeSearchQuery(query);
    const exact = matches.find(name => {
        const meta = getCompanyMeta(name);
        return name.toLowerCase() === q || String(meta?.ticker || "").toLowerCase() === q;
    });
    return exact || matches[0];
}

// ── Filters ─────────────────────────────────────────────────────────────────
function getFilters() {
    return {
        sector:  $sector()?.value  || "all",
        company: $company()?.value || "all",
        search: topSearchQuery,
    };
}

function getBbHistoryFilters() {
    return {
        sector:  $bbHistSector()?.value  || "all",
        company: $bbHistCompany()?.value || "all",
        search: bbHistorySearchQuery,
    };
}

function applyFilters(rows) {
    const { sector, company, search } = getFilters();
    return rows.filter(r => {
        if (sector  !== "all" && (r.sector  || "") !== sector)      return false;
        if (company !== "all" && (r.company_alias||"") !== company) return false;
        if (search && !matchesCompanySearch(r.company_alias || "", r.ticker || "", search)) return false;
        return true;
    });
}

function selectedPeriod() {
    const y = $year()?.value, m = $month()?.value;
    return (y && m) ? `${y}-${m}` : "";
}

function filterByPeriod(rows, ym) {
    if (!ym) return [];
    return rows.filter(r => (r.reference_year_month || r.delivery_year_month || "") === ym);
}

// ── Top-N / display row logic ───────────────────────────────────────────────
// No filter → Top 10 global; sector selected → all in sector; company → all for company
function getDisplayRows(rows, sortKey) {
    const { sector, company, search } = getFilters();
    const key = sortKey || "financial_volume";
    const isMcap = key === "pct_market_cap";
    const sorted = [...rows]
        .filter(r => isMcap
            ? (r.pct_market_cap != null && r.pct_market_cap > 0)
            : (Number(r.financial_volume ?? 0) > 0))
        .sort((a, b) => (b[key] ?? 0) - (a[key] ?? 0));
    if (sector === "all" && company === "all" && !search) return sorted.slice(0, TOP_N);
    return sorted;
}

// ── Buyback sort toggle ─────────────────────────────────────────────────────
function setBbSort(mode) {
    bbSortMode = mode;
    document.getElementById("sortByVolume")?.classList.toggle("active", mode === "volume");
    document.getElementById("sortByMcap")?.classList.toggle("active",   mode === "mcap");
    renderAll(cache);
}


function setInSort(mode) {
    inSortMode = mode;
    document.getElementById("inSortByVolume")?.classList.toggle("active", mode === "volume");
    document.getElementById("inSortByPct")?.classList.toggle("active", mode === "pct");
    renderAll(cache);
}

// ── Context badge ───────────────────────────────────────────────────────────
function updateContextBadge() {
    const badge = document.getElementById("contextBadge");
    if (!badge) return;
    const { sector, company } = getFilters();
    let text = "Top 10 — All Companies";
    if (company !== "all")      text = company;
    else if (sector !== "all")  text = sector + " — All Companies";
    badge.textContent = text;
}

// ── Rolling aggregation ─────────────────────────────────────────────────────
function buildRolling(rows, from, to, qKey) {
    const s = ymIndex(from), e = ymIndex(to);
    if (s < 0 || e < 0) return [];
    const grouped = new Map();
    rows.forEach(r => {
        const idx = ymIndex(r.reference_year_month || r.delivery_year_month || "");
        if (idx < s || idx > e) return;
        const qty = Number(r[qKey] || 0), vol = Number(r.financial_volume || 0);
        const price = Number(r.avg_price || 0);
        const key = r.company_alias || "";
        if (!grouped.has(key)) grouped.set(key, {
            company_alias: r.company_alias, ticker: r.ticker, market: r.market || "BZ",
            sector: r.sector, [qKey]: 0, financial_volume: 0, _wp: 0, trade_count: 0,
            market_cap: r.market_cap || null,
        });
        const g = grouped.get(key);
        g[qKey] += qty; g.financial_volume += vol; g._wp += price * qty;
        g.trade_count += Number(r.trade_count || 0);
        // Keep market_cap from any row that has it
        if (r.market_cap && !g.market_cap) g.market_cap = r.market_cap;
    });
    return [...grouped.values()].map(g => {
        const q = g[qKey] || 0;
        g.avg_price = q ? g._wp / q : 0;
        delete g._wp;
        // Compute % market cap on aggregated volume
        if (g.market_cap && g.market_cap > 0 && g.financial_volume > 0) {
            g.pct_market_cap = Math.round(g.financial_volume / g.market_cap * 100 * 1e4) / 1e4;
        } else {
            g.pct_market_cap = null;
        }
        return g;
    }).sort((a,b) => b.financial_volume - a.financial_volume);
}

function buildInsiderRolling(rows, from, to) {
    const s = ymIndex(from), e = ymIndex(to);
    if (s < 0 || e < 0) return [];
    const grouped = new Map();
    rows.forEach(r => {
        const idx = ymIndex(r.reference_year_month || r.delivery_year_month || "");
        if (idx < s || idx > e) return;
        const key = (r.company_alias||"") + "||" + (r.organ||"");
        if (!grouped.has(key)) grouped.set(key, {
            company_alias: r.company_alias, ticker: r.ticker, market: r.market||"BZ",
            sector: r.sector, organ: r.organ, shares: 0, financial_volume: 0, _wp: 0,
            trade_count: 0, sum_initial_quantity: 0,
        });
        const g = grouped.get(key);
        g.shares += Number(r.shares||0);
        g.financial_volume += Number(r.financial_volume||0);
        g._wp += Number(r.avg_price||0) * Number(r.shares||0);
        g.trade_count += Number(r.trade_count||0);
        if (Number(r.sum_initial_quantity||0) > g.sum_initial_quantity)
            g.sum_initial_quantity = Number(r.sum_initial_quantity||0);
    });
    return [...grouped.values()].map(g => {
        g.avg_price = g.shares ? g._wp / g.shares : 0;
        delete g._wp;
        // Only show % when initial >= shares (parsable, non-misleading)
        g.pct_shares_traded = (g.sum_initial_quantity > 0 && g.sum_initial_quantity >= g.shares)
            ? Math.round(g.shares / g.sum_initial_quantity * 100 * 1e4) / 1e4
            : null;
        return g;
    }).sort((a,b) => b.financial_volume - a.financial_volume);
}

// ── Group / pivot helpers ───────────────────────────────────────────────────
const ORGAN_MAP = {
    "controller":"Controller","executive directors":"Management","board of directors":"Board",
    "fiscal board":"Fiscal Board","advisory":"Advisory","director":"Board",
    "officer":"Management","10% owner":"Controller","other":"Other",
};
function mapOrgan(organ) {
    if (!organ) return "Other";
    const l = organ.toLowerCase();
    return ORGAN_MAP[l] || (l.includes("control")||l.includes("10%") ? "Controller"
        : l.includes("exec")||l.includes("officer")||l.includes("diret") ? "Management"
        : l.includes("board") ? "Board"
        : l.includes("fiscal") ? "Fiscal"
        : organ);
}

const MAIN_GROUPS = ["Controller", "Management", "Board"];

// ── Top N net balance filter ─────────────────────────────────────────────────
// Returns top n sellers (most negative) + top n buyers (most positive).
// Input must be sorted ascending (most negative first).
function topNetCompanies(companies, n = 5) {
    const sellers = companies.filter(c => c.net < 0).slice(0, n);
    const buyers  = companies.filter(c => c.net > 0).slice(-n);
    return [...sellers, ...buyers];
}

// ── Tab switching ───────────────────────────────────────────────────────────
function switchTab(tab) {
    document.querySelectorAll(".main-tab").forEach(t => t.classList.remove("active"));
    document.querySelectorAll(".tr-content").forEach(c => c.classList.remove("active"));
    document.getElementById("tab-" + tab).classList.add("active");
    document.getElementById("content-" + tab).classList.add("active");
    populatePeriodFilters(cache);
    renderAll(cache);
}

// ── Period filter population ────────────────────────────────────────────────
function allPeriods(analytics) {
    const activeTab = document.querySelector(".main-tab.active")?.id === "tab-buybacks" ? "buybacks" : "insiders";
    const rows = activeTab === "buybacks"
        ? (analytics.buybacks_executed || [])
        : [...(analytics.insiders_buying || []), ...(analytics.insiders_selling || [])];
    const set = new Set(
        rows.filter(r => Number(r.financial_volume||0) > 0 && r.reference_year_month)
            .map(r => r.reference_year_month)
    );
    return [...set].sort((a,b) => ymIndex(b) - ymIndex(a));
}

function populatePeriodFilters(analytics) {
    const periods = allPeriods(analytics);
    const prevYM = selectedPeriod();
    if (!periods.length) {
        $year().innerHTML = `<option value="">No data</option>`;
        $month().innerHTML = `<option value="">No data</option>`;
        return;
    }

    const preferred = periods.includes(prevYM) ? prevYM : periods[0];
    const years = [...new Set(periods.map(p => p.slice(0,4)))];
    $year().innerHTML = years.map(y => `<option value="${y}">${y}</option>`).join("");
    $year().value = preferred.slice(0,4);
    syncMonths(periods, preferred.slice(5,7));

    const sortedAsc = [...periods].sort((a,b) => ymIndex(a) - ymIndex(b));
    const opts = sortedAsc.map(p => `<option value="${p}">${ymLabel(p)}</option>`).join("");
    const latest = sortedAsc[sortedAsc.length - 1] || "";
    const latestIdx = ymIndex(latest);
    const defFrom = latestIdx >= 0 ? ymIndexToStr(latestIdx - 11) : "";

    [$bbFrom, $bbTo, $inFrom, $inTo].forEach(fn => {
        const el = fn(); if (!el) return;
        const prev = el.value;
        el.innerHTML = opts;
        el.value = sortedAsc.includes(prev) ? prev : "";
    });
    const bbFrom = $bbFrom(), bbTo = $bbTo(), inFrom = $inFrom(), inTo = $inTo();
    if (bbTo   && !bbTo.value)   bbTo.value   = latest;
    if (bbFrom && !bbFrom.value) bbFrom.value = sortedAsc.includes(defFrom) ? defFrom : sortedAsc[0] || "";
    if (inTo   && !inTo.value)   inTo.value   = latest;
    if (inFrom && !inFrom.value) inFrom.value = sortedAsc.includes(defFrom) ? defFrom : sortedAsc[0] || "";

    const histFrom = document.getElementById("bbHistoryFrom");
    const histTo   = document.getElementById("bbHistoryTo");
    if (histFrom) {
        const prevHF = histFrom.value;
        histFrom.innerHTML = opts;
        histFrom.value = sortedAsc.includes(prevHF) ? prevHF : (sortedAsc.includes(defFrom) ? defFrom : sortedAsc[0] || "");
    }
    if (histTo) {
        const prevHT = histTo.value;
        histTo.innerHTML = opts;
        histTo.value = sortedAsc.includes(prevHT) ? prevHT : latest;
    }

    const inHistFrom = document.getElementById("inHistoryFrom");
    const inHistTo   = document.getElementById("inHistoryTo");
    if (inHistFrom) {
        const prevIF = inHistFrom.value;
        inHistFrom.innerHTML = opts;
        inHistFrom.value = sortedAsc.includes(prevIF) ? prevIF : (sortedAsc.includes(defFrom) ? defFrom : sortedAsc[0] || "");
    }
    if (inHistTo) {
        const prevIT = inHistTo.value;
        inHistTo.innerHTML = opts;
        inHistTo.value = sortedAsc.includes(prevIT) ? prevIT : latest;
    }
}

function syncMonths(periods, prefMonth) {
    const year = $year()?.value;
    const months = [...new Set(periods.filter(p => p.slice(0,4) === year).map(p => p.slice(5,7)))]
        .sort((a,b) => Number(b) - Number(a));
    $month().innerHTML = months.map(m => `<option value="${m}">${MONFULL[Number(m)-1]||m}</option>`).join("");
    $month().value = months.includes(prefMonth) ? prefMonth : months[0] || "";
}

// ── Sector filter ───────────────────────────────────────────────────────────
function populateSectorFilter(sectors) {
    const sel = $sector();
    if (!sel) return;
    const prev = sel.value;
    sel.innerHTML = `<option value="all">All Sectors</option>` +
        sectors.map(s => `<option value="${s}">${s}</option>`).join("");
    if ([...sel.options].some(o => o.value === prev)) sel.value = prev;
}

// ── Company filter ──────────────────────────────────────────────────────────
function populateCompanyFilter(analytics) {
    const { sector } = getFilters();
    // Companies with actual analytics data
    const all = [...(analytics.buybacks_executed||[]), ...(analytics.insiders_buying||[]), ...(analytics.insiders_selling||[])]
        .filter(r => sector === "all" || (r.sector||"") === sector);
    const namesFromData = new Set(all.map(r => r.company_alias||"").filter(Boolean));
    // All configured companies (shown even with no trades)
    const namesFromConfig = allCompanies
        .filter(c => sector === "all" || (c.sector||"") === sector)
        .map(c => c.name||"").filter(Boolean);
    let names = [...new Set([...namesFromData, ...namesFromConfig])].sort();
    if (topSearchQuery) names = filterCompanyNamesBySearch(names, topSearchQuery);
    const prev = $company()?.value || "all";
    $company().innerHTML = `<option value="all">All Companies</option>` +
        names.map(n => `<option value="${n}">${n}</option>`).join("");
    $company().value = [...$company().options].some(o => o.value === prev) ? prev : "all";
}

function ensureBuybackHistoryFilters() {
    const periodControls = document.getElementById("bbHistoryFrom")?.closest(".tr-rolling-controls");
    if (!periodControls) return;

    const legacyBar = document.getElementById("bbHistoryFiltersBar");
    if (legacyBar) legacyBar.remove();
    if ($bbHistSearch() && $bbHistSector() && $bbHistCompany()) return;

    const marker = document.createElement("span");
    marker.id = "bbHistoryFiltersInline";
    marker.hidden = true;

    const label = document.createElement("span");
    label.className = "tr-filter-label";
    label.textContent = "Filters";

    const search = document.createElement("div");
    search.className = "search-box tr-history-search";
    search.innerHTML = `<input type="text" id="bbHistoryTickerSearch" placeholder="Search company or ticker..." oninput="onBbHistorySearchInput(event)">`;

    const sector = document.createElement("select");
    sector.id = "bbHistorySectorFilter";
    sector.className = "tr-sel";
    sector.setAttribute("onchange", "onBbHistorySectorChange()");
    sector.innerHTML = `<option value="all">All Sectors</option>`;

    const company = document.createElement("select");
    company.id = "bbHistoryCompanyFilter";
    company.className = "tr-sel";
    company.setAttribute("onchange", "onBbHistoryCompanyChange()");
    company.innerHTML = `<option value="all">All Companies</option>`;

    periodControls.insertBefore(marker, periodControls.firstChild);
    periodControls.insertBefore(label, marker.nextSibling);
    periodControls.insertBefore(sector, label.nextSibling);
    periodControls.insertBefore(company, sector.nextSibling);
    periodControls.insertBefore(search, company.nextSibling);

    const searchInput = search.querySelector("input");
    if (searchInput) searchInput.value = bbHistorySearchQuery;
}

function populateBuybackHistorySectorFilter(analytics) {
    ensureBuybackHistoryFilters();
    const sel = $bbHistSector();
    if (!sel) return;

    const prev = sel.value || "all";
    const rows = analytics.buybacks_executed || [];
    const sectorsFromData = rows.map(r => r.sector || "").filter(Boolean);
    const sectorsFromConfig = allCompanies.map(c => c.sector || "").filter(Boolean);
    const sectors = [...new Set([...(allSectors || []), ...sectorsFromData, ...sectorsFromConfig])].sort();

    sel.innerHTML = `<option value="all">All Sectors</option>` +
        sectors.map(s => `<option value="${s}">${s}</option>`).join("");
    sel.value = [...sel.options].some(o => o.value === prev) ? prev : "all";
}

function populateBuybackHistoryCompanyFilter(analytics) {
    ensureBuybackHistoryFilters();
    const sel = $bbHistCompany();
    if (!sel) return;

    const { sector } = getBbHistoryFilters();
    const rows = (analytics.buybacks_executed || [])
        .filter(r => sector === "all" || (r.sector || "") === sector);
    const namesFromData = new Set(rows.map(r => r.company_alias || "").filter(Boolean));
    const namesFromConfig = allCompanies
        .filter(c => sector === "all" || (c.sector || "") === sector)
        .map(c => c.name || "")
        .filter(Boolean);
    let names = [...new Set([...namesFromData, ...namesFromConfig])].sort();
    if (bbHistorySearchQuery) names = filterCompanyNamesBySearch(names, bbHistorySearchQuery);
    const prev = sel.value || "all";

    sel.innerHTML = `<option value="all">All Companies</option>` +
        names.map(n => `<option value="${n}">${n}</option>`).join("");
    let nextValue = [...sel.options].some(o => o.value === prev) ? prev : "all";
    if (bbHistorySearchQuery) nextValue = resolveSearchCompany(names, bbHistorySearchQuery) || "all";
    sel.value = [...sel.options].some(o => o.value === nextValue) ? nextValue : "all";
}

function updateBuybackHistoryFilterHighlights() {
    const s = $bbHistSector();
    const c = $bbHistCompany();
    if (s) s.classList.toggle("tr-sel-active", s.value !== "all");
    if (c) c.classList.toggle("tr-sel-active", c.value !== "all");
}


// ── Section title helper ────────────────────────────────────────────────────
function sectionTitleHTML(text) {
    return `<div class="tr-section-title">${text}</div>`;
}

// ── KPI card ────────────────────────────────────────────────────────────────
function kpi(label, value, sub, accent) {
    return `<div class="tr-kpi ${accent}"><div class="tr-kpi-label">${label}</div><div class="tr-kpi-value ${value.cls||""}">${value.text}</div><div class="tr-kpi-sub">${sub}</div></div>`;
}

// ── Horizontal bar chart ────────────────────────────────────────────────────
function barChart(rows, valueKey, labelFn, subFn, shBadge, shTitle, shSub, fmtFn) {
    const fmt_ = fmtFn || fmtVol;
    const active = rows.filter(r => Number(r[valueKey]||0) > 0);
    const max = active.length ? Math.max(...active.map(r => Number(r[valueKey]))) : 1;
    const bars = active.map(r => {
        const v = Number(r[valueKey]||0);
        const pct = Math.max(0.5, (v / max) * 100).toFixed(1);
        const isLarge = pct > 75;
        return `<div class="tr-bar-row">
            <div class="tr-bar-label"><span class="tr-bar-label-name">${labelFn(r)}</span></div>
            <div class="tr-bar-track">
                <div class="tr-bar-fill${isLarge ? " label-end" : ""}" style="width:${pct}%">${isLarge ? fmt_(v) : ""}</div>
                ${!isLarge ? `<span class="tr-bar-ext-val">${fmt_(v)}</span>` : ""}
            </div>
        </div>`;
    }).join("");

    const badge = shBadge ? `<span class="tr-sb blue">${shBadge}</span>` : "";
    return `<div class="tr-sc tr-sc-fill">
        <div class="tr-sh"><div class="tr-sh-left">
            <div class="tr-st"><span class="tr-dot blue"></span>${shTitle}</div>
            <div class="tr-ss">${shSub}</div>
        </div>${badge}</div>
        <div class="tr-bar-wrap">${bars || '<div class="tr-empty">No data</div>'}</div>
    </div>`;
}

// ── Buyback table (with Mkt Cap and % Mkt Cap columns) ──────────────────────
// sortMode: "mcap" | "volume" — controls which column is highlighted
function buybackTable(rows, shTitle, shSub, dotCls, badgeCls, showRefMonth, sortMode) {
    if (!rows.length) return `<div class="tr-sc"><div class="tr-sh"><div class="tr-st"><span class="tr-dot ${dotCls}"></span>${shTitle}</div></div><div class="tr-empty">No data for the selected period.</div></div>`;

    const badge = `<span class="tr-sb ${badgeCls}">${rows.length} compan${rows.length===1?"y":"ies"}</span>`;
    const activeSortMode = sortMode || bbSortMode;
    const isMcap = activeSortMode === "mcap" && rows.some(r => r.pct_market_cap != null && r.pct_market_cap > 0);
    const isVol  = activeSortMode === "volume" || !isMcap;
    const mcapThCls = isMcap ? ' class="r mcap-active"' : ' class="r"';
    const mcapTdCls = isMcap ? ' class="r mcap-active"' : ' class="r"';
    const volThCls  = isVol  ? ' class="r mcap-active"' : ' class="r"';
    const volTdCls  = isVol  ? ' class="r mcap-active"' : ' class="r"';

    const refMonthHeader = showRefMonth ? `<th class="r">Ref.</th>` : "";
    const thead = `<tr><th>Company</th><th>Ticker</th>${refMonthHeader}<th class="r">Shares</th><th${volThCls}>Amount</th><th class="r">Mkt Cap</th><th${mcapThCls}>% Mkt Cap</th><th class="r">Avg. Price</th><th class="r">Trades</th></tr>`;

    const tbody = rows.map(r => `<tr>
        <td><span class="tr-cn">${r.company_alias||"—"}</span></td>
        <td><span class="tr-tk">${r.ticker||"—"}</span></td>
        ${showRefMonth ? `<td class="r"><span class="tr-mn">${ymLabel(r.reference_year_month)}</span></td>` : ""}
        <td class="r"><span class="tr-mn">${fmt(r.shares_reacquired??0,0)}</span></td>
        <td${volTdCls}><span class="tr-mn lg${isVol ? " blue" : ""}">${fmtVol(r.financial_volume)}</span></td>
        <td class="r"><span class="tr-mn muted">${r.market_cap ? fmtMcap(r.market_cap) : "—"}</span></td>
        <td${mcapTdCls}><span class="tr-mn${r.pct_market_cap != null ? (isMcap ? " blue" : "") : " muted"}">${r.pct_market_cap != null ? fmt(r.pct_market_cap, 1) + "%" : "—"}</span></td>
        <td class="r"><span class="tr-mn">${fmtPrice(r.avg_price)}</span></td>
        <td class="r"><span class="tr-cnt">${fmt(r.trade_count,0)}</span></td>
    </tr>`).join("");

    return `<div class="tr-sc">
        <div class="tr-sh"><div class="tr-sh-left">
            <div class="tr-st"><span class="tr-dot ${dotCls}"></span>${shTitle}</div>
            <div class="tr-ss">Mkt Cap figures are calculated using the current market cap as the denominator.</div>
        </div>${badge}</div>
        <div class="tr-tbl-wrap"><table class="tr-tbl tr-tbl-sm"><thead>${thead}</thead><tbody>${tbody}</tbody></table></div>
    </div>`;
}

// ── Net balance chart ───────────────────────────────────────────────────────
function nbChart(companies, shTitle, shSub, badge, sortMode) {
    const isPctMode = sortMode === "pct";
    const abs = companies.map(c => isPctMode ? Math.abs(c.pct ?? 0) : Math.abs(c.net));
    const maxAbs = abs.length ? Math.max(...abs) : 1;

    // Buyers: largest to smallest (top → bottom), bars right
    const buyers  = companies.filter(c => c.net > 0)
        .sort((a, b) => (isPctMode ? (b.pct??0)-(a.pct??0) : b.net - a.net));
    // Sellers: smallest magnitude to largest (top → bottom), bars left
    const sellers = companies.filter(c => c.net < 0)
        .sort((a, b) => (isPctMode ? (b.pct??0)-(a.pct??0) : b.net - a.net));

    function makeRow(c) {
        const rawVal = isPctMode ? Math.abs(c.pct ?? 0) : Math.abs(c.net);
        const pct = Math.min(48, rawVal / maxAbs * 48).toFixed(1);
        const isSell = c.net < 0;
        const isLarge = pct > 15;
        const valFmt = isPctMode ? fmt(Math.abs(c.pct??0),1)+"%" : fmtVol(Math.abs(c.net));
        let barContent = "";
        let extValHTML = "";
        if (isLarge) {
            barContent = valFmt;
        } else {
            const posStyle = isSell
                ? `right:calc(50% + ${pct}% + 8px)`
                : `left:calc(50% + ${pct}% + 8px)`;
            extValHTML = `<span class="tr-nb-ext-val ${isSell ? "sell" : "buy"}" style="${posStyle}">${valFmt}</span>`;
        }
        const bar = isSell
            ? `<div class="tr-nb-bar sell" style="width:${pct}%">${barContent}</div>`
            : `<div class="tr-nb-bar buy"  style="width:${pct}%">${barContent}</div>`;
        return `<div class="tr-nb-row">
            <div class="tr-nb-label"><div class="tr-nb-label-name">${c.name}</div></div>
            <div class="tr-nb-chart"><div class="tr-nb-axis"></div>${bar}${extValHTML}</div>
        </div>`;
    }

    let content = "";
    if (buyers.length) {
        content += `<div class="tr-nb-section-label buy">Buyers</div>`;
        content += buyers.map(makeRow).join("");
    }
    if (sellers.length) {
        if (buyers.length) content += `<div class="tr-nb-section-divider"></div>`;
        content += `<div class="tr-nb-section-label sell">Sellers</div>`;
        content += sellers.map(makeRow).join("");
    }
    if (!content) content = '<div class="tr-empty">No data</div>';

    const badgeHtml = badge ? `<span class="tr-sb grey">${badge}</span>` : "";
    return `<div class="tr-sc tr-sc-fill">
        <div class="tr-sh"><div class="tr-sh-left">
            <div class="tr-st"><span class="tr-dot blue"></span>${shTitle}</div>
            <div class="tr-ss">${shSub}</div>
        </div>${badgeHtml}</div>
        <div class="tr-nb-wrap">
            <div class="tr-nb-header"><span>← Selling</span><span>Buying →</span></div>
            <div class="tr-nb-rows">${content}</div>
        </div>
    </div>`;
}

// ── Insiders table (monthly or accumulated) ─────────────────────────────────
// side: "buy" | "sell"
// period: label string (e.g. "Feb/2025") or period range for accumulated
// isAccum: true = accumulated table (use "shares" key, slightly different styling)
function insiderTable(rows, side, period, isAccum) {
    const isGreen = side === "buy";
    const dotCls  = isGreen ? "green" : "red";
    const title   = isGreen
        ? (isAccum ? "Accumulated Insider Buying" : "Insider Buying")
        : (isAccum ? "Accumulated Insider Selling" : "Insider Selling");
    const badgeCls= isGreen ? "green" : "red";

    const sharesKey = "shares";
    const active = rows.filter(r => Number(r.financial_volume||0) > 0);

    const isPctSort = inSortMode === "pct";
    const sign = isGreen ? "+" : "−";
    const { sector, company, search } = getFilters();
    const isFiltered = sector !== "all" || company !== "all" || !!search;

    let trs, badge;

    if (isPctSort) {
        // Flat list: each (company, organ) row ranked independently by % ownership.
        // Rows with null pct_shares_traded (e.g. accumulated data) are shown last.
        const sorted = [...active].sort((a, b) => {
            const ap = a.pct_shares_traded, bp = b.pct_shares_traded;
            if (ap == null && bp == null) return Number(b.financial_volume||0) - Number(a.financial_volume||0);
            if (ap == null) return 1;
            if (bp == null) return -1;
            return bp - ap;
        });
        const displayRows = isFiltered ? sorted : sorted.slice(0, TOP_N);
        badge = "";
        trs = displayRows.map(sr => `<tr>
            <td><span class="tr-cn">${sr.company_alias||"—"}</span></td>
            <td class="tr-organ-col"><span class="tr-rt">${mapOrgan(sr.organ||"")}</span></td>
            <td class="r"><span class="tr-mn">${fmt(sr[sharesKey]??0,0)}</span></td>
            <td class="r"><span class="tr-mn ${dotCls}">${sign}${fmtVolNoPrefix(sr.financial_volume)}</span></td>
            <td class="r mcap-active"><span class="tr-mn blue">${sr.pct_shares_traded != null ? fmt(sr.pct_shares_traded,1)+"%" : "—"}</span></td>
            <td class="r"><span class="tr-mn">${fmtPrice(sr.avg_price)}</span></td>
        </tr>`).join("");
    } else {
        // Volume mode: group by company with rowspan, sort by aggregate volume
        const byComp = new Map();
        active.forEach(r => {
            const k = r.company_alias||"";
            if (!byComp.has(k)) byComp.set(k, { rows: [], vol: 0 });
            const c = byComp.get(k);
            c.rows.push(r);
            c.vol += Number(r.financial_volume||0);
        });
        const allComps = [...byComp.values()].sort((a, b) => b.vol - a.vol);
        const comps = isFiltered ? allComps : allComps.slice(0, TOP_N);
        badge = "";
        trs = comps.flatMap((comp, compIdx) => {
            return comp.rows.map((sr, rowIdx) => {
                const isFirstRow = rowIdx === 0;
                const compCell = isFirstRow
                    ? `<td rowspan="${comp.rows.length}" class="tr-comp-cell"><span class="tr-cn">${sr.company_alias||"—"}</span></td>`
                    : "";
                // Amount cell: merged across all rows for the same company via rowspan
                const volCell = isFirstRow
                    ? `<td rowspan="${comp.rows.length}" class="r mcap-active"><span class="tr-mn ${dotCls} blue">${sign}${fmtVolNoPrefix(comp.vol)}</span></td>`
                    : "";
                // Separator border on EVERY company boundary (compIdx > 0), regardless of row count
                const trCls = !isFirstRow ? ' class="tr-comp-continuation"' : (compIdx > 0 ? ' class="tr-comp-first"' : "");
                return `<tr${trCls}>
                    ${compCell}
                    <td class="tr-organ-col"><span class="tr-rt">${mapOrgan(sr.organ||"")}</span></td>
                    <td class="r"><span class="tr-mn">${fmt(sr[sharesKey]??0,0)}</span></td>
                    ${volCell}
                    <td class="r"><span class="tr-mn${sr.pct_shares_traded != null ? "" : " muted"}">${sr.pct_shares_traded != null ? fmt(sr.pct_shares_traded,1)+"%" : "—"}</span></td>
                    <td class="r"><span class="tr-mn">${fmtPrice(sr.avg_price)}</span></td>
                </tr>`;
            });
        }).join("");
    }

    if (!trs) return `<div class="tr-sc"><div class="tr-sh"><div class="tr-st"><span class="tr-dot ${dotCls}"></span>${title}</div></div><div class="tr-empty">No ${title.toLowerCase()} for the selected period.</div></div>`;

    const thead = isPctSort
        ? `<tr><th>Company</th><th class="tr-organ-col">Group</th><th class="r">Shares</th><th class="r">Amount (R$)</th><th class="r mcap-active">% Ownership</th><th class="r">Avg. Price</th></tr>`
        : `<tr><th>Company</th><th class="tr-organ-col">Group</th><th class="r">Shares</th><th class="r mcap-active">Amount (R$)</th><th class="r">% Ownership</th><th class="r">Avg. Price</th></tr>`;

    return `<div class="tr-sc">
        <div class="tr-sh"><div class="tr-sh-left">
            <div class="tr-st"><span class="tr-dot ${dotCls}"></span>${title}</div>
            <div class="tr-ss">${isAccum ? period : "Ref. " + ymLabel(period)}</div>
        </div>${badge}</div>
        <div class="tr-tbl-wrap"><table class="tr-tbl tr-tbl-sm"><thead>${thead}</thead><tbody>${trs}</tbody></table></div>
    </div>`;
}

// ── Insider pivot table ─────────────────────────────────────────────────────
function insiderPivot(buyRows, sellRows, from, to) {
    const s = ymIndex(from), e = ymIndex(to);
    if (s < 0 || e < 0) return `<div class="tr-empty">Select a period to view Insider Activity.</div>`;

    const data = new Map();
    function addRows(rows, side) {
        rows.forEach(r => {
            const period = r.reference_year_month || r.delivery_year_month || "";
            const idx = ymIndex(period);
            if (idx < s || idx > e) return;
            const c = r.company_alias||"";
            const g = mapOrgan(r.organ||"");
            const v = Number(r.financial_volume||0);
            if (!data.has(c)) data.set(c, { ticker: r.ticker||"", groups: new Map(), othBuy: 0, othSell: 0 });
            const cd = data.get(c);
            if (MAIN_GROUPS.includes(g)) {
                if (!cd.groups.has(g)) cd.groups.set(g, { buy: 0, sell: 0 });
                cd.groups.get(g)[side] += v;
            } else {
                if (side === "buy")  cd.othBuy  += v;
                else                 cd.othSell += v;
            }
        });
    }
    addRows(buyRows, "buy");
    addRows(sellRows, "sell");

    const activeMain = MAIN_GROUPS.filter(g => {
        for (const d of data.values()) if (d.groups.has(g)) return true;
        return false;
    });
    const hasOthers = [...data.values()].some(d => d.othBuy > 0 || d.othSell > 0);

    const allCompanies = [...data.entries()].map(([name, d]) => {
        let tb = 0, ts = 0;
        const gd = {};
        activeMain.forEach(g => {
            const v = d.groups.get(g) || { buy: 0, sell: 0 };
            gd[g] = { buy: v.buy, sell: v.sell, bal: v.buy - v.sell };
            tb += v.buy; ts += v.sell;
        });
        const othBal = d.othBuy - d.othSell;
        tb += d.othBuy; ts += d.othSell;
        return { name, ticker: d.ticker, tb, ts, bal: tb-ts, gd, othBal };
    }).filter(c => c.tb > 0 || c.ts > 0).sort((a,b) => a.bal - b.bal);

    // Show top 5 sellers (most negative) + top 5 buyers (most positive)
    const TOP_PIVOT = 5;
    const sellers = allCompanies.filter(c => c.bal < 0).slice(0, TOP_PIVOT);
    const buyers  = allCompanies.filter(c => c.bal > 0).slice(-TOP_PIVOT);
    const companies = [...sellers, ...buyers];
    const pivotTruncated = companies.length < allCompanies.length;

    function pv(val, bold = false) {
        if (!val) return `<span class="tr-mn muted">—</span>`;
        const abs = Math.abs(val);
        let str;
        if (abs >= 1e9)      str = fmt(abs / 1e9, 1) + "bn";
        else if (abs >= 1e6) str = fmt(abs / 1e6, 0) + "mn";
        else                 str = fmt(abs / 1e6, 1) + "mn";
        const cls  = val > 0 ? "green" : "red";
        const sign = val > 0 ? "+" : "−";
        const style = bold ? ` style="font-weight:700"` : "";
        return `<span class="tr-mn ${cls}"${style}>${sign}${str}</span>`;
    }

    const colCount = 2 + activeMain.length * 3 + (hasOthers ? 1 : 0);
    const dividerRow = `<tr class="pivot-divider"><td colspan="${colCount}"></td></tr>`;
    const rows = companies.map((c, i) => {
        let cells = `<td><span class="tr-cn">${c.name}</span></td>`;
        cells += `<td class="r">${pv(c.bal, true)}</td>`;
        activeMain.forEach(g => {
            const gd = c.gd[g]||{buy:0,sell:0,bal:0};
            cells += `<td class="r" style="border-left:1px solid #edf1f5">${pv(gd.buy)}</td>`;
            cells += `<td class="r">${pv(-gd.sell)}</td>`;
            cells += `<td class="r">${pv(gd.bal)}</td>`;
        });
        if (hasOthers) cells += `<td class="r" style="border-left:1px solid #edf1f5">${pv(c.othBal)}</td>`;
        const row = `<tr>${cells}</tr>`;
        // Insert divider between last seller and first buyer
        return (i === sellers.length - 1 && buyers.length > 0) ? row + dividerRow : row;
    }).join("");

    let hdr1 = `<th rowspan="2" style="text-align:left;min-width:130px">Company</th><th rowspan="2" class="r">Net Balance</th>`;
    activeMain.forEach(g => { hdr1 += `<th colspan="3" style="text-align:center;border-left:2px solid rgba(255,255,255,0.2)">${g}</th>`; });
    if (hasOthers) hdr1 += `<th colspan="1" style="text-align:center;border-left:2px solid rgba(255,255,255,0.2)">Others</th>`;

    let hdr2 = "";
    activeMain.forEach(() => { hdr2 += `<th class="r" style="border-left:1px solid rgba(255,255,255,0.15)">Buy</th><th class="r">Sell</th><th class="r">Balance</th>`; });
    if (hasOthers) hdr2 += `<th class="r" style="border-left:1px solid rgba(255,255,255,0.15)">Balance</th>`;

    const pivotNote = pivotTruncated
        ? `<div style="font-size:11px;color:var(--tr-faint);padding:6px 18px 2px">Showing top ${sellers.length} sellers &amp; top ${buyers.length} buyers by net balance</div>`
        : "";
    return `${pivotNote}<div class="tr-tbl-wrap">
        <table class="tr-tbl tr-pivot-tbl">
            <thead><tr>${hdr1}</tr><tr class="pivot-sub">${hdr2}</tr></thead>
            <tbody>${rows}</tbody>
        </table>
    </div>`;
}

// ── Per-company buyback history chart ──────────────────────────────────────
function buybackHistoryChart(allRows, company, from, to) {
    const s = ymIndex(from), e = ymIndex(to);

    const monthly = new Map();
    allRows.forEach(r => {
        if ((r.company_alias||"") !== company) return;
        const ym = r.reference_year_month || r.delivery_year_month || "";
        const idx = ymIndex(ym);
        if (s >= 0 && idx < s) return;
        if (e >= 0 && idx > e) return;
        if (!monthly.has(ym)) monthly.set(ym, 0);
        monthly.set(ym, monthly.get(ym) + Number(r.financial_volume||0));
    });

    if (s < 0 || e < 0) {
        return `<div class="tr-sc"><div class="tr-sh"><div class="tr-st"><span class="tr-dot blue"></span>Repurchase History — ${company}</div></div><div class="tr-empty">Select a valid period.</div></div>`;
    }

    const allMonths = [];
    for (let i = s; i <= e; i++) {
        const ym = ymIndexToStr(i);
        allMonths.push([ym, monthly.get(ym) || 0]);
    }

    const maxVol = Math.max(...allMonths.map(([,v]) => v), 0);
    const activeCount = allMonths.filter(([,v]) => v > 0).length;

    if (activeCount === 0) {
        return `<div class="tr-sc"><div class="tr-sh"><div class="tr-st"><span class="tr-dot blue"></span>Repurchase History — ${company}</div></div><div class="tr-empty">No repurchase activity recorded for the selected period.</div></div>`;
    }

    const cols = allMonths.map(([ym, vol]) => {
        const heightPx = vol > 0 && maxVol > 0 ? Math.max(3, Math.round((vol / maxVol) * 125)) : 0;
        return `<div class="tr-timeline-col">
            <div class="tr-timeline-bar-val">${vol > 0 ? fmtVol(vol) : ""}</div>
            <div class="tr-timeline-bar${vol === 0 ? " empty" : ""}" style="height:${heightPx}px"></div>
            <div class="tr-timeline-lbl">${ymLabel(ym)}</div>
        </div>`;
    }).join("");

    const periodLbl = (from && to) ? `${ymLabel(from)} → ${ymLabel(to)}` : "All periods";
    return `<div class="tr-sc">
        <div class="tr-sh"><div class="tr-sh-left">
            <div class="tr-st"><span class="tr-dot blue"></span>Repurchase History — ${company}</div>
            <div class="tr-ss">${periodLbl} · ${activeCount} month${activeCount===1?"":"s"} with activity · Amount (R$)</div>
        </div></div>
        <div class="tr-timeline-wrap"><div class="tr-timeline-bars">${cols}</div></div>
    </div>`;
}

// ── Insider net activity history chart (per company) ────────────────────────
function insiderHistoryChart(buyAll, sellAll, company, from, to) {
    const s = ymIndex(from), e = ymIndex(to);
    if (s < 0 || e < 0) {
        return `<div class="tr-sc"><div class="tr-sh"><div class="tr-st"><span class="tr-dot green"></span>Insider Activity History — ${company||"(select company)"}</div></div><div class="tr-empty">Select a valid period.</div></div>`;
    }
    if (!company) {
        return `<div class="tr-sc"><div class="tr-empty">Select a company above to view its insider activity history.</div></div>`;
    }

    const netMonthly = new Map();
    for (let i = s; i <= e; i++) netMonthly.set(ymIndexToStr(i), 0);

    function addRows(rows, sign) {
        rows.forEach(r => {
            if ((r.company_alias||"") !== company) return;
            const ym = r.reference_year_month || r.delivery_year_month || "";
            const idx = ymIndex(ym);
            if (idx < s || idx > e) return;
            netMonthly.set(ym, (netMonthly.get(ym)||0) + sign * Number(r.financial_volume||0));
        });
    }
    addRows(buyAll,  +1);
    addRows(sellAll, -1);

    const allMonths = [...netMonthly.entries()];
    const activeCount = allMonths.filter(([,v]) => v !== 0).length;

    if (activeCount === 0) {
        return `<div class="tr-sc"><div class="tr-sh"><div class="tr-st"><span class="tr-dot green"></span>Insider Activity History — ${company}</div></div><div class="tr-empty">No insider activity data for this company in the selected period.</div></div>`;
    }

    // Same approach as buybackHistoryChart: bars from baseline, green = net buy, red = net sell
    const maxVal = Math.max(...allMonths.map(([,v]) => Math.abs(v)), 1);
    const BAR_MAX = 125;

    const cols = allMonths.map(([ym, net]) => {
        const absVal = Math.abs(net);
        const heightPx = absVal > 0 ? Math.max(3, Math.round((absVal / maxVal) * BAR_MAX)) : 0;
        const cls = net > 0 ? "buy" : net < 0 ? "sell" : "empty";
        const valLbl = net !== 0 ? fmtVolNoPrefix(absVal) : "";
        return `<div class="tr-timeline-col">
            <div class="tr-timeline-bar-val ${cls}">${valLbl}</div>
            <div class="tr-timeline-bar ${cls}" style="height:${heightPx}px"></div>
            <div class="tr-timeline-lbl">${ymLabel(ym)}</div>
        </div>`;
    }).join("");

    const periodLbl = (from && to) ? `${ymLabel(from)} → ${ymLabel(to)}` : "All periods";
    return `<div class="tr-sc">
        <div class="tr-sh"><div class="tr-sh-left">
            <div class="tr-st"><span class="tr-dot green"></span>Insider Activity History — ${company}</div>
            <div class="tr-ss">${periodLbl} · ${activeCount} month${activeCount===1?"":"s"} with activity · Amount (R$)</div>
        </div>
        <div class="tr-timeline-legend">
            <span class="tr-tl-item buy"><span class="tr-tl-dot"></span>Insider Buying</span>
            <span class="tr-tl-item sell"><span class="tr-tl-dot"></span>Insider Selling</span>
        </div></div>
        <div class="tr-timeline-wrap"><div class="tr-timeline-bars">${cols}</div></div>
    </div>`;
}


// ── Main render ─────────────────────────────────────────────────────────────
function renderAll(analytics) {
    const ym    = selectedPeriod();
    const ymLbl = ym ? ymLabel(ym) : "—";

    const bbAll    = applyFilters(analytics.buybacks_executed || []);
    const inBuyAll = applyFilters(analytics.insiders_buying   || []);
    const inSellAll= applyFilters(analytics.insiders_selling  || []);

    // Sort key depends on mode; fall back to volume if no mcap data available
    const hasMcapData = (analytics.buybacks_executed || []).some(r => r.pct_market_cap != null && r.pct_market_cap > 0);
    const effectiveBbSort = (bbSortMode === "mcap" && !hasMcapData) ? "volume" : bbSortMode;
    const bbSortKey       = effectiveBbSort === "mcap" ? "pct_market_cap" : "financial_volume";
    const bbRollSortKey   = bbSortKey;

    const bbMonRaw    = filterByPeriod(bbAll, ym);
    const inBuyMon    = filterByPeriod(inBuyAll, ym).sort((a,b) => b.financial_volume - a.financial_volume);
    const inSellMon   = filterByPeriod(inSellAll, ym).sort((a,b) => b.financial_volume - a.financial_volume);

    const bbMon = getDisplayRows(bbMonRaw, bbSortKey);

    const bbFrom = $bbFrom()?.value || "", bbTo = $bbTo()?.value || "";
    const inFrom = $inFrom()?.value || "", inTo = $inTo()?.value || "";

    const bbRollRaw = buildRolling(bbAll,    bbFrom, bbTo, "shares_reacquired");
    const inBuyRoll = buildInsiderRolling(inBuyAll, inFrom, inTo);
    const inSellRoll= buildInsiderRolling(inSellAll,inFrom, inTo);
    const bbRoll    = getDisplayRows(bbRollRaw, bbRollSortKey);

    const bbRollLbl = (bbFrom && bbTo) ? `${ymLabel(bbFrom)} → ${ymLabel(bbTo)}` : "Accumulated";
    const inRollLbl = (inFrom && inTo) ? `${ymLabel(inFrom)} → ${ymLabel(inTo)}` : "Accumulated";

    // Update context badge and filter count
    updateContextBadge();
    const { sector, company } = getFilters();
    const parts = [];
    if (topSearchQuery) parts.push(`Search: ${topSearchQuery.toUpperCase()}`);
    if (sector  !== "all") parts.push(sector);
    if (company !== "all") parts.push(company);
    if (ym) parts.push(ymLbl);
    document.getElementById("filterCount").textContent =
        (allCompanies.length ? allCompanies.length + " companies · " : "") +
        (parts.length ? parts.join(" · ") : "No filters");

    // ── BUYBACKS ────────────────────────────────────────────────────────────
    const bbFmtFn     = effectiveBbSort     === "mcap" ? (v => fmt(v, 1) + "%") : null;
    const bbRollFmtFn = effectiveBbSort === "mcap" ? (v => fmt(v, 1) + "%") : null;

    set("bbMonthlyPeriodLabel", sectionTitleHTML(`Reference Month — ${ymLbl}`));
    set("bbRollingPeriodLabel", sectionTitleHTML("Accumulated Repurchases"));

    // History chart — section-specific company filter
    set("bbHistoryPeriodLabel", sectionTitleHTML("Repurchase History by Company"));
    populateBuybackHistorySectorFilter(analytics);
    populateBuybackHistoryCompanyFilter(analytics);
    updateBuybackHistoryFilterHighlights();
    const histCompany = getBbHistoryFilters().company !== "all" ? getBbHistoryFilters().company : "";
    const histFrom    = document.getElementById("bbHistoryFrom")?.value || bbFrom;
    const histTo      = document.getElementById("bbHistoryTo")?.value   || bbTo;
    const histEmpty = bbHistorySearchQuery
        ? "No company matches the current search."
        : "Select a company in the history filters above to view its repurchase history.";
    set("bbHistoryBlock", histCompany
        ? buybackHistoryChart(analytics.buybacks_executed || [], histCompany, histFrom, histTo)
        : `<div class="tr-sc"><div class="tr-empty">${histEmpty}</div></div>`);

    // ── BUYBACKS: pass per-table sortMode ───────────────────────────────────
    set("bbMonthlyBlock", `
        ${buybackTable(bbMon, "Monthly Repurchases", `Ref. ${ymLbl}`, "blue", "blue", false, effectiveBbSort)}
        ${barChart(bbMon, bbSortKey, r=>r.company_alias||"—", r=>`${r.ticker||""} · ${fmt(r.shares_reacquired??0,0)} shares`, null, `${effectiveBbSort === "mcap" ? "% Mkt Cap" : "Amount"} by Company — ${ymLbl}`, "Monthly repurchase reference", bbFmtFn)}
    `);

    set("bbRollingBlock", `
        ${buybackTable(bbRoll, "Accumulated Repurchases", bbRollLbl, "grey", "grey", false, effectiveBbSort)}
        ${barChart(bbRoll, bbRollSortKey, r=>r.company_alias||"—", r=>`${r.ticker||""} · ${fmt(r.shares_reacquired??0,0)} shares`, null, `${effectiveBbSort === "mcap" ? "% Mkt Cap" : "Amount"} by Company — ${bbRollLbl}`, "Ranking of accumulated repurchases", bbRollFmtFn)}
    `);

    // ── INSIDERS ────────────────────────────────────────────────────────────
    set("inMonthlyPeriodLabel", sectionTitleHTML("Insider Activity in the Month"));

    // Net balance: compute net = buys - sells per company, then rank by net.
    // Universe = top N companies from each table (buy + sell), unioned.
    // A company that both bought and sold appears once with its true net position.
    function buildNetChart(buyRows, sellRows, n) {
        const { sector, company, search } = getFilters();
        const isFiltered = sector !== "all" || company !== "all" || !!search;

        // Determine which companies are "visible" in each table (mirrors insiderTable logic).
        // Uses TOP_N (full table size) so the universe is large enough to find n net buyers/sellers.
        function visibleNames(rows) {
            const active = rows.filter(r => Number(r.financial_volume||0) > 0);
            if (inSortMode === "pct") {
                // Sort by pct descending; rows without pct fall to end sorted by volume
                const sorted = [...active].sort((a, b) => {
                    const ap = a.pct_shares_traded, bp = b.pct_shares_traded;
                    if (ap == null && bp == null) return Number(b.financial_volume||0) - Number(a.financial_volume||0);
                    if (ap == null) return 1;
                    if (bp == null) return -1;
                    return bp - ap;
                });
                const top = isFiltered ? sorted : sorted.slice(0, TOP_N);
                return new Set(top.map(r => r.company_alias||""));
            }
            const byComp = new Map();
            active.forEach(r => byComp.set(r.company_alias||"", (byComp.get(r.company_alias||"")||0) + Number(r.financial_volume||0)));
            const sorted = [...byComp.entries()].sort((a,b) => b[1]-a[1]);
            const top = isFiltered ? sorted : sorted.slice(0, TOP_N);
            return new Set(top.map(([k]) => k));
        }

        const universe = new Set([...visibleNames(buyRows), ...visibleNames(sellRows)]);

        // Accumulate net per company across both buy and sell rows
        const map = new Map();
        buyRows.filter(r => universe.has(r.company_alias||"")).forEach(r => {
            const k = r.company_alias||"";
            if (!map.has(k)) map.set(k, {name:k, ticker:r.ticker||"", net:0, netShares:0, maxIq:0});
            const e = map.get(k);
            e.net       += Number(r.financial_volume||0);
            e.netShares += Number(r.shares||0);
            e.maxIq      = Math.max(e.maxIq, Number(r.sum_initial_quantity||0));
        });
        sellRows.filter(r => universe.has(r.company_alias||"")).forEach(r => {
            const k = r.company_alias||"";
            if (!map.has(k)) map.set(k, {name:k, ticker:r.ticker||"", net:0, netShares:0, maxIq:0});
            const e = map.get(k);
            e.net       -= Number(r.financial_volume||0);
            e.netShares -= Number(r.shares||0);
            e.maxIq      = Math.max(e.maxIq, Number(r.sum_initial_quantity||0));
        });

        // Derive pct from net shares
        map.forEach(e => {
            const abs = Math.abs(e.netShares);
            e.pct = (e.maxIq > 0 && e.maxIq >= abs) ? e.netShares / e.maxIq * 100 : null;
        });

        const sortKey = inSortMode === "pct"
            ? (a, b) => (b.pct??0) - (a.pct??0)
            : (a, b) => b.net - a.net;

        const all = [...map.values()];
        const buyers  = all.filter(e => e.net > 0).sort(sortKey).slice(0, n);
        const sellers = all.filter(e => e.net < 0).sort((a,b) => -sortKey(a,b)).slice(0, n);
        return [...buyers.map(e => ({...e})), ...sellers.map(e => ({...e}))];
    }

    const netMonTop  = buildNetChart(inBuyMon,  inSellMon,  5);
    const netRollTop = buildNetChart(inBuyRoll, inSellRoll, 5);

    set("inMonthlyBlock", `
        ${insiderTable(inBuyMon, "buy", ym)}
        ${insiderTable(inSellMon, "sell", ym)}
        ${nbChart(netMonTop, `Net Balance — ${ymLbl}`, `The net balance reflects the net value of purchases minus sales across all company insiders for the period.`, undefined, inSortMode)}
    `);

    set("inRollingPeriodLabel", sectionTitleHTML("Accumulated Insider Activity"));

    set("inAccumBlock", `
        ${insiderTable(inBuyRoll, "buy", inRollLbl, true)}
        ${insiderTable(inSellRoll, "sell", inRollLbl, true)}
        ${nbChart(netRollTop, `Net Balance — ${inRollLbl}`, `The net balance reflects the net value of purchases minus sales across all company insiders for the period.`, undefined, inSortMode)}
    `);

    // Insider history chart — company comes from the top-level filter
    set("inHistoryPeriodLabel", sectionTitleHTML("Insider Activity History by Company"));
    const inHistCompany = company !== "all"
        ? company
        : (topSearchQuery ? resolveSearchCompany(allCompanies.map(c => c.name || "").filter(Boolean), topSearchQuery) : "");
    const inHistFrom = document.getElementById("inHistoryFrom")?.value || inFrom;
    const inHistTo   = document.getElementById("inHistoryTo")?.value  || inTo;
    set("inHistoryBlock", !inHistCompany && topSearchQuery
        ? `<div class="tr-sc"><div class="tr-empty">No company matches the current search.</div></div>`
        : insiderHistoryChart(
            analytics.insiders_buying  || [],
            analytics.insiders_selling || [],
            inHistCompany, inHistFrom, inHistTo
        ));

    // inRollingBlock no longer used for pivot — clear it
    set("inRollingBlock", "");
}

function pivotCard(buyAll, sellAll, from, to, lbl) {
    return `<div class="tr-sc">
        <div class="tr-sh"><div class="tr-sh-left">
            <div class="tr-st"><span class="tr-dot grey"></span>Insider Activity — Accumulated</div>
            <div class="tr-ss">${lbl} · All categories</div>
        </div><span class="tr-sb grey">selected period</span></div>
        ${insiderPivot(buyAll, sellAll, from, to)}
    </div>`;
}

function set(id, html) {
    const el = document.getElementById(id);
    if (el) el.innerHTML = html;
}

// ── Event handlers ──────────────────────────────────────────────────────────
function updateFilterHighlights() {
    const s = $sector();
    const c = $company();
    const hasSearch = !!normalizeSearchQuery(topSearchQuery);
    if (s) s.classList.toggle("tr-sel-active", s.value !== "all");
    if (c) c.classList.toggle("tr-sel-active", c.value !== "all");
    // Show reset button when any top-level filter is active
    const btn = document.getElementById("resetFiltersBtn");
    if (btn) btn.style.display = (hasSearch || (s && s.value !== "all") || (c && c.value !== "all")) ? "" : "none";
}

function clearTopSearch() {
    topSearchQuery = "";
    const input = $topSearch();
    if (input) input.value = "";
}

function clearBbHistorySearch() {
    bbHistorySearchQuery = "";
    const input = $bbHistSearch();
    if (input) input.value = "";
}

function onTickerSearchInput(event) {
    topSearchQuery = event.target.value || "";
    const s = $sector();
    const c = $company();
    if (s) s.value = "all";
    if (c) c.value = "all";
    populateCompanyFilter(cache);
    updateFilterHighlights();
    renderAll(cache);
}

function onBbHistorySearchInput(event) {
    bbHistorySearchQuery = event.target.value || "";
    const s = $bbHistSector();
    const c = $bbHistCompany();
    if (s) s.value = "all";
    if (c) c.value = "all";
    populateBuybackHistoryCompanyFilter(cache);
    updateBuybackHistoryFilterHighlights();
    renderAll(cache);
}

function resetFilters() {
    const s = $sector();
    const c = $company();
    const hs = $bbHistSector();
    const hc = $bbHistCompany();
    clearTopSearch();
    clearBbHistorySearch();
    if (s) s.value = "all";
    if (c) { c.innerHTML = "<option value=\"all\">All Companies</option>"; c.value = "all"; }
    if (hs) hs.value = "all";
    if (hc) hc.value = "all";
    // Re-populate dropdowns for the full universe
    populateCompanyFilter(cache);
    populateBuybackHistoryCompanyFilter(cache);
    updateFilterHighlights();
    updateBuybackHistoryFilterHighlights();
    renderAll(cache);
}
function onFilterChange() {
    clearTopSearch();
    populateCompanyFilter(cache);
    updateFilterHighlights();
    renderAll(cache);
}
function onSectorChange() {
    clearTopSearch();
    updateFilterHighlights();
    populateCompanyFilter(cache);
    renderAll(cache);
}
function onBbHistoryCompanyChange() {
    clearBbHistorySearch();
    updateBuybackHistoryFilterHighlights();
    renderAll(cache);
}
function onBbHistorySectorChange() {
    clearBbHistorySearch();
    populateBuybackHistoryCompanyFilter(cache);
    updateBuybackHistoryFilterHighlights();
    renderAll(cache);
}
function onYearChange() {
    const periods = allPeriods(cache);
    syncMonths(periods, "");
    renderAll(cache);
}

// ── Data loading with first-run detection ───────────────────────────────────
async function loadStatus() {
    try {
        const data = await fetch("/tracker/api/status").then(r => r.json());
        const hasData = (
            (data.analytics?.buybacks_executed?.length > 0) ||
            (data.analytics?.insiders_buying?.length   > 0) ||
            (data.analytics?.insiders_selling?.length  > 0)
        );
        const isSyncing = !data.last_success_at && !hasData;

        // Show/hide first-run banner
        const banner = document.getElementById("syncBanner");
        if (banner) banner.style.display = isSyncing ? "" : "none";

        if (isSyncing) {
            // Poll aggressively every 5s until data arrives
            if (!syncInterval) syncInterval = setInterval(loadStatus, 5000);
            return;
        }

        // Data available — switch to 30s polling
        if (syncInterval) { clearInterval(syncInterval); syncInterval = null; }

        cache = data.analytics || cache;
        allCompanies = data.companies || [];
        allSectors   = data.sectors   || [];

        populateSectorFilter(allSectors);
        populateCompanyFilter(cache);
        populatePeriodFilters(cache);
        updateFilterHighlights();
        renderAll(cache);

    } catch(e) {
        // silent fail
    }
}

loadStatus();
setInterval(loadStatus, 30000);
