/* Buyback & Insider Tracker — corporate.js */

// ── State ──────────────────────────────────────────────────────────────────
let cache = { buybacks_executed: [], insiders_buying: [], insiders_selling: [] };
let allCompanies = [];

// ── Month names ────────────────────────────────────────────────────────────
const MON     = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const MONFULL = ["January","February","March","April","May","June","July","August","September","October","November","December"];

// ── DOM refs ───────────────────────────────────────────────────────────────
const $sector  = () => document.getElementById("sectorFilter");
const $company = () => document.getElementById("companyFilter");
const $year    = () => document.getElementById("analyticsYearFilter");
const $month   = () => document.getElementById("analyticsMonthFilter");
const $bbFrom  = () => document.getElementById("buybacksRollingFrom");
const $bbTo    = () => document.getElementById("buybacksRollingTo");
const $inFrom  = () => document.getElementById("insidersRollingFrom");
const $inTo    = () => document.getElementById("insidersRollingTo");

// ── Format helpers ─────────────────────────────────────────────────────────
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

// ── Filters ────────────────────────────────────────────────────────────────
function getFilters() {
    return {
        sector:  $sector()?.value  || "all",
        company: $company()?.value || "all",
    };
}

function applyFilters(rows) {
    const { sector, company } = getFilters();
    return rows.filter(r => {
        if (sector  !== "all" && (r.sector  || "") !== sector)    return false;
        if (company !== "all" && (r.company_alias||"") !== company) return false;
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

// ── Rolling aggregation ────────────────────────────────────────────────────
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
        });
        const g = grouped.get(key);
        g[qKey] += qty; g.financial_volume += vol; g._wp += price * qty;
        g.trade_count += Number(r.trade_count || 0);
    });
    return [...grouped.values()].map(g => {
        const q = g[qKey] || 0;
        g.avg_price = q ? g._wp / q : 0;
        delete g._wp;
        return g;
    }).sort((a,b) => b.financial_volume - a.financial_volume);
}

// ── Organ / pivot helpers ──────────────────────────────────────────────────
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

// Main groups shown with full Buy/Sell/Balance; all others collapsed into "Others" (balance only)
const MAIN_GROUPS = ["Controller", "Management", "Board"];

// ── Tab switching ──────────────────────────────────────────────────────────
function switchTab(tab) {
    document.querySelectorAll(".tr-tab").forEach(t => t.classList.remove("active"));
    document.querySelectorAll(".tr-content").forEach(c => c.classList.remove("active"));
    document.getElementById("tab-" + tab).classList.add("active");
    document.getElementById("content-" + tab).classList.add("active");
    populatePeriodFilters(cache);
    renderAll(cache);
}

// ── Period filter population ───────────────────────────────────────────────
function allPeriods(analytics) {
    const activeTab = document.querySelector(".tr-tab.active")?.id === "tab-buybacks" ? "buybacks" : "insiders";
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

    // Rolling selectors
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

    // History chart period selectors — same options
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
}

function syncMonths(periods, prefMonth) {
    const year = $year()?.value;
    const months = [...new Set(periods.filter(p => p.slice(0,4) === year).map(p => p.slice(5,7)))]
        .sort((a,b) => Number(b) - Number(a));
    $month().innerHTML = months.map(m => `<option value="${m}">${MONFULL[Number(m)-1]||m}</option>`).join("");
    $month().value = months.includes(prefMonth) ? prefMonth : months[0] || "";
}

// ── Company filter ─────────────────────────────────────────────────────────
function populateCompanyFilter(analytics) {
    const { sector } = getFilters();
    const all = [...(analytics.buybacks_executed||[]), ...(analytics.insiders_buying||[]), ...(analytics.insiders_selling||[])]
        .filter(r => sector === "all" || (r.sector||"") === sector);
    const names = [...new Set(all.map(r => r.company_alias||"").filter(Boolean))].sort();
    const prev = $company()?.value || "all";
    $company().innerHTML = `<option value="all">All Companies</option>` +
        names.map(n => `<option value="${n}">${n}</option>`).join("");
    $company().value = [...$company().options].some(o => o.value === prev) ? prev : "all";
}

function populateHistoryCompanyFilter(bbRows) {
    const el = document.getElementById("bbHistoryCompany");
    if (!el) return;
    const names = [...new Set(bbRows.map(r => r.company_alias||"").filter(Boolean))].sort();
    const prev = el.value;
    el.innerHTML = `<option value="">Select a company</option>` +
        names.map(n => `<option value="${n}">${n}</option>`).join("");
    if (names.includes(prev)) el.value = prev;
}

// ── Section title helper (Telecom style) ──────────────────────────────────
function sectionTitleHTML(text) {
    return `<div class="tr-section-title">${text}</div>`;
}

// ── KPI card ───────────────────────────────────────────────────────────────
function kpi(label, value, sub, accent) {
    return `<div class="tr-kpi ${accent}"><div class="tr-kpi-label">${label}</div><div class="tr-kpi-value ${value.cls||""}">${value.text}</div><div class="tr-kpi-sub">${sub}</div></div>`;
}

// ── Horizontal bar chart ───────────────────────────────────────────────────
function barChart(rows, valueKey, labelFn, subFn, shBadge, shTitle, shSub) {
    const active = rows.filter(r => Number(r[valueKey]||0) > 0);
    const max = active.length ? Math.max(...active.map(r => Number(r[valueKey]))) : 1;
    const bars = active.map(r => {
        const v = Number(r[valueKey]||0);
        const pct = Math.max(0.5, (v / max) * 100).toFixed(1);
        const isLarge = pct > 75;
        return `<div class="tr-bar-row">
            <div class="tr-bar-label"><span class="tr-bar-label-name">${labelFn(r)}</span><span class="tr-bar-label-sub">${subFn(r)}</span></div>
            <div class="tr-bar-track">
                <div class="tr-bar-fill${isLarge ? " label-end" : ""}" style="width:${pct}%">${isLarge ? fmtVol(v) : ""}</div>
                ${!isLarge ? `<span class="tr-bar-ext-val">${fmtVol(v)}</span>` : ""}
            </div>
        </div>`;
    }).join("");

    const badge = shBadge ? `<span class="tr-sb blue">${shBadge}</span>` : "";
    return `<div class="tr-sc">
        <div class="tr-sh"><div class="tr-sh-left">
            <div class="tr-st"><span class="tr-dot blue"></span>${shTitle}</div>
            <div class="tr-ss">${shSub}</div>
        </div>${badge}</div>
        <div class="tr-bar-wrap">${bars || '<div class="tr-empty">No data</div>'}</div>
    </div>`;
}

// ── Buyback table ──────────────────────────────────────────────────────────
function buybackTable(rows, shTitle, shSub, dotCls, badgeCls, showRefMonth) {
    if (!rows.length) return `<div class="tr-sc"><div class="tr-sh"><div class="tr-st"><span class="tr-dot ${dotCls}"></span>${shTitle}</div></div><div class="tr-empty">No data for the selected period.</div></div>`;

    const badge = `<span class="tr-sb ${badgeCls}">${rows.length} compan${rows.length===1?"y":"ies"}</span>`;
    const refMonthHeader = showRefMonth ? `<th class="r">Ref.</th>` : "";
    const thead = `<tr><th>Company</th><th>Ticker</th>${refMonthHeader}<th class="r">Shares</th><th class="r">Volume</th><th class="r">Avg. Price</th><th class="r">Trades</th></tr>`;

    const tbody = rows.map(r => `<tr>
        <td><span class="tr-cn">${r.company_alias||"—"}</span></td>
        <td><span class="tr-tk">${r.ticker||"—"}</span></td>
        ${showRefMonth ? `<td class="r"><span class="tr-mn">${ymLabel(r.reference_year_month)}</span></td>` : ""}
        <td class="r"><span class="tr-mn">${fmt(r.shares_reacquired??0,0)}</span></td>
        <td class="r"><span class="tr-mn lg blue">${fmtVol(r.financial_volume)}</span></td>
        <td class="r"><span class="tr-mn">${fmtPrice(r.avg_price)}</span></td>
        <td class="r"><span class="tr-cnt">${fmt(r.trade_count,0)}</span></td>
    </tr>`).join("");

    return `<div class="tr-sc">
        <div class="tr-sh"><div class="tr-sh-left">
            <div class="tr-st"><span class="tr-dot ${dotCls}"></span>${shTitle}</div>
            <div class="tr-ss">${shSub}</div>
        </div>${badge}</div>
        <div class="tr-tbl-wrap"><table class="tr-tbl"><thead>${thead}</thead><tbody>${tbody}</tbody></table></div>
    </div>`;
}

// ── Net balance chart ──────────────────────────────────────────────────────
function nbChart(companies, shTitle, shSub) {
    const abs = companies.map(c => Math.abs(c.net));
    const maxAbs = abs.length ? Math.max(...abs) : 1;

    const rows = companies.map(c => {
        const pct = Math.min(48, Math.abs(c.net) / maxAbs * 48).toFixed(1);
        const isSell = c.net < 0;
        const isLarge = pct > 15;
        const valFmt = fmtVol(Math.abs(c.net));
        let barContent = "";
        let extValHTML = "";
        if (isLarge) {
            barContent = fmtVol(Math.abs(c.net));
        } else {
            const posStyle = isSell
                ? `right:calc(50% + ${pct}% + 4px)`
                : `left:calc(50% + ${pct}% + 4px)`;
            extValHTML = `<span class="tr-nb-ext-val ${isSell ? "sell" : "buy"}" style="${posStyle}">${valFmt}</span>`;
        }
        const bar = isSell
            ? `<div class="tr-nb-bar sell" style="width:${pct}%">${barContent}</div>`
            : `<div class="tr-nb-bar buy"  style="width:${pct}%">${barContent}</div>`;
        return `<div class="tr-nb-row">
            <div class="tr-nb-label"><div class="tr-nb-label-name">${c.name}</div><div class="tr-nb-label-sub">${c.ticker||""}</div></div>
            <div class="tr-nb-chart"><div class="tr-nb-axis"></div>${bar}${extValHTML}</div>
        </div>`;
    }).join("");

    return `<div class="tr-sc tr-sc-fill">
        <div class="tr-sh"><div class="tr-sh-left">
            <div class="tr-st"><span class="tr-dot blue"></span>${shTitle}</div>
            <div class="tr-ss">${shSub}</div>
        </div></div>
        <div class="tr-nb-wrap">
            <div class="tr-nb-header"><span>← Selling</span><span>Buying →</span></div>
            <div class="tr-nb-rows">${rows || '<div class="tr-empty">No data</div>'}</div>
        </div>
    </div>`;
}

// ── Insiders monthly tables ────────────────────────────────────────────────
function insiderTable(rows, side, period) {
    const isGreen = side === "buy";
    const dotCls  = isGreen ? "green" : "red";
    const title   = isGreen ? "Insider Buying" : "Insider Selling";
    const badgeCls= isGreen ? "green" : "red";

    const active = rows.filter(r => Number(r.financial_volume||0) > 0)
        .sort((a,b) => Number(b.financial_volume||0) - Number(a.financial_volume||0));

    const badge = `<span class="tr-sb ${badgeCls}">${active.length} event${active.length===1?"":"s"}</span>`;

    // Group by company then sort by total vol
    const byComp = new Map();
    active.forEach(r => {
        const k = r.company_alias||"";
        if (!byComp.has(k)) byComp.set(k, { rows: [] });
        byComp.get(k).rows.push(r);
    });
    const comps = [...byComp.values()].sort((a,b) => {
        const av = a.rows.reduce((s,r) => s + Number(r.financial_volume||0), 0);
        const bv = b.rows.reduce((s,r) => s + Number(r.financial_volume||0), 0);
        return bv - av;
    });

    const sign = isGreen ? "+" : "−";
    const trs = comps.flatMap(comp => comp.rows.map(sr => `<tr>
        <td><span class="tr-cn">${sr.company_alias||"—"}</span></td>
        <td class="tr-organ-col"><span class="tr-rt">${mapOrgan(sr.organ||"")}</span></td>
        <td class="r"><span class="tr-mn">${fmt(sr.shares??0,0)}</span></td>
        <td class="r"><span class="tr-mn ${dotCls}">${sign}${fmtVol(sr.financial_volume)}</span></td>
        <td class="r"><span class="tr-mn">${fmtPrice(sr.avg_price)}</span></td>
        <td class="r"><span class="tr-cnt">${fmt(sr.trade_count,0)}</span></td>
    </tr>`)).join("");

    const thead = `<tr><th>Company</th><th class="tr-organ-col">Group</th><th class="r">Shares</th><th class="r">Volume</th><th class="r">Avg. Price</th><th class="r">Trades</th></tr>`;

    return `<div class="tr-sc">
        <div class="tr-sh"><div class="tr-sh-left">
            <div class="tr-st"><span class="tr-dot ${dotCls}"></span>${title}</div>
            <div class="tr-ss">Ref. ${ymLabel(period)}</div>
        </div>${badge}</div>
        ${trs ? `<div class="tr-tbl-wrap"><table class="tr-tbl tr-tbl-sm"><thead>${thead}</thead><tbody>${trs}</tbody></table></div>` : `<div class="tr-empty">No ${title.toLowerCase()} for the selected period.</div>`}
    </div>`;
}

// ── Insider pivot table ────────────────────────────────────────────────────
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
                // Consolidate into Others
                if (side === "buy")  cd.othBuy  += v;
                else                 cd.othSell += v;
            }
        });
    }
    addRows(buyRows, "buy");
    addRows(sellRows, "sell");

    // Which main groups actually appear?
    const activeMain = MAIN_GROUPS.filter(g => {
        for (const d of data.values()) if (d.groups.has(g)) return true;
        return false;
    });
    const hasOthers = [...data.values()].some(d => d.othBuy > 0 || d.othSell > 0);

    const companies = [...data.entries()].map(([name, d]) => {
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

    function pv(val, bold = false) {
        if (!val) return `<span class="tr-mn muted">—</span>`;
        const m = val / 1e6;
        const abs = Math.abs(m);
        const decimals = abs < 1 ? 1 : 0;
        const cls  = val > 0 ? "green" : "red";
        const sign = val > 0 ? "+" : "−";
        const style = bold ? ` style="font-weight:700"` : "";
        return `<span class="tr-mn ${cls}"${style}>${sign}${fmt(abs, decimals)}mn</span>`;
    }

    const rows = companies.map(c => {
        let cells = `<td><span class="tr-cn">${c.name}</span></td>`;
        cells += `<td class="r">${pv(c.bal, true)}</td>`;
        activeMain.forEach(g => {
            const gd = c.gd[g]||{buy:0,sell:0,bal:0};
            cells += `<td class="r" style="border-left:1px solid #edf1f5">${pv(gd.buy)}</td>`;
            cells += `<td class="r">${pv(-gd.sell)}</td>`;
            cells += `<td class="r">${pv(gd.bal)}</td>`;
        });
        if (hasOthers) cells += `<td class="r" style="border-left:1px solid #edf1f5">${pv(c.othBal)}</td>`;
        return `<tr>${cells}</tr>`;
    }).join("");

    let hdr1 = `<th rowspan="2" style="text-align:left;min-width:90px">Company</th><th rowspan="2" class="r">Net Balance</th>`;
    activeMain.forEach(g => { hdr1 += `<th colspan="3" style="text-align:center;border-left:2px solid rgba(255,255,255,0.2)">${g}</th>`; });
    if (hasOthers) hdr1 += `<th colspan="1" style="text-align:center;border-left:2px solid rgba(255,255,255,0.2)">Others</th>`;

    let hdr2 = "";
    activeMain.forEach(() => { hdr2 += `<th class="r" style="border-left:1px solid rgba(255,255,255,0.15)">Buy</th><th class="r">Sell</th><th class="r">Balance</th>`; });
    if (hasOthers) hdr2 += `<th class="r" style="border-left:1px solid rgba(255,255,255,0.15)">Balance</th>`;

    return `<div class="tr-tbl-wrap tr-pivot-wrap">
        <table class="tr-tbl tr-pivot-tbl">
            <thead><tr>${hdr1}</tr><tr class="pivot-sub">${hdr2}</tr></thead>
            <tbody>${rows}</tbody>
        </table>
    </div>`;
}

// ── Per-company buyback history chart ─────────────────────────────────────
function buybackHistoryChart(allRows, company, from, to) {
    const s = ymIndex(from), e = ymIndex(to);

    // Build month-by-month series for selected company
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

    // Build complete month range (show empty bars for months with no data)
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
        return `<div class="tr-sc"><div class="tr-sh"><div class="tr-st"><span class="tr-dot blue"></span>Repurchase History — ${company}</div></div><div class="tr-empty">No repurchase data for this company in the selected period.</div></div>`;
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
            <div class="tr-ss">${periodLbl} · ${activeCount} month${activeCount===1?"":"s"} with activity</div>
        </div></div>
        <div class="tr-timeline-wrap"><div class="tr-timeline-bars">${cols}</div></div>
    </div>`;
}

// ── Main render ────────────────────────────────────────────────────────────
function renderAll(analytics) {
    const ym    = selectedPeriod();
    const ymLbl = ym ? ymLabel(ym) : "—";

    const bbAll    = applyFilters(analytics.buybacks_executed || []);
    const inBuyAll = applyFilters(analytics.insiders_buying   || []);
    const inSellAll= applyFilters(analytics.insiders_selling  || []);

    const bbMon     = filterByPeriod(bbAll, ym).sort((a,b) => b.financial_volume - a.financial_volume);
    const inBuyMon  = filterByPeriod(inBuyAll, ym).sort((a,b) => b.financial_volume - a.financial_volume);
    const inSellMon = filterByPeriod(inSellAll, ym).sort((a,b) => b.financial_volume - a.financial_volume);

    const bbFrom = $bbFrom()?.value || "", bbTo = $bbTo()?.value || "";
    const inFrom = $inFrom()?.value || "", inTo = $inTo()?.value || "";

    const bbRoll    = buildRolling(bbAll,    bbFrom, bbTo, "shares_reacquired");
    const inBuyRoll = buildRolling(inBuyAll, inFrom, inTo, "shares");
    const inSellRoll= buildRolling(inSellAll,inFrom, inTo, "shares");

    const bbRollLbl = (bbFrom && bbTo) ? `${ymLabel(bbFrom)} → ${ymLabel(bbTo)}` : "Accumulated";
    const inRollLbl = (inFrom && inTo) ? `${ymLabel(inFrom)} → ${ymLabel(inTo)}` : "Accumulated";

    // Filter count
    const { sector, company } = getFilters();
    const parts = [];
    if (sector  !== "all") parts.push(sector);
    if (company !== "all") parts.push(company);
    if (ym) parts.push(ymLbl);
    document.getElementById("filterCount").textContent =
        (allCompanies.length ? allCompanies.length + " companies · " : "") +
        (parts.length ? parts.join(" · ") : "No filters");

    // ── BUYBACKS ──────────────────────────────────────────────────────────
    set("bbMonthlyPeriodLabel", sectionTitleHTML(`Reference Month — ${ymLbl}`));

    const bbMonVol = bbMon.reduce((s,r) => s + Number(r.financial_volume||0), 0);
    const bbTopMon = bbMon[0];
    const bbActive = new Set(bbMon.map(r => r.company_alias));
    set("bbMonthlyBlock", `
        ${buybackTable(bbMon, "Monthly Repurchases", `Ref. ${ymLbl}`, "blue", "blue", false)}
        ${barChart(bbMon, "financial_volume", r=>r.company_alias||"—", r=>`${r.ticker||""} · ${fmt(r.shares_reacquired??0,0)} shares`, null, `Volume by Company — ${ymLbl}`, "Monthly repurchase reference")}
    `);

    set("bbRollingPeriodLabel", sectionTitleHTML("Accumulated Repurchases"));

    const bbRollVol = bbRoll.reduce((s,r) => s + Number(r.financial_volume||0), 0);
    const bbTopRoll = bbRoll[0];
    set("bbRollingBlock", `
        ${buybackTable(bbRoll, "Accumulated Repurchases", bbRollLbl, "grey", "grey", false)}
        ${barChart(bbRoll, "financial_volume", r=>r.company_alias||"—", r=>`${r.ticker||""} · ${fmt(r.shares_reacquired??0,0)} shares`, null, `Volume by Company — ${bbRollLbl}`, "Ranking of accumulated repurchases")}
    `);

    // History chart
    set("bbHistoryPeriodLabel", sectionTitleHTML("Repurchase History by Company"));
    populateHistoryCompanyFilter(bbAll);
    const histCompany = document.getElementById("bbHistoryCompany")?.value || "";
    const histFrom    = document.getElementById("bbHistoryFrom")?.value    || bbFrom;
    const histTo      = document.getElementById("bbHistoryTo")?.value      || bbTo;
    set("bbHistoryBlock", histCompany
        ? buybackHistoryChart(bbAll, histCompany, histFrom, histTo)
        : `<div class="tr-sc"><div class="tr-empty">Select a company above to view its monthly repurchase history.</div></div>`);

    // ── INSIDERS ──────────────────────────────────────────────────────────
    set("inMonthlyPeriodLabel", sectionTitleHTML(`Reference Month — ${ymLbl}`));

    const inBuyMonVol  = inBuyMon.reduce((s,r)=>s+Number(r.financial_volume||0),0);
    const inSellMonVol = inSellMon.reduce((s,r)=>s+Number(r.financial_volume||0),0);
    const netMon       = inBuyMonVol - inSellMonVol;
    const topBuyer     = inBuyMon[0];
    const topSeller    = inSellMon[0];
    const inActive     = new Set([...inBuyMon,...inSellMon].map(r => r.company_alias));

    const netByComp = new Map();
    [...inBuyMon, ...inSellMon].forEach(r => {
        const k = r.company_alias||"";
        if (!netByComp.has(k)) netByComp.set(k,{name:k,ticker:r.ticker,net:0});
        netByComp.get(k).net += (inBuyMon.includes(r)?1:-1) * Number(r.financial_volume||0);
    });
    const netCompsMon = [...netByComp.values()].sort((a,b) => a.net - b.net);

    const sellByComp = new Map();
    inSellMon.forEach(r => {
        const k = r.company_alias||"";
        if (!sellByComp.has(k)) sellByComp.set(k,{name:k,ticker:r.ticker,vol:0,organ:""});
        const sc = sellByComp.get(k); sc.vol += Number(r.financial_volume||0); sc.organ = r.organ||"";
    });
    const topSellerComp = [...sellByComp.values()].sort((a,b)=>b.vol-a.vol)[0];

    const inBuyRollVol  = inBuyRoll.reduce((s,r)=>s+Number(r.financial_volume||0),0);
    const inSellRollVol = inSellRoll.reduce((s,r)=>s+Number(r.financial_volume||0),0);
    const netRoll = inBuyRollVol - inSellRollVol;

    set("inMonthlyBlock", `
        ${insiderTable(inBuyMon, "buy", ym)}
        ${insiderTable(inSellMon, "sell", ym)}
        ${nbChart(netCompsMon, `Net Balance — ${ymLbl}`, `Net insider balance by company · month`)}
    `);

    set("inRollingPeriodLabel", sectionTitleHTML("Insider Activity"));

    const netByCompRoll = new Map();
    [...inBuyRoll,...inSellRoll].forEach(r => {
        const k = r.company_alias||"";
        if (!netByCompRoll.has(k)) netByCompRoll.set(k,{name:k,ticker:r.ticker,net:0});
        netByCompRoll.get(k).net += (inBuyRoll.includes(r)?1:-1) * Number(r.financial_volume||0);
    });
    const netCompsRoll  = [...netByCompRoll.values()].sort((a,b)=>a.net-b.net);
    const topSellerRoll = [...netByCompRoll.values()].filter(c=>c.net<0).sort((a,b)=>a.net-b.net)[0];

    set("inRollingBlock", `
        <div>${pivotCard(inBuyAll, inSellAll, inFrom, inTo, inRollLbl)}</div>
        ${nbChart(netCompsRoll, `Net Balance — ${inRollLbl}`, `Net insider balance by company · accumulated`)}
    `);
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

// ── Event handlers ─────────────────────────────────────────────────────────
function onFilterChange() {
    populateCompanyFilter(cache);
    renderAll(cache);
}
function onYearChange() {
    const periods = allPeriods(cache);
    syncMonths(periods, "");
    renderAll(cache);
}

// ── Data loading ───────────────────────────────────────────────────────────
async function loadStatus() {
    try {
        const data = await fetch("/tracker/api/status").then(r => r.json());
        cache = data.analytics || cache;
        allCompanies = data.companies || [];

        const syncAt = data.last_success_at;
        if (syncAt) {
            const d = new Date(syncAt);
            document.getElementById("lastUpdated").textContent =
                `Last update: ${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}-${String(d.getDate()).padStart(2,"0")}`;
        }

        populateCompanyFilter(cache);
        populatePeriodFilters(cache);
        renderAll(cache);
    } catch(e) {
        document.getElementById("lastUpdated").textContent = "Sync error";
    }
}

loadStatus();
setInterval(loadStatus, 30000);
