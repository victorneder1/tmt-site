// ── Pair Trades frontend ──────────────────────────────────────────────────
let pairsData = [];
let pairsLoaded = false;
let chartInstances = {};

// Format price
function formatPrice(price) {
    return Number(price).toFixed(2);
}

// Format performance
function formatPerf(perf) {
    const pct = (perf * 100).toFixed(2);
    return `${perf >= 0 ? "+" : ""}${pct}%`;
}

// Calculate average return for display
function calcAvgReturn(current, entry) {
    if (Array.isArray(current) && Array.isArray(entry)) {
        const returns = current.map((c, i) => ((c / entry[i]) - 1) * 100);
        return returns.reduce((s, r) => s + r, 0) / returns.length;
    }
    return ((current / entry) - 1) * 100;
}

// Format tickers for display
function fmtTickers(tickers) {
    if (Array.isArray(tickers)) {
        return tickers.length > 3
            ? tickers.slice(0, 3).join(", ") + ` (+${tickers.length - 3})`
            : tickers.join(", ");
    }
    return tickers;
}

function fmtTickersShort(tickers) {
    if (Array.isArray(tickers)) return `Basket (${tickers.length})`;
    return tickers;
}

// ── Render a single pair card ──────────────────────────────────────────────
function renderPairCard(pair) {
    const card = document.createElement("div");
    card.className = "pair-card";

    const isLongBasket = Array.isArray(pair.long_ticker);
    const isShortBasket = Array.isArray(pair.short_ticker);
    const longReturn = calcAvgReturn(pair.current_price_long, pair.entry_price_long);
    const shortReturn = calcAvgReturn(pair.current_price_short, pair.entry_price_short);
    const perfClass = pair.performance > 0 ? "positive" : pair.performance < 0 ? "negative" : "neutral";
    const isClosed = pair.status === "closed";

    // Header
    let headerHTML = `
        <div class="pair-header">
            <div class="pair-tickers">
                <h3>${fmtTickersShort(pair.long_ticker)} <span class="sep">/</span> ${fmtTickersShort(pair.short_ticker)}</h3>
                <span class="pair-type">${fmtTickers(pair.long_ticker)} / ${fmtTickers(pair.short_ticker)}</span>
            </div>
            <div class="pair-perf ${perfClass}">
                <div class="pair-perf-value">${formatPerf(pair.performance)}</div>
                <div class="pair-perf-label">${isClosed ? "Final" : "Performance"}</div>
            </div>
        </div>`;

    // Positions
    function renderPosition(label, tickers, entryPrices, currentPrices, ret, isBasket) {
        const retClass = ret >= 0 ? "positive" : "negative";
        let body = "";
        if (isBasket) {
            const ts = tickers;
            const eps = entryPrices;
            const cps = currentPrices;
            body = '<div class="basket-items">';
            ts.forEach((t, i) => {
                const tr = ((cps[i] / eps[i]) - 1) * 100;
                const trc = tr >= 0 ? "positive" : "negative";
                body += `<div class="basket-item">
                    <span class="basket-ticker">${t}</span>
                    <span class="basket-prices">$${formatPrice(eps[i])} &rarr; $${formatPrice(cps[i])}</span>
                    <span class="basket-return ${trc}">${tr >= 0 ? "+" : ""}${tr.toFixed(2)}%</span>
                </div>`;
            });
            body += "</div>";
        } else {
            body = `<div class="prices-row">
                <div class="price-item"><span class="pl">Entry:</span><span class="pv">$${formatPrice(entryPrices)}</span></div>
                <div class="price-item"><span class="pl">Current:</span><span class="pv">$${formatPrice(currentPrices)}</span></div>
            </div>`;
        }
        return `<div class="position ${label.toLowerCase()}">
            <div class="position-hdr">
                <span class="position-label">${label}${isBasket ? ` (${tickers.length})` : ""}</span>
                <span class="ret ${retClass}">${ret >= 0 ? "+" : ""}${ret.toFixed(2)}%${isBasket ? " avg" : ""}</span>
            </div>
            ${body}
        </div>`;
    }

    const detailsHTML = `<div class="pair-details">
        ${renderPosition("Long", pair.long_ticker, pair.entry_price_long, pair.current_price_long, longReturn, isLongBasket)}
        ${renderPosition("Short", pair.short_ticker, pair.entry_price_short, pair.current_price_short, shortReturn, isShortBasket)}
    </div>`;

    // Dates
    let datesHTML = "";
    if (pair.inception_date) {
        datesHTML += `<span class="pair-date">Inception: ${pair.inception_date.split("T")[0]}</span>`;
    }
    if (isClosed && pair.closed_date) {
        datesHTML += `<span class="pair-date">Closed: ${pair.closed_date.split("T")[0]}</span>`;
    }
    if (datesHTML) {
        datesHTML = `<div class="pair-dates">${datesHTML}</div>`;
    }

    // Chart container (always since inception)
    const chartId = `chart-${pair.id}`;
    const chartHTML = `<div class="pair-chart-container">
        <div class="chart-header-row">
            <span class="chart-title">Performance Since Inception</span>
        </div>
        <div class="chart-wrap"><canvas id="${chartId}" height="180"></canvas></div>
    </div>`;

    card.innerHTML = headerHTML + detailsHTML + datesHTML + chartHTML;

    return card;
}

// Format date as "mon-yy" (e.g., "jan-26")
const MONTH_ABBR = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
function fmtDateMonYY(dt) {
    return MONTH_ABBR[dt.getMonth()] + "-" + String(dt.getFullYear()).slice(2);
}

// ── Load chart data (always since inception) ─────────────────────────────
async function loadChart(pairId, canvasId) {
    const now = new Date();
    const from = new Date(0);

    try {
        const res = await fetch(`/api/pairs/${pairId}/history?from=${from.toISOString()}&to=${now.toISOString()}`);
        const data = await res.json();

        const canvas = document.getElementById(canvasId);
        if (!canvas) return;

        // Destroy existing chart
        if (chartInstances[canvasId]) {
            chartInstances[canvasId].destroy();
        }

        if (!data.length) {
            canvas.parentElement.innerHTML = '<div class="chart-empty-msg">No historical data available</div>';
            return;
        }

        const labels = data.map(d => fmtDateMonYY(new Date(d.timestamp)));
        const values = data.map(d => d.performance * 100);

        chartInstances[canvasId] = new Chart(canvas, {
            type: "line",
            data: {
                labels: labels,
                datasets: [{
                    data: values,
                    borderColor: "#001F62",
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHoverBackgroundColor: "#D4AF37",
                    tension: 0.3,
                    fill: false,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: "index",
                    intersect: false,
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        enabled: true,
                        callbacks: {
                            title: (items) => {
                                if (!items.length) return "";
                                const idx = items[0].dataIndex;
                                const dt = new Date(data[idx].timestamp);
                                return dt.toLocaleDateString("en-US", { day: "numeric", month: "short", year: "numeric" });
                            },
                            label: ctx => `Performance: ${ctx.parsed.y >= 0 ? "+" : ""}${ctx.parsed.y.toFixed(2)}%`,
                        },
                    },
                },
                scales: {
                    x: {
                        ticks: { font: { size: 10 }, color: "#888", maxTicksLimit: 8 },
                        grid: { display: false },
                    },
                    y: {
                        ticks: {
                            font: { size: 10 },
                            color: "#888",
                            callback: v => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`,
                        },
                        grid: { color: "#f0f0f0" },
                    },
                },
            },
        });
    } catch (err) {
        console.error("Chart load error:", err);
    }
}

// ── Load all pairs ─────────────────────────────────────────────────────────
async function loadPairs() {
    try {
        const res = await fetch("/api/pairs");
        pairsData = await res.json();
        renderPairs();
    } catch (err) {
        console.error("Failed to load pairs:", err);
    }
}

function renderPairs() {
    const openList = document.getElementById("pairs-open-list");
    const closedList = document.getElementById("pairs-closed-list");
    const openEmpty = document.getElementById("pairs-open-empty");
    const closedEmpty = document.getElementById("pairs-closed-empty");

    openList.innerHTML = "";
    closedList.innerHTML = "";

    // Use server order (controlled via admin page)
    const openPairs = pairsData.filter(p => p.status === "open");
    const closedPairs = pairsData.filter(p => p.status === "closed");

    openEmpty.style.display = openPairs.length ? "none" : "block";
    closedEmpty.style.display = closedPairs.length ? "none" : "block";

    openPairs.forEach(p => {
        const card = renderPairCard(p);
        openList.appendChild(card);
        setTimeout(() => loadChart(p.id, `chart-${p.id}`), 100);
    });

    closedPairs.forEach(p => {
        const card = renderPairCard(p);
        card.classList.add("closed");
        closedList.appendChild(card);
        setTimeout(() => loadChart(p.id, `chart-${p.id}`), 100);
    });
}

// ── Init pairs when tab is shown ──────────────────────────────────────────
function initPairs() {
    if (!pairsLoaded) {
        pairsLoaded = true;
        loadPairs();
        // Auto-refresh every 30s
        setInterval(() => {
            const pairsMain = document.getElementById("pairs-main");
            if (pairsMain && pairsMain.classList.contains("active")) {
                loadPairs();
            }
        }, 30000);
    }
}
