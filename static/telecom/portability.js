// ── Portability Dashboard ──

const PORT_TABLE_OPS = ["Claro", "TIM", "Vivo", "Brisanet", "Unifique"];
const PORT_TABLE_MAX_MONTHS = 12;

const PORT_COLORS = {
    "Claro":    "#E31E24",
    "Vivo":     "#7B2D8E",
    "TIM":      "#003399",
    "Oi":       "#F5A623",
    "Brisanet": "#00A651",
    "Unifique": "#00BCD4",
    "Algar":    "#FF6B35",
    "Sercomtel":"#2D5F2D",
    "Others":   "#999999",
};

let portCharts = {};
let portAllMonths = [];
let portRawData = [];

function portFmtMonth(m) {
    const [y, mo] = m.split("-");
    const names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    return names[parseInt(mo) - 1] + "-" + y.slice(2);
}

function portFmtNum(val) {
    const abs = Math.abs(val);
    const str = abs.toLocaleString("en-US");
    return val < 0 ? "(" + str + ")" : str;
}

// ── Init ──

async function initPortability() {
    const [months, states] = await Promise.all([
        fetch("/telecom/api/portability/months").then(r => r.json()),
        fetch("/telecom/api/broadband/states").then(r => r.json()),
    ]);

    portAllMonths = months;

    // Populate state dropdown
    const ufSel = document.getElementById("port-uf-select");
    while (ufSel.options.length > 1) ufSel.remove(1);
    states.forEach(s => {
        const opt = document.createElement("option");
        opt.value = s.code;
        opt.textContent = s.code + " - " + s.name;
        ufSel.appendChild(opt);
    });

    // Populate company dropdown
    const companySel = document.getElementById("port-company-select");
    PORT_TABLE_OPS.forEach(op => {
        companySel.appendChild(new Option(op, op));
    });

    ufSel.addEventListener("change", loadPortability);
    companySel.addEventListener("change", function () { renderPortBreakdown(portRawData); });

    await loadPortability();
}

// ── Data Loading ──

async function loadPortability() {
    const uf = document.getElementById("port-uf-select").value;
    const params = new URLSearchParams();
    if (uf) params.set("uf", uf);

    const data = await fetch("/telecom/api/portability?" + params).then(r => r.json());
    portRawData = data;
    renderPortDashboard(data);
}

// ── Aggregation ──

function buildPortMaps(data, operators) {
    const monthsSet = new Set();
    const received = {};
    const given = {};

    data.forEach(d => {
        monthsSet.add(d.month);
        const g = d.giver;
        const r = d.receiver;

        if (operators.includes(g)) {
            if (!given[g]) given[g] = {};
            given[g][d.month] = (given[g][d.month] || 0) + d.quantity;
        }
        if (operators.includes(r)) {
            if (!received[r]) received[r] = {};
            received[r][d.month] = (received[r][d.month] || 0) + d.quantity;
        }
    });

    const months = Array.from(monthsSet).sort();
    const netMap = {};
    const giverMap = {};
    const receiverMap = {};

    operators.forEach(op => {
        netMap[op] = {};
        giverMap[op] = {};
        receiverMap[op] = {};
        months.forEach(m => {
            const recv = (received[op] && received[op][m]) || 0;
            const give = (given[op] && given[op][m]) || 0;
            netMap[op][m] = recv - give;
            giverMap[op][m] = give;
            receiverMap[op][m] = recv;
        });
    });

    return { months, netMap, giverMap, receiverMap };
}

// ── Dashboard Render ──

function renderPortDashboard(data) {
    const { months, netMap, giverMap, receiverMap } = buildPortMaps(data, PORT_TABLE_OPS);
    const displayMonths = months.slice(-PORT_TABLE_MAX_MONTHS);

    // Sort operators once by net portability (last month, descending)
    const lastMonth = displayMonths[displayMonths.length - 1];
    const sortedOps = [...PORT_TABLE_OPS].sort((a, b) => {
        const va = (netMap[a] && netMap[a][lastMonth]) || 0;
        const vb = (netMap[b] && netMap[b][lastMonth]) || 0;
        return vb - va;
    });

    renderPortTable("port-net-table", displayMonths, sortedOps, netMap, true);
    renderPortTable("port-giver-table", displayMonths, sortedOps, giverMap, false);
    renderPortTable("port-receiver-table", displayMonths, sortedOps, receiverMap, false);
    renderNetPortabilityChart(displayMonths, PORT_TABLE_OPS, netMap);

    renderPortBreakdown(data);
}

// ── Table Rendering ──

function renderPortTable(tableId, months, operators, opMonthMap, isNet) {
    const table = document.getElementById(tableId);
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");

    // Header row
    thead.innerHTML = "";
    const hr = document.createElement("tr");
    hr.appendChild(Object.assign(document.createElement("th"), { textContent: "" }));
    months.forEach(m => {
        hr.appendChild(Object.assign(document.createElement("th"), { textContent: portFmtMonth(m) }));
    });
    thead.appendChild(hr);

    tbody.innerHTML = "";
    operators.forEach(op => {
        const tr = document.createElement("tr");

        // Operator name cell
        const tdName = document.createElement("td");
        tdName.textContent = op;
        tdName.style.fontWeight = "700";
        tr.appendChild(tdName);

        // Value cells
        months.forEach(m => {
            const td = document.createElement("td");
            const val = (opMonthMap[op] && opMonthMap[op][m]) || 0;
            td.textContent = portFmtNum(val);
            if (isNet) {
                td.className = val >= 0 ? "val-positive" : "val-negative";
            }
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
}

// ── Consolidated Net Portability Chart ──

function renderNetPortabilityChart(months, operators, netMap) {
    const labels = months.map(portFmtMonth);
    const chartKey = "port-net-chart";

    if (portCharts[chartKey]) portCharts[chartKey].destroy();

    portCharts[chartKey] = new Chart(document.getElementById(chartKey), {
        type: "bar",
        data: {
            labels: labels,
            datasets: operators.filter(op => netMap[op]).map(op => ({
                label: op,
                data: months.map(m => (netMap[op] && netMap[op][m]) || 0),
                backgroundColor: (PORT_COLORS[op] || PORT_COLORS["Others"]) + "CC",
                borderColor: PORT_COLORS[op] || PORT_COLORS["Others"],
                borderWidth: 1,
                borderRadius: 2,
            })),
        },
        options: portChartOpts(),
    });
}

// ── Breakdown by Operator ──

function renderPortBreakdown(data) {
    const selectedOp = document.getElementById("port-company-select").value;
    if (!selectedOp) return;

    document.getElementById("port-breakdown-title").textContent =
        selectedOp + " — Net Portability vs. Counterparties";
    document.getElementById("port-breakdown-chart-title").textContent =
        selectedOp + " — Net Portability by Operator";

    // Build pair-wise net for selected operator vs each counterparty
    const monthsSet = new Set();
    const pairNet = {};

    data.forEach(d => {
        monthsSet.add(d.month);
        if (d.giver === selectedOp) {
            const cp = d.receiver;
            if (!pairNet[cp]) pairNet[cp] = {};
            pairNet[cp][d.month] = (pairNet[cp][d.month] || 0) - d.quantity;
        }
        if (d.receiver === selectedOp) {
            const cp = d.giver;
            if (!pairNet[cp]) pairNet[cp] = {};
            pairNet[cp][d.month] = (pairNet[cp][d.month] || 0) + d.quantity;
        }
    });

    const months = Array.from(monthsSet).sort();
    const displayMonths = months.slice(-PORT_TABLE_MAX_MONTHS);

    // Show main operators as counterparties (exclude self), plus "Others" aggregate
    const mainCPs = PORT_TABLE_OPS.filter(op => op !== selectedOp);
    const otherOps = Object.keys(pairNet).filter(op => !PORT_TABLE_OPS.includes(op));

    // Aggregate all non-main counterparties into "Others"
    if (otherOps.length > 0) {
        pairNet["Others"] = {};
        otherOps.forEach(op => {
            months.forEach(m => {
                pairNet["Others"][m] = (pairNet["Others"][m] || 0) + ((pairNet[op] && pairNet[op][m]) || 0);
            });
        });
    }

    const counterparties = [...mainCPs.filter(cp => pairNet[cp]), ...(otherOps.length > 0 ? ["Others"] : [])];

    renderPortTable("port-breakdown-table", displayMonths, counterparties, pairNet, true);
    renderPortBreakdownChart(displayMonths, counterparties, pairNet);
}

function renderPortBreakdownChart(months, counterparties, pairNet) {
    const labels = months.map(portFmtMonth);
    const chartKey = "port-breakdown-chart";

    if (portCharts[chartKey]) portCharts[chartKey].destroy();

    portCharts[chartKey] = new Chart(document.getElementById(chartKey), {
        type: "bar",
        data: {
            labels: labels,
            datasets: counterparties.map(cp => ({
                label: cp,
                data: months.map(m => (pairNet[cp] && pairNet[cp][m]) || 0),
                backgroundColor: (PORT_COLORS[cp] || PORT_COLORS["Others"]) + "CC",
                borderColor: PORT_COLORS[cp] || PORT_COLORS["Others"],
                borderWidth: 1,
                borderRadius: 2,
            })),
        },
        options: portChartOpts(),
    });
}

function portChartOpts() {
    return {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
            legend: { position: "bottom", labels: { usePointStyle: true, padding: 14, font: { size: 11 } } },
            tooltip: {
                callbacks: {
                    label: function (ctx) {
                        const v = ctx.raw;
                        return ctx.dataset.label + ": " + portFmtNum(v);
                    }
                }
            },
        },
        scales: {
            x: { grid: { display: false }, ticks: { font: { size: 10 }, maxRotation: 45 } },
            y: {
                grid: { color: "#f0f0f0" },
                ticks: {
                    font: { size: 10 },
                    callback: function (v) { return (v / 1000).toFixed(0) + "k"; }
                }
            },
        },
    };
}
