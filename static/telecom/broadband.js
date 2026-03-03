// ── Broadband Dashboard ──

const BB_ALL_OPERATORS = [
    "Vivo", "Claro", "Niu", "Brisanet", "Giga+",
    "Vero", "Tecpar", "Desktop", "TIM", "Unifique", "Others"
];

const BB_TABLE_OPS = ["Vivo", "Claro", "Niu", "Brisanet", "Giga+", "Vero", "Tecpar", "Desktop", "TIM", "Unifique"];
const BB_BIG = ["Claro", "Vivo", "Niu"];
const BB_SMALL = ["Brisanet", "Giga+", "Vero", "Tecpar", "Desktop", "TIM", "Unifique"];

const OPERATOR_COLORS = {
    "Vivo":     "#7B2D8E",
    "Claro":    "#E31E24",
    "Niu":      "#F5A623",
    "Brisanet": "#00A651",
    "Giga+":    "#0098DB",
    "Vero":     "#FF6B35",
    "Tecpar":   "#2D5F2D",
    "Desktop":  "#1A73E8",
    "TIM":      "#003399",
    "Unifique": "#00BCD4",
    "Others":   "#999999",
};

const TABLE_MAX_MONTHS = 12;

let bbCharts = {};

let bbAllMonths = [];
const FTTH_MIN_MONTH = "2021-01";

async function initBroadband() {
    const [months, states] = await Promise.all([
        fetch("/telecom/api/broadband/months").then(r => r.json()),
        fetch("/telecom/api/broadband/states").then(r => r.json()),
    ]);

    bbAllMonths = months;

    const ufSel = document.getElementById("bb-uf-select");
    states.forEach(s => {
        const opt = document.createElement("option");
        opt.value = s.code;
        opt.textContent = `${s.code} - ${s.name}`;
        ufSel.appendChild(opt);
    });

    const fromSel = document.getElementById("bb-from-select");
    const toSel = document.getElementById("bb-to-select");
    months.forEach(m => {
        fromSel.appendChild(new Option(fmtMonth(m), m));
        toSel.appendChild(new Option(fmtMonth(m), m));
    });

    const defaultFrom = months.length >= 12 ? months[months.length - 12] : months[0];
    fromSel.value = defaultFrom;
    toSel.value = months[months.length - 1];

    const ftthToggle = document.getElementById("bb-ftth-toggle");

    ufSel.addEventListener("change", loadBroadband);
    fromSel.addEventListener("change", loadBroadband);
    toSel.addEventListener("change", loadBroadband);
    ftthToggle.addEventListener("change", () => {
        enforceFtthDateLimit();
        loadBroadband();
    });

    await loadBroadband();
}

function enforceFtthDateLimit() {
    const ftthOn = document.getElementById("bb-ftth-toggle").checked;
    const fromSel = document.getElementById("bb-from-select");
    if (ftthOn && fromSel.value < FTTH_MIN_MONTH) {
        fromSel.value = FTTH_MIN_MONTH;
    }
}

async function loadBroadband() {
    const uf = document.getElementById("bb-uf-select").value;
    const from = document.getElementById("bb-from-select").value;
    const to = document.getElementById("bb-to-select").value;
    const ftthOn = document.getElementById("bb-ftth-toggle").checked;
    const tech = ftthOn ? "FTTH" : "";

    // Chart data uses filter dates
    const params = new URLSearchParams();
    if (uf) params.set("uf", uf);
    if (from) params.set("from", from);
    if (to) params.set("to", to);
    if (tech) params.set("tech", tech);

    // Table data: always last 12 months (ignore from/to)
    const tableParams = new URLSearchParams();
    if (uf) tableParams.set("uf", uf);
    if (tech) tableParams.set("tech", tech);

    const [data, tableData] = await Promise.all([
        fetch("/telecom/api/broadband?" + params).then(r => r.json()),
        fetch("/telecom/api/broadband?" + tableParams).then(r => r.json()),
    ]);
    renderBBDashboard(data, tableData);
}

function buildOpMonthMap(data, operators) {
    const monthsSet = new Set();
    const opMonthMap = {};
    data.forEach(d => {
        if (!operators.includes(d.operator)) return;
        monthsSet.add(d.month);
        if (!opMonthMap[d.operator]) opMonthMap[d.operator] = {};
        opMonthMap[d.operator][d.month] = (opMonthMap[d.operator][d.month] || 0) + d.accesses;
    });
    return { months: Array.from(monthsSet).sort(), opMonthMap };
}

function renderBBDashboard(data, tableData) {
    // Table: always last 12 months from unfiltered data
    const tableAllMap = buildOpMonthMap(tableData, BB_ALL_OPERATORS);
    renderBBMultiMonthTable(tableAllMap.months, tableAllMap.opMonthMap);

    // Charts use filtered data
    const allMap = buildOpMonthMap(data, BB_ALL_OPERATORS);

    const bigMap = buildOpMonthMap(data, BB_BIG);
    renderChartGroup("bb-big", bigMap.months, BB_BIG, bigMap.opMonthMap, allMap);

    const smallMap = buildOpMonthMap(data, BB_SMALL);
    renderChartGroup("bb-small", smallMap.months, BB_SMALL, smallMap.opMonthMap, allMap);
}

function fmtMonth(m) {
    const [y, mo] = m.split("-");
    const names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    return names[parseInt(mo) - 1] + "-" + y.slice(2);
}

function renderBBMultiMonthTable(allMonths, opMonthMap) {
    const table = document.getElementById("bb-summary-table");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");

    const months = allMonths.slice(-TABLE_MAX_MONTHS);

    const monthTotals = {};
    months.forEach(m => {
        monthTotals[m] = 0;
        Object.values(opMonthMap).forEach(mm => { monthTotals[m] += (mm[m] || 0); });
    });

    thead.innerHTML = "";
    const hr = document.createElement("tr");
    const thEmpty = document.createElement("th");
    thEmpty.textContent = "";
    hr.appendChild(thEmpty);
    months.forEach(m => {
        const th = document.createElement("th");
        th.textContent = fmtMonth(m);
        hr.appendChild(th);
    });
    thead.appendChild(hr);

    tbody.innerHTML = "";

    const lastMonth = months[months.length - 1];
    const sorted = [...BB_TABLE_OPS].filter(op => opMonthMap[op]).sort((a, b) => {
        return ((opMonthMap[b] && opMonthMap[b][lastMonth]) || 0) - ((opMonthMap[a] && opMonthMap[a][lastMonth]) || 0);
    });

    sorted.forEach(op => {
        // Operator header row (light)
        const trHeader = document.createElement("tr");
        trHeader.className = "segment-label-row";
        const tdName = document.createElement("td");
        tdName.colSpan = months.length + 1;
        tdName.textContent = op;
        trHeader.appendChild(tdName);
        tbody.appendChild(trHeader);

        // Accesses row
        const trAcc = document.createElement("tr");
        const tdAccLabel = document.createElement("td");
        tdAccLabel.textContent = "Accesses";
        tdAccLabel.className = "metric-label";
        trAcc.appendChild(tdAccLabel);
        months.forEach(m => {
            const td = document.createElement("td");
            const val = (opMonthMap[op] && opMonthMap[op][m]) || 0;
            td.textContent = (val / 1e6).toFixed(2) + "M";
            trAcc.appendChild(td);
        });
        tbody.appendChild(trAcc);

        // Net Adds row
        const trNet = document.createElement("tr");
        const tdNetLabel = document.createElement("td");
        tdNetLabel.textContent = "Net Adds";
        tdNetLabel.className = "metric-label";
        trNet.appendChild(tdNetLabel);
        months.forEach((m, i) => {
            const td = document.createElement("td");
            const allIdx = allMonths.indexOf(m);
            if (allIdx > 0) {
                const prevM = allMonths[allIdx - 1];
                const curr = (opMonthMap[op] && opMonthMap[op][m]) || 0;
                const prev = (opMonthMap[op] && opMonthMap[op][prevM]) || 0;
                const net = curr - prev;
                const sign = net >= 0 ? "+" : "";
                td.textContent = sign + (net / 1000).toFixed(1) + "k";
                td.className = net >= 0 ? "val-positive" : "val-negative";
            } else {
                td.textContent = "-";
            }
            trNet.appendChild(td);
        });
        tbody.appendChild(trNet);

        // Market Share row
        const trShare = document.createElement("tr");
        trShare.className = "last-metric-row";
        const tdShareLabel = document.createElement("td");
        tdShareLabel.textContent = "Mkt Share";
        tdShareLabel.className = "metric-label";
        trShare.appendChild(tdShareLabel);
        months.forEach(m => {
            const td = document.createElement("td");
            const val = (opMonthMap[op] && opMonthMap[op][m]) || 0;
            const total = monthTotals[m] || 1;
            td.textContent = (val / total * 100).toFixed(1) + "%";
            trShare.appendChild(td);
        });
        tbody.appendChild(trShare);
    });
}

function renderChartGroup(prefix, months, operators, opMonthMap, allMap) {
    const ops = operators.filter(op => opMonthMap[op]);
    const labels = months.map(fmtMonth);

    const marketTotals = {};
    months.forEach(m => {
        marketTotals[m] = 0;
        Object.values(allMap.opMonthMap).forEach(mm => { marketTotals[m] += (mm[m] || 0); });
    });

    // Total Accesses (line)
    const accKey = prefix + "-accesses";
    if (bbCharts[accKey]) bbCharts[accKey].destroy();
    bbCharts[accKey] = new Chart(document.getElementById(accKey), {
        type: "line",
        data: {
            labels: labels,
            datasets: ops.map(op => ({
                label: op,
                data: months.map(m => (opMonthMap[op][m]) || 0),
                borderColor: OPERATOR_COLORS[op],
                backgroundColor: OPERATOR_COLORS[op] + "30",
                borderWidth: 2, fill: false, tension: 0.3,
                pointRadius: months.length > 24 ? 0 : 3, pointHoverRadius: 5,
            })),
        },
        options: chartOpts(v => (v / 1e6).toFixed(1) + "M", ctx => `${ctx.dataset.label}: ${(ctx.raw / 1e6).toFixed(2)}M`),
    });

    // Net Adds (bar)
    const netKey = prefix + "-netadds";
    if (bbCharts[netKey]) bbCharts[netKey].destroy();
    bbCharts[netKey] = new Chart(document.getElementById(netKey), {
        type: "bar",
        data: {
            labels: labels,
            datasets: ops.map(op => ({
                label: op,
                data: months.map((m, i) => {
                    if (i === 0) return 0;
                    return ((opMonthMap[op][m]) || 0) - ((opMonthMap[op][months[i - 1]]) || 0);
                }),
                backgroundColor: OPERATOR_COLORS[op] + "CC",
                borderColor: OPERATOR_COLORS[op], borderWidth: 1, borderRadius: 2,
            })),
        },
        options: chartOpts(
            v => (v / 1000).toFixed(0) + "k",
            ctx => { const v = ctx.raw; return `${ctx.dataset.label}: ${v >= 0 ? "+" : ""}${(v / 1000).toFixed(1)}k`; }
        ),
    });

    // Market Share (individual lines)
    const shareKey = prefix + "-share";
    if (bbCharts[shareKey]) bbCharts[shareKey].destroy();
    bbCharts[shareKey] = new Chart(document.getElementById(shareKey), {
        type: "line",
        data: {
            labels: labels,
            datasets: ops.map(op => ({
                label: op,
                data: months.map(m => {
                    const total = marketTotals[m];
                    return total > 0 ? ((opMonthMap[op][m] || 0) / total * 100) : 0;
                }),
                borderColor: OPERATOR_COLORS[op],
                backgroundColor: OPERATOR_COLORS[op] + "20",
                borderWidth: 2, fill: false, tension: 0.3,
                pointRadius: months.length > 24 ? 0 : 3, pointHoverRadius: 5,
            })),
        },
        options: {
            ...chartOpts(v => v.toFixed(0) + "%", ctx => `${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%`),
            scales: {
                x: { grid: { display: false }, ticks: { font: { size: 10 }, maxRotation: 45 } },
                y: { beginAtZero: true, grid: { color: "#f0f0f0" }, ticks: { font: { size: 10 }, callback: v => v + "%" } },
            },
        },
    });
}

function chartOpts(yTickCb, tooltipCb) {
    return {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
            legend: { position: "bottom", labels: { usePointStyle: true, padding: 14, font: { size: 11 } } },
            tooltip: { callbacks: { label: tooltipCb } },
        },
        scales: {
            x: { grid: { display: false }, ticks: { font: { size: 10 }, maxRotation: 45 } },
            y: { grid: { color: "#f0f0f0" }, ticks: { font: { size: 10 }, callback: yTickCb } },
        },
    };
}

initBroadband();
