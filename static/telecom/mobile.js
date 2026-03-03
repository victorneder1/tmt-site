// ── Mobile Dashboard ──

const MOB_TABLE_OPS = ["Vivo", "Claro", "TIM", "Brisanet", "Unifique"];
const MOB_CHART_OPS = ["Vivo", "Claro", "TIM"];

const MOB_COLORS = {
    "Vivo":     "#7B2D8E",
    "Claro":    "#E31E24",
    "TIM":      "#003399",
    "Brisanet": "#00A651",
    "Unifique": "#00BCD4",
    "Others":   "#999999",
};

const MOB_TABLE_MAX_MONTHS = 12;

let mobCharts = {};

async function initMobile() {
    const [months, states] = await Promise.all([
        fetch("/telecom/api/mobile/months").then(r => r.json()),
        fetch("/telecom/api/broadband/states").then(r => r.json()),
    ]);

    const ufSel = document.getElementById("mob-uf-select");
    while (ufSel.options.length > 1) ufSel.remove(1);
    states.forEach(s => {
        const opt = document.createElement("option");
        opt.value = s.code;
        opt.textContent = `${s.code} - ${s.name}`;
        ufSel.appendChild(opt);
    });

    const fromSel = document.getElementById("mob-from-select");
    const toSel = document.getElementById("mob-to-select");
    fromSel.innerHTML = "";
    toSel.innerHTML = "";
    months.forEach(m => {
        fromSel.appendChild(new Option(fmtMonth(m), m));
        toSel.appendChild(new Option(fmtMonth(m), m));
    });

    const defaultFrom = months.length >= 12 ? months[months.length - 12] : months[0];
    fromSel.value = defaultFrom;
    toSel.value = months[months.length - 1];

    ufSel.addEventListener("change", loadMobile);
    fromSel.addEventListener("change", loadMobile);
    toSel.addEventListener("change", loadMobile);

    await loadMobile();
}

async function loadMobile() {
    const uf = document.getElementById("mob-uf-select").value;
    const from = document.getElementById("mob-from-select").value;
    const to = document.getElementById("mob-to-select").value;

    const params = new URLSearchParams();
    if (uf) params.set("uf", uf);
    if (from) params.set("from", from);
    if (to) params.set("to", to);

    const tableParams = new URLSearchParams();
    if (uf) tableParams.set("uf", uf);

    const [data, tableData] = await Promise.all([
        fetch("/telecom/api/mobile?" + params).then(r => r.json()),
        fetch("/telecom/api/mobile?" + tableParams).then(r => r.json()),
    ]);
    const filtered = data.filter(d => d.segment === "Postpaid" || d.segment === "Prepaid");
    const tableFiltered = tableData.filter(d => d.segment === "Postpaid" || d.segment === "Prepaid");
    renderMobDashboard(filtered, tableFiltered);
}

function mobBuildMap(data, operators, segment) {
    const monthsSet = new Set();
    const opMonthMap = {};
    data.forEach(d => {
        if (!operators.includes(d.operator)) return;
        if (segment && d.segment !== segment) return;
        monthsSet.add(d.month);
        if (!opMonthMap[d.operator]) opMonthMap[d.operator] = {};
        opMonthMap[d.operator][d.month] = (opMonthMap[d.operator][d.month] || 0) + d.accesses;
    });
    return { months: Array.from(monthsSet).sort(), opMonthMap };
}

function fmtMonth(m) {
    const [y, mo] = m.split("-");
    const names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    return names[parseInt(mo) - 1] + "-" + y.slice(2);
}

function renderMobDashboard(data, tableData) {
    const allOps = [...MOB_TABLE_OPS, "Others"];

    const tablePostMap = mobBuildMap(tableData, allOps, "Postpaid");
    const tablePreMap = mobBuildMap(tableData, allOps, "Prepaid");
    renderMobMultiMonthTable(tablePostMap, tablePreMap);

    const allPostMap = mobBuildMap(data, allOps, "Postpaid");
    const allPreMap = mobBuildMap(data, allOps, "Prepaid");

    const postChartMap = mobBuildMap(data, MOB_CHART_OPS, "Postpaid");
    renderMobChartGroup("mob-post", postChartMap.months, MOB_CHART_OPS, postChartMap.opMonthMap, allPostMap);

    const preChartMap = mobBuildMap(data, MOB_CHART_OPS, "Prepaid");
    renderMobChartGroup("mob-pre", preChartMap.months, MOB_CHART_OPS, preChartMap.opMonthMap, allPreMap);
}

function renderMobMultiMonthTable(postMap, preMap) {
    const table = document.getElementById("mob-summary-table");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");

    const allMonths = postMap.months;
    const months = allMonths.slice(-MOB_TABLE_MAX_MONTHS);

    const postTotals = {};
    const preTotals = {};
    months.forEach(m => {
        postTotals[m] = 0;
        preTotals[m] = 0;
        Object.values(postMap.opMonthMap).forEach(mm => { postTotals[m] += (mm[m] || 0); });
        Object.values(preMap.opMonthMap).forEach(mm => { preTotals[m] += (mm[m] || 0); });
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

    const sortedPost = [...MOB_TABLE_OPS].filter(op => postMap.opMonthMap[op]).sort((a, b) => {
        return ((postMap.opMonthMap[b] && postMap.opMonthMap[b][lastMonth]) || 0) -
               ((postMap.opMonthMap[a] && postMap.opMonthMap[a][lastMonth]) || 0);
    });

    const sortedPre = [...MOB_TABLE_OPS].filter(op => preMap.opMonthMap[op]).sort((a, b) => {
        return ((preMap.opMonthMap[b] && preMap.opMonthMap[b][lastMonth]) || 0) -
               ((preMap.opMonthMap[a] && preMap.opMonthMap[a][lastMonth]) || 0);
    });

    addSegmentSection(tbody, "Postpaid (ex-M2M)", months, allMonths, sortedPost, postMap.opMonthMap, postTotals);
    addSegmentSection(tbody, "Prepaid", months, allMonths, sortedPre, preMap.opMonthMap, preTotals);
}

function addSegmentSection(tbody, segLabel, months, allMonths, operators, opMonthMap, totals) {
    // Segment header row (dark)
    const trSeg = document.createElement("tr");
    trSeg.className = "op-group-header";
    const tdSeg = document.createElement("td");
    tdSeg.textContent = segLabel;
    tdSeg.colSpan = months.length + 1;
    trSeg.appendChild(tdSeg);
    tbody.appendChild(trSeg);

    operators.forEach((op, opIdx) => {
        const isLastOp = opIdx === operators.length - 1;

        // Operator header row (light)
        const trHeader = document.createElement("tr");
        trHeader.className = "segment-label-row";
        const tdName = document.createElement("td");
        tdName.colSpan = months.length + 1;
        tdName.textContent = op;
        trHeader.appendChild(tdName);
        tbody.appendChild(trHeader);

        // Accesses
        const trAcc = document.createElement("tr");
        const tdAccL = document.createElement("td");
        tdAccL.textContent = "Accesses";
        tdAccL.className = "metric-label";
        trAcc.appendChild(tdAccL);
        months.forEach(m => {
            const td = document.createElement("td");
            const val = (opMonthMap[op] && opMonthMap[op][m]) || 0;
            td.textContent = (val / 1e6).toFixed(2) + "M";
            trAcc.appendChild(td);
        });
        tbody.appendChild(trAcc);

        // Net Adds
        const trNet = document.createElement("tr");
        const tdNetL = document.createElement("td");
        tdNetL.textContent = "Net Adds";
        tdNetL.className = "metric-label";
        trNet.appendChild(tdNetL);
        months.forEach(m => {
            const td = document.createElement("td");
            const allIdx = allMonths.indexOf(m);
            if (allIdx > 0) {
                const prevM = allMonths[allIdx - 1];
                const curr = (opMonthMap[op] && opMonthMap[op][m]) || 0;
                const prev = (opMonthMap[op] && opMonthMap[op][prevM]) || 0;
                const net = curr - prev;
                td.textContent = (net >= 0 ? "+" : "") + (net / 1000).toFixed(1) + "k";
                td.className = net >= 0 ? "val-positive" : "val-negative";
            } else {
                td.textContent = "-";
            }
            trNet.appendChild(td);
        });
        tbody.appendChild(trNet);

        // Market Share
        const trShare = document.createElement("tr");
        trShare.className = isLastOp ? "last-metric-row" : "";
        const tdShareL = document.createElement("td");
        tdShareL.textContent = "Mkt Share";
        tdShareL.className = "metric-label";
        trShare.appendChild(tdShareL);
        months.forEach(m => {
            const td = document.createElement("td");
            const val = (opMonthMap[op] && opMonthMap[op][m]) || 0;
            const total = totals[m] || 1;
            td.textContent = (val / total * 100).toFixed(1) + "%";
            trShare.appendChild(td);
        });
        tbody.appendChild(trShare);
    });
}

function renderMobChartGroup(prefix, months, operators, opMonthMap, allMap) {
    const ops = operators.filter(op => opMonthMap[op]);
    const labels = months.map(fmtMonth);

    const marketTotals = {};
    months.forEach(m => {
        marketTotals[m] = 0;
        Object.values(allMap.opMonthMap).forEach(mm => { marketTotals[m] += (mm[m] || 0); });
    });

    const accKey = prefix + "-accesses";
    if (mobCharts[accKey]) mobCharts[accKey].destroy();
    mobCharts[accKey] = new Chart(document.getElementById(accKey), {
        type: "line",
        data: {
            labels: labels,
            datasets: ops.map(op => ({
                label: op, data: months.map(m => (opMonthMap[op][m]) || 0),
                borderColor: MOB_COLORS[op], backgroundColor: MOB_COLORS[op] + "30",
                borderWidth: 2, fill: false, tension: 0.3,
                pointRadius: months.length > 24 ? 0 : 3, pointHoverRadius: 5,
            })),
        },
        options: mobChartOpts(v => (v / 1e6).toFixed(0) + "M", ctx => `${ctx.dataset.label}: ${(ctx.raw / 1e6).toFixed(2)}M`),
    });

    const netKey = prefix + "-netadds";
    if (mobCharts[netKey]) mobCharts[netKey].destroy();
    mobCharts[netKey] = new Chart(document.getElementById(netKey), {
        type: "bar",
        data: {
            labels: labels,
            datasets: ops.map(op => ({
                label: op,
                data: months.map((m, i) => {
                    if (i === 0) return 0;
                    return ((opMonthMap[op][m]) || 0) - ((opMonthMap[op][months[i - 1]]) || 0);
                }),
                backgroundColor: MOB_COLORS[op] + "CC",
                borderColor: MOB_COLORS[op], borderWidth: 1, borderRadius: 2,
            })),
        },
        options: mobChartOpts(
            v => (v / 1000).toFixed(0) + "k",
            ctx => { const v = ctx.raw; return `${ctx.dataset.label}: ${v >= 0 ? "+" : ""}${(v / 1000).toFixed(1)}k`; }
        ),
    });

    const shareKey = prefix + "-share";
    if (mobCharts[shareKey]) mobCharts[shareKey].destroy();
    mobCharts[shareKey] = new Chart(document.getElementById(shareKey), {
        type: "line",
        data: {
            labels: labels,
            datasets: ops.map(op => ({
                label: op,
                data: months.map(m => {
                    const total = marketTotals[m];
                    return total > 0 ? ((opMonthMap[op][m] || 0) / total * 100) : 0;
                }),
                borderColor: MOB_COLORS[op], backgroundColor: MOB_COLORS[op] + "20",
                borderWidth: 2, fill: false, tension: 0.3,
                pointRadius: months.length > 24 ? 0 : 3, pointHoverRadius: 5,
            })),
        },
        options: {
            ...mobChartOpts(v => v.toFixed(0) + "%", ctx => `${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%`),
            scales: {
                x: { grid: { display: false }, ticks: { font: { size: 10 }, maxRotation: 45 } },
                y: { beginAtZero: true, grid: { color: "#f0f0f0" }, ticks: { font: { size: 10 }, callback: v => v + "%" } },
            },
        },
    });
}

function mobChartOpts(yTickCb, tooltipCb) {
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
