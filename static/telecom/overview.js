// Anatel management overview: consolidated market charts built from existing APIs.

const OV_COLORS = {
    "Vivo": "#7B2D8E",
    "Claro": "#E31E24",
    "Nio": "#F5A623",
    "Oi": "#F5A623",
    "Brisanet": "#D96E20",
    "Giga+": "#0098DB",
    "Vero": "#D0388A",
    "Tecpar": "#2D5F2D",
    "Desktop": "#BE4040",
    "TIM": "#003399",
    "Unifique": "#2AACBF",
    "Starlink": "#1E1E1E",
    "Others": "#999999",
    "Postpaid": "#195AB4",
    "Prepaid": "#60A5FA",
    "FTTH": "#60A5FA",
    "Total": "#001F62",
};

const OV_BB_SHARE_OPS = ["Vivo", "Claro", "Nio", "Brisanet", "Giga+", "Vero", "Tecpar", "Desktop", "TIM", "Unifique", "Starlink", "Others"];
const OV_POSTPAID_OPS = ["Vivo", "Claro", "TIM", "Brisanet", "Unifique", "Others"];
const OV_PORT_OPS = ["Claro", "TIM", "Vivo", "Brisanet", "Unifique", "Oi"];

let ovCharts = {};
let ovAllMonths = [];

const IBGE_TO_UF = {
    "11":"RO","12":"AC","13":"AM","14":"RR","15":"PA","16":"AP","17":"TO",
    "21":"MA","22":"PI","23":"CE","24":"RN","25":"PB","26":"PE","27":"AL","28":"SE","29":"BA",
    "31":"MG","32":"ES","33":"RJ","35":"SP",
    "41":"PR","42":"SC","43":"RS",
    "50":"MS","51":"MT","52":"GO","53":"DF"
};

let ovBrazilGeo = null;
let ovRegionalData = { broadband: [], mobile: [], portability: [] };
let ovRegionalDataVersion = 0;
let ovStatePanelRenderKey = "";
let ovSelUf = "SP";
let ovLtmCtx = { bbRows: [], mobileRows: [], displayMonths: [] };
let ovBbLtmMode = "netadds";
let ovPostpaidLtmMode = "netadds";

const OV_MAP_METRICS = [
    { key: "bb_accesses",    label: "Broadband Accesses",     signed: false, needsCompany: false, fmt: "int" },
    { key: "bb_share",       label: "Broadband Market Share", signed: false, needsCompany: true,  fmt: "pct" },
    { key: "bb_netadds",     label: "Broadband Net Adds LTM", signed: true,  needsCompany: false, fmt: "int" },
    { key: "ftth_pen",       label: "FTTH Penetration",       signed: false, needsCompany: false, fmt: "pct" },
    { key: "postpaid_share", label: "Postpaid Market Share",  signed: false, needsCompany: true,  fmt: "pct" },
    { key: "portability",    label: "Net Portability LTM",    signed: true,  needsCompany: true,  fmt: "int" },
];
const OV_MAP_COMPANIES = ["All", "Vivo", "Claro", "TIM", "Nio", "Oi", "Brisanet", "Unifique", "Giga+", "Vero", "Tecpar", "Desktop", "Starlink"];
const OV_UF_FULL = {
    AC:"Acre", AL:"Alagoas", AM:"Amazonas", AP:"Amapá", BA:"Bahia", CE:"Ceará", DF:"Distrito Federal",
    ES:"Espírito Santo", GO:"Goiás", MA:"Maranhão", MG:"Minas Gerais", MS:"Mato Grosso do Sul",
    MT:"Mato Grosso", PA:"Pará", PB:"Paraíba", PE:"Pernambuco", PI:"Piauí", PR:"Paraná",
    RJ:"Rio de Janeiro", RN:"Rio Grande do Norte", RO:"Rondônia", RR:"Roraima", RS:"Rio Grande do Sul",
    SC:"Santa Catarina", SE:"Sergipe", SP:"São Paulo", TO:"Tocantins",
};

async function initAnatelOverview() {
    const [months, states] = await Promise.all([
        fetch("/telecom/api/broadband/months").then(r => r.json()),
        fetch("/telecom/api/broadband/states").then(r => r.json()),
    ]);

    ovAllMonths = months;

    const ufSel = document.getElementById("ov-uf-select");
    while (ufSel.options.length > 1) ufSel.remove(1);
    states.forEach(s => {
        const opt = document.createElement("option");
        opt.value = s.code;
        opt.textContent = `${s.code} - ${s.name}`;
        ufSel.appendChild(opt);
    });

    const fromSel = document.getElementById("ov-from-select");
    const toSel = document.getElementById("ov-to-select");
    fromSel.innerHTML = "";
    toSel.innerHTML = "";
    months.forEach(m => {
        fromSel.appendChild(new Option(ovFmtMonth(m), m));
        toSel.appendChild(new Option(ovFmtMonth(m), m));
    });

    fromSel.value = months[0];
    toSel.value = months[months.length - 1];

    ufSel.addEventListener("change", loadAnatelOverview);
    fromSel.addEventListener("change", loadAnatelOverview);
    toSel.addEventListener("change", loadAnatelOverview);

    // Regional View controls
    const metricSel = document.getElementById("ov-map-metric");
    metricSel.innerHTML = "";
    OV_MAP_METRICS.forEach(m => metricSel.appendChild(new Option(m.label, m.key)));
    metricSel.value = "bb_netadds";

    const compSel = document.getElementById("ov-map-company");
    compSel.innerHTML = "";
    OV_MAP_COMPANIES.forEach(c => compSel.appendChild(new Option(c === "All" ? "All operators" : c, c)));
    compSel.value = "All";

    metricSel.addEventListener("change", () => renderRegionalView().catch(e => console.error("[Map]", e)));
    compSel.addEventListener("change", () => renderRegionalView().catch(e => console.error("[Map]", e)));
    document.getElementById("ov-map-month").addEventListener("change", () => renderRegionalView().catch(e => console.error("[Map]", e)));

    document.querySelectorAll(".ov-chart-toggle button").forEach(btn => {
        btn.addEventListener("click", () => {
            const toggle = btn.closest(".ov-chart-toggle");
            toggle.querySelectorAll("button").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            const mode = btn.dataset.mode;
            if (toggle.dataset.target === "bb-ltm") {
                ovBbLtmMode = mode;
                renderBroadbandLtmChart(ovLtmCtx.bbRows, ovLtmCtx.displayMonths, mode);
            } else if (toggle.dataset.target === "postpaid-ltm") {
                ovPostpaidLtmMode = mode;
                renderPostpaidLtmChart(ovLtmCtx.mobileRows, ovLtmCtx.displayMonths, mode);
            }
        });
    });

    await loadAnatelOverview();
}

async function loadAnatelOverview() {
    const uf = document.getElementById("ov-uf-select").value;
    const from = document.getElementById("ov-from-select").value;
    const to = document.getElementById("ov-to-select").value;
    if (!from || !to) return;

    const fromIdx = ovAllMonths.indexOf(from);
    const extendedIdx = Math.max(0, fromIdx - 12);
    const extendedFrom = ovAllMonths[extendedIdx] || from;

    const baseParams = new URLSearchParams();
    if (uf) baseParams.set("uf", uf);
    baseParams.set("from", extendedFrom);
    baseParams.set("to", to);

    const ftthParams = new URLSearchParams(baseParams);
    ftthParams.set("tech", "FTTH");

    const regionalParams = new URLSearchParams();
    regionalParams.set("from", extendedFrom);
    regionalParams.set("to", to);

    let bbRows, ftthRows, mobileRows, portRows, regionalRows;
    try {
        [bbRows, ftthRows, mobileRows, portRows, regionalRows] = await Promise.all([
            fetch("/telecom/api/broadband?" + baseParams).then(r => r.json()),
            fetch("/telecom/api/broadband?" + ftthParams).then(r => r.json()),
            fetch("/telecom/api/mobile?" + baseParams).then(r => r.json()),
            fetch("/telecom/api/portability?" + baseParams).then(r => r.json()),
            fetch("/telecom/api/overview/regional?" + regionalParams).then(r => r.ok ? r.json() : { broadband: [], mobile: [], portability: [] }),
        ]);
    } catch (err) {
        console.error("[Overview] fetch error:", err);
        return;
    }

    const displayMonths = ovAllMonths.filter(m => m >= from && m <= to);

    ovRegionalData = {
        broadband: regionalRows.broadband || [],
        mobile: regionalRows.mobile || [],
        portability: regionalRows.portability || [],
    };
    ovRegionalDataVersion += 1;
    const monthSel = document.getElementById("ov-map-month");
    const prevMonth = monthSel.value;
    monthSel.innerHTML = "";
    displayMonths.forEach(m => monthSel.appendChild(new Option(ovFmtMonth(m), m)));
    monthSel.value = displayMonths.includes(prevMonth) ? prevMonth : displayMonths[displayMonths.length - 1];

    try {
        renderOverviewCharts({ bbRows, ftthRows, mobileRows, portRows, regionalRows, displayMonths, from, to });
    } catch (err) {
        console.error("[Overview] render error:", err);
    }
}

function renderOverviewCharts(ctx) {
    ovLtmCtx = { bbRows: ctx.bbRows, mobileRows: ctx.mobileRows, displayMonths: ctx.displayMonths };
    renderMarketSizeChart(ctx.bbRows, ctx.mobileRows, ctx.displayMonths);
    renderMobileMixChart(ctx.mobileRows, ctx.displayMonths);
    renderFtthChart(ctx.bbRows, ctx.ftthRows, ctx.displayMonths);
    renderBroadbandNetAddsChart(ctx.bbRows, ctx.displayMonths);
    renderBroadbandLtmChart(ctx.bbRows, ctx.displayMonths, ovBbLtmMode);
    renderBroadbandShareChangeChart(ctx.bbRows, ctx.displayMonths);
    renderPostpaidLtmChart(ctx.mobileRows, ctx.displayMonths, ovPostpaidLtmMode);
    renderPortabilityLtmChart(ctx.portRows, ctx.displayMonths);
    renderRegionalView().catch(err => console.error("[Map]", err));
    renderRegionalBroadbandLtmChart(ctx.regionalRows.broadband || [], ctx.to);
    renderRegionalPostpaidLtmChart(ctx.regionalRows.mobile || [], ctx.to);
}

function ovFmtMonth(m) {
    const [y, mo] = m.split("-");
    const names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    return names[parseInt(mo, 10) - 1] + "-" + y.slice(2);
}

function ovFmtMillions(v) {
    return (v / 1e6).toFixed(1) + "M";
}

function ovFmtThousands(v) {
    return (v / 1000).toFixed(0) + "k";
}

function ovMonthTotals(rows, valueField, filterFn) {
    const totals = {};
    rows.forEach(row => {
        if (filterFn && !filterFn(row)) return;
        totals[row.month] = (totals[row.month] || 0) + (row[valueField] || 0);
    });
    return totals;
}

function ovMonthsFromFirstData(months, totals) {
    const firstIdx = months.findIndex(m => (totals[m] || 0) > 0);
    return firstIdx >= 0 ? months.slice(firstIdx) : months;
}

function ovOperatorMonthMap(rows, operatorField, valueField, filterFn) {
    const map = {};
    rows.forEach(row => {
        if (filterFn && !filterFn(row)) return;
        const op = row[operatorField];
        if (!map[op]) map[op] = {};
        map[op][row.month] = (map[op][row.month] || 0) + (row[valueField] || 0);
    });
    return map;
}

function ovDestroy(chartId) {
    if (ovCharts[chartId]) {
        ovCharts[chartId].destroy();
    }
}

function ovSetFootnote(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
}

function ovSetTitle(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
}

function ovRenderStatePanelIfNeeded(uf, month, metric, company, ufData) {
    const nextKey = `${ovRegionalDataVersion}|${uf}|${month}`;
    if (nextKey === ovStatePanelRenderKey) return;
    ovStatePanelRenderKey = nextKey;
    ovRenderStatePanel(uf, month, metric, company, ufData);
}

function ovLtmStartMonth(toMonth) {
    const toIdx = ovAllMonths.indexOf(toMonth);
    return toIdx >= 12 ? ovAllMonths[toIdx - 12] : null;
}

function ovLineOptions(yTickCb, tooltipCb, extraScales) {
    const scales = {
        x: { grid: { display: false }, ticks: { font: { size: 12 }, maxRotation: 45 } },
        y: { grid: { color: "#f0f0f0" }, ticks: { font: { size: 12 }, callback: yTickCb } },
    };
    Object.assign(scales, extraScales || {});
    return {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
            legend: { position: "bottom", labels: { usePointStyle: true, padding: 14, font: { size: 13 } } },
            tooltip: { callbacks: { label: tooltipCb } },
        },
        scales,
    };
}

function ovCompanyLineOptions(yTickCb, tooltipCb, extraScales) {
    const options = ovLineOptions(yTickCb, tooltipCb, extraScales);
    options.plugins.legend.display = false;
    return options;
}

function ovLatestNumeric(data) {
    for (let i = data.length - 1; i >= 0; i--) {
        const v = data[i];
        if (typeof v === "number" && Number.isFinite(v)) return v;
    }
    return null;
}

function ovApplyTopCompanies(datasets, count, options) {
    const useAbs = options && options.absolute;
    const top = datasets
        .map(ds => ({ label: ds.label, value: ovLatestNumeric(ds.data) }))
        .filter(item => item.value !== null)
        .sort((a, b) => (useAbs ? Math.abs(b.value) - Math.abs(a.value) : b.value - a.value))
        .slice(0, count)
        .map(item => item.label);
    const fallback = top.length ? top : datasets.slice(0, count).map(ds => ds.label);
    const topSet = new Set(fallback);
    datasets.forEach(ds => {
        ds.hidden = !topSet.has(ds.label);
    });
}

function ovRenderCompanyButtons(containerId, chartId) {
    const container = document.getElementById(containerId);
    const chart = ovCharts[chartId];
    if (!container || !chart) return;
    container.innerHTML = "";

    chart.data.datasets.forEach((ds, idx) => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = chart.isDatasetVisible(idx) ? "active" : "";
        btn.style.setProperty("--company-color", ds.borderColor || OV_COLORS.Others);
        btn.innerHTML = `<span class="ov-company-dot"></span><span>${ds.label}</span>`;
        btn.addEventListener("click", () => {
            const nextVisible = !chart.isDatasetVisible(idx);
            chart.setDatasetVisibility(idx, nextVisible);
            btn.classList.toggle("active", nextVisible);
            chart.update();
        });
        container.appendChild(btn);
    });
}

function ovBarOptions(yTickCb, tooltipCb, stacked) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
            legend: { position: "bottom", labels: { usePointStyle: true, padding: 14, font: { size: 13 } } },
            tooltip: { callbacks: { label: tooltipCb } },
        },
        scales: {
            x: { stacked: !!stacked, grid: { display: false }, ticks: { font: { size: 12 }, maxRotation: 45 } },
            y: { stacked: !!stacked, grid: { color: "#f0f0f0" }, ticks: { font: { size: 12 }, callback: yTickCb } },
        },
    };
}

function renderMarketSizeChart(bbRows, mobileRows, months) {
    const totals = ovMonthTotals(bbRows, "accesses");
    const chartId = "ov-market-size-chart";
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "line",
        data: {
            labels: months.map(ovFmtMonth),
            datasets: [{
                label: "Broadband",
                data: months.map(m => totals[m] || null),
                borderColor: OV_COLORS.Total,
                backgroundColor: OV_COLORS.Total + "18",
                borderWidth: 2,
                fill: true,
                tension: 0.3,
                pointRadius: months.length > 24 ? 0 : 3,
            }],
        },
        options: ovLineOptions(v => ovFmtMillions(v), ctx => `Broadband: ${ovFmtMillions(ctx.raw)}`),
    });
}

function renderMobileMixChart(rows, months) {
    const postpaid = ovMonthTotals(rows, "accesses", r => r.segment === "Postpaid");
    const prepaid = ovMonthTotals(rows, "accesses", r => r.segment === "Prepaid");
    const segmentTotals = {};
    months.forEach(m => { segmentTotals[m] = (postpaid[m] || 0) + (prepaid[m] || 0); });
    const mobileMonths = ovMonthsFromFirstData(months, segmentTotals);
    const chartId = "ov-mobile-mix-chart";
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "bar",
        data: {
            labels: mobileMonths.map(ovFmtMonth),
            datasets: [
                {
                    label: "Postpaid",
                    data: mobileMonths.map(m => postpaid[m] || 0),
                    backgroundColor: OV_COLORS.Postpaid + "CC",
                    borderColor: OV_COLORS.Postpaid,
                    borderWidth: 1,
                    borderRadius: 2,
                },
                {
                    label: "Prepaid",
                    data: mobileMonths.map(m => prepaid[m] || 0),
                    backgroundColor: OV_COLORS.Prepaid + "CC",
                    borderColor: OV_COLORS.Prepaid,
                    borderWidth: 1,
                    borderRadius: 2,
                },
            ],
        },
        options: ovBarOptions(v => ovFmtMillions(v), ctx => `${ctx.dataset.label}: ${ovFmtMillions(ctx.raw)}`, true),
    });
}

function renderBroadbandTotalChart(rows, months) {
    const totals = ovMonthTotals(rows, "accesses");
    const chartId = "ov-bb-total-chart";
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "line",
        data: {
            labels: months.map(ovFmtMonth),
            datasets: [{
                label: "Broadband",
                data: months.map(m => totals[m] || 0),
                borderColor: OV_COLORS.Total,
                backgroundColor: OV_COLORS.Total + "18",
                borderWidth: 2,
                fill: true,
                tension: 0.3,
                pointRadius: months.length > 24 ? 0 : 3,
                pointHoverRadius: 5,
            }],
        },
        options: ovLineOptions(v => ovFmtMillions(v), ctx => `${ctx.dataset.label}: ${ovFmtMillions(ctx.raw)}`),
    });
}

function renderBroadbandNetAddsChart(rows, months) {
    const totals = ovMonthTotals(rows, "accesses");
    const chartId = "ov-bb-netadds-chart";
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "bar",
        data: {
            labels: months.map(ovFmtMonth),
            datasets: [{
                label: "Net Adds",
                data: months.map(m => {
                    const idx = ovAllMonths.indexOf(m);
                    const prev = idx > 0 ? ovAllMonths[idx - 1] : null;
                    return prev ? (totals[m] || 0) - (totals[prev] || 0) : 0;
                }),
                backgroundColor: OV_COLORS.Total + "CC",
                borderColor: OV_COLORS.Total,
                borderWidth: 1,
                borderRadius: 2,
            }],
        },
        options: ovBarOptions(v => ovFmtThousands(v), ctx => `Net Adds: ${ctx.raw >= 0 ? "+" : ""}${ovFmtThousands(ctx.raw)}`),
    });
}

function renderFtthChart(bbRows, ftthRows, months) {
    const bbTotals = ovMonthTotals(bbRows, "accesses");
    const ftthTotals = ovMonthTotals(ftthRows, "accesses");
    const ftthMonths = ovMonthsFromFirstData(months, ftthTotals);
    const chartId = "ov-ftth-chart";
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "bar",
        data: {
            labels: ftthMonths.map(ovFmtMonth),
            datasets: [
                {
                    label: "FTTH Accesses",
                    data: ftthMonths.map(m => ftthTotals[m] || 0),
                    backgroundColor: OV_COLORS.FTTH + "99",
                    borderColor: OV_COLORS.FTTH,
                    borderWidth: 1,
                    borderRadius: 2,
                    yAxisID: "y",
                    type: "bar",
                },
                {
                    label: "Penetration",
                    data: ftthMonths.map(m => bbTotals[m] ? +(((ftthTotals[m] || 0) / bbTotals[m]) * 100).toFixed(1) : null),
                    borderColor: OV_COLORS.Total,
                    backgroundColor: OV_COLORS.Total + "18",
                    borderWidth: 2,
                    fill: false,
                    tension: 0.3,
                    pointRadius: ftthMonths.length > 24 ? 0 : 3,
                    yAxisID: "y1",
                    type: "line",
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: "index", intersect: false },
            plugins: {
                legend: { position: "bottom", labels: { usePointStyle: true, padding: 14, font: { size: 13 } } },
                tooltip: { callbacks: { label: ctx => ctx.dataset.yAxisID === "y1" ? `${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%` : `${ctx.dataset.label}: ${ovFmtMillions(ctx.raw)}` } },
            },
            scales: {
                x: { grid: { display: false }, ticks: { font: { size: 12 }, maxRotation: 45 } },
                y: { grid: { color: "#f0f0f0" }, ticks: { font: { size: 12 }, callback: v => ovFmtMillions(v) } },
                y1: { position: "right", grid: { display: false }, ticks: { font: { size: 12 }, callback: v => v + "%" } },
            },
        },
    });
}

function renderMobileSegmentChart(rows, months) {
    const postpaid = ovMonthTotals(rows, "accesses", r => r.segment === "Postpaid");
    const prepaid = ovMonthTotals(rows, "accesses", r => r.segment === "Prepaid");
    const segmentTotals = {};
    months.forEach(m => {
        segmentTotals[m] = (postpaid[m] || 0) + (prepaid[m] || 0);
    });
    const mobileMonths = ovMonthsFromFirstData(months, segmentTotals);
    const chartId = "ov-mobile-segment-chart";
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "bar",
        data: {
            labels: mobileMonths.map(ovFmtMonth),
            datasets: [
                {
                    label: "Postpaid",
                    data: mobileMonths.map(m => postpaid[m] || 0),
                    backgroundColor: OV_COLORS.Postpaid + "CC",
                    borderColor: OV_COLORS.Postpaid,
                    borderWidth: 1,
                    borderRadius: 2,
                },
                {
                    label: "Prepaid",
                    data: mobileMonths.map(m => prepaid[m] || 0),
                    backgroundColor: OV_COLORS.Prepaid + "CC",
                    borderColor: OV_COLORS.Prepaid,
                    borderWidth: 1,
                    borderRadius: 2,
                },
            ],
        },
        options: ovBarOptions(v => ovFmtMillions(v), ctx => `${ctx.dataset.label}: ${ovFmtMillions(ctx.raw)}`, true),
    });
}

function renderBroadbandShareChart(rows, months) {
    const totals = ovMonthTotals(rows, "accesses");
    const opMap = ovOperatorMonthMap(rows, "operator", "accesses");
    const chartId = "ov-bb-share-chart";
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "line",
        data: {
            labels: months.map(ovFmtMonth),
            datasets: OV_BB_SHARE_OPS.filter(op => opMap[op]).map(op => ({
                label: op,
                data: months.map(m => totals[m] ? +(((opMap[op][m] || 0) / totals[m]) * 100).toFixed(1) : 0),
                borderColor: OV_COLORS[op] || OV_COLORS.Others,
                backgroundColor: (OV_COLORS[op] || OV_COLORS.Others) + "18",
                borderWidth: 2,
                fill: false,
                tension: 0.3,
                pointRadius: months.length > 24 ? 0 : 3,
            })),
        },
        options: ovLineOptions(v => v + "%", ctx => `${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%`),
    });
}

function renderPostpaidShareChart(rows, months) {
    const postRows = rows.filter(r => r.segment === "Postpaid");
    const totals = ovMonthTotals(postRows, "accesses");
    const opMap = ovOperatorMonthMap(postRows, "operator", "accesses");
    const postpaidMonths = ovMonthsFromFirstData(months, totals);
    const chartId = "ov-postpaid-share-chart";
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "line",
        data: {
            labels: postpaidMonths.map(ovFmtMonth),
            datasets: OV_POSTPAID_OPS.filter(op => opMap[op]).map(op => ({
                label: op,
                data: postpaidMonths.map(m => totals[m] ? +(((opMap[op][m] || 0) / totals[m]) * 100).toFixed(1) : 0),
                borderColor: OV_COLORS[op] || OV_COLORS.Others,
                backgroundColor: (OV_COLORS[op] || OV_COLORS.Others) + "18",
                borderWidth: 2,
                fill: false,
                tension: 0.3,
                pointRadius: postpaidMonths.length > 24 ? 0 : 3,
            })),
        },
        options: ovLineOptions(v => v + "%", ctx => `${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%`),
    });
}

function renderBroadbandLtmChart(rows, displayMonths, mode) {
    mode = mode || "netadds";
    const subs = mode === "subscribers";
    const opMap = ovOperatorMonthMap(rows, "operator", "accesses");
    ovSetFootnote("ov-bb-ltm-note", "");
    ovSetTitle("ov-bb-ltm-title", `Broadband ${subs ? "Subscribers" : "Net Adds LTM"} by Operator`);
    const ops = OV_BB_SHARE_OPS.filter(op => opMap[op]);
    const months = subs ? displayMonths.filter(m => ops.some(op => opMap[op][m] !== undefined)) : displayMonths;
    const chartId = "ov-bb-ltm-chart";
    const datasets = ops.map(op => ({
        label: op,
        data: months.map(m => {
            if (subs) return opMap[op][m] !== undefined ? opMap[op][m] : null;
            const mIdx = ovAllMonths.indexOf(m);
            const prev = mIdx >= 12 ? ovAllMonths[mIdx - 12] : null;
            if (!prev || opMap[op][prev] === undefined) return null;
            return (opMap[op][m] || 0) - opMap[op][prev];
        }),
        borderColor: OV_COLORS[op] || OV_COLORS.Others,
        backgroundColor: (OV_COLORS[op] || OV_COLORS.Others) + "18",
        borderWidth: 2,
        fill: false,
        tension: 0.3,
        pointRadius: months.length > 24 ? 0 : 3,
    }));
    ovApplyTopCompanies(datasets, 3);
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "line",
        data: {
            labels: months.map(ovFmtMonth),
            datasets,
        },
        options: ovCompanyLineOptions(
            v => subs ? ovFmtMillions(v) : ovFmtThousands(v),
            ctx => `${ctx.dataset.label}: ${subs ? ovFmtMillions(ctx.raw) : (ctx.raw >= 0 ? "+" : "") + ovFmtThousands(ctx.raw)}`
        ),
    });
    ovRenderCompanyButtons("ov-bb-ltm-buttons", chartId);
}

function renderBroadbandShareChangeChart(rows, months) {
    const totals = ovMonthTotals(rows, "accesses");
    const opMap = ovOperatorMonthMap(rows, "operator", "accesses");
    ovSetFootnote("ov-bb-share-change-note", "");
    const chartId = "ov-bb-share-change-chart";
    const datasets = OV_BB_SHARE_OPS.filter(op => opMap[op]).map(op => ({
        label: op,
        data: months.map(m => totals[m] ? +(((opMap[op][m] || 0) / totals[m]) * 100).toFixed(1) : null),
        borderColor: OV_COLORS[op] || OV_COLORS.Others,
        backgroundColor: (OV_COLORS[op] || OV_COLORS.Others) + "18",
        borderWidth: 2,
        fill: false,
        tension: 0.3,
        pointRadius: months.length > 24 ? 0 : 3,
    }));
    ovApplyTopCompanies(datasets, 3);
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "line",
        data: {
            labels: months.map(ovFmtMonth),
            datasets,
        },
        options: ovCompanyLineOptions(v => v + "%", ctx => `${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%`),
    });
    ovRenderCompanyButtons("ov-bb-share-change-buttons", chartId);
}

function renderPostpaidLtmChart(rows, displayMonths, mode) {
    mode = mode || "netadds";
    const subs = mode === "subscribers";
    const postRows = rows.filter(r => r.segment === "Postpaid");
    const opMap = ovOperatorMonthMap(postRows, "operator", "accesses");
    ovSetFootnote("ov-postpaid-ltm-note", "");
    ovSetTitle("ov-postpaid-ltm-title", `Postpaid ${subs ? "Subscribers" : "Net Adds LTM"} by Operator`);
    const ops = OV_POSTPAID_OPS.filter(op => opMap[op]);
    const validMonths = displayMonths.filter(m => {
        if (subs) return ops.some(op => opMap[op][m] !== undefined);
        const mIdx = ovAllMonths.indexOf(m);
        const prev = mIdx >= 12 ? ovAllMonths[mIdx - 12] : null;
        return prev && ops.some(op => opMap[op][prev] !== undefined);
    });
    const chartId = "ov-postpaid-ltm-chart";
    const datasets = ops.map(op => ({
        label: op,
        data: validMonths.map(m => {
            if (subs) return opMap[op][m] !== undefined ? opMap[op][m] : null;
            const mIdx = ovAllMonths.indexOf(m);
            const prev = mIdx >= 12 ? ovAllMonths[mIdx - 12] : null;
            if (!prev || opMap[op][prev] === undefined) return null;
            return (opMap[op][m] || 0) - opMap[op][prev];
        }),
        borderColor: OV_COLORS[op] || OV_COLORS.Others,
        backgroundColor: (OV_COLORS[op] || OV_COLORS.Others) + "18",
        borderWidth: 2,
        fill: false,
        tension: 0.3,
        pointRadius: validMonths.length > 24 ? 0 : 3,
    }));
    ovApplyTopCompanies(datasets, 3);
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "line",
        data: {
            labels: validMonths.map(ovFmtMonth),
            datasets,
        },
        options: ovCompanyLineOptions(
            v => subs ? ovFmtMillions(v) : ovFmtThousands(v),
            ctx => `${ctx.dataset.label}: ${subs ? ovFmtMillions(ctx.raw) : (ctx.raw >= 0 ? "+" : "") + ovFmtThousands(ctx.raw)}`
        ),
    });
    ovRenderCompanyButtons("ov-postpaid-ltm-buttons", chartId);
}

function renderPortabilityLtmChart(rows, displayMonths) {
    const monthlyNet = {};
    rows.forEach(row => {
        if (!monthlyNet[row.month]) monthlyNet[row.month] = {};
        monthlyNet[row.month][row.receiver] = (monthlyNet[row.month][row.receiver] || 0) + row.quantity;
        monthlyNet[row.month][row.giver] = (monthlyNet[row.month][row.giver] || 0) - row.quantity;
    });
    ovSetFootnote("ov-port-ltm-note", "");
    const ops = OV_PORT_OPS.filter(op => rows.some(r => r.receiver === op || r.giver === op));
    const validMonths = displayMonths.filter(m => {
        const mIdx = ovAllMonths.indexOf(m);
        if (mIdx < 0) return false;
        const ltmMonths = ovAllMonths.slice(Math.max(0, mIdx - 11), mIdx + 1);
        return ltmMonths.some(lm => monthlyNet[lm]);
    });
    const chartId = "ov-port-ltm-chart";
    const datasets = ops.map(op => ({
        label: op,
        data: validMonths.map(m => {
            const mIdx = ovAllMonths.indexOf(m);
            const ltmMonths = ovAllMonths.slice(Math.max(0, mIdx - 11), mIdx + 1);
            return ltmMonths.reduce((sum, lm) => sum + ((monthlyNet[lm] && monthlyNet[lm][op]) || 0), 0);
        }),
        borderColor: OV_COLORS[op] || OV_COLORS.Others,
        backgroundColor: (OV_COLORS[op] || OV_COLORS.Others) + "18",
        borderWidth: 2,
        fill: false,
        tension: 0.3,
        pointRadius: validMonths.length > 24 ? 0 : 3,
    }));
    ovApplyTopCompanies(datasets, 3, { absolute: true });
    ovDestroy(chartId);
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "line",
        data: {
            labels: validMonths.map(ovFmtMonth),
            datasets,
        },
        options: ovCompanyLineOptions(
            v => ovFmtThousands(v),
            ctx => `${ctx.dataset.label}: ${ctx.raw >= 0 ? "+" : ""}${ctx.raw.toLocaleString("en-US")}`
        ),
    });
    ovRenderCompanyButtons("ov-port-ltm-buttons", chartId);
}

function renderRegionalBroadbandLtmChart(rows, toMonth) {
    const prevMonth = ovLtmStartMonth(toMonth);
    ovSetTitle("ov-regional-bb-ltm-title", `Broadband Net Adds LTM (${ovFmtMonth(toMonth)})`);
    const ufMap = ovRegionalMonthMap(rows, "accesses");
    const values = Object.keys(ufMap)
        .filter(uf => prevMonth)
        .map(uf => ({ uf, value: (ufMap[uf][toMonth] || 0) - (ufMap[uf][prevMonth] || 0) }))
        .filter(item => item.value !== 0)
        .sort((a, b) => b.value - a.value)
        .slice(0, 12);

    renderRegionalLtmBar("ov-regional-bb-ltm-chart", values, ctx => `${ctx.label}: ${ctx.raw >= 0 ? "+" : ""}${ovFmtThousands(ctx.raw)}`);
}

function renderRegionalPostpaidLtmChart(rows, toMonth) {
    const prevMonth = ovLtmStartMonth(toMonth);
    ovSetTitle("ov-regional-postpaid-ltm-title", `Postpaid Net Adds LTM (${ovFmtMonth(toMonth)})`);
    const postRows = rows.filter(r => r.segment === "Postpaid");
    const ufMap = ovRegionalMonthMap(postRows, "accesses");
    const values = Object.keys(ufMap)
        .filter(uf => prevMonth)
        .map(uf => ({ uf, value: (ufMap[uf][toMonth] || 0) - (ufMap[uf][prevMonth] || 0) }))
        .filter(item => item.value !== 0)
        .sort((a, b) => b.value - a.value)
        .slice(0, 12);

    renderRegionalLtmBar("ov-regional-postpaid-ltm-chart", values, ctx => `${ctx.label}: ${ctx.raw >= 0 ? "+" : ""}${ovFmtThousands(ctx.raw)}`);
}

function ovRegionalMonthMap(rows, valueField) {
    const map = {};
    rows.forEach(row => {
        const uf = row.UF;
        if (!map[uf]) map[uf] = {};
        map[uf][row.month] = (map[uf][row.month] || 0) + (row[valueField] || 0);
    });
    return map;
}

function renderRegionalLtmBar(chartId, values, tooltipCb) {
    ovDestroy(chartId);
    const opts = ovHorizontalBarOptions(tooltipCb);
    opts.layout = { padding: { right: 48 } };
    opts.plugins.datalabels = {
        anchor: "end",
        align: "end",
        font: { size: 11, weight: "600" },
        color: "#3a3f4b",
        formatter: v => (v >= 0 ? "+" : "") + ovFmtThousands(v),
    };
    ovCharts[chartId] = new Chart(document.getElementById(chartId), {
        type: "bar",
        data: {
            labels: values.map(v => v.uf),
            datasets: [{
                label: "Net Adds LTM",
                data: values.map(v => v.value),
                backgroundColor: values.map(v => v.value >= 0 ? OV_COLORS.Total + "CC" : "#9BAABFCC"),
                borderColor: values.map(v => v.value >= 0 ? OV_COLORS.Total : "#7C8BA1"),
                borderWidth: 1,
                borderRadius: 2,
            }],
        },
        options: opts,
        plugins: [ChartDataLabels],
    });
}

async function fetchBrazilGeo() {
    if (ovBrazilGeo) return ovBrazilGeo;
    const r = await fetch("/static/telecom/brazil-states.geojson?v=3");
    ovBrazilGeo = await r.json();
    return ovBrazilGeo;
}

// Blue sequential palette (matches site identity) for unsigned metrics.
const OV_MAP_BLUE = d3.interpolateRgbBasis(["#eff6ff", "#bfdbfe", "#60a5fa", "#2563eb", "#001F62"]);
// Diverging red↔navy for signed metrics (net adds, share change, portability).
const OV_MAP_DIV = d3.interpolateRgbBasis(["#c0392b", "#e8c5be", "#f4f4f4", "#9bb0d6", "#001F62"]);

async function renderRegionalView() {
    const geo = await fetchBrazilGeo().catch(() => null);
    if (!geo) return;

    const metricKey = document.getElementById("ov-map-metric").value;
    const company = document.getElementById("ov-map-company").value;
    const month = document.getElementById("ov-map-month").value;
    const metric = OV_MAP_METRICS.find(m => m.key === metricKey) || OV_MAP_METRICS[0];
    if (!month) return;

    const compLabel = company === "All" ? "All operators" : company;
    document.getElementById("ov-map-title").textContent =
        `${metric.label}${metric.needsCompany || company !== "All" ? " — " + compLabel : ""} (${ovFmtMonth(month)})`;

    // "All operators" + a share metric → market-share-leader map + national pie
    if (company === "All" && (metric.key === "bb_share" || metric.key === "postpaid_share")) {
        const ufLeader = ovComputeUfLeader(metric.key, month);
        const inset = { kind: "donut", cap: "Brazil — share by operator", data: ovNationalShareData(metric.key, month) };
        ovDrawLeaderMap(geo, ufLeader, inset);
        ovRenderStatePanelIfNeeded(ovSelUf, month, metric, company, {});
        return;
    }

    // "All operators" + net portability → portability-winner map + national bars
    if (company === "All" && metric.key === "portability") {
        const ufLeader = ovComputePortLeader(month);
        const inset = { kind: "bars", cap: "Brazil — net portability LTM", data: ovNationalPortRanked(month) };
        ovDrawLeaderMap(geo, ufLeader, inset);
        ovRenderStatePanelIfNeeded(ovSelUf, month, metric, company, {});
        return;
    }

    const needsCompanyButAll = metric.needsCompany && company === "All";
    const ufData = needsCompanyButAll ? {} : ovComputeUfMetric(metric.key, company, month);

    ovDrawMap(geo, ufData, metric, needsCompanyButAll);
    ovRenderStatePanelIfNeeded(ovSelUf, month, metric, company, ufData);
}

// Leading operator per UF by market share (excludes the aggregate "Others").
function ovComputeUfLeader(metricKey, month) {
    const rows = metricKey === "postpaid_share"
        ? ovRegionalData.mobile.filter(r => r.segment === "Postpaid")
        : ovRegionalData.broadband;
    const byUfOp = {}, totByUf = {};
    rows.forEach(r => {
        if (r.month !== month) return;
        byUfOp[r.UF] = byUfOp[r.UF] || {};
        byUfOp[r.UF][r.operator] = (byUfOp[r.UF][r.operator] || 0) + r.accesses;
        totByUf[r.UF] = (totByUf[r.UF] || 0) + r.accesses;
    });
    const out = {};
    Object.keys(byUfOp).forEach(uf => {
        const ops = byUfOp[uf];
        let best = null, bestV = -1;
        Object.keys(ops).forEach(op => { if (op !== "Others" && ops[op] > bestV) { bestV = ops[op]; best = op; } });
        out[uf] = { op: best, label: (totByUf[uf] ? (bestV / totByUf[uf] * 100).toFixed(1) : "0") + "%" };
    });
    return out;
}

function ovNationalShareData(metricKey, month) {
    const rows = metricKey === "postpaid_share"
        ? ovRegionalData.mobile.filter(r => r.segment === "Postpaid")
        : ovRegionalData.broadband;
    return ovShareDonutData(rows, month, "accesses", 5);
}

// Per-UF winner of net portability over the trailing 12 months (top net gainer).
function ovComputePortLeader(month) {
    const mIdx = ovAllMonths.indexOf(month);
    const ltm = new Set(ovAllMonths.slice(Math.max(0, mIdx - 11), mIdx + 1));
    const net = {};
    ovRegionalData.portability.forEach(r => {
        if (!ltm.has(r.month)) return;
        net[r.UF] = net[r.UF] || {};
        net[r.UF][r.receiver] = (net[r.UF][r.receiver] || 0) + r.quantity;
        net[r.UF][r.giver] = (net[r.UF][r.giver] || 0) - r.quantity;
    });
    const out = {};
    Object.keys(net).forEach(uf => {
        const ops = net[uf];
        let best = null, bestV = -Infinity;
        Object.keys(ops).forEach(op => { if (op !== "Others" && ops[op] > bestV) { bestV = ops[op]; best = op; } });
        out[uf] = { op: best, label: (bestV >= 0 ? "+" : "") + ovFmtFull(bestV) };
    });
    return out;
}

// National net portability LTM per operator, ranked (for the inset bars).
function ovNationalPortRanked(month) {
    const mIdx = ovAllMonths.indexOf(month);
    const ltm = new Set(ovAllMonths.slice(Math.max(0, mIdx - 11), mIdx + 1));
    const net = {};
    ovRegionalData.portability.forEach(r => {
        if (!ltm.has(r.month)) return;
        net[r.receiver] = (net[r.receiver] || 0) + r.quantity;
        net[r.giver] = (net[r.giver] || 0) - r.quantity;
    });
    return Object.entries(net).filter(([o]) => o !== "Others").sort((a, b) => b[1] - a[1]);
}

// ── Per-UF metric computation ──
function ovComputeUfMetric(metricKey, company, month) {
    const bb = ovRegionalData.broadband;
    const mob = ovRegionalData.mobile;
    const port = ovRegionalData.portability;
    const mIdx = ovAllMonths.indexOf(month);
    const prev = mIdx >= 12 ? ovAllMonths[mIdx - 12] : null;
    const ufData = {};
    const ufs = [...new Set(bb.map(r => r.UF))];

    if (metricKey === "bb_accesses" || metricKey === "bb_share" || metricKey === "bb_netadds" || metricKey === "ftth_pen") {
        // build bb maps: total per UF/month and per UF/op/month (+ftth)
        const tot = {}, comp = {}, totFtth = {}, compFtth = {};
        bb.forEach(r => {
            tot[r.UF] = tot[r.UF] || {}; tot[r.UF][r.month] = (tot[r.UF][r.month] || 0) + r.accesses;
            totFtth[r.UF] = totFtth[r.UF] || {}; totFtth[r.UF][r.month] = (totFtth[r.UF][r.month] || 0) + (r.ftth_accesses || 0);
            if (r.operator === company) {
                comp[r.UF] = comp[r.UF] || {}; comp[r.UF][r.month] = (comp[r.UF][r.month] || 0) + r.accesses;
                compFtth[r.UF] = compFtth[r.UF] || {}; compFtth[r.UF][r.month] = (compFtth[r.UF][r.month] || 0) + (r.ftth_accesses || 0);
            }
        });
        ufs.forEach(uf => {
            const T = (tot[uf] || {}), C = (comp[uf] || {}), TF = (totFtth[uf] || {}), CF = (compFtth[uf] || {});
            if (metricKey === "bb_accesses") {
                ufData[uf] = company === "All" ? (T[month] || 0) : (C[month] || 0);
            } else if (metricKey === "bb_share") {
                ufData[uf] = T[month] ? +(((C[month] || 0) / T[month]) * 100).toFixed(1) : 0;
            } else if (metricKey === "bb_netadds") {
                const src = company === "All" ? T : C;
                ufData[uf] = prev !== null && src[prev] !== undefined ? (src[month] || 0) - src[prev] : null;
            } else { // ftth_pen
                const num = company === "All" ? (TF[month] || 0) : (CF[month] || 0);
                const den = company === "All" ? (T[month] || 0) : (C[month] || 0);
                ufData[uf] = den ? +((num / den) * 100).toFixed(1) : 0;
            }
        });
    } else if (metricKey === "postpaid_share") {
        const tot = {}, comp = {};
        mob.filter(r => r.segment === "Postpaid").forEach(r => {
            tot[r.UF] = tot[r.UF] || {}; tot[r.UF][r.month] = (tot[r.UF][r.month] || 0) + r.accesses;
            if (r.operator === company) {
                comp[r.UF] = comp[r.UF] || {}; comp[r.UF][r.month] = (comp[r.UF][r.month] || 0) + r.accesses;
            }
        });
        ufs.forEach(uf => {
            const T = (tot[uf] || {}), C = (comp[uf] || {});
            ufData[uf] = T[month] ? +(((C[month] || 0) / T[month]) * 100).toFixed(1) : 0;
        });
    } else if (metricKey === "portability") {
        // net portability for `company` over the trailing 12 months ending at `month`
        const ltm = new Set(ovAllMonths.slice(Math.max(0, mIdx - 11), mIdx + 1));
        const net = {};
        port.forEach(r => {
            if (!ltm.has(r.month)) return;
            if (r.receiver === company) net[r.UF] = (net[r.UF] || 0) + r.quantity;
            if (r.giver === company) net[r.UF] = (net[r.UF] || 0) - r.quantity;
        });
        ufs.forEach(uf => { ufData[uf] = net[uf] || 0; });
    }
    return ufData;
}

// ── Map drawing ──
function ovDrawMap(geo, ufData, metric, disabled) {
    const container = document.getElementById("ov-brazil-map-container");
    container.innerHTML = "";
    const W = container.getBoundingClientRect().width || 700;
    const H = container.getBoundingClientRect().height || 480;
    const svg = d3.select(container).append("svg").attr("width", W).attr("height", H);

    if (disabled) {
        svg.append("text").attr("x", W / 2).attr("y", H / 2).attr("text-anchor", "middle")
            .attr("font-size", 14).attr("fill", "#8a93a6")
            .text("Select a company for this metric");
        return;
    }

    const projection = d3.geoIdentity().reflectY(true).fitExtent([[10, 10], [W - 10, H - 46]], geo);
    const pathGen = d3.geoPath().projection(projection);

    const vals = Object.values(ufData).filter(v => v !== null && v !== undefined);
    let colorFn;
    if (metric.signed) {
        const clamp = ovMapClampDomain(vals);
        const scale = d3.scaleDiverging(t => OV_MAP_DIV(t)).domain([-clamp, 0, clamp]).clamp(true);
        colorFn = uf => (ufData[uf] === null || ufData[uf] === undefined) ? "#ddd" : scale(ufData[uf]);
    } else {
        const [lo, hi] = ovMapSeqDomain(vals);
        const scale = d3.scaleSequential(t => OV_MAP_BLUE(t)).domain([lo, hi]).clamp(true);
        colorFn = uf => (ufData[uf] === null || ufData[uf] === undefined) ? "#ddd" : scale(ufData[uf]);
    }

    const tip = d3.select(container).append("div").attr("class", "ov-map-tip")
        .style("position", "absolute").style("background", "rgba(0,31,98,0.9)").style("color", "#fff")
        .style("padding", "5px 10px").style("border-radius", "4px").style("font-size", "12px")
        .style("pointer-events", "none").style("display", "none").style("white-space", "nowrap");

    svg.selectAll("path")
        .data(geo.features)
        .join("path")
        .attr("d", pathGen)
        .attr("fill", d => colorFn(d.properties.uf))
        .attr("stroke", d => d.properties.uf === ovSelUf ? "#001F62" : "#7a8aa0")
        .attr("stroke-width", d => d.properties.uf === ovSelUf ? 2.2 : 0.6)
        .style("cursor", "pointer")
        .on("mouseover", function (event, d) {
            const uf = d.properties.uf;
            const v = ufData[uf];
            tip.style("display", "block").html(`<b>${uf}</b> ${ovFmtMetricVal(v, metric)}`);
            d3.select(this).attr("stroke", "#001F62").attr("stroke-width", 2.2);
        })
        .on("mousemove", event => {
            const rect = container.getBoundingClientRect();
            tip.style("left", (event.clientX - rect.left + 14) + "px")
               .style("top", (event.clientY - rect.top - 36) + "px");
        })
        .on("mouseout", function (event, d) {
            tip.style("display", "none");
            const sel = d.properties.uf === ovSelUf;
            d3.select(this).attr("stroke", sel ? "#001F62" : "#7a8aa0").attr("stroke-width", sel ? 2.2 : 0.6);
        })
        .on("click", (event, d) => {
            ovSelUf = d.properties.uf;
            renderRegionalView().catch(e => console.error("[Map]", e));
        });

    ovDrawLegend(svg, W, H, vals, metric);
}

function ovDrawLegend(svg, W, H, vals, metric) {
    const legG = svg.append("g").attr("transform", `translate(16, ${H - 34})`);
    const gradId = "ov-map-grad-" + Math.random().toString(36).slice(2);
    const defs = svg.append("defs");
    const grad = defs.append("linearGradient").attr("id", gradId);
    const interp = metric.signed ? OV_MAP_DIV : OV_MAP_BLUE;
    [0, 0.25, 0.5, 0.75, 1].forEach(t => {
        grad.append("stop").attr("offset", `${t * 100}%`).attr("stop-color", interp(t));
    });
    legG.append("rect").attr("width", 150).attr("height", 10).attr("fill", `url(#${gradId})`);
    let lo, hi;
    if (metric.signed) {
        const clamp = ovMapClampDomain(vals);
        const over = vals.some(v => Math.abs(v) > clamp);
        lo = `-${ovFmtMetricNum(clamp, metric)}${over ? "+" : ""}`;
        hi = `+${ovFmtMetricNum(clamp, metric)}${over ? "+" : ""}`;
        legG.append("text").attr("x", 75).attr("y", 24).attr("text-anchor", "middle").attr("font-size", 10).attr("fill", "#777").text("0");
    } else {
        const [d0, d1] = ovMapSeqDomain(vals);
        const overLo = vals.some(v => v < d0);
        const overHi = vals.some(v => v > d1);
        lo = `${overLo ? "≤" : ""}${ovFmtMetricNum(d0, metric)}`;
        hi = `${ovFmtMetricNum(d1, metric)}${overHi ? "+" : ""}`;
    }
    legG.append("text").attr("x", 0).attr("y", 24).attr("font-size", 10).attr("fill", "#777").text(lo);
    legG.append("text").attr("x", 150).attr("y", 24).attr("text-anchor", "end").attr("font-size", 10).attr("fill", "#777").text(hi);
}

// "All operators" view: color each state by its leading operator, with a
// national operator-share donut inset (whose legend doubles as the map key).
function ovDrawLeaderMap(geo, ufLeader, inset) {
    const container = document.getElementById("ov-brazil-map-container");
    container.innerHTML = "";
    const W = container.getBoundingClientRect().width || 700;
    const H = container.getBoundingClientRect().height || 480;
    const svg = d3.select(container).append("svg").attr("width", W).attr("height", H);
    const projection = d3.geoIdentity().reflectY(true).fitExtent([[10, 10], [W - 10, H - 10]], geo);
    const pathGen = d3.geoPath().projection(projection);

    const colorFn = uf => (ufLeader[uf] && ufLeader[uf].op) ? (OV_COLORS[ufLeader[uf].op] || "#999") : "#ddd";

    const tip = d3.select(container).append("div").attr("class", "ov-map-tip")
        .style("position", "absolute").style("background", "rgba(0,31,98,0.9)").style("color", "#fff")
        .style("padding", "5px 10px").style("border-radius", "4px").style("font-size", "12px")
        .style("pointer-events", "none").style("display", "none").style("white-space", "nowrap");

    svg.selectAll("path")
        .data(geo.features)
        .join("path")
        .attr("d", pathGen)
        .attr("fill", d => colorFn(d.properties.uf))
        .attr("stroke", d => d.properties.uf === ovSelUf ? "#001F62" : "#ffffff")
        .attr("stroke-width", d => d.properties.uf === ovSelUf ? 2.2 : 0.6)
        .style("cursor", "pointer")
        .on("mouseover", function (event, d) {
            const uf = d.properties.uf, l = ufLeader[uf];
            tip.style("display", "block").html(`<b>${uf}</b> ${l && l.op ? l.op + " " + l.label : "n/a"}`);
            d3.select(this).attr("stroke", "#001F62").attr("stroke-width", 2.2);
        })
        .on("mousemove", event => {
            const rect = container.getBoundingClientRect();
            tip.style("left", (event.clientX - rect.left + 14) + "px").style("top", (event.clientY - rect.top - 36) + "px");
        })
        .on("mouseout", function (event, d) {
            tip.style("display", "none");
            const sel = d.properties.uf === ovSelUf;
            d3.select(this).attr("stroke", sel ? "#001F62" : "#ffffff").attr("stroke-width", sel ? 2.2 : 0.6);
        })
        .on("click", (event, d) => {
            ovSelUf = d.properties.uf;
            renderRegionalView().catch(e => console.error("[Map]", e));
        });

    // National inset (donut for share, bars for portability) — doubles as key.
    const wrap = document.createElement("div");
    wrap.className = "ov-nat-donut-inset" + (inset.kind === "bars" ? " ov-nat-bars" : "");
    wrap.innerHTML = `<div class="ov-nat-donut-cap">${inset.cap}</div><div class="ov-nat-donut-canvas"><canvas id="ov-nat-inset"></canvas></div>`;
    container.appendChild(wrap);
    if (inset.kind === "bars") ovMiniPortBars("ov-nat-inset", inset.data);
    else ovMiniDonut("ov-nat-inset", inset.data);
}

// ── State info panel ──
function ovRenderStatePanel(uf, month, metric, company, ufData) {
    const el = document.getElementById("ov-state-panel");
    if (!el) return;
    const bb = ovRegionalData.broadband.filter(r => r.UF === uf);
    const mob = ovRegionalData.mobile.filter(r => r.UF === uf);
    const port = ovRegionalData.portability.filter(r => r.UF === uf);
    const mIdx = ovAllMonths.indexOf(month);
    const prev = mIdx >= 12 ? ovAllMonths[mIdx - 12] : null;

    // Active metric value + national rank
    let metricHtml = "";
    if (!(metric.needsCompany && company === "All")) {
        const v = ufData[uf];
        const ranked = Object.entries(ufData).filter(([, x]) => x !== null && x !== undefined).sort((a, b) => b[1] - a[1]);
        const rank = ranked.findIndex(([u]) => u === uf) + 1;
        metricHtml = `<div class="ov-state-metric-box">
            <div class="ov-state-metric-label">${metric.label}${company !== "All" ? " — " + company : ""}</div>
            <div><span class="ov-state-metric-value">${ovFmtMetricVal(v, metric)}</span>
            <span class="ov-state-metric-rank">${rank ? "#" + rank + " of " + ranked.length : ""}</span></div>
        </div>`;
    }

    // Broadband block
    const bbTot = ovSumAt(bb, month, "accesses");
    const bbPrevTot = prev !== null ? ovSumAt(bb, prev, "accesses") : null;
    const bbNet = bbPrevTot !== null ? bbTot - bbPrevTot : null;
    const ftthTot = ovSumAt(bb, month, "ftth_accesses");
    const ftthPen = bbTot ? (ftthTot / bbTot * 100) : 0;
    const bbTop = ovTopOps(bb, month, "accesses", bbTot, 3);

    // Mobile block
    const postTot = ovSumAt(mob.filter(r => r.segment === "Postpaid"), month, "accesses");
    const preTot = ovSumAt(mob.filter(r => r.segment === "Prepaid"), month, "accesses");
    const mobTot = postTot + preTot;
    const postMix = mobTot ? (postTot / mobTot * 100) : 0;
    const postTop = ovTopOps(mob.filter(r => r.segment === "Postpaid"), month, "accesses", postTot, 3);

    // Portability LTM winners/losers
    const ltm = new Set(ovAllMonths.slice(Math.max(0, mIdx - 11), mIdx + 1));
    const portNet = {};
    port.forEach(r => {
        if (!ltm.has(r.month)) return;
        portNet[r.receiver] = (portNet[r.receiver] || 0) + r.quantity;
        portNet[r.giver] = (portNet[r.giver] || 0) - r.quantity;
    });
    const portRanked = Object.entries(portNet).filter(([o]) => o !== "Others").sort((a, b) => b[1] - a[1]);
    const winner = portRanked[0], loser = portRanked[portRanked.length - 1];

    el.innerHTML = `
        <div class="ov-state-head">
            <span class="ov-state-name">${OV_UF_FULL[uf] || uf}</span>
            <span class="ov-state-month">${ovFmtMonth(month)}</span>
        </div>
        ${metricHtml}
        <div class="ov-state-block">
            <h4>Broadband</h4>
            <div class="ov-state-chart-row">
                <div class="ov-state-chart-box">
                    <div class="ov-state-row"><span class="lbl">Total accesses</span><span class="val">${ovFmtFull(bbTot)}</span></div>
                    <div class="ov-state-row"><span class="lbl">Net adds LTM</span><span class="val ${bbNet > 0 ? "pos" : bbNet < 0 ? "neg" : ""}">${bbNet === null ? "n/a" : (bbNet >= 0 ? "+" : "") + ovFmtFull(bbNet)}</span></div>
                    <div class="ov-state-chart-cap">Market share</div>
                    <div class="ov-mini-donut"><canvas id="ov-mini-bb-donut"></canvas></div>
                </div>
                <div class="ov-state-chart-box"><div class="ov-state-chart-cap">FTTH penetration — ${ftthPen.toFixed(1)}%</div><div class="ov-mini-spark"><canvas id="ov-mini-ftth-spark"></canvas></div></div>
            </div>
        </div>
        <div class="ov-state-block">
            <div class="ov-state-chart-row">
                <div class="ov-state-chart-box">
                    <h4>Mobile</h4>
                    <div class="ov-state-row"><span class="lbl">Total mobile</span><span class="val">${ovFmtFull(mobTot)}</span></div>
                    <div class="ov-state-row"><span class="lbl">Postpaid mix</span><span class="val">${postMix.toFixed(1)}%</span></div>
                    <div class="ov-state-chart-cap">Postpaid market share</div>
                    <div class="ov-mini-donut"><canvas id="ov-mini-postpaid-donut"></canvas></div>
                </div>
                <div class="ov-state-chart-box">
                    <h4>Portability</h4>
                    <div class="ov-state-chart-cap">Net portability LTM</div>
                    <div class="ov-mini-portbars"><canvas id="ov-mini-port-bars"></canvas></div>
                </div>
            </div>
        </div>
    `;

    ovRenderPanelCharts(uf, month, mIdx, bb, mob, bbTop, postTop, portRanked);
}

let ovPanelCharts = {};
function ovDestroyPanelChart(id) { if (ovPanelCharts[id]) { ovPanelCharts[id].destroy(); delete ovPanelCharts[id]; } }

function ovRenderPanelCharts(uf, month, mIdx, bb, mob, bbTop, postTop, portRanked) {
    // 1. Broadband market share donut (top 4 + Others)
    ovMiniDonut("ov-mini-bb-donut", ovShareDonutData(bb, month, "accesses", 4));
    // 2. FTTH penetration sparkline (last 36 months)
    const months = ovAllMonths.slice(Math.max(0, mIdx - 35), mIdx + 1);
    const ftthSeries = months.map(m => {
        const t = ovSumAt(bb, m, "accesses"), f = ovSumAt(bb, m, "ftth_accesses");
        return t ? +(f / t * 100).toFixed(1) : null;
    });
    ovMiniSpark("ov-mini-ftth-spark", months, ftthSeries, "%");
    // 3. Postpaid market share donut
    ovMiniDonut("ov-mini-postpaid-donut", ovShareDonutData(mob.filter(r => r.segment === "Postpaid"), month, "accesses", 4));
    // 4. Net portability LTM by operator — mini horizontal bars
    ovMiniPortBars("ov-mini-port-bars", portRanked.slice(0, 6));
}

function ovShareDonutData(rows, month, field, topN) {
    const by = {};
    rows.forEach(r => { if (r.month === month) by[r.operator] = (by[r.operator] || 0) + (r[field] || 0); });
    // "Others" is a real Anatel operator — keep it out of the top-N ranking and
    // always fold it into a single combined Others bucket with the long tail.
    const named = Object.entries(by).filter(([op]) => op !== "Others").sort((a, b) => b[1] - a[1]);
    const top = named.slice(0, topN);
    let othersVal = (by["Others"] || 0) + named.slice(topN).reduce((s, [, v]) => s + v, 0);
    const labels = top.map(([op]) => op);
    const data = top.map(([, v]) => v);
    const colors = top.map(([op]) => OV_COLORS[op] || "#999");
    if (othersVal > 0) { labels.push("Others"); data.push(othersVal); colors.push("#c4c9d4"); }
    return { labels, data, colors };
}

function ovMiniDonut(id, d) {
    ovDestroyPanelChart(id);
    const el = document.getElementById(id);
    if (!el || !d.data.length) return;
    const total = d.data.reduce((a, b) => a + b, 0);
    ovPanelCharts[id] = new Chart(el, {
        type: "doughnut",
        data: { labels: d.labels, datasets: [{ data: d.data, backgroundColor: d.colors, borderWidth: 1, borderColor: "#fff" }] },
        options: {
            responsive: true, maintainAspectRatio: false, cutout: "58%",
            plugins: {
                legend: { position: "right", labels: { boxWidth: 9, font: { size: 11 }, padding: 6, usePointStyle: true } },
                tooltip: { callbacks: { label: c => `${c.label}: ${total ? (c.raw / total * 100).toFixed(1) : 0}%` } },
                datalabels: { display: false },
            },
        },
    });
}

function ovMiniSpark(id, months, series, suffix) {
    ovDestroyPanelChart(id);
    const el = document.getElementById(id);
    if (!el) return;
    ovPanelCharts[id] = new Chart(el, {
        type: "line",
        data: {
            labels: months.map(ovFmtMonth),
            datasets: [{
                data: series, borderColor: OV_COLORS.Total, backgroundColor: OV_COLORS.Total + "1A",
                borderWidth: 2, fill: true, tension: 0.35, pointRadius: 0, spanGaps: true,
            }],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: "index", intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: { callbacks: { title: items => items[0].label, label: c => (c.raw === null ? "n/a" : c.raw + (suffix || "")) } },
                datalabels: { display: false },
            },
            scales: {
                x: {
                    display: true,
                    grid: { display: false },
                    ticks: { font: { size: 9 }, color: "#9aa3b2", maxRotation: 0, autoSkip: true, maxTicksLimit: 4 },
                },
                y: { display: false, grace: "10%" },
            },
        },
    });
}

function ovMiniPortBars(id, ranked) {
    ovDestroyPanelChart(id);
    const el = document.getElementById(id);
    if (!el || !ranked.length) return;
    const labels = ranked.map(([op]) => op);
    const data = ranked.map(([, v]) => v);
    ovPanelCharts[id] = new Chart(el, {
        type: "bar",
        data: {
            labels,
            datasets: [{
                data,
                backgroundColor: data.map(v => v >= 0 ? OV_COLORS.Total + "CC" : "#c0392bCC"),
                borderColor: data.map(v => v >= 0 ? OV_COLORS.Total : "#c0392b"),
                borderWidth: 1, borderRadius: 2,
            }],
        },
        options: {
            indexAxis: "y", responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: { callbacks: { label: c => `${c.label}: ${c.raw >= 0 ? "+" : ""}${ovFmtFull(c.raw)}` } },
                datalabels: { display: false },
            },
            scales: {
                x: { grid: { color: "#f0f0f0" }, ticks: { font: { size: 10 }, callback: v => ovFmtThousands(v) } },
                y: { grid: { display: false }, ticks: { font: { size: 11 } } },
            },
        },
    });
}

function ovSumAt(rows, month, field) {
    return rows.reduce((s, r) => r.month === month ? s + (r[field] || 0) : s, 0);
}
function ovTopOps(rows, month, field, total, n) {
    const by = {};
    rows.forEach(r => { if (r.month === month) by[r.operator] = (by[r.operator] || 0) + (r[field] || 0); });
    return Object.entries(by).sort((a, b) => b[1] - a[1]).slice(0, n)
        .map(([op, v]) => ({ op, share: total ? v / total * 100 : 0 }));
}

function ovFmtMetricVal(v, metric) {
    if (v === null || v === undefined) return "n/a";
    if (metric.fmt === "pct") return v.toFixed(1) + "%";
    return (v >= 0 ? "" : "") + ovFmtFull(v);
}
function ovFmtMetricNum(v, metric) {
    if (metric.fmt === "pct") return v.toFixed(0) + "%";
    return ovFmtThousands(v);
}
function ovFmtFull(v) {
    return Math.round(v).toLocaleString("en-US");
}

// 80th-percentile symmetric clamp so one outlier UF doesn't flatten the palette.
function ovMapClampDomain(vals) {
    if (!vals.length) return 1;
    const sorted = vals.map(Math.abs).sort((a, b) => a - b);
    return sorted[Math.floor(sorted.length * 0.8)] || sorted[sorted.length - 1] || 1;
}
// Sequential domain spanning the p5–p95 range of the actual data, so clustered
// metrics (e.g. FTTH penetration mostly 60–85%) spread across the full palette
// instead of all saturating at the top.
function ovMapSeqDomain(vals) {
    if (!vals.length) return [0, 1];
    const s = [...vals].sort((a, b) => a - b);
    const lo = s[Math.floor(s.length * 0.05)];
    const hi = s[Math.ceil(s.length * 0.95) - 1];
    return lo === hi ? [lo, lo + 1] : [lo, hi];
}

function ovHorizontalBarOptions(tooltipCb, xTickCb) {
    return {
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { display: false },
            tooltip: { callbacks: { label: tooltipCb } },
        },
        scales: {
            x: {
                grid: { color: "#f0f0f0" },
                ticks: { font: { size: 12 }, callback: xTickCb || (v => ovFmtThousands(v)) },
            },
            y: {
                grid: { display: false },
                ticks: { font: { size: 12 } },
            },
        },
    };
}
