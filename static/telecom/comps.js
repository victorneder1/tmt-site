// Consolidated Telecom comps dashboard: Big Telcos Data + Anatel.

let compsPayload = null;
let compsCharts = {};
let compsAnatelRows = null;
let compsPeriodMode = "quarter";


const COMPS_OPS = ["Vivo", "TIM", "Claro"];
const BRAZIL_TELCO_FINANCIAL_OPS = ["Vivo", "TIM", "Claro"];
const COMPS_SHARE_OPS = ["Vivo", "TIM", "Claro", "Others"];
const ISP_COMPANIES = ["Brisanet", "Unifique", "Desktop", "Vero"];
const BRAZIL_FINANCIAL_COMPANIES = new Set(["Vivo", "TIM", "Claro", "Desktop", "Brisanet", "Unifique", "Vero"]);
const ISP_START_PERIOD = "1Q21";
const DEFAULT_QUARTER_START = "1Q24";
const TELCOS_OVERVIEW_START = "1Q24";
const DEFAULT_YEAR_START = "2022";
const COMPS_DEFAULT_MONTH_START = "2025-01";
const COMPANY_DISPLAY_ORDER = ["Vivo", "TIM", "Claro", "AMX", "Entel", "TEO", "Megacable", "Brisanet", "Unifique", "Desktop", "Vero"];
const BENCHMARK_COMPANIES = new Set([
    "Median",
    "Telecom Integrated",
    "US Median",
    "European Median",
    "Asian Median",
    "Inflation",
    "Exchange Rate",
]);
const COMPS_COLORS = {
    Vivo: "#7B2D8E",
    TIM: "#003399",
    Claro: "#E31E24",
    AMX: "#E31E24",
    Brisanet: "#FF8000",
    "Giga+": "#0098DB",
    Vero: "#E91E8C",
    Tecpar: "#2D5F2D",
    Desktop: "#D32F2F",
    Unifique: "#00BCD4",
    Others: "#9BAABF",
};
const COMBO_LINE_COLOR = "#123C69";
const COMPANY_VIEW_PRIMARY_COLOR = COMPS_COLORS.Vivo;
const COMPANY_ARPU_COLORS = {
    ARPU: COMPS_COLORS.Vivo,
    Postpaid: "#195AB4",
    Prepaid: "#C7782A",
};

const VALUATION_METRICS = ["EV/Sales", "EV/EBITDA", "P/E", "Dividend Yield", "FCFE Yield", "EV/OpFCF"];
const VALUATION_YEARS = ["2026", "2027"];
const VALUATION_GROUPS = [
    {
        label: "LatAm Telcos",
        medianLabel: "LatAm Telcos Median",
        names: ["Telefonica Brasil", "TIM Brasil", "AMX", "Entel", "Megacable", "TEO"],
        aliases: {
            "Telefonica Brasil": ["Telefonica Brasil", "Vivo"],
            "TIM Brasil": ["TIM Brasil", "TIM"],
        },
    },
    {
        label: "Brazil ISPs",
        medianLabel: "Brazil ISPs Median",
        names: ["Unifique", "Brisanet", "Desktop"],
    },
];
const GLOBAL_REGION_GROUPS = [
    {
        label: "US Median",
        companies: ["T-Mobile US Inc", "Verizon Communications Inc", "AT&T Inc", "Comcast Corp", "EchoStar Corp"],
    },
    {
        label: "Europe Median",
        companies: ["Deutsche Telekom AG", "Orange SA", "Telefonica SA", "Telecom Italia SpA/Milano", "Vodafone Group PLC", "Telenor ASA", "BT Group PLC"],
    },
    {
        label: "Asia Median",
        companies: ["SoftBank Group Corp", "China Mobile Ltd", "Bharti Airtel Ltd", "NTT Inc", "Telstra Group Ltd", "Chunghwa Telecom Co Ltd", "Singapore Telecommunications Ltd"],
    },
];

async function initComps() {
    const response = await fetch("/telecom/api/comps");
    compsPayload = await response.json();
    if (compsPayload.error) return;
    updateCompsLastUpdated();

    populateCompanySelect();
    populateExcelRangeSelects();
    document.getElementById("comps-big-telcos-btn").addEventListener("click", showCompsConsolidated);
    document.getElementById("comps-telcos-overview-btn").addEventListener("click", showTelcosOverview);
    document.getElementById("comps-isps-btn").addEventListener("click", showCompsISPs);
    document.querySelectorAll(".comps-quarter-toggle").forEach(button => {
        button.addEventListener("click", () => setCompsPeriodMode("quarter"));
    });
    document.querySelectorAll(".comps-year-toggle").forEach(button => {
        button.addEventListener("click", () => setCompsPeriodMode("year"));
    });
    document.getElementById("comps-company-select").addEventListener("change", handleCompanySelection);
    document.getElementById("comps-excel-from-select").addEventListener("change", renderAllFinancialViews);
    document.getElementById("comps-excel-to-select").addEventListener("change", renderAllFinancialViews);
    if (document.getElementById("comps-anatel-from-select")) {
        document.getElementById("comps-anatel-from-select").addEventListener("change", renderAnatelCharts);
        document.getElementById("comps-anatel-to-select").addEventListener("change", renderAnatelCharts);
    }
    ["vivo-financial-from-select", "vivo-financial-to-select"].forEach(id => {
        document.getElementById(id).addEventListener("change", renderCompanyFinancialCharts);
    });
    ["comps-isp-from-select", "comps-isp-to-select"].forEach(id => {
        document.getElementById(id).addEventListener("change", renderISPOverview);
    });
    ["vivo-operational-from-select", "vivo-operational-to-select", "vivo-ftth-toggle"].forEach(id => {
        document.getElementById(id).addEventListener("change", renderCompanyOperationalCharts);
    });

    renderValuationTable();
    renderAllFinancialViews();
    populateISPRangeSelects();
    renderISPOverview();
    if (document.getElementById("comps-bb-share-chart")) await renderAnatelCharts();
}

function updateCompsLastUpdated() {
    const el = document.getElementById("last-updated");
    if (!el) return;
    const lastModified = compsPayload.last_modified || compsPayload.global_telecom_comps?.last_modified;
    if (!lastModified) return;
    el.textContent = `Last Update: ${String(lastModified).slice(0, 10)}`;
}

function setCompsPeriodMode(mode) {
    compsPeriodMode = mode;
    document.querySelectorAll(".comps-quarter-toggle").forEach(button => button.classList.toggle("active", mode === "quarter"));
    document.querySelectorAll(".comps-year-toggle").forEach(button => button.classList.toggle("active", mode === "year"));
    populateExcelRangeSelects();
    populateISPRangeSelects();
    const company = document.getElementById("comps-company-select").value;
    if (company) populateCompanyFinancialRange(getCompanyConfig(company));
    renderAllFinancialViews();
    renderISPOverview();
    if (company) renderCompanyFinancialCharts();
}

function populateExcelRangeSelects() {
    const sourcePeriods = getPrimaryExcelPeriods();
    const periods = compsPeriodMode === "year" ? annualPeriodsFrom(sourcePeriods) : sourcePeriods;
    const from = document.getElementById("comps-excel-from-select");
    const to = document.getElementById("comps-excel-to-select");
    from.innerHTML = "";
    to.innerHTML = "";
    periods.forEach(period => {
        from.appendChild(new Option(fmtMonth(period), period));
        to.appendChild(new Option(fmtMonth(period), period));
    });
    const preferred = compsPeriodMode === "year" ? DEFAULT_YEAR_START : DEFAULT_QUARTER_START;
    from.value = periods.includes(preferred) ? preferred : periods[Math.max(0, periods.length - 24)] || "";
    to.value = periods[periods.length - 1] || "";
}

function getPrimaryExcelPeriods() {
    const candidates = [
        findSection("Service Revenue", "Service Revenue"),
        findSection("ARPU", "ARPU (R$)"),
        findSection("Big telcos", "Capex-to-sales"),
    ];
    const section = candidates.find(Boolean);
    return section ? section.periods : [];
}

function periodYear(period) {
    const text = String(period || "");
    const quarterMatch = text.match(/^[1-4]Q(\d{2}|\d{4})$/);
    if (quarterMatch) {
        const year = quarterMatch[1];
        return year.length === 2 ? "20" + year : year;
    }
    const yearMatch = text.match(/^(\d{4})/);
    return yearMatch ? yearMatch[1] : null;
}

function isQuarterPeriod(period) {
    return /^[1-4]Q(\d{2}|\d{4})$/.test(String(period || ""));
}

function annualPeriodsFrom(periods) {
    return Array.from(new Set(periods.map(periodYear).filter(year => year && Number(year) <= 2025))).sort();
}

function annualAggregationFor(title) {
    const label = String(title || "").toLowerCase();
    if (label.includes("growth") || label.includes("margin") || label.includes("arpu") ||
        label.includes("share") || label.includes("capex-to-sales") || label.includes("nd/")) {
        return "avg";
    }
    return "sum";
}

function annualizeSection(section, aggregation) {
    const years = annualPeriodsFrom(section.periods);
    const series = section.series.map(serie => ({
        ...serie,
        values: years.map(year => {
            const values = section.periods
                .map((period, idx) => periodYear(period) === year ? serie.values[idx] : null)
                .filter(value => typeof value === "number");
            if (!values.length) return null;
            return aggregation === "avg"
                ? values.reduce((sum, value) => sum + value, 0) / values.length
                : values.reduce((sum, value) => sum + value, 0);
        }),
    }));
    return { ...section, periods: years, series };
}

function annualGrowthSection(baseSection, title) {
    const annual = annualizeSection(baseSection, annualAggregationFor(baseSection.title));
    return {
        ...annual,
        title,
        series: annual.series.map(serie => ({
            ...serie,
            values: serie.values.map((value, idx) => {
                const previous = idx > 0 ? serie.values[idx - 1] : null;
                if (typeof value !== "number" || typeof previous !== "number" || previous === 0) return null;
                return value / previous - 1;
            }),
        })),
    };
}

function sectionForCurrentPeriod(section, aggregation) {
    if (!section || compsPeriodMode !== "year") return section;
    return annualizeSection(section, aggregation || annualAggregationFor(section.title));
}

function populateCompanySelect() {
    const select = document.getElementById("comps-company-select");
    select.innerHTML = "";
    select.appendChild(new Option("Select a specific company", ""));
    getSelectableCompanies().forEach(name => select.appendChild(new Option(companyDisplayName(name), name)));
}

function getSelectableCompanies() {
    const companies = new Set();
    Object.values(compsPayload.sheets || {}).forEach(sections => {
        sections.forEach(section => {
            section.series.forEach(serie => {
                const company = canonicalSelectableCompany(String(serie.company || "").trim());
                if (company && !BENCHMARK_COMPANIES.has(company)) companies.add(company);
            });
        });
    });
    (compsPayload.summary?.rows || []).forEach(row => {
        const company = canonicalSelectableCompany(String(row.company || "").trim());
        if (company && !row.is_benchmark && !BENCHMARK_COMPANIES.has(company)) companies.add(company);
    });
    return Array.from(companies).sort((a, b) => {
        const ia = COMPANY_DISPLAY_ORDER.indexOf(a);
        const ib = COMPANY_DISPLAY_ORDER.indexOf(b);
        if (ia !== -1 || ib !== -1) return (ia === -1 ? 999 : ia) - (ib === -1 ? 999 : ib);
        return companyDisplayName(a).localeCompare(companyDisplayName(b));
    });
}

function canonicalSelectableCompany(company) {
    return {
        "Telefonica Brasil": "Vivo",
        "Telefônica Brasil": "Vivo",
        "TIM Brasil": "TIM",
    }[company] || company;
}

async function handleCompanySelection() {
    const value = document.getElementById("comps-company-select").value;
    if (!value) {
        showCompsConsolidated();
        return;
    }
    showFinancialView("company");
    document.getElementById("comps-company-placeholder").style.display = "none";
    document.getElementById("comps-vivo-view").style.display = "block";
    await renderCompanyPage(value);
}

function showCompsConsolidated() {
    document.getElementById("comps-company-select").value = "";
    showFinancialView("brazil");
}

function showTelcosOverview() {
    document.getElementById("comps-company-select").value = "";
    showFinancialView("overview");
    renderTelcosOverviewTables();
}

function showCompsISPs() {
    document.getElementById("comps-company-select").value = "";
    showFinancialView("isps");
    renderISPOverview();
}

function showFinancialView(activeView) {
    const buttonByView = {
        brazil: "comps-big-telcos-btn",
        overview: "comps-telcos-overview-btn",
        isps: "comps-isps-btn",
    };
    Object.values(buttonByView).forEach(id => document.getElementById(id).classList.remove("active"));
    if (buttonByView[activeView]) document.getElementById(buttonByView[activeView]).classList.add("active");

    const sectionByView = {
        brazil: "comps-consolidated-view",
        overview: "comps-telcos-overview-view",
        isps: "comps-isps-view",
        company: "comps-company-view",
    };
    Object.values(sectionByView).forEach(id => document.getElementById(id).style.display = "none");
    document.getElementById(sectionByView[activeView]).style.display = "block";
}

function getCompanyOperator(company) {
    if (company === "Telefonica Brasil" || company === "Vivo") return "Vivo";
    if (company === "TIM Brasil" || company === "TIM") return "TIM";
    if (company === "Claro") return "Claro";
    if (company === "AMX") return "AMX";
    return compsPayload.company_operator_map?.[company] || company;
}

function destroyChart(id) {
    if (compsCharts[id]) compsCharts[id].destroy();
}

function fmtMonth(m) {
    const text = String(m || "");
    if (text.includes("-")) {
        const [y, mo] = text.split("-");
        const names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
        return names[parseInt(mo, 10) - 1] + "-" + y.slice(2);
    }
    return text;
}

function fmtVal(value, metric) {
    const label = String(metric || "").toLowerCase();
    const isMultiple = label.includes("ev/") || label.includes("p/e") || label.includes("multiple");
    if (value === null || value === undefined || value === "") return "n.a.";
    if (typeof value === "string") return value;
    const isPct = label.includes("yield") || label.includes("growth") || label.includes("margin") ||
        label.includes("share") || label.includes("capex");
    if (isMultiple) return value < 0 ? "n.a." : value.toLocaleString("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + "x";
    if (isPct) return (value * 100).toFixed(1) + "%";
    if (Math.abs(value) >= 1000) return value.toLocaleString("en-US", { maximumFractionDigits: 0 });
    return value.toLocaleString("en-US", { maximumFractionDigits: 1 });
}

function renderValuationTable() {
    const table = document.getElementById("comps-valuation-table");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");

    thead.innerHTML = "";
    const row1 = document.createElement("tr");
    row1.appendChild(th("Company", 2));
    row1.appendChild(th("Market Cap", 2));
    VALUATION_METRICS.forEach((metric, metricIndex) => {
        const header = th(metric, 1, 2);
        header.classList.add("metric-group-header", `metric-band-${metricIndex % 2}`);
        row1.appendChild(header);
    });
    thead.appendChild(row1);

    const row2 = document.createElement("tr");
    VALUATION_METRICS.forEach((metric, metricIndex) => {
        VALUATION_YEARS.forEach((year, yearIndex) => {
            const yearHeader = th(year + "E");
            yearHeader.classList.add(`metric-band-${metricIndex % 2}`);
            if (yearIndex === 0) yearHeader.classList.add("metric-start");
            row2.appendChild(yearHeader);
        });
    });
    thead.appendChild(row2);

    tbody.innerHTML = "";
    const localCompanies = compsPayload.valuation_2026_2027.companies || [];
    const globalCompanies = compsPayload.global_telecom_comps?.companies || [];

    VALUATION_GROUPS.forEach(group => {
        const rows = group.names.map(name => findValuationCompany(localCompanies, group.aliases?.[name] || [name]) || {
            company: name,
            market_cap: null,
            market_cap_currency: "",
            values: {},
        });
        appendValuationGroup(tbody, group.label, rows, group.medianLabel);
    });

    appendValuationSpacer(tbody);
    appendGlobalValuationGroup(tbody, "Global Telcos", globalCompanies);
}

function appendValuationGroup(tbody, label, companies, medianLabel) {
    const groupRow = document.createElement("tr");
    groupRow.className = "comps-group-row";
    const groupCell = td(label);
    groupCell.colSpan = 2 + VALUATION_METRICS.length * VALUATION_YEARS.length;
    groupRow.appendChild(groupCell);
    tbody.appendChild(groupRow);

    const sortedCompanies = [...companies].sort((a, b) => {
        const av = typeof a.market_cap === "number" ? a.market_cap : -Infinity;
        const bv = typeof b.market_cap === "number" ? b.market_cap : -Infinity;
        return bv - av;
    });

    sortedCompanies.forEach(company => {
        const tr = document.createElement("tr");
        tr.appendChild(td(company.company));
        tr.appendChild(td(fmtMarketCap(company)));
        VALUATION_METRICS.forEach((metric, metricIndex) => {
            VALUATION_YEARS.forEach((year, yearIndex) => {
                const value = company.values?.[year] && company.values[year][metric];
                const cell = valueTd(value, metric);
                cell.classList.add(`metric-band-${metricIndex % 2}`);
                if (yearIndex === 0) cell.classList.add("metric-start");
                tr.appendChild(cell);
            });
        });
        tbody.appendChild(tr);
    });

    const median = medianValuation(sortedCompanies);
    const medianRow = document.createElement("tr");
    medianRow.className = "comps-median-row";
    medianRow.appendChild(td(medianLabel));
    medianRow.appendChild(td(""));
    VALUATION_METRICS.forEach((metric, metricIndex) => {
        VALUATION_YEARS.forEach((year, yearIndex) => {
            const cell = valueTd(median[year][metric], metric);
            cell.classList.add(`metric-band-${metricIndex % 2}`);
            if (yearIndex === 0) cell.classList.add("metric-start");
            medianRow.appendChild(cell);
        });
    });
    tbody.appendChild(medianRow);
}

function appendValuationSpacer(tbody) {
    const spacer = document.createElement("tr");
    spacer.className = "comps-spacer-row";
    const cell = td("");
    cell.colSpan = 2 + VALUATION_METRICS.length * VALUATION_YEARS.length;
    spacer.appendChild(cell);
    tbody.appendChild(spacer);
}

function appendGlobalValuationGroup(tbody, label, companies) {
    const groupRow = document.createElement("tr");
    groupRow.className = "comps-group-row comps-global-group-row";
    const groupCell = td(label);
    groupCell.colSpan = 2 + VALUATION_METRICS.length * VALUATION_YEARS.length;
    groupRow.appendChild(groupCell);
    tbody.appendChild(groupRow);

    const sortedCompanies = [...companies].sort((a, b) => {
        const av = typeof a.market_cap === "number" ? a.market_cap : -Infinity;
        const bv = typeof b.market_cap === "number" ? b.market_cap : -Infinity;
        return bv - av;
    });

    sortedCompanies.forEach(company => appendValuationCompanyRow(tbody, company));
    GLOBAL_REGION_GROUPS.forEach(region => {
        const regionCompanies = sortedCompanies.filter(company => region.companies.includes(company.company));
        appendMedianRow(tbody, region.label, regionCompanies);
    });
    appendMedianRow(tbody, "Global Median", sortedCompanies);
}

function appendValuationCompanyRow(tbody, company) {
    const tr = document.createElement("tr");
    tr.appendChild(td(company.company));
    tr.appendChild(td(fmtMarketCap(company)));
    VALUATION_METRICS.forEach((metric, metricIndex) => {
        VALUATION_YEARS.forEach((year, yearIndex) => {
            const value = company.values?.[year] && company.values[year][metric];
            const cell = valueTd(value, metric);
            cell.classList.add(`metric-band-${metricIndex % 2}`);
            if (yearIndex === 0) cell.classList.add("metric-start");
            tr.appendChild(cell);
        });
    });
    tbody.appendChild(tr);
}

function appendMedianRow(tbody, label, companies) {
    const median = medianValuation(companies);
    const medianRow = document.createElement("tr");
    medianRow.className = "comps-median-row";
    medianRow.appendChild(td(label));
    medianRow.appendChild(td(""));
    VALUATION_METRICS.forEach((metric, metricIndex) => {
        VALUATION_YEARS.forEach((year, yearIndex) => {
            const cell = valueTd(median[year][metric], metric);
            cell.classList.add(`metric-band-${metricIndex % 2}`);
            if (yearIndex === 0) cell.classList.add("metric-start");
            medianRow.appendChild(cell);
        });
    });
    tbody.appendChild(medianRow);
}

function fmtMarketCap(company) {
    const value = company.market_cap;
    if (typeof value !== "number" || !Number.isFinite(value)) return "n.a.";
    const currency = company.market_cap_currency || "";
    return `${currency} ${value.toLocaleString("en-US", { maximumFractionDigits: 0 })} mn`.trim();
}

function findValuationCompany(companies, names) {
    return companies.find(company => names.includes(company.company));
}

function medianValuation(companies) {
    const result = {};
    VALUATION_YEARS.forEach(year => {
        result[year] = {};
        VALUATION_METRICS.forEach(metric => {
            const values = companies
                .map(company => company.values?.[year]?.[metric])
                .filter(value => typeof value === "number" && Number.isFinite(value))
                .sort((a, b) => a - b);
            if (!values.length) {
                result[year][metric] = null;
            } else {
                const mid = Math.floor(values.length / 2);
                result[year][metric] = values.length % 2 ? values[mid] : (values[mid - 1] + values[mid]) / 2;
            }
        });
    });
    return result;
}

function renderISPValuationTable() {
    const table = document.getElementById("comps-isp-valuation-table");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");
    thead.innerHTML = "";
    tbody.innerHTML = "";
    const row1 = document.createElement("tr");
    row1.appendChild(th("Company", 2));
    row1.appendChild(th("Ticker", 2));
    VALUATION_METRICS.forEach(metric => row1.appendChild(th(metric, 1, 2)));
    thead.appendChild(row1);
    const row2 = document.createElement("tr");
    VALUATION_METRICS.forEach(() => VALUATION_YEARS.forEach(year => row2.appendChild(th(year + "E"))));
    thead.appendChild(row2);
    (compsPayload.valuation_2026_2027.companies || [])
        .filter(company => ISP_COMPANIES.includes(company.company))
        .forEach(company => {
            const tr = document.createElement("tr");
            tr.appendChild(td(company.company));
            tr.appendChild(td(company.ticker || "-"));
            VALUATION_METRICS.forEach(metric => {
                VALUATION_YEARS.forEach(year => {
                    const value = company.values[year] && company.values[year][metric];
                    tr.appendChild(valueTd(value, metric));
                });
            });
            tbody.appendChild(tr);
        });
}


function populateISPRangeSelects() {
    const section = findSection("Net Revenue", "Net Revenue");
    const periods = section ? (compsPeriodMode === "year" ? annualPeriodsFrom(section.periods) : section.periods) : [];
    fillRangeSelect("comps-isp-from-select", "comps-isp-to-select", periods, 24, ISP_START_PERIOD);
}

function renderISPOverview() {
    renderPeerMetricSplit("Net Revenue", "Net Revenue", "Net Revenue Growth", "isp-net-revenue-nominal-chart", "isp-net-revenue-ratio-chart", ISP_COMPANIES);
    renderPeerMetricSplit("EBITDA", "EBITDA", "EBITDA Growth", "isp-ebitda-nominal-chart", "isp-ebitda-ratio-chart", ISP_COMPANIES);
    renderPeerMetricSplit("Net Income", "Net Income", "Net Income growth", "isp-net-income-nominal-chart", "isp-net-income-ratio-chart", ISP_COMPANIES);
    renderPeerMetricSplit("Net Debt", null, "ND/annualized EBITDA", null, "isp-net-debt-ratio-chart", ISP_COMPANIES);
    renderPeerMetricSplit("Capex", "Capex", "Capex-to-Sales", "isp-capex-nominal-chart", "isp-capex-ratio-chart", ISP_COMPANIES);
    renderPeerMetricSplit("OpFCF", "OpFCF", "OpFCF Margin", "isp-opfcf-nominal-chart", "isp-opfcf-ratio-chart", ISP_COMPANIES);
    updateISPMissingDataWarning();
}

function getISPSlice(section) {
    const fromValue = document.getElementById("comps-isp-from-select").value;
    const toValue = document.getElementById("comps-isp-to-select").value;
    const slice = getSliceForValues(section.periods, fromValue, toValue);
    if (compsPeriodMode !== "year") {
        const minStart = section.periods.indexOf(ISP_START_PERIOD);
        if (minStart >= 0 && slice.start < minStart) slice.start = minStart;
    }
    return slice;
}

function updateISPMissingDataWarning() {
    const warning = document.getElementById("isp-data-warning");
    if (!warning) return;
    const checks = [
        ["Net Revenue", "Net Revenue", "Net Revenue"],
        ["Net Revenue Growth y/y", "Net Revenue", "Net Revenue Growth"],
        ["EBITDA", "EBITDA", "EBITDA"],
        ["EBITDA Growth y/y", "EBITDA", "EBITDA Growth"],
        ["Net Income", "Net Income", "Net Income"],
        ["Net Income Growth y/y", "Net Income", "Net Income growth"],
        ["ND / Annualized EBITDA", "Net Debt", "ND/annualized EBITDA"],
        ["Capex", "Capex", "Capex"],
        ["Capex-to-Sales", "Capex", "Capex-to-Sales"],
        ["OpFCF", "OpFCF", "OpFCF"],
        ["OpFCF Margin", "OpFCF", "OpFCF Margin"],
    ];
    const missing = [];
    checks.forEach(([label, sheetName, title]) => {
        const rawSection = findSection(sheetName, title);
        const section = label.includes("Growth") && compsPeriodMode === "year"
            ? annualGrowthSection(findSection(sheetName, sheetName), title)
            : sectionForCurrentPeriod(rawSection, label.includes("Growth") ? "avg" : annualAggregationFor(title));
        if (!section) {
            missing.push(`todos: ${label}`);
            return;
        }
        const slice = getISPSlice(section);
        ISP_COMPANIES.forEach(company => {
            const serie = section.series.find(item => item.company === company);
            const hasData = serie && serie.values.slice(slice.start, slice.end).some(value => typeof value === "number");
            if (!hasData) missing.push(`${company}: ${label}`);
        });
    });
    if (!missing.length) {
        warning.style.display = "none";
        warning.textContent = "";
        return;
    }
    warning.style.display = "block";
    warning.textContent = `Dados faltantes no range selecionado: ${missing.join("; ")}.`;
}

function getPeerMetricSlice(section, canvasId) {
    if (canvasId.startsWith("isp-")) return getISPSlice(section);
    const fromValue = document.getElementById("comps-excel-from-select").value;
    const toValue = document.getElementById("comps-excel-to-select").value;
    return getSliceForValues(section.periods, fromValue, toValue);
}

function renderPeerMetricCombo(sheetName, nominalTitle, ratioTitle, canvasId, companies) {
    const nominal = findSection(sheetName, nominalTitle);
    const ratio = findSection(sheetName, ratioTitle);
    if (!nominal) return;
    const { start, end } = getPeerMetricSlice(nominal, canvasId);
    const labels = nominal.periods.slice(start, end).map(fmtMonth);
    const datasets = [];
    const colorMap = {
        Vivo: COMPS_COLORS.Vivo,
        TIM: COMPS_COLORS.TIM,
        AMX: COMPS_COLORS.Claro,
        Claro: COMPS_COLORS.Claro,
        Megacable: COMPS_COLORS.Claro,
        Brisanet: COMPS_COLORS.Brisanet,
        Unifique: COMPS_COLORS.Unifique,
        Desktop: COMPS_COLORS.Desktop,
        Vero: COMPS_COLORS.Vero,
        Entel: "#4B5563",
        TEO: "#0F766E",
    };
    companies.forEach(company => {
        const displayName = company;
        const nominalSerie = nominal.series.find(s => s.company === company);
        if (nominalSerie) {
            datasets.push({
                type: "bar",
                label: `${displayName} ${nominalTitle}`,
                data: nominalSerie.values.slice(start, end).map(v => typeof v === "number" ? v : null),
                backgroundColor: colorMap[company],
                borderColor: colorMap[company],
                yAxisID: "y",
                order: 2,
            });
        }
        const ratioSerie = ratio ? ratio.series.find(s => s.company === company) : null;
        if (ratioSerie) {
            const ratioLookup = new Map(ratio.periods.map((p, idx) => [p, ratioSerie.values[idx]]));
            datasets.push({
                type: "line",
                label: `${displayName} ${ratioTitle}`,
                data: nominal.periods.slice(start, end).map(period => {
                    const value = ratioLookup.get(period);
                    return typeof value === "number" ? value : null;
                }),
                borderColor: colorMap[company],
                backgroundColor: colorMap[company],
                borderDash: [],
                tension: 0.25,
                pointRadius: 2,
                yAxisID: "y1",
                order: 1,
            });
        }
    });
    destroyChart(canvasId);
    compsCharts[canvasId] = new Chart(document.getElementById(canvasId), {
        data: { labels, datasets },
        options: comboOptions(value => fmtVal(value, "mn"), value => fmtVal(value, "growth")),
    });
}

function renderPeerMetricSplit(sheetName, nominalTitle, ratioTitle, nominalCanvasId, ratioCanvasId, companies) {
    const rawNominal = nominalTitle ? findSection(sheetName, nominalTitle) : null;
    const rawRatio = ratioTitle ? findSection(sheetName, ratioTitle) : null;
    const nominal = sectionForCurrentPeriod(rawNominal, "sum");
    let ratio = sectionForCurrentPeriod(rawRatio, ratioFormatMetric(ratioTitle) === "growth" ? "avg" : annualAggregationFor(ratioTitle));
    if (compsPeriodMode === "year" && ratioFormatMetric(ratioTitle) === "growth" && rawNominal) {
        ratio = annualGrowthSection(rawNominal, ratioTitle);
    }
    if (nominal && nominalCanvasId) renderPeerSectionChart(nominal, nominalCanvasId, companies, "bar", "mn");
    if (ratio && ratioCanvasId) renderPeerSectionChart(ratio, ratioCanvasId, companies, "line", ratioFormatMetric(ratioTitle));
}

function renderPeerSectionChart(section, canvasId, companies, type, formatMetric) {
    const isOverviewLine = canvasId.startsWith("all-") && type === "line";
    const slice = getPeerMetricSlice(section, canvasId);
    const series = companies
        .map(company => {
            const serie = section.series.find(s => s.company === company);
            if (!serie) return null;
            const values = serie.values.slice(slice.start, slice.end).map(v => typeof v === "number" ? v : null);
            if (!values.some(value => value !== null)) return null;
            return { company, values };
        })
        .filter(Boolean);
    if (!series.length) {
        destroyChart(canvasId);
        return;
    }
    const completeStart = canvasId.startsWith("isp-") ? 0 : firstCompleteIndex(series.map(item => item.values));
    const labels = section.periods.slice(slice.start + completeStart, slice.end).map(fmtMonth);
    const datasets = series
        .map(item => {
            return {
                label: peerDisplayName(item.company),
                data: item.values.slice(completeStart),
                borderColor: peerColor(item.company),
                backgroundColor: peerColor(item.company),
                tension: isOverviewLine ? 0.18 : 0.25,
                pointRadius: type === "bar" ? 0 : (isOverviewLine ? 3 : 2),
                hoverRadius: isOverviewLine ? 5 : 4,
                borderWidth: isOverviewLine ? 2.5 : 2,
            };
        });

    const chartData = reserveEndLabelSlot(labels, datasets, type !== "bar");

    destroyChart(canvasId);
    compsCharts[canvasId] = new Chart(document.getElementById(canvasId), {
        type,
        data: chartData,
        plugins: type === "bar" ? [ChartDataLabels] : [],
        options: baseOptions(value => fmtVal(value, formatMetric), {
            datalabels: type === "bar",
            companyLabels: type !== "bar",
            denseLabels: type !== "bar" && !isOverviewLine,
            endLabelSlot: type !== "bar",
            spacious: isOverviewLine,
            percent: formatMetric === "growth" || formatMetric === "margin",
        }),
    });
}

function peerColor(company) {
    return {
        Vivo: COMPS_COLORS.Vivo,
        TIM: COMPS_COLORS.TIM,
        AMX: COMPS_COLORS.Claro,
        Claro: COMPS_COLORS.Claro,
        Megacable: COMPS_COLORS.Claro,
        Brisanet: COMPS_COLORS.Brisanet,
        Unifique: COMPS_COLORS.Unifique,
        Desktop: COMPS_COLORS.Desktop,
        Vero: COMPS_COLORS.Vero,
        Entel: "#4B5563",
        TEO: "#0F766E",
    }[company] || "#195AB4";
}

function peerDisplayName(company) {
    return company;
}

function colorWithAlpha(color, alpha) {
    if (!color) return `rgba(123, 45, 142, ${alpha})`;
    if (color.startsWith("rgba")) return color;
    if (color.startsWith("rgb(")) return color.replace("rgb(", "rgba(").replace(")", `, ${alpha})`);
    const hex = color.replace("#", "");
    if (hex.length !== 6) return color;
    const r = parseInt(hex.slice(0, 2), 16);
    const g = parseInt(hex.slice(2, 4), 16);
    const b = parseInt(hex.slice(4, 6), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function comboBarFill(color) {
    return colorWithAlpha(color || COMPS_COLORS.Vivo, 0.42);
}

function ratioFormatMetric(title) {
    const label = String(title || "").toLowerCase();
    if (label.includes("nd/")) return "multiple";
    if (label.includes("growth")) return "growth";
    if (label.includes("margin") || label.includes("sales")) return "margin";
    return title;
}

function firstCompleteIndex(seriesValues) {
    if (!seriesValues.length) return 0;
    const length = Math.min(...seriesValues.map(values => values.length));
    for (let index = 0; index < length; index += 1) {
        if (seriesValues.every(values => typeof values[index] === "number")) return index;
    }
    return 0;
}

function th(text, rowspan, colspan) {
    const node = document.createElement("th");
    node.textContent = text;
    if (rowspan) node.rowSpan = rowspan;
    if (colspan) node.colSpan = colspan;
    return node;
}

function td(text) {
    const node = document.createElement("td");
    node.textContent = text;
    return node;
}

function valueTd(value, metric) {
    const node = td(fmtVal(value, metric));
    if (typeof value === "number" && value < 0) node.classList.add("val-negative-soft");
    return node;
}

function renderAllFinancialViews() {
    renderExcelCharts();
    renderTelcosOverviewTables();
}

function renderExcelCharts() {
    renderSectionPair("Service Revenue", "comps-service-revenue-chart", "comps-service-growth-chart", false);
    renderSectionPair("Mobile Service Revenue", "comps-mobile-service-chart", "comps-mobile-service-growth-chart", false);
    renderSectionPair("Fixed Revenue", "comps-fixed-revenue-chart", "comps-fixed-growth-chart", false);

    renderSingleSection("ARPU", "ARPU (R$)", "comps-arpu-chart", "line", false);
    renderSingleSection("ARPU", "ARPU Growth", "comps-arpu-growth-chart", "line", true);
    renderSingleSection("ARPU", "Postpaid ARPU (R$)", "comps-postpaid-arpu-chart", "line", false);
    renderSingleSection("ARPU", "Postpaid Growth", "comps-postpaid-arpu-growth-chart", "line", true);
    renderSingleSection("ARPU", "Prepaid ARPU (R$)", "comps-prepaid-arpu-chart", "line", false);
    renderSingleSection("ARPU", "Prepaid Growth", "comps-prepaid-arpu-growth-chart", "line", true);

    renderSingleSection("Big telcos", "Capex-to-sales", "comps-capex-sales-chart", "bar", true);
    renderSingleSection("EBITDA", "EBITDA Margin", "comps-ebitda-margin-chart", "bar", true);
    renderBigTelcoFinancialOverview();
}

function renderBigTelcoFinancialOverview() {
    renderPeerMetricSplit("Net Revenue", "Net Revenue", "Net Revenue Growth", "big-net-revenue-nominal-chart", "big-net-revenue-ratio-chart", BRAZIL_TELCO_FINANCIAL_OPS);
    renderPeerMetricSplit("EBITDA", "EBITDA", "EBITDA Growth", "big-ebitda-nominal-chart", "big-ebitda-ratio-chart", BRAZIL_TELCO_FINANCIAL_OPS);
    renderPeerMetricSplit("Net Debt", null, "ND/annualized EBITDA", null, "big-net-debt-ratio-chart", BRAZIL_TELCO_FINANCIAL_OPS);
    renderPeerMetricSplit("Capex", "Capex", "Capex-to-Sales", "big-capex-nominal-chart", "big-capex-ratio-chart", BRAZIL_TELCO_FINANCIAL_OPS);
    renderPeerMetricSplit("OpFCF", "OpFCF", "OpFCF Margin", "big-opfcf-nominal-chart", "big-opfcf-ratio-chart", BRAZIL_TELCO_FINANCIAL_OPS);
}

function getFinancialCompanies() {
    return getSelectableCompanies().filter(company => company !== "Inflation");
}

function renderTelcosOverview() {
    renderTelcosOverviewCharts("all", getFinancialCompanies());
}

function renderTelcosOverviewTables() {
    const companies = getFinancialCompanies();
    const specs = [
        ["overview-net-revenue-growth-table", "Net Revenue", "Net Revenue", "Net Revenue Growth", "growth"],
        ["overview-ebitda-growth-table", "EBITDA", "EBITDA", "EBITDA Growth", "growth"],
        ["overview-net-income-growth-table", "Net Income", "Net Income", "Net Income growth", "growth"],
        ["overview-capex-sales-table", "Capex", null, "Capex-to-Sales", "margin"],
        ["overview-opfcf-margin-table", "OpFCF", null, "OpFCF Margin", "margin"],
    ];
    specs.forEach(([tableId, sheetName, baseTitle, title, formatMetric]) => {
        const section = overviewMetricSection(sheetName, baseTitle, title, formatMetric);
        renderOverviewHistoryTable(tableId, section, companies, formatMetric);
    });
}

function renderOverviewHistoryTable(tableId, section, companies, formatMetric) {
    const table = document.getElementById(tableId);
    if (!table) return;
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");
    thead.innerHTML = "";
    tbody.innerHTML = "";
    if (!section) return;

    const periods = overviewHistoryPeriods(section);
    const header = document.createElement("tr");
    header.appendChild(th("Company"));
    periods.forEach(period => header.appendChild(th(fmtMonth(period))));
    thead.appendChild(header);

    companies.forEach(company => {
        const serie = section.series.find(item => item.company === company);
        if (!serie) return;
        const valuesByPeriod = new Map(section.periods.map((period, idx) => [period, serie.values[idx]]));
        const rowValues = periods.map(period => valuesByPeriod.get(period));
        if (!rowValues.some(value => typeof value === "number")) return;
        const tr = document.createElement("tr");
        tr.appendChild(td(`${companyDisplayName(company)} (${companyCurrency({ financialCompany: company })})`));
        rowValues.forEach(value => tr.appendChild(valueTd(value, formatMetric)));
        tbody.appendChild(tr);
    });
}

function overviewHistoryPeriods(section) {
    let periods = section.periods;
    if (compsPeriodMode === "year") {
        const fromYear = DEFAULT_YEAR_START;
        return annualPeriodsFrom(periods).filter(p => !fromYear || p >= fromYear);
    }
    const startIndex = periods.indexOf(TELCOS_OVERVIEW_START);
    return periods.slice(startIndex >= 0 ? startIndex : 0);
}

function renderTelcosOverviewTable() {
    const table = document.getElementById("telcos-overview-table");
    if (!table) return;
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");
    const metrics = [
        ["Net Revenue Growth y/y", "Net Revenue", "Net Revenue", "Net Revenue Growth", "growth"],
        ["EBITDA Growth y/y", "EBITDA", "EBITDA", "EBITDA Growth", "growth"],
        ["Net Income Growth y/y", "Net Income", "Net Income", "Net Income growth", "growth"],
        ["Capex-to-Sales", "Capex", null, "Capex-to-Sales", "margin"],
        ["OpFCF Margin", "OpFCF", null, "OpFCF Margin", "margin"],
    ];
    thead.innerHTML = "";
    tbody.innerHTML = "";
    const header = document.createElement("tr");
    header.appendChild(th("Company"));
    header.appendChild(th("Period"));
    metrics.forEach(([label]) => header.appendChild(th(label)));
    thead.appendChild(header);

    getFinancialCompanies().forEach(company => {
        const tr = document.createElement("tr");
        tr.appendChild(td(companyDisplayName(company)));
        let rowPeriod = "";
        const cells = metrics.map(([, sheetName, baseTitle, title, formatMetric]) => {
            const section = overviewMetricSection(sheetName, baseTitle, title, formatMetric);
            const valueInfo = latestSelectedValue(section, company);
            if (!rowPeriod && valueInfo.period) rowPeriod = valueInfo.period;
            return valueTd(valueInfo.value, formatMetric);
        });
        tr.appendChild(td(rowPeriod ? fmtMonth(rowPeriod) : "-"));
        cells.forEach(cell => tr.appendChild(cell));
        tbody.appendChild(tr);
    });
}

function overviewMetricSection(sheetName, baseTitle, title, formatMetric) {
    if (formatMetric === "growth" && compsPeriodMode === "year") {
        const base = findSection(sheetName, baseTitle);
        return base ? annualGrowthSection(base, title) : null;
    }
    return sectionForCurrentPeriod(findSection(sheetName, title), annualAggregationFor(title));
}

function latestSelectedValue(section, company) {
    if (!section) return { value: null, period: null };
    const serie = section.series.find(item => item.company === company);
    if (!serie) return { value: null, period: null };
    const fromValue = document.getElementById("comps-excel-from-select").value;
    const toValue = document.getElementById("comps-excel-to-select").value;
    const { start, end } = getSliceForValues(section.periods, fromValue, toValue);
    for (let idx = end - 1; idx >= start; idx -= 1) {
        const value = serie.values[idx];
        if (typeof value === "number") return { value, period: section.periods[idx] };
    }
    return { value: null, period: null };
}

function renderTelcosOverviewCharts(prefix, companies) {
    renderGrowthOnlyChart("Net Revenue", "Net Revenue", "Net Revenue Growth", `${prefix}-net-revenue-growth-chart`, companies);
    renderGrowthOnlyChart("EBITDA", "EBITDA", "EBITDA Growth", `${prefix}-ebitda-growth-chart`, companies);
    renderGrowthOnlyChart("Net Income", "Net Income", "Net Income growth", `${prefix}-net-income-growth-chart`, companies);
    renderMetricLineChart("Capex", "Capex-to-Sales", `${prefix}-capex-sales-chart`, companies, "margin");
    renderMetricLineChart("OpFCF", "OpFCF Margin", `${prefix}-opfcf-margin-chart`, companies, "margin");
}

function renderGrowthOnlyChart(sheetName, baseTitle, growthTitle, canvasId, companies) {
    const rawBase = findSection(sheetName, baseTitle);
    const section = compsPeriodMode === "year" && rawBase
        ? annualGrowthSection(rawBase, growthTitle)
        : sectionForCurrentPeriod(findSection(sheetName, growthTitle), "avg");
    if (section) renderPeerSectionChart(section, canvasId, companies, "line", "growth");
}

function renderMetricLineChart(sheetName, title, canvasId, companies, formatMetric) {
    const section = sectionForCurrentPeriod(findSection(sheetName, title), annualAggregationFor(title));
    if (section) renderPeerSectionChart(section, canvasId, companies, "line", formatMetric);
}

function renderSectionPair(sheetName, nominalId, growthId) {
    const rawNominal = findSection(sheetName, sheetName);
    const nominal = sectionForCurrentPeriod(rawNominal, "sum");
    const growth = compsPeriodMode === "year"
        ? (rawNominal ? annualGrowthSection(rawNominal, `${sheetName} Growth`) : null)
        : findSection(sheetName, `${sheetName} Growth`);
    if (nominal) renderSectionChart(nominal, nominalId, "line", false, null, "mn");
    if (growth) renderSectionChart(growth, growthId, "line", true, null, "growth");
}

function renderSingleSection(sheetName, title, canvasId, type, percent, maxPoints) {
    let section = findSection(sheetName, title);
    if (compsPeriodMode === "year" && title.toLowerCase().includes("growth")) {
        const baseTitle = {
            "ARPU Growth": "ARPU (R$)",
            "Postpaid Growth": "Postpaid ARPU (R$)",
            "Prepaid Growth": "Prepaid ARPU (R$)",
        }[title];
        const base = baseTitle ? findSection(sheetName, baseTitle) : null;
        if (base) section = annualGrowthSection(base, title);
    } else {
        section = sectionForCurrentPeriod(section);
    }
    if (section) renderSectionChart(section, canvasId, type, percent, maxPoints, percent ? "growth" : section.title);
}

function findSection(sheetName, title) {
    return (compsPayload.sheets[sheetName] || []).find(s => s.title === title);
}

function getExcelSlice(section, maxPoints) {
    const fromValue = document.getElementById("comps-excel-from-select").value;
    const toValue = document.getElementById("comps-excel-to-select").value;
    let start = section.periods.indexOf(fromValue);
    let end = section.periods.indexOf(toValue);
    if (start < 0) start = 0;
    if (end < 0) end = section.periods.length - 1;
    if (start > end) [start, end] = [end, start];
    if (maxPoints && end - start + 1 > maxPoints) start = end - maxPoints + 1;
    return { start, end: end + 1 };
}

function renderSectionChart(section, canvasId, type, percent, maxPoints, formatMetric) {
    let filtered = section.series.filter(serie => {
        const name = String(serie.company || "");
        return COMPS_OPS.includes(name);
    });
    const { start, end } = getExcelSlice(section, maxPoints);
    filtered = filtered.filter(serie => serie.values.slice(start, end).some(v => typeof v === "number"));
    const completeStart = firstCompleteIndex(filtered.map(serie => (
        serie.values.slice(start, end).map(v => typeof v === "number" ? v : null)
    )));
    const labels = section.periods.slice(start + completeStart, end).map(fmtMonth);
    const datasets = filtered.map(serie => {
        const name = serie.company;
        return {
            label: name,
            data: serie.values.slice(start + completeStart, end).map(v => typeof v === "number" ? v : null),
            borderColor: COMPS_COLORS[name] || "#195AB4",
            backgroundColor: COMPS_COLORS[name] || "#195AB4",
            tension: 0.25,
            pointRadius: type === "bar" ? 0 : 2,
            borderWidth: 2,
        };
    });

    const chartData = reserveEndLabelSlot(labels, datasets, type !== "bar");

    destroyChart(canvasId);
    compsCharts[canvasId] = new Chart(document.getElementById(canvasId), {
        type,
        data: chartData,
        plugins: type === "bar" ? [ChartDataLabels] : [],
        options: baseOptions(value => fmtVal(value, formatMetric || (percent ? "growth" : section.title)), {
            datalabels: type === "bar",
            companyLabels: type !== "bar",
            denseLabels: type !== "bar",
            endLabelSlot: type !== "bar",
            percent,
        }),
    });
}

async function renderAnatelCharts() {
    if (!document.getElementById("comps-bb-share-chart")) return;
    if (!compsAnatelRows) {
        const [bb, ftth, postpaid, prepaid] = await Promise.all([
            fetch("/telecom/api/broadband").then(r => r.json()),
            fetch("/telecom/api/broadband?tech=FTTH").then(r => r.json()),
            fetch("/telecom/api/mobile?segment=Postpaid").then(r => r.json()),
            fetch("/telecom/api/mobile?segment=Prepaid").then(r => r.json()),
        ]);
        compsAnatelRows = { bb, ftth, postpaid, prepaid };
        populateAnatelRangeSelects([...bb, ...ftth, ...postpaid, ...prepaid]);
    }
    const { bb, ftth, postpaid, prepaid } = compsAnatelRows;
    renderShareChart(bb, "comps-bb-share-chart", "accesses");
    renderShareChart(ftth, "comps-ftth-share-chart", "accesses");
    renderShareChart(postpaid, "comps-postpaid-share-chart", "accesses");
    renderShareChart(prepaid, "comps-prepaid-share-chart", "accesses");
}

function populateAnatelRangeSelects(rows) {
    if (!document.getElementById("comps-anatel-from-select")) return;
    const months = Array.from(new Set(rows.map(row => row.month))).sort();
    const from = document.getElementById("comps-anatel-from-select");
    const to = document.getElementById("comps-anatel-to-select");
    from.innerHTML = "";
    to.innerHTML = "";
    months.forEach(month => {
        from.appendChild(new Option(fmtMonth(month), month));
        to.appendChild(new Option(fmtMonth(month), month));
    });
    from.value = months.includes(COMPS_DEFAULT_MONTH_START)
        ? COMPS_DEFAULT_MONTH_START
        : months[Math.max(0, months.length - 18)] || "";
    to.value = months[months.length - 1] || "";
}

function renderShareChart(rows, canvasId, valueField) {
    const fromValue = document.getElementById("comps-anatel-from-select").value;
    const toValue = document.getElementById("comps-anatel-to-select").value;
    let months = Array.from(new Set(rows.map(row => row.month))).sort();
    months = months.filter(month => (!fromValue || month >= fromValue) && (!toValue || month <= toValue));
    const buckets = {};
    months.forEach(month => {
        buckets[month] = { Vivo: 0, TIM: 0, Claro: 0, Others: 0 };
    });
    rows.forEach(row => {
        if (!buckets[row.month]) return;
        const op = COMPS_OPS.includes(row.operator) ? row.operator : "Others";
        buckets[row.month][op] += row[valueField] || 0;
    });

    const datasets = COMPS_SHARE_OPS.map(op => ({
        label: op,
        data: months.map(month => {
            const total = COMPS_SHARE_OPS.reduce((sum, key) => sum + buckets[month][key], 0);
            return total ? buckets[month][op] / total : 0;
        }),
        backgroundColor: COMPS_COLORS[op],
        stack: "share",
    }));

    destroyChart(canvasId);
    compsCharts[canvasId] = new Chart(document.getElementById(canvasId), {
        type: "bar",
        data: { labels: months.map(fmtMonth), datasets },
        plugins: [ChartDataLabels],
        options: baseOptions(value => (value * 100).toFixed(0) + "%", {
            stacked: true,
            datalabels: true,
            percent: true,
            max: 1,
        }),
    });
}

async function renderCompanyPage(companyName) {
    const config = getCompanyConfig(companyName);
    updateCompanyHeader(config);
    populateCompanyFinancialRange(config);
    await ensureAnatelRows();
    populateCompanyOperationalRange();
    renderCompanyKpis(config);
    renderCompanyValuationTable(config);
    renderCompanyFinancialCharts();
    renderCompanyOperationalCharts();
}

function getSelectedCompanyConfig() {
    const companyName = document.getElementById("comps-company-select").value || "Vivo";
    return getCompanyConfig(companyName);
}

function getCompanyConfig(companyName) {
    const operator = getCompanyOperator(companyName);
    const isBigTelco = hasCompanyInAnySection(companyName, [
        ["Service Revenue", "Service Revenue"],
        ["Mobile Service Revenue", "Mobile Service Revenue"],
        ["Fixed Revenue", "Fixed Revenue"],
        ["ARPU", "ARPU (R$)"],
    ]);
    const isISP = ISP_COMPANIES.includes(companyName) || (
        !isBigTelco && hasCompanyInAnySection(companyName, [["Net Revenue", "Net Revenue"]])
    );
    const hasBrazilOps = ["Vivo", "TIM", "Claro", "Brisanet", "Unifique"].includes(operator) && companyName !== "AMX";
    return {
        companyName,
        displayName: companyDisplayName(companyName),
        operator,
        financialCompany: companyName,
        isISP,
        isBigTelco,
        hasBrazilOps,
    };
}

function companyDisplayName(companyName) {
    return {
        Vivo: "Vivo / Telefonica Brasil",
        TIM: "TIM Brasil",
        Claro: "Claro Brasil",
        AMX: "AMX",
    }[companyName] || companyName;
}

function hasCompanyInAnySection(companyName, specs) {
    return specs.some(([sheetName, title]) => hasCompanyInSection(sheetName, title, companyName));
}

function hasCompanyInSection(sheetName, title, companyName) {
    const section = findSection(sheetName, title);
    return !!section && section.series.some(serie => serie.company === companyName);
}

function updateCompanyHeader(config) {
    document.getElementById("company-specific-title").textContent = config.displayName;
    document.getElementById("company-specific-operator").textContent = config.operator;
    document.getElementById("company-specific-subtitle").textContent = config.hasBrazilOps
        ? "Financial series from Big Telcos Data and operational data from Anatel."
        : "Financial series from Big Telcos Data. Brazil operational data is not available for this company yet.";
}

function getCompanyValuation(config) {
    const valuationNames = {
        Vivo: ["Telefonica Brasil", "Vivo"],
        TIM: ["TIM Brasil", "TIM"],
        Claro: ["Claro"],
        AMX: ["AMX"],
        Megacable: ["Megacable"],
        Brisanet: ["Brisanet"],
        Unifique: ["Unifique"],
    }[config.companyName] || [config.companyName];
    return (compsPayload.valuation_2026_2027.companies || []).find(c => valuationNames.includes(c.company));
}

function renderCompanyKpis(config) {
    const company = getCompanyValuation(config);
    document.getElementById("company-kpi-grid").style.display = company ? "grid" : "none";
    document.getElementById("company-valuation-card").style.display = company ? "block" : "none";
    if (!company) return;
    const y2026 = company && company.values["2026"] ? company.values["2026"] : {};
    document.getElementById("vivo-kpi-ev-ebitda").textContent = fmtVal(y2026["EV/EBITDA"], "multiple");
    document.getElementById("vivo-kpi-pe").textContent = fmtVal(y2026["P/E"], "multiple");
    document.getElementById("vivo-kpi-dividend").textContent = fmtVal(y2026["Dividend Yield"], "yield");
}

function renderCompanyValuationTable(config) {
    const company = getCompanyValuation(config);
    const table = document.getElementById("vivo-valuation-table");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");
    thead.innerHTML = "";
    tbody.innerHTML = "";
    if (!company) return;
    const trh = document.createElement("tr");
    trh.appendChild(th("Metric"));
    VALUATION_YEARS.forEach(year => trh.appendChild(th(year + "E")));
    thead.appendChild(trh);
    VALUATION_METRICS.forEach(metric => {
        const tr = document.createElement("tr");
        tr.appendChild(td(metric));
        VALUATION_YEARS.forEach(year => {
            const value = company && company.values[year] ? company.values[year][metric] : null;
            tr.appendChild(valueTd(value, metric));
        });
        tbody.appendChild(tr);
    });
}

function populateCompanyFinancialRange(config) {
    const rawPrimary = getCompanyPrimarySection(config);
    const primary = rawPrimary
        ? sectionForCurrentPeriod(rawPrimary, annualAggregationFor(rawPrimary.title))
        : null;
    const periods = primary
        ? primary.periods
        : getPrimaryExcelPeriods();
    const firstPeriod = primary ? firstCompanyDataPeriod(primary, config.financialCompany) : null;
    fillRangeSelect("vivo-financial-from-select", "vivo-financial-to-select", periods, 24, firstPeriod, true);
}

function getCompanyPrimarySection(config) {
    const candidates = [
        ["Service Revenue", "Service Revenue"],
        ["Net Revenue", "Net Revenue"],
        ["Mobile Service Revenue", "Mobile Service Revenue"],
        ["Fixed Revenue", "Fixed Revenue"],
        ["EBITDA", "EBITDA"],
        ["Capex", "Capex"],
        ["OpFCF", "OpFCF"],
        ["ARPU", "ARPU (R$)"],
    ];
    for (const [sheetName, title] of candidates) {
        const section = findSection(sheetName, title);
        if (section?.series.some(serie => serie.company === config.financialCompany)) return section;
    }
    return null;
}

function populateCompanyOperationalRange() {
    const rows = Object.values(compsAnatelRows || {}).flat();
    const months = Array.from(new Set(rows.map(row => row.month))).sort();
    fillRangeSelect("vivo-operational-from-select", "vivo-operational-to-select", months, 18);
}

function fillRangeSelect(fromId, toId, values, defaultLength, minValue, preferMin = false) {
    const from = document.getElementById(fromId);
    const to = document.getElementById(toId);
    const minIndex = minValue ? values.indexOf(minValue) : -1;
    const selectValues = minIndex >= 0 ? values.slice(minIndex) : values;
    from.innerHTML = "";
    to.innerHTML = "";
    selectValues.forEach(value => {
        from.appendChild(new Option(fmtMonth(value), value));
        to.appendChild(new Option(fmtMonth(value), value));
    });
    const defaultFrom = selectValues[Math.max(0, selectValues.length - defaultLength)] || "";
    const preferredStart = selectValues.includes(DEFAULT_QUARTER_START)
        ? DEFAULT_QUARTER_START
        : (selectValues.includes(DEFAULT_YEAR_START)
            ? DEFAULT_YEAR_START
            : (selectValues.includes(COMPS_DEFAULT_MONTH_START) ? COMPS_DEFAULT_MONTH_START : null));
    from.value = preferMin && minValue && selectValues.includes(minValue)
        ? minValue
        : (preferredStart || (minValue && selectValues.includes(minValue) ? minValue : defaultFrom));
    to.value = selectValues[selectValues.length - 1] || "";
}

function getCompanyFinancialSlice(section) {
    const config = getSelectedCompanyConfig();
    const fromValue = document.getElementById("vivo-financial-from-select").value;
    const toValue = document.getElementById("vivo-financial-to-select").value;
    const slice = getSliceForValues(section.periods, fromValue, toValue);
    const firstDataIndex = firstCompanyDataIndex(section, config.financialCompany);
    if (firstDataIndex >= 0 && slice.start < firstDataIndex) slice.start = firstDataIndex;
    return slice;
}

function firstCompanyDataPeriod(section, company) {
    const index = firstCompanyDataIndex(section, company);
    return index >= 0 ? section.periods[index] : null;
}

function firstCompanyDataIndex(section, company) {
    const serie = section?.series.find(item => item.company === company);
    if (!serie) return -1;
    return serie.values.findIndex(value => typeof value === "number");
}

function getSliceForValues(values, fromValue, toValue) {
    let start = values.indexOf(fromValue);
    let end = values.indexOf(toValue);
    if (start < 0) start = 0;
    if (end < 0) end = values.length - 1;
    if (start > end) [start, end] = [end, start];
    return { start, end: end + 1 };
}

function renderCompanyFinancialCharts() {
    const config = getSelectedCompanyConfig();
    updateCompanyFinancialTitles(config);
    setCompanyFinancialCards(config);
    renderCompanyCard("company-service-card", () => renderCompanyRevenueCombo("Service Revenue", "Service Revenue", "Service Revenue Growth", "vivo-service-combo-chart", config.financialCompany));
    renderCompanyCard("company-mobile-service-card", () => renderCompanyRevenueCombo("Mobile Service Revenue", "Mobile Service Revenue", "Mobile Service Revenue Growth", "vivo-mobile-service-combo-chart", config.financialCompany));
    renderCompanyCard("company-fixed-revenue-card", () => renderCompanyRevenueCombo("Fixed Revenue", "Fixed Revenue", "Fixed Revenue Growth", "vivo-fixed-revenue-combo-chart", config.financialCompany));
    renderCompanyCard("company-net-revenue-card", () => renderSingleCompanyMetricCombo("Net Revenue", "Net Revenue", "Net Revenue Growth", "company-net-revenue-combo-chart", config.financialCompany, "growth"));
    renderCompanyCard("company-ebitda-card", () => renderSingleCompanyMetricCombo("EBITDA", "EBITDA", "EBITDA Growth", "company-ebitda-combo-chart", config.financialCompany, "growth"));
    renderCompanyCard("company-net-income-card", () => renderSingleCompanyMetricCombo("Net Income", "Net Income", "Net Income growth", "company-net-income-combo-chart", config.financialCompany, "growth"));
    renderCompanyCard("company-net-debt-card", () => renderSingleCompanyRatioChart("Net Debt", "ND/annualized EBITDA", "company-net-debt-ratio-chart", config.financialCompany, "multiple"));
    renderCompanyCard("company-capex-card", () => renderSingleCompanyMetricCombo("Capex", "Capex", "Capex-to-Sales", "vivo-capex-combo-chart", config.financialCompany, "margin"));
    renderCompanyCard("company-opfcf-card", () => renderSingleCompanyMetricCombo("OpFCF", "OpFCF", "OpFCF Margin", "vivo-opfcf-combo-chart", config.financialCompany, "margin"));
    renderCompanyCard("company-arpu-card", () => renderCompanyMultiSeries("ARPU", [
        ["ARPU (R$)", "ARPU"],
        ["Postpaid ARPU (R$)", "Postpaid"],
        ["Prepaid ARPU (R$)", "Prepaid"],
    ], "vivo-arpu-chart", false, null, config.financialCompany));
}

function renderCompanyCard(cardId, renderFn) {
    const rendered = renderFn();
    document.getElementById(cardId).style.display = rendered ? "block" : "none";
}

function updateCompanyFinancialTitles(config) {
    const currency = companyCurrency(config);
    const moneyUnit = `${currency} mn`;
    setCardTitle("company-service-card", `Service Revenue (${moneyUnit}) and Growth`);
    setCardTitle("company-mobile-service-card", `Mobile Service Revenue (${moneyUnit}) and Growth`);
    setCardTitle("company-fixed-revenue-card", `Fixed Line Revenue (${moneyUnit}) and Growth`);
    setCardTitle("company-net-revenue-card", `Net Revenue (${moneyUnit}) and Growth`);
    setCardTitle("company-ebitda-card", `EBITDA (${moneyUnit}) and Growth`);
    setCardTitle("company-net-income-card", `Net Income (${moneyUnit}) and Growth`);
    setCardTitle("company-net-debt-card", "ND / Annualized EBITDA");
    setCardTitle("company-capex-card", `Capex (${moneyUnit}) and Capex-to-Sales`);
    setCardTitle("company-opfcf-card", `OpFCF (${moneyUnit}) and OpFCF Margin`);
    setCardTitle("company-arpu-card", `ARPU (${currency})`);
}

function setCardTitle(cardId, title) {
    const titleNode = document.querySelector(`#${cardId} .card-title`);
    if (titleNode) titleNode.textContent = title;
}

function companyCurrency(config) {
    const company = config?.financialCompany || config?.companyName || "";
    return {
        Vivo: "R$",
        "Telefonica Brasil": "R$",
        TIM: "R$",
        "TIM Brasil": "R$",
        Claro: "R$",
        Desktop: "R$",
        Brisanet: "R$",
        Unifique: "R$",
        Vero: "R$",
        AMX: "MXN",
        Megacable: "MXN",
        Entel: "CLP",
        TEO: "US$",
    }[company] || "Local";
}

function setCompanyFinancialCards(config) {
    [
        "company-service-card",
        "company-mobile-service-card",
        "company-fixed-revenue-card",
        "company-net-revenue-card",
        "company-ebitda-card",
        "company-net-income-card",
        "company-net-debt-card",
        "company-capex-card",
        "company-opfcf-card",
        "company-arpu-card",
    ].forEach(id => document.getElementById(id).style.display = "none");
}

function renderCompanyRevenueCombo(sheetName, nominalTitle, growthTitle, canvasId, company) {
    const rawNominal = findSection(sheetName, nominalTitle);
    const nominal = sectionForCurrentPeriod(rawNominal, "sum");
    const growth = compsPeriodMode === "year"
        ? (rawNominal ? annualGrowthSection(rawNominal, growthTitle) : null)
        : findSection(sheetName, growthTitle);
    if (!nominal || !growth) return false;
    const nominalSerie = nominal.series.find(s => s.company === company);
    const growthSerie = growth.series.find(s => s.company === company);
    if (!nominalSerie || !growthSerie) return false;
    const { start, end } = getCompanyFinancialSlice(nominal);
    const labels = nominal.periods.slice(start, end).map(fmtMonth);
    const growthLookup = new Map(growth.periods.map((p, idx) => [p, growthSerie.values[idx]]));
    const growthValues = nominal.periods.slice(start, end).map(period => {
        const value = growthLookup.get(period);
        return typeof value === "number" ? value : null;
    });
    const barData = nominalSerie.values.slice(start, end).map(v => typeof v === "number" ? v : null);
    if (!barData.some(value => value !== null)) return false;
    renderComboChart(canvasId, labels, {
        barLabel: nominalTitle,
        barData,
        lineLabel: "Growth y/y",
        lineData: growthValues,
        barFormatter: value => fmtVal(value, "mn"),
        lineFormatter: value => fmtVal(value, "growth"),
        color: COMPANY_VIEW_PRIMARY_COLOR,
    });
    return true;
}

function renderSingleCompanyMetricCombo(sheetName, nominalTitle, ratioTitle, canvasId, company, ratioMetric) {
    const rawNominal = findSection(sheetName, nominalTitle);
    const rawRatio = findSection(sheetName, ratioTitle);
    const nominal = sectionForCurrentPeriod(rawNominal, "sum");
    const ratio = compsPeriodMode === "year" && ratioMetric === "growth"
        ? (rawNominal ? annualGrowthSection(rawNominal, ratioTitle) : null)
        : sectionForCurrentPeriod(rawRatio, ratioMetric === "multiple" || ratioMetric === "margin" ? "avg" : undefined);
    if (!nominal || !ratio) return false;
    const nominalSerie = nominal.series.find(s => s.company === company);
    const ratioSerie = ratio.series.find(s => s.company === company);
    if (!nominalSerie || !ratioSerie) return false;
    const { start, end } = getCompanyFinancialSlice(nominal);
    const barData = nominalSerie.values.slice(start, end).map(v => typeof v === "number" ? Math.abs(v) : null);
    if (!barData.some(value => value !== null)) return false;
    const labels = nominal.periods.slice(start, end).map(fmtMonth);
    const ratioLookup = new Map(ratio.periods.map((p, idx) => [p, ratioSerie.values[idx]]));
    renderComboChart(canvasId, labels, {
        barLabel: nominalTitle,
        barData,
        lineLabel: ratioTitle,
        lineData: nominal.periods.slice(start, end).map(period => {
            const value = ratioLookup.get(period);
            return typeof value === "number" ? value : null;
        }),
        barFormatter: value => fmtVal(value, "mn"),
        lineFormatter: value => fmtVal(value, ratioMetric || "margin"),
        color: COMPANY_VIEW_PRIMARY_COLOR,
    });
    return true;
}

function renderSingleCompanyRatioChart(sheetName, ratioTitle, canvasId, company, ratioMetric) {
    const rawRatio = findSection(sheetName, ratioTitle);
    const ratio = sectionForCurrentPeriod(rawRatio, ratioMetric === "multiple" || ratioMetric === "margin" ? "avg" : undefined);
    if (!ratio) return false;
    const serie = ratio.series.find(s => s.company === company);
    if (!serie) return false;
    const { start, end } = getCompanyFinancialSlice(ratio);
    const labels = ratio.periods.slice(start, end).map(fmtMonth);
    const data = serie.values.slice(start, end).map(value => typeof value === "number" ? value : null);
    if (!data.some(value => value !== null)) return false;
    const chartData = reserveEndLabelSlot(labels, [{
        label: ratioTitle,
        data,
        borderColor: COMPANY_VIEW_PRIMARY_COLOR,
        backgroundColor: COMPANY_VIEW_PRIMARY_COLOR,
        tension: 0.25,
        pointRadius: 2,
        borderWidth: 2,
    }], true);

    destroyChart(canvasId);
    compsCharts[canvasId] = new Chart(document.getElementById(canvasId), {
        type: "line",
        data: chartData,
        options: baseOptions(value => fmtVal(value, ratioMetric || "multiple"), {
            companyLabels: true,
            endLabelSlot: true,
        }),
    });
    return true;
}

function renderComboChart(canvasId, labels, config) {
    destroyChart(canvasId);
    compsCharts[canvasId] = new Chart(document.getElementById(canvasId), {
        data: {
            labels,
            datasets: [
                {
                    type: "bar",
                    label: config.barLabel,
                    data: config.barData,
                    backgroundColor: comboBarFill(config.color),
                    borderColor: config.color || COMPS_COLORS.Vivo,
                    borderWidth: 1.5,
                    yAxisID: "y",
                    order: 2,
                },
                {
                    type: "line",
                    label: config.lineLabel,
                    data: config.lineData,
                    borderColor: COMBO_LINE_COLOR,
                    backgroundColor: "#ffffff",
                    pointBorderColor: COMBO_LINE_COLOR,
                    pointBackgroundColor: "#ffffff",
                    borderWidth: 2.5,
                    tension: 0.25,
                    pointRadius: 2.5,
                    pointHoverRadius: 4,
                    yAxisID: "y1",
                    order: 1,
                },
            ],
        },
        options: comboOptions(config.barFormatter, config.lineFormatter, config),
    });
}

function renderCompanySingleSeries(sheetName, title, canvasId, percent, company) {
    const section = findSection(sheetName, title);
    if (!section) return false;
    const serie = section.series.find(s => s.company === company);
    if (!serie) return false;
    const { start, end } = getCompanyFinancialSlice(section);
    const labels = section.periods.slice(start, end).map(fmtMonth);
    const datasets = [{
        label: company,
        data: serie.values.slice(start, end).map(v => typeof v === "number" ? v : null),
        borderColor: COMPANY_VIEW_PRIMARY_COLOR,
        backgroundColor: COMPANY_VIEW_PRIMARY_COLOR,
        tension: 0.25,
        pointRadius: 2,
    }];
    const chartData = reserveEndLabelSlot(labels, datasets, true);

    destroyChart(canvasId);
    compsCharts[canvasId] = new Chart(document.getElementById(canvasId), {
        type: "line",
        data: chartData,
        options: baseOptions(value => fmtVal(value, percent ? "growth" : "mn"), { percent, companyLabels: true, endLabelSlot: true }),
    });
    return true;
}

function renderCompanyMultiSeries(sheetName, specs, canvasId, percent, type, company) {
    const datasets = [];
    let labels = [];
    specs.forEach(([title, label], index) => {
        const section = sectionForCurrentPeriod(findSection(sheetName, title), "avg");
        if (!section) return;
        const serie = section.series.find(s => s.company === company);
        if (!serie) return;
        const { start, end } = getCompanyFinancialSlice(section);
        if (!labels.length) labels = section.periods.slice(start, end).map(fmtMonth);
        datasets.push({
            label,
            data: serie.values.slice(start, end).map(v => typeof v === "number" ? v : null),
            borderColor: COMPANY_ARPU_COLORS[label] || COMPANY_VIEW_PRIMARY_COLOR,
            backgroundColor: COMPANY_ARPU_COLORS[label] || COMPANY_VIEW_PRIMARY_COLOR,
            tension: 0.25,
            pointRadius: type === "bar" ? 0 : 2,
        });
    });
    if (!datasets.length) return false;
    const chartData = reserveEndLabelSlot(labels, datasets, type !== "bar");

    destroyChart(canvasId);
    compsCharts[canvasId] = new Chart(document.getElementById(canvasId), {
        type: type || "line",
        data: chartData,
        plugins: type === "bar" ? [ChartDataLabels] : [],
        options: baseOptions(value => fmtVal(value, percent ? "margin" : "mn"), {
            percent,
            datalabels: type === "bar",
            companyLabels: type !== "bar",
            endLabelSlot: type !== "bar",
        }),
    });
    return true;
}

async function ensureAnatelRows() {
    if (compsAnatelRows && compsAnatelRows.portability) return;
    const [bb, ftth, postpaid, prepaid, portability] = await Promise.all([
        fetch("/telecom/api/broadband").then(r => r.json()),
        fetch("/telecom/api/broadband?tech=FTTH").then(r => r.json()),
        fetch("/telecom/api/mobile?segment=Postpaid").then(r => r.json()),
        fetch("/telecom/api/mobile?segment=Prepaid").then(r => r.json()),
        fetch("/telecom/api/portability").then(r => r.json()),
    ]);
    compsAnatelRows = { bb, ftth, postpaid, prepaid, portability };
    populateAnatelRangeSelects([...bb, ...ftth, ...postpaid, ...prepaid]);
}

function getCompanyOperationalMonths() {
    const rows = Object.values(compsAnatelRows || {}).flat();
    const months = Array.from(new Set(rows.map(row => row.month))).sort();
    const fromValue = document.getElementById("vivo-operational-from-select").value;
    const toValue = document.getElementById("vivo-operational-to-select").value;
    return months.filter(month => (!fromValue || month >= fromValue) && (!toValue || month <= toValue));
}

function renderCompanyOperationalCharts() {
    if (!compsAnatelRows) return;
    const config = getSelectedCompanyConfig();
    setCompanyOperationalVisibility(config);
    if (!config.hasBrazilOps) return;
    const months = getCompanyOperationalMonths();
    renderCompanyBroadbandCharts(months, config);
    renderCompanyMobileCharts(months, config);
    renderCompanyPortabilityTable(months, config);
}

function setCompanyOperationalVisibility(config) {
    const display = config.hasBrazilOps ? "block" : "none";
    ["company-operational-header", "company-operational-filters", "company-broadband-card", "company-postpaid-card", "company-prepaid-card", "company-portability-card"]
        .forEach(id => document.getElementById(id).style.display = display);
}

function renderCompanyBroadbandCharts(months, config) {
    const ftthOnly = document.getElementById("vivo-ftth-toggle").checked;
    const rows = ftthOnly ? compsAnatelRows.ftth : compsAnatelRows.bb;
    const label = ftthOnly ? "FTTH" : "Broadband";
    document.getElementById("company-broadband-card").style.display =
        renderOperationalCombo("vivo-broadband-combo-chart", months, rows, label, config.operator) ? "block" : "none";
}

function renderCompanyMobileCharts(months, config) {
    document.getElementById("company-postpaid-card").style.display =
        renderOperationalCombo("vivo-postpaid-combo-chart", months, compsAnatelRows.postpaid, "Postpaid (ex-M2M)", config.operator) ? "block" : "none";
    document.getElementById("company-prepaid-card").style.display =
        renderOperationalCombo("vivo-prepaid-combo-chart", months, compsAnatelRows.prepaid, "Prepaid", config.operator) ? "block" : "none";
}

function renderOperationalCombo(canvasId, months, rows, label, operator) {
    const accessData = months.map(month => sumRows(rows, { operator, month }));
    if (!accessData.some(value => value > 0)) return false;
    const shareData = months.map(month => {
        const companyAccesses = sumRows(rows, { operator, month });
        const total = rows.filter(r => r.month === month).reduce((sum, r) => sum + (r.accesses || 0), 0);
        return total ? companyAccesses / total : null;
    });
    destroyChart(canvasId);
    compsCharts[canvasId] = new Chart(document.getElementById(canvasId), {
        data: {
            labels: months.map(fmtMonth),
            datasets: [
                {
                    type: "bar",
                    label: `${label} accesses`,
                    data: accessData,
                    backgroundColor: comboBarFill(peerColor(operator)),
                    borderColor: peerColor(operator),
                    borderWidth: 1.5,
                    yAxisID: "y",
                    order: 2,
                },
                {
                    type: "line",
                    label: `${label} market share`,
                    data: shareData,
                    borderColor: COMBO_LINE_COLOR,
                    backgroundColor: "#ffffff",
                    pointBorderColor: COMBO_LINE_COLOR,
                    pointBackgroundColor: "#ffffff",
                    borderWidth: 2.5,
                    tension: 0.25,
                    pointRadius: 2.5,
                    pointHoverRadius: 4,
                    yAxisID: "y1",
                    order: 1,
                },
            ],
        },
        options: comboOptions(formatAccesses, value => (value * 100).toFixed(1) + "%"),
    });
    return true;
}

function renderCompanyPortabilityTable(months, config) {
    const rows = compsAnatelRows.portability || [];
    const tableMonths = months.slice(-12);
    const table = document.getElementById("vivo-portability-table");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");

    thead.innerHTML = "";
    const header = document.createElement("tr");
    header.appendChild(th("Counterparty"));
    tableMonths.forEach(month => header.appendChild(th(fmtMonth(month))));
    thead.appendChild(header);

    tbody.innerHTML = "";
    const counterparties = ["Vivo", "TIM", "Claro", "Brisanet", "Unifique"].filter(name => name !== config.operator);
    document.getElementById("company-portability-title").textContent = `Net Portability - ${config.operator}`;
    counterparties.forEach(counterparty => {
        const tr = document.createElement("tr");
        tr.appendChild(td(counterparty));
        tableMonths.forEach(month => {
            const value = netPortability(rows, month, config.operator, counterparty);
            const cell = td(formatSignedInt(value));
            if (value > 0) cell.className = "val-positive";
            if (value < 0) cell.className = "val-negative";
            tr.appendChild(cell);
        });
        tbody.appendChild(tr);
    });
}

function netPortability(rows, month, company, counterparty) {
    return rows
        .filter(row => row.month === month)
        .reduce((sum, row) => {
            if (row.receiver === company && row.giver === counterparty) return sum + (row.quantity || 0);
            if (row.giver === company && row.receiver === counterparty) return sum - (row.quantity || 0);
            return sum;
        }, 0);
}

function formatSignedInt(value) {
    const text = Math.abs(value).toLocaleString("en-US", { maximumFractionDigits: 0 });
    if (value > 0) return "+" + text;
    if (value < 0) return "(" + text + ")";
    return "0";
}

function sumRows(rows, filters) {
    return rows
        .filter(row => Object.entries(filters).every(([key, value]) => row[key] === value))
        .reduce((sum, row) => sum + (row.accesses || 0), 0);
}

function formatAccesses(value) {
    if (value === null || value === undefined) return "-";
    if (Math.abs(value) >= 1e6) return (value / 1e6).toFixed(1) + "M";
    if (Math.abs(value) >= 1e3) return (value / 1e3).toFixed(0) + "k";
    return value.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function lastDataIndex(data) {
    for (let i = data.length - 1; i >= 0; i--) {
        if (data[i] !== null && data[i] !== undefined) return i;
    }
    return -1;
}

const END_LABEL_SLOT_WIDTH = 16;

function reserveEndLabelSlot(labels, datasets, enabled) {
    if (!enabled || !labels.length) return { labels, datasets };
    return {
        labels: [...labels, ""],
        datasets: datasets.map(dataset => ({
            ...dataset,
            data: [...dataset.data, null],
        })),
    };
}

function comboOptions(barFormatter, lineFormatter, config) {
    const hasEndLabelSlot = !!(config && config.endLabelSlot);
    return {
        responsive: true,
        maintainAspectRatio: false,
        layout: { padding: { right: hasEndLabelSlot ? END_LABEL_SLOT_WIDTH : 28, top: 8 } },
        interaction: { mode: "index", intersect: false },
        plugins: {
            legend: { position: "bottom", labels: { boxWidth: 10, usePointStyle: true } },
            tooltip: {
                callbacks: {
                    label: ctx => {
                        const formatter = ctx.dataset.yAxisID === "y1" ? lineFormatter : barFormatter;
                        return `${ctx.dataset.label}: ${formatter(ctx.parsed.y)}`;
                    },
                },
            },
            datalabels: {
                display: ctx => {
                    if (ctx.dataset.data[ctx.dataIndex] === null || ctx.dataset.data[ctx.dataIndex] === undefined) return false;
                    const last = ctx.dataIndex === lastDataIndex(ctx.dataset.data);
                    if (ctx.dataset.type === "line") return last;
                    return true;
                },
                anchor: "end",
                align: ctx => ctx.dataset.type === "line" ? "right" : "top",
                offset: ctx => ctx.dataset.type === "line" ? 6 : 4,
                clamp: true,
                color: ctx => datasetLabelColor(ctx.dataset),
                font: { size: 9, weight: "700" },
                formatter: (value, ctx) => {
                    const formatter = ctx.dataset.yAxisID === "y1" ? lineFormatter : barFormatter;
                    return formatter(value);
                },
            },
        },
        scales: {
            x: { grid: { display: false }, ticks: { maxRotation: 60, minRotation: 0 } },
            y: {
                position: "left",
                grace: "18%",
                ticks: { callback: value => barFormatter(value) },
                grid: { color: "rgba(0, 31, 98, 0.08)" },
            },
            y1: {
                position: "right",
                grace: "22%",
                ticks: { callback: value => lineFormatter(value) },
                grid: { drawOnChartArea: false },
            },
        },
    };
}

function datasetLabelColor(dataset) {
    const color = dataset.borderColor || dataset.backgroundColor || "#1f2937";
    if (Array.isArray(color)) return color[0] || "#1f2937";
    if (typeof color === "string" && color.startsWith("rgba")) {
        return dataset.borderColor || COMPS_COLORS.Vivo;
    }
    return color;
}

function baseOptions(formatter, config) {
    const hasEndLabelSlot = !!(config.companyLabels || config.denseLabels || config.endLabelSlot);
    const rightPadding = config.spacious ? 58 : (hasEndLabelSlot ? END_LABEL_SLOT_WIDTH : (config.endLabel !== false ? 28 : 0));
    return {
        responsive: true,
        maintainAspectRatio: false,
        layout: { padding: { right: rightPadding, top: config.spacious ? 14 : 8, left: config.spacious ? 8 : 0 } },
        interaction: { mode: "index", intersect: false },
        plugins: {
            legend: { position: "bottom", labels: { boxWidth: 10, usePointStyle: true } },
            tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${formatter(ctx.parsed.y)}` } },
            datalabels: {
                display: ctx => {
                    if (ctx.dataset.data[ctx.dataIndex] === null || ctx.dataset.data[ctx.dataIndex] === undefined) return false;
                    if (config.datalabels) return "auto";
                    if (config.denseLabels) return ctx.dataIndex === lastDataIndex(ctx.dataset.data);
                    if (config.companyLabels) return ctx.dataIndex === lastDataIndex(ctx.dataset.data);
                    if (config.endLabel === false) return false;
                    return ctx.dataIndex === lastDataIndex(ctx.dataset.data);
                },
                anchor: config.stacked ? "center" : "end",
                align: config.stacked ? "center" : (config.datalabels ? "top" : "right"),
                offset: config.stacked ? 2 : (config.datalabels ? 4 : 6),
                clamp: true,
                color: ctx => config.stacked ? "#ffffff" : datasetLabelColor(ctx.dataset),
                font: { size: config.denseLabels ? 9 : 10, weight: "700" },
                formatter: value => {
                    if (config.stacked && value < 0.06) return "";
                    return formatter(value);
                },
            },
        },
        scales: {
            x: {
                stacked: !!config.stacked,
                grid: { display: false },
                ticks: {
                    maxRotation: config.spacious ? 0 : 70,
                    minRotation: 0,
                    autoSkip: true,
                    maxTicksLimit: config.spacious ? 10 : undefined,
                },
            },
            y: {
                stacked: !!config.stacked,
                max: config.max,
                grace: config.spacious ? "28%" : "18%",
                ticks: { callback: value => formatter(value) },
                grid: { color: "rgba(0, 31, 98, 0.08)" },
            },
        },
    };
}
