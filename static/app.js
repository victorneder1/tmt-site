// ── Dynamic column rendering ─────────────────────────────────────────────
// Columns and groups are built from the API response, not hardcoded.

// Build groups from flat column definitions returned by the API
function buildGroups(columns) {
    const groups = [];
    let currentGroup = null;

    for (const col of columns) {
        if (currentGroup && currentGroup.label === col.group) {
            currentGroup.cols.push({
                key: col.key,
                sub: col.label,
                type: col.type,
                decimals: col.decimals,
            });
        } else {
            currentGroup = {
                label: col.group,
                cols: [{
                    key: col.key,
                    sub: col.label,
                    type: col.type,
                    decimals: col.decimals,
                }],
            };
            groups.push(currentGroup);
        }
    }

    return groups;
}

// Flatten groups into a flat column list
function flatCols(groups) {
    const cols = [];
    groups.forEach(g => g.cols.forEach(c => cols.push(c)));
    return cols;
}

// State
let softwareData = [];
let softwareGroups = [];
let itservicesData = [];
let itservicesGroups = [];
let softwareSort = { key: null, dir: "desc" };
let itservicesSort = { key: null, dir: "desc" };
let currentView = "gaap";

// Clean ticker: remove suffixes like _CA, _FR, _IN, _US, _GB, _IL
function cleanTicker(ticker) {
    return ticker.replace(/_[A-Z]{2,3}$/, "");
}

// BTG estimate companies
const BTG_COMPANIES = ["Braze", "Zeta Global", "Globant", "CI&T", "VTEX"];

// Formatting
function formatValue(val, col) {
    if (val === null || val === undefined) return "-";
    if (col.type === "text") return val;
    if (col.type === "number") {
        return Number(val).toLocaleString("en-US", {
            minimumFractionDigits: col.decimals,
            maximumFractionDigits: col.decimals,
        });
    }
    if (col.type === "multiple") {
        return Number(val).toFixed(col.decimals) + "x";
    }
    if (col.type === "percent") {
        return (Number(val) * 100).toFixed(col.decimals) + "%";
    }
    return val;
}

function getColorClass(val, col) {
    if (col.type !== "percent" || val === null || val === undefined) return "";
    return val >= 0 ? "val-positive" : "val-negative";
}

// Median calculation
function computeMedian(data, key) {
    const vals = data.map(d => d[key]).filter(v => v !== null && v !== undefined && !isNaN(v));
    if (vals.length === 0) return null;
    vals.sort((a, b) => a - b);
    const mid = Math.floor(vals.length / 2);
    return vals.length % 2 !== 0 ? vals[mid] : (vals[mid - 1] + vals[mid]) / 2;
}

// Sorting
function sortData(data, sortState) {
    if (!sortState.key) return data;
    return [...data].sort((a, b) => {
        const va = a[sortState.key];
        const vb = b[sortState.key];
        if (va === null && vb === null) return 0;
        if (va === null) return 1;
        if (vb === null) return -1;
        let cmp;
        if (typeof va === "string") {
            cmp = va.localeCompare(vb);
        } else {
            cmp = va - vb;
        }
        return sortState.dir === "asc" ? cmp : -cmp;
    });
}

// Render table with grouped headers
function renderTable(tableId, data, groups, sortState, searchQuery) {
    const table = document.getElementById(tableId);
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");
    const tfoot = table.querySelector("tfoot");

    // Filter
    let filtered = data;
    if (searchQuery) {
        const q = searchQuery.toLowerCase();
        filtered = data.filter(
            d =>
                (d.name && d.name.toLowerCase().includes(q)) ||
                (d.ticker && cleanTicker(d.ticker).toLowerCase().includes(q))
        );
    }

    // Sort
    const sorted = sortData(filtered, sortState);

    // === HEADER ROW 1: Group names ===
    thead.innerHTML = "";
    const groupRow = document.createElement("tr");
    groupRow.classList.add("group-header");
    groups.forEach((g, gi) => {
        const th = document.createElement("th");
        th.textContent = g.label;
        th.colSpan = g.cols.length;
        if (g.cols.length > 1) {
            th.classList.add("grouped");
        }
        // Single-col groups span 2 rows
        if (g.cols.length === 1) {
            th.rowSpan = 2;
            th.classList.add("single-col");
            const col = g.cols[0];
            th.dataset.key = col.key;
            if (sortState.key === col.key) {
                th.classList.add(sortState.dir === "asc" ? "sort-asc" : "sort-desc");
            }
            th.addEventListener("click", () => {
                if (sortState.key === col.key) {
                    sortState.dir = sortState.dir === "asc" ? "desc" : "asc";
                } else {
                    sortState.key = col.key;
                    sortState.dir = col.type === "text" ? "asc" : "desc";
                }
                renderTable(tableId, data, groups, sortState, searchQuery);
            });
            // Set widths for fixed table layout
            if (col.key === "name") th.style.width = "150px";
            else if (col.key === "ticker") th.style.width = "65px";
        }
        if (g.label === "Company") th.classList.add("col-company");
        if (gi > 0) th.classList.add("group-border-left");
        groupRow.appendChild(th);
    });
    thead.appendChild(groupRow);

    // === HEADER ROW 2: Sub-headers (years) ===
    const subRow = document.createElement("tr");
    subRow.classList.add("sub-header");
    groups.forEach((g, gi) => {
        if (g.cols.length === 1) {
            return;
        }
        g.cols.forEach((col, ci) => {
            const th = document.createElement("th");
            th.textContent = col.sub;
            th.dataset.key = col.key;
            if (sortState.key === col.key) {
                th.classList.add(sortState.dir === "asc" ? "sort-asc" : "sort-desc");
            }
            th.addEventListener("click", () => {
                if (sortState.key === col.key) {
                    sortState.dir = sortState.dir === "asc" ? "desc" : "asc";
                } else {
                    sortState.key = col.key;
                    sortState.dir = col.type === "text" ? "asc" : "desc";
                }
                renderTable(tableId, data, groups, sortState, searchQuery);
            });
            if (ci === 0 && gi > 0) th.classList.add("group-border-left");
            subRow.appendChild(th);
        });
    });
    thead.appendChild(subRow);

    // === BODY ===
    tbody.innerHTML = "";
    sorted.forEach(row => {
        const isBTG = BTG_COMPANIES.includes(row.name);
        const tr = document.createElement("tr");
        if (isBTG) tr.classList.add("btg-row");
        groups.forEach((g, gIdx) => {
            g.cols.forEach((col, colInGroup) => {
                const td = document.createElement("td");
                let val = row[col.key];
                if (col.key === "ticker" && val) val = cleanTicker(val);

                if (col.key === "name" && isBTG) {
                    const nameSpan = document.createElement("span");
                    nameSpan.textContent = val;
                    const badge = document.createElement("span");
                    badge.className = "btg-badge";
                    badge.textContent = "BTGe";
                    td.appendChild(nameSpan);
                    td.appendChild(badge);
                } else {
                    td.textContent = col.key === "ticker" ? val : formatValue(val, col);
                }

                const cls = getColorClass(row[col.key], col);
                if (cls) td.classList.add(cls);
                if (col.key === "name") td.classList.add("col-company");
                if (colInGroup === 0 && gIdx > 0) td.classList.add("group-border-left");
                tr.appendChild(td);
            });
        });
        tbody.appendChild(tr);
    });

    // === MEDIAN FOOTER ===
    tfoot.innerHTML = "";
    const medianRow = document.createElement("tr");
    let footIdx = 0;
    groups.forEach((g, gIdx) => {
        g.cols.forEach((col, colInGroup) => {
            const td = document.createElement("td");
            if (footIdx === 0) {
                td.textContent = "Median";
                td.classList.add("col-company");
            } else if (col.type !== "text") {
                const median = computeMedian(filtered, col.key);
                td.textContent = formatValue(median, col);
            } else {
                td.textContent = "";
            }
            if (colInGroup === 0 && gIdx > 0) td.classList.add("group-border-left");
            medianRow.appendChild(td);
            footIdx++;
        });
    });
    tfoot.appendChild(medianRow);
}

// Set default sort to first numeric column if not already set
function ensureDefaultSort(sortState, groups) {
    if (sortState.key) {
        // Verify key still exists in columns
        const allKeys = flatCols(groups).map(c => c.key);
        if (allKeys.includes(sortState.key)) return;
    }
    // Default: sort alphabetically by company name
    sortState.key = "name";
    sortState.dir = "asc";
}

// Fetch & render
async function loadSoftware() {
    const res = await fetch(`/api/software?view=${currentView}`);
    const result = await res.json();
    softwareData = result.data;
    softwareGroups = buildGroups(result.columns);
    ensureDefaultSort(softwareSort, softwareGroups);
    const query = document.getElementById("software-search").value;
    renderTable("software-table", softwareData, softwareGroups, softwareSort, query);
}

async function loadITServices() {
    const res = await fetch("/api/itservices");
    const result = await res.json();
    itservicesData = result.data;
    itservicesGroups = buildGroups(result.columns);
    ensureDefaultSort(itservicesSort, itservicesGroups);
    const query = document.getElementById("itservices-search").value;
    renderTable("itservices-table", itservicesData, itservicesGroups, itservicesSort, query);
}

async function loadLastUpdated() {
    const res = await fetch("/api/last-updated");
    const data = await res.json();
    if (data.last_updated) {
        document.getElementById("last-updated").textContent =
            `Last Update: ${data.last_updated}`;
    }
}

// ── Main tab switching (Screening / Pair Trades) ──
document.querySelectorAll(".main-tab").forEach(tab => {
    tab.addEventListener("click", () => {
        document.querySelectorAll(".main-tab").forEach(t => t.classList.remove("active"));
        document.querySelectorAll(".main-content").forEach(s => s.classList.remove("active"));
        tab.classList.add("active");

        const target = tab.dataset.main;
        if (target === "screening") {
            document.getElementById("screening-main").classList.add("active");
            document.getElementById("screening-subtabs").style.display = "";
        } else if (target === "pairs") {
            document.getElementById("pairs-main").classList.add("active");
            document.getElementById("screening-subtabs").style.display = "none";
            if (typeof initPairs === "function") initPairs();
        }
    });
});

// ── Sub-tab switching (Software / IT Services) ──
document.querySelectorAll(".sub-tab").forEach(tab => {
    tab.addEventListener("click", () => {
        document.querySelectorAll(".sub-tab").forEach(t => t.classList.remove("active"));
        document.querySelectorAll(".tab-content").forEach(s => s.classList.remove("active"));
        tab.classList.add("active");
        const target = tab.dataset.tab;
        document.getElementById(`${target}-section`).classList.add("active");
        if (target === "itservices" && itservicesData.length === 0) {
            loadITServices();
        }
    });
});

// GAAP toggle
const gaapToggle = document.getElementById("gaap-toggle");
const gaapLabel = document.getElementById("gaap-label");
const nongaapLabel = document.getElementById("nongaap-label");

function updateToggleLabels() {
    if (gaapToggle.checked) {
        gaapLabel.classList.remove("active");
        nongaapLabel.classList.add("active");
        currentView = "nongaap";
    } else {
        gaapLabel.classList.add("active");
        nongaapLabel.classList.remove("active");
        currentView = "gaap";
    }
}

gaapToggle.addEventListener("change", () => {
    updateToggleLabels();
    loadSoftware();
});

// Search
document.getElementById("software-search").addEventListener("input", (e) => {
    renderTable("software-table", softwareData, softwareGroups, softwareSort, e.target.value);
});

document.getElementById("itservices-search").addEventListener("input", (e) => {
    renderTable("itservices-table", itservicesData, itservicesGroups, itservicesSort, e.target.value);
});

// Init
updateToggleLabels();
loadSoftware();
loadLastUpdated();

// Auto-refresh every 5 minutes
setInterval(() => {
    loadLastUpdated();
    loadSoftware();
    if (itservicesData.length > 0) {
        loadITServices();
    }
}, 5 * 60 * 1000);
