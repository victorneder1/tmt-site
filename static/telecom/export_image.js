(function () {
    const EXPORT_SELECTOR = "#anatel-main .anatel-content .card";
    const EXPORT_SCALE = 3;

    function slugify(text) {
        return String(text || "anatel-card")
            .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, "-")
            .replace(/^-+|-+$/g, "")
            .slice(0, 90) || "anatel-card";
    }

    function activeTabName(card) {
        const panel = card.closest(".anatel-content");
        return panel ? panel.id.replace("-panel", "") : "anatel";
    }

    function currentPeriodLabel(card) {
        const panel = card.closest(".anatel-content");
        if (!panel) return "";
        const to = panel.querySelector("select[id$='to-select']");
        const month = panel.querySelector("#ov-map-month");
        const selected = to || month;
        if (!selected || !selected.value) return "";
        return selected.value;
    }

    function cardTitle(card) {
        const title = card.querySelector(".card-title");
        return title ? title.textContent.trim() : "Anatel Card";
    }

    function displayTitle(card) {
        return cardTitle(card).replace(/\s+/g, " ").trim().toUpperCase();
    }

    function filenameFor(card) {
        const parts = ["anatel", activeTabName(card), cardTitle(card), currentPeriodLabel(card)]
            .filter(Boolean)
            .map(slugify);
        return parts.join("_") + ".png";
    }

    function addExportButtons() {
        document.querySelectorAll(EXPORT_SELECTOR).forEach(card => {
            if (card.dataset.exportImageReady === "1") return;
            if (!card.querySelector(".chart-container canvas, .table-wrapper table")) return;

            card.dataset.exportImageReady = "1";
            card.classList.add("exportable-card");

            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "card-export-image-btn";
            btn.title = "Export as image";
            btn.setAttribute("aria-label", "Export this card as image");
            btn.innerHTML = `
                <svg viewBox="0 0 24 24" aria-hidden="true">
                    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                    <path d="M7 10l5 5 5-5"></path>
                    <path d="M12 15V3"></path>
                </svg>
            `;
            btn.addEventListener("click", event => {
                event.preventDefault();
                event.stopPropagation();
                exportCard(card, btn).catch(err => {
                    console.error("[Export image]", err);
                    btn.classList.remove("is-exporting");
                    btn.disabled = false;
                    alert("Could not export this card as image.");
                });
            });
            const header = card.querySelector(".ov-card-head");
            if (header) {
                btn.classList.add("in-flow");
                header.appendChild(btn);
            } else {
                card.appendChild(btn);
            }
        });
    }

    function copyComputedStyles(source, target) {
        if (source.nodeType !== 1 || target.nodeType !== 1) return;
        const computed = window.getComputedStyle(source);
        const style = target.style;
        for (const prop of computed) {
            style.setProperty(prop, computed.getPropertyValue(prop), computed.getPropertyPriority(prop));
        }
        for (let i = 0; i < source.children.length; i++) {
            copyComputedStyles(source.children[i], target.children[i]);
        }
    }

    function replaceCanvasWithImages(source, clone) {
        const sourceCanvases = source.querySelectorAll("canvas");
        const cloneCanvases = clone.querySelectorAll("canvas");
        sourceCanvases.forEach((canvas, idx) => {
            const cloneCanvas = cloneCanvases[idx];
            if (!cloneCanvas) return;
            const img = document.createElement("img");
            img.src = canvas.toDataURL("image/png");
            img.style.width = canvas.getBoundingClientRect().width + "px";
            img.style.height = canvas.getBoundingClientRect().height + "px";
            img.style.display = "block";
            cloneCanvas.replaceWith(img);
        });
    }

    function prepareClone(source) {
        const rect = source.getBoundingClientRect();
        const clone = source.cloneNode(true);
        copyComputedStyles(source, clone);
        replaceCanvasWithImages(source, clone);

        clone.querySelectorAll(".card-export-image-btn").forEach(btn => btn.remove());
        const header = document.createElement("div");
        header.className = "export-image-header";
        header.innerHTML = `<span>${displayTitle(source)}</span><strong>TMT BTG Pactual</strong>`;
        header.setAttribute(
            "style",
            "display:flex;align-items:center;justify-content:space-between;gap:24px;" +
            "padding-bottom:9px;margin-bottom:14px;border-bottom:2px solid #195AB4;" +
            "color:#001F62;font-family:'BTG Pactual','Helvetica Neue',Helvetica,Arial,sans-serif;"
        );
        header.querySelector("span").setAttribute(
            "style",
            "min-width:0;font-size:15px;line-height:1.25;font-weight:700;text-transform:uppercase;"
        );
        header.querySelector("strong").setAttribute(
            "style",
            "flex:0 0 auto;font-size:12px;line-height:1.25;font-weight:700;color:#195AB4;white-space:nowrap;"
        );
        clone.insertBefore(header, clone.firstChild);

        const firstTitle = clone.querySelector(".card-title");
        if (firstTitle) firstTitle.remove();
        clone.querySelectorAll(".table-wrapper").forEach(wrapper => {
            wrapper.style.maxHeight = "none";
            wrapper.style.height = "auto";
            wrapper.style.overflow = "visible";
        });
        clone.querySelectorAll(".chart-container").forEach(container => {
            container.style.overflow = "visible";
        });
        clone.style.position = "absolute";
        clone.style.left = "0";
        clone.style.top = "0";
        clone.style.width = Math.ceil(rect.width) + "px";
        clone.style.height = "auto";
        clone.style.margin = "0";
        clone.style.boxSizing = "border-box";
        clone.style.background = "#ffffff";
        clone.style.paddingTop = "18px";
        clone.classList.add("export-image-clone");

        return clone;
    }

    function imageFromSvg(svgText) {
        return new Promise((resolve, reject) => {
            const blob = new Blob([svgText], { type: "image/svg+xml;charset=utf-8" });
            const url = URL.createObjectURL(blob);
            const img = new Image();
            img.onload = () => {
                URL.revokeObjectURL(url);
                resolve(img);
            };
            img.onerror = () => {
                URL.revokeObjectURL(url);
                reject(new Error("SVG render failed"));
            };
            img.src = url;
        });
    }

    function triggerDownload(blob, filename) {
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        link.remove();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
    }

    async function exportCard(card, button) {
        button.disabled = true;
        button.classList.add("is-exporting");

        const canvas = card.querySelector(".table-wrapper table")
            ? renderTableCard(card)
            : renderChartCard(card);
        await downloadCanvas(canvas, filenameFor(card));

        button.classList.remove("is-exporting");
        button.disabled = false;
    }

    function makeCanvas(width, height) {
        const canvas = document.createElement("canvas");
        canvas.width = Math.ceil(width * EXPORT_SCALE);
        canvas.height = Math.ceil(height * EXPORT_SCALE);
        const ctx = canvas.getContext("2d");
        ctx.scale(EXPORT_SCALE, EXPORT_SCALE);
        ctx.fillStyle = "#ffffff";
        ctx.fillRect(0, 0, width, height);
        ctx.textBaseline = "middle";
        return { canvas, ctx };
    }

    function drawExportHeader(ctx, width, title) {
        const pad = 24;
        ctx.fillStyle = "#001F62";
        ctx.font = "700 15px Arial, sans-serif";
        ctx.textAlign = "left";
        ctx.fillText(title, pad, 28);
        ctx.fillStyle = "#195AB4";
        ctx.font = "700 12px Arial, sans-serif";
        ctx.textAlign = "right";
        ctx.fillText("TMT BTG Pactual", width - pad, 28);
        ctx.strokeStyle = "#195AB4";
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.moveTo(pad, 48);
        ctx.lineTo(width - pad, 48);
        ctx.stroke();
    }

    function drawTextInCell(ctx, text, x, y, width, height, options) {
        const pad = options && options.pad !== undefined ? options.pad : 8;
        ctx.save();
        ctx.beginPath();
        ctx.rect(x + 1, y + 1, width - 2, height - 2);
        ctx.clip();
        ctx.textAlign = options && options.align ? options.align : "left";
        ctx.fillText(String(text || ""), x + pad, y + height / 2);
        ctx.restore();
    }

    function renderTableCard(card) {
        const table = card.querySelector(".table-wrapper table");
        const rows = Array.from(table.querySelectorAll("tr")).map(tr =>
            Array.from(tr.children).map(cell => cell.innerText.trim().replace(/\s+/g, " "))
        ).filter(row => row.length);
        const colCount = Math.max(...rows.map(row => row.length));
        const colWidths = Array.from({ length: colCount }, (_, idx) => {
            const sample = rows.map(row => row[idx] || "");
            const maxChars = Math.max(...sample.map(text => text.length), 8);
            if (idx === 0) return Math.min(Math.max(maxChars * 7 + 22, 120), 230);
            if (idx === 1 || idx === 2) return Math.min(Math.max(maxChars * 7 + 22, 100), 170);
            return Math.min(Math.max(maxChars * 7 + 22, 78), 120);
        });
        const width = Math.max(860, colWidths.reduce((a, b) => a + b, 0) + 48);
        const rowH = 28;
        const headerH = 64;
        const height = headerH + rows.length * rowH + 28;
        const { canvas, ctx } = makeCanvas(width, height);

        drawExportHeader(ctx, width, displayTitle(card));
        let y = headerH;
        rows.forEach((row, rowIdx) => {
            const isHead = rowIdx < table.tHead?.rows.length;
            ctx.fillStyle = isHead ? "#17365D" : (rowIdx % 2 ? "#ffffff" : "#f5f8fc");
            ctx.fillRect(24, y, width - 48, rowH);
            ctx.strokeStyle = "#d9e2f3";
            ctx.lineWidth = 1;

            let x = 24;
            row.forEach((text, colIdx) => {
                const w = colWidths[colIdx];
                ctx.strokeRect(x, y, w, rowH);
                ctx.fillStyle = isHead ? "#ffffff" : "#1f2937";
                ctx.font = `${isHead ? "700" : "500"} 11px Arial, sans-serif`;
                drawTextInCell(ctx, text, x, y, w, rowH, { align: colIdx >= 3 ? "right" : "left", pad: colIdx >= 3 ? w - 8 : 8 });
                x += w;
            });
            y += rowH;
        });
        return canvas;
    }

    function activeLegendItems(card) {
        return Array.from(card.querySelectorAll(".ov-company-buttons button.active")).map(btn => {
            const label = btn.textContent.trim();
            const color = btn.style.getPropertyValue("--company-color") || "#001F62";
            return { label, color };
        });
    }

    function renderChartCard(card) {
        const sourceCanvas = card.querySelector(".chart-container canvas");
        const rect = card.getBoundingClientRect();
        const sourceRect = sourceCanvas.getBoundingClientRect();
        const width = Math.max(860, Math.ceil(rect.width));
        const chartW = width - 48;
        const chartH = Math.max(280, Math.ceil(sourceRect.height * chartW / Math.max(sourceRect.width, 1)));
        const legend = activeLegendItems(card);
        const footnote = card.querySelector(".chart-footnote")?.innerText.trim() || "";
        const legendRows = legend.length ? Math.ceil(legend.length / 4) : 0;
        const legendH = legendRows * 24;
        const footH = footnote ? 28 : 0;
        const height = 64 + chartH + legendH + footH + 28;
        const { canvas, ctx } = makeCanvas(width, height);

        drawExportHeader(ctx, width, displayTitle(card));
        ctx.drawImage(sourceCanvas, 24, 64, chartW, chartH);

        let y = 64 + chartH + 14;
        if (legend.length) {
            ctx.font = "700 11px Arial, sans-serif";
            ctx.textAlign = "left";
            legend.forEach((item, idx) => {
                const col = idx % 4;
                const row = Math.floor(idx / 4);
                const x = 28 + col * ((width - 56) / 4);
                const yy = y + row * 24;
                ctx.fillStyle = item.color;
                ctx.beginPath();
                ctx.arc(x, yy, 4, 0, Math.PI * 2);
                ctx.fill();
                ctx.fillStyle = "#4b5563";
                ctx.fillText(item.label, x + 10, yy);
            });
            y += legendH;
        }

        if (footnote) {
            ctx.fillStyle = "#8a93a6";
            ctx.font = "italic 11px Arial, sans-serif";
            ctx.textAlign = "center";
            ctx.fillText(footnote, width / 2, y + 10);
        }
        return canvas;
    }

    function downloadCanvas(canvas, filename) {
        return new Promise((resolve, reject) => {
            canvas.toBlob(blob => {
                if (!blob) {
                    reject(new Error("PNG creation failed"));
                    return;
                }
                triggerDownload(blob, filename);
                resolve();
            }, "image/png");
        });
    }

    function initExportImages() {
        addExportButtons();
        const observer = new MutationObserver(() => addExportButtons());
        const root = document.getElementById("anatel-main");
        if (root) observer.observe(root, { childList: true, subtree: true });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initExportImages);
    } else {
        initExportImages();
    }
})();
