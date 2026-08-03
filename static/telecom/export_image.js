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
            if (!card.querySelector("canvas, table, svg, #ov-brazil-map-container, #ov-state-panel")) return;

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

        const staging = document.createElement("div");
        staging.className = "export-image-stage";
        const clone = prepareClone(card);
        staging.appendChild(clone);
        document.body.appendChild(staging);

        await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));

        const width = Math.ceil(clone.scrollWidth);
        const height = Math.ceil(clone.scrollHeight);
        const xhtml = new XMLSerializer().serializeToString(clone);
        const svg = `
            <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">
                <foreignObject width="100%" height="100%">
                    <div xmlns="http://www.w3.org/1999/xhtml">${xhtml}</div>
                </foreignObject>
            </svg>
        `;

        const img = await imageFromSvg(svg);
        const canvas = document.createElement("canvas");
        canvas.width = width * EXPORT_SCALE;
        canvas.height = height * EXPORT_SCALE;
        const ctx = canvas.getContext("2d");
        ctx.fillStyle = "#ffffff";
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.drawImage(img, 0, 0, canvas.width, canvas.height);

        await new Promise((resolve, reject) => {
            canvas.toBlob(blob => {
                if (!blob) {
                    reject(new Error("PNG creation failed"));
                    return;
                }
                triggerDownload(blob, filenameFor(card));
                resolve();
            }, "image/png");
        });

        staging.remove();
        button.classList.remove("is-exporting");
        button.disabled = false;
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
