const form = document.getElementById("podcasts-form");
const statusEl = document.getElementById("podcasts-status");

function setStatus(message, type) {
    statusEl.textContent = message;
    statusEl.className = type ? `podcasts-status-${type}` : "";
}

form.addEventListener("submit", async event => {
    event.preventDefault();
    const data = new FormData(form);
    const payload = {
        name: data.get("name"),
        email: data.get("email"),
        podcasts: data.getAll("podcasts"),
    };

    setStatus("Saving...", "pending");

    try {
        const res = await fetch("/api/podcasts", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });
        const result = await res.json();
        if (!res.ok) throw new Error(result.error || "Unable to save preferences");
        setStatus("Saved.", "success");
    } catch (err) {
        setStatus(err.message, "error");
    }
});
