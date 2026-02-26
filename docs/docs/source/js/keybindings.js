function isTyping() {
    var active = document.activeElement;
    var tag = active.tagName.toLowerCase();
    return (
        tag === "input" ||
        tag === "textarea" ||
        tag === "select" ||
        active.isContentEditable
    );
}

function getNextLink() {
    return (
        document.querySelector("link[rel='next']") ||
        document.querySelector(".rst-footer-buttons .btn-neutral.float-right")
    );
}

function getPrevLink() {
    return (
        document.querySelector("link[rel='prev']") ||
        document.querySelector(".rst-footer-buttons .btn-neutral:not(.float-right)")
    );
}

function createHelpModal() {
    var modal = document.createElement("div");
    modal.id = "keybinding-help";
    modal.innerHTML =
        '<div id="keybinding-overlay" style="' +
            "position: fixed;" +
            "inset: 0;" +
            "background: rgba(0, 0, 0, 0.5);" +
            "z-index: 9998;" +
        '"></div>' +
        '<div style="' +
            "position: fixed;" +
            "top: 50%;" +
            "left: 50%;" +
            "transform: translate(-50%, -50%);" +
            "background: white;" +
            "padding: 24px 32px;" +
            "border-radius: 10px;" +
            "z-index: 9999;" +
            "box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);" +
            "min-width: 320px;" +
            "font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;" +
        '">' +
            '<h3 style="margin-top: 0; margin-bottom: 16px;">⌨️ Keyboard Shortcuts</h3>' +
            '<table style="width: 100%; border-collapse: collapse;">' +
                "<tr>" +
                    '<td style="padding: 6px 12px 6px 0;"><kbd style="' +
                        "background: #f4f4f4; border: 1px solid #ccc; border-radius: 3px;" +
                        "padding: 2px 8px; font-size: 13px;" +
                    '">→</kbd></td>' +
                    '<td style="padding: 6px 0;">Next page</td>' +
                "</tr>" +
                "<tr>" +
                    '<td style="padding: 6px 12px 6px 0;"><kbd style="' +
                        "background: #f4f4f4; border: 1px solid #ccc; border-radius: 3px;" +
                        "padding: 2px 8px; font-size: 13px;" +
                    '">←</kbd></td>' +
                    '<td style="padding: 6px 0;">Previous page</td>' +
                "</tr>" +
                "<tr>" +
                    '<td style="padding: 6px 12px 6px 0;"><kbd style="' +
                        "background: #f4f4f4; border: 1px solid #ccc; border-radius: 3px;" +
                        "padding: 2px 8px; font-size: 13px;" +
                    '">Esc</kbd></td>' +
                    '<td style="padding: 6px 0;">Close this dialog</td>' +
                "</tr>" +
                "<tr>" +
                    '<td style="padding: 6px 12px 6px 0;"><kbd style="' +
                        "background: #f4f4f4; border: 1px solid #ccc; border-radius: 3px;" +
                        "padding: 2px 8px; font-size: 13px;" +
                    '">?</kbd></td>' +
                    '<td style="padding: 6px 0;">Toggle this help</td>' +
                "</tr>" +
            "</table>" +
            '<p style="color: #808080; font-size: 0.85em; margin-bottom: 0; margin-top: 16px;">' +
                "Press <kbd>Esc</kbd>, <kbd>?</kbd>, or click outside to close" +
            "</p>" +
        "</div>";

    return modal;
}

function toggleHelpModal() {
    var existing = document.getElementById("keybinding-help");
    if (existing) {
        existing.remove();
        return;
    }

    var modal = createHelpModal();
    document.body.appendChild(modal);

    document.getElementById("keybinding-overlay").addEventListener("click", function () {
        modal.remove();
    });
}

document.addEventListener("keydown", function (event) {
    // Allow Escape to close modal even when typing
    if (event.key === "Escape") {
        var modal = document.getElementById("keybinding-help");
        if (modal) {
            modal.remove();
            return;
        }
    }

    if (isTyping()) return;

    // ? key → toggle help modal
    if (event.key === "?") {
        event.preventDefault();
        toggleHelpModal();
        return;
    }

    // Don't navigate if help modal is open
    if (document.getElementById("keybinding-help")) return;

    // Right arrow key → next page
    if (event.key === "ArrowRight") {
        var nextLink = getNextLink();
        if (nextLink) {
            window.location.href = nextLink.href;
        }
    }

    // Left arrow key → previous page
    if (event.key === "ArrowLeft") {
        var prevLink = getPrevLink();
        if (prevLink) {
            window.location.href = prevLink.href;
        }
    }
});