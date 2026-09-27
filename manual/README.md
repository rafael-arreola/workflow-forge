# Integration manual

Open [index.html](index.html) directly in a browser. The English manual covers eight progressive
integration recipes, with a binding/control reference and links to complete runnable examples.

The independent [example workspace](ejemplos/Cargo.toml) keeps existing fixture names and sample
business payloads stable. It is not published. Run all examples with
`python3 scripts/run-examples.py` from the repository root.

Diagram source lives in `pre[data-flow]` elements. [assets/flows.js](assets/flows.js) loads pinned
Mermaid 11.12.0 with strict security and a white/black/gray palette. Styles use Tailwind and daisyUI
via CDN. Without CDN access, textual sources and captions remain readable. Large diagrams and
code samples scroll within their panels on narrow screens.

The manual and technical contracts are the maintained documentation. Historical generated viewer
bundles, screenshots, phase diaries and capacity snapshots were retired during release cleanup;
their history remains in Git.
