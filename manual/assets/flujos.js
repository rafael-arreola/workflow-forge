// Mermaid is presentation only; these diagrams are not executable workflows.
// Pinned CDN import also works when the manual is opened through file://.
(async () => {
  const diagrams = [...document.querySelectorAll('[data-flow]')];
  if (!diagrams.length) return;
  try {
    const { default: mermaid } = await import('https://cdn.jsdelivr.net/npm/mermaid@11.12.0/dist/mermaid.esm.min.mjs');
    mermaid.initialize({
      startOnLoad: false, securityLevel: 'strict', theme: 'base',
      fontFamily: 'system-ui, sans-serif',
      themeVariables: {
        primaryColor: '#ffffff', primaryTextColor: '#171717', primaryBorderColor: '#737373',
        secondaryColor: '#f5f5f5', tertiaryColor: '#e5e5e5', lineColor: '#525252',
        textColor: '#171717', mainBkg: '#ffffff', nodeBorder: '#737373',
        edgeLabelBackground: '#ffffff', clusterBkg: '#f5f5f5', clusterBorder: '#a3a3a3',
        actorBkg: '#ffffff', actorBorder: '#737373', actorTextColor: '#171717',
        actorLineColor: '#737373', signalColor: '#525252', signalTextColor: '#171717',
        labelBoxBkgColor: '#f5f5f5', labelBoxBorderColor: '#737373', labelTextColor: '#171717',
        activationBkgColor: '#e5e5e5', activationBorderColor: '#737373',
        noteBkgColor: '#f5f5f5', noteBorderColor: '#737373', noteTextColor: '#171717'
      },
      flowchart: { htmlLabels: false, useMaxWidth: true },
      sequence: { useMaxWidth: true, actorMargin: 30, width: 110, wrap: true }
    });
    for (const [index, diagram] of diagrams.entries()) {
      const { svg } = await mermaid.render(`manual-flow-${index}`, diagram.textContent);
      diagram.innerHTML = svg;
      if (diagram.dataset.minWidth) {
        diagram.querySelector('svg').style.minWidth = `${diagram.dataset.minWidth}px`;
      }
      diagram.dataset.rendered = 'true';
    }
    document.documentElement.dataset.flows = 'ready';
  } catch (error) {
    // Preserve the readable source and captions if the CDN cannot load.
    document.documentElement.dataset.flows = 'unavailable';
    console.warn('No se pudieron dibujar todos los flujos; se conserva su descripción.', error);
  }
})();
