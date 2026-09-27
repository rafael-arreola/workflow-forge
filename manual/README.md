# Manual de integración embebida

Abre [index.html](index.html) o entra directamente a [recetas.html](recetas.html).
El motor se integra dentro del host Rust; no hay servidor HTTP del motor ni cliente remoto.
Los conectores HTTP documentados son operaciones salientes configuradas por el host.

## Recorrido práctico

Las ocho recetas conectan los capítulos de referencia con diez programas completos en
[ejemplos](ejemplos/Cargo.toml): inicio, sistema, cancelacion, modulos, respuestas,
importacion, controles, artefactos, durable y senales.

Cada receta distingue fragmentos de programas completos, explica entradas/salidas y
mantiene el cierre del runtime bajo responsabilidad del host. Los ejemplos ordinarios
usan `execute`; recibos, deduplicación y señales conservan la API explícita de seguimiento.
Los fixtures locales no acreditan integraciones reales ni rendimiento en producción.

## Diagramas

Los diagramas Archify existentes conservan su [mantenimiento](diagramas/README.md).
Las recetas añaden siete diagramas Mermaid inline, autorizados para ilustrar los datos
y el comportamiento sin regenerar los diagramas Archify.

- La fuente de cada flujo está en `recetas.html`, dentro de `pre[data-flow]`.
- [assets/flujos.js](assets/flujos.js) carga Mermaid **11.12.0** por CDN, en modo estricto.
- La paleta es blanca, negra y gris; no se habilitan animaciones.
- El manual abre como archivo local. La fuente textual y los pies de figura permanecen
  disponibles si falla la carga del CDN; los diagramas anchos se desplazan en móvil.
- Referencias: [API de Mermaid](https://mermaid.js.org/config/usage.html) y
  [configuración de temas](https://mermaid.js.org/config/theming.html).

## Verificación de esta actualización · 2026-09-27

Se ejecutaron los ocho programas nuevos o modificados: `inicio`, `sistema`, `cancelacion`,
`modulos`, `respuestas`, `importacion`, `controles` y `artefactos`; todos finalizaron
con éxito y comprobaron sus resultados. Casos borde incluidos: cancelación, deadline,
status no previsto, campo ausente, fallback general, lote vacío y error parcial.

```sh
cargo check --manifest-path manual/ejemplos/Cargo.toml --locked --bins
cargo fmt --manifest-path manual/ejemplos/Cargo.toml --all --check
cargo clippy --manifest-path manual/ejemplos/Cargo.toml -p workflow-forge-manual --bins --locked --no-deps -- -D warnings
cargo run --manifest-path manual/ejemplos/Cargo.toml --locked --bin respuestas
```

También se revisaron destinos locales y anchors de los capítulos, renderizado de los
siete diagramas y ausencia de desbordamiento horizontal de página en navegador.
Se revisaron capturas de escritorio y móvil. No se repitieron la suite del engine,
benchmarks ni integraciones externas. `durable` y `senales` no cambiaron en esta actualización
ni se volvieron a ejecutar.
