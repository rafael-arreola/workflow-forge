# Diagramas del manual

Diagramas creados con Archify 2.17 y adaptados a la paleta neutra del manual por petición del usuario. El contenido explica la librería embebida y los ejemplos del manual; conserva el texto de los capítulos. Abre [el índice del manual](../index.html#diagramas) para recorrerlos.

| Diagrama | Tipo Archify | Capítulos | Evidencia del contenido |
|---|---|---|---|
| [Arquitectura](arquitectura.html) | `architecture` | Arquitectura | [Composición](../../crates/forge/src/v2.rs), [runtime](../../crates/engine/src/runtime/lifecycle.rs), [API](../../crates/engine/src/runtime/application.rs) |
| [Arranque y cierre](arranque.html) | `sequence` | Integración | [Programa completo](../ejemplos/src/bin/inicio.rs) |
| [Módulos](modulos.html) | `workflow` | Módulos | [Contrato](../../crates/protocol/src/operation.rs), [extensión de saludo](../ejemplos/modulo-saludos/src/lib.rs) |
| [Datos](datos.html) | `dataflow` | Workflows y datos | [Definición ejecutable](../ejemplos/workflows/saludo.json) |
| [Señales](senales.html) | `sequence` | Procesos | [Reserva y entrega](../ejemplos/src/bin/senales.rs), [contrato de esperas](../../crates/engine/src/runtime/waits.rs) |

El diagrama de señales añade un transporte conceptual implementado por el host; el ejemplo Rust simula la entrega dentro del mismo proceso. No representa una integración externa ya comprobada. Los diagramas omiten detalles para enseñar un recorrido; los contratos y ejemplos enlazados mantienen la autoridad sobre la API.

## Archivos

- `NOMBRE.json`: especificación editable de Archify. Estos JSON **no son workflows ejecutables de Workflow Forge**.
- `NOMBRE.html`: visor interactivo autocontenido generado por Archify y adaptado con `apply-theme.py`. Se abre en claro por defecto; ambos temas usan blancos, negros y grises.
- `NOMBRE.svg`: vista estática clara, derivada del SVG del HTML entregado con sus estilos calculados; se inserta en el manual como imagen enlazada al visor.
- `NOMBRE.delivery.json`: recibo del HTML original de Archify, anterior a la adaptación de colores.
- `manual-theme.css` y `apply-theme.py`: paleta compartida y transformación reproducible. Se adaptan las variables semánticas y los colores fijos de los estilos, conservando geometría y movimiento.
- `NOMBRE.theme.json`: hashes del HTML original, la paleta, el transformador y el HTML final.
- `NOMBRE.check.json`: comprobación del HTML final con la paleta del manual.
- `NOMBRE.visual-check.*`: evidencia automática de navegador, capturas y hoja de contacto. Su campo `visualReview: pending` no representa una revisión perceptual.
- [verificacion.json](verificacion.json): resumen de entrega, evidencia automática y revisión visual por separado, ligado a los hashes finales.

El contenido del diagrama está en español; Archify utiliza controles y `<html lang>` en inglés. El manual sigue en español y conserva Tailwind por CDN. Las vistas SVG no necesitan JavaScript.

## Actualización

Edita primero la especificación y comprueba los nombres y relaciones contra el código enlazado. Desde la raíz del repositorio, por ejemplo:

```sh
node "$HOME/.agents/skills/archify/bin/archify.mjs" validate architecture manual/diagramas/arquitectura.json --quality showcase --json
node "$HOME/.agents/skills/archify/bin/archify.mjs" deliver architecture manual/diagramas/arquitectura.json manual/diagramas/arquitectura.html --quality showcase --json > manual/diagramas/arquitectura.delivery.json
python3 manual/diagramas/apply-theme.py arquitectura
node "$HOME/.agents/skills/archify/bin/archify.mjs" check manual/diagramas/arquitectura.html
node "$HOME/.agents/skills/archify/bin/archify.mjs" visual-check manual/diagramas/arquitectura.html --json
```

No continúes al siguiente comando si el anterior falla. Usa el tipo de la tabla para cada archivo. Después de entregar, actualiza la vista SVG desde el visor en tema claro y comprueba su lectura dentro del capítulo. Revisa las capturas y renueva `verificacion.json` con los hashes actuales. Para cambiar colores, edita `manual-theme.css` y vuelve a generar y adaptar el HTML. El recibo de `deliver` acredita la salida original; `theme.json`, `check.json` y la evidencia visual corresponden a la salida final adaptada. No edites el HTML final manualmente, para conservar esta trazabilidad.

Esta revisión cubre diagramas, enlaces y su presentación. No repite las pruebas del motor ni acredita nuevas integraciones reales.
