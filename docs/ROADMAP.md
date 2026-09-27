# Workflow Forge — hoja de ruta vigente

El objetivo es una librería Rust embebida en un subsistema mayor. Un editor futuro genera workflows; el código Rust implementa capacidades reutilizables. El servicio y la CLI quedaron fuera del alcance por decisión del usuario. Las fases históricas F-0–F-5 permanecen en Git y no acreditan el reajuste actual.

## Reajuste de la librería

| Entrega | Criterio de salida |
|---|---|
| Fronteras | Workspace sin servicio/CLI; host dueño de transportes y executor. |
| Propiedad | `execute` devuelve valor/error, admite cancelación y cancela al descartar su future; limpieza supervisada y cierre del runtime. |
| Arranque | No reanudar trabajo automáticamente; rechazar pendientes por defecto y permitir recuperación explícita con el deadline original. |
| Resultados | Operaciones con schemas; HTTP expone status/body; utilidades permiten evaluar salidas. |
| Errores | `try`, códigos específicos y fallback; propagación al ámbito exterior; incertidumbre y límites no se ocultan. |
| Extensión | Protocolos, módulos y extensiones permanecen separados; patrones documentados sin nuevas jerarquías innecesarias. |
| Adopción | API, ejemplos, PRD, arquitectura, TDD, contratos, manual, diagramas y AGENTS coherentes con embedding. |
| Evidencia | Casos esenciales y borde de los contratos afectados; enlaces y diagramas comprobados. |

[PROJECT](PROJECT.md) registra qué partes están implementadas y cuáles verificadas. [EMBEDDING](EMBEDDING.md) contiene los contratos detallados; no sustituir este alcance por conservar únicamente compatibilidad con la implementación previa.

## Verificación

Comprobar resultado correcto, rutas por datos, handler específico/default, fallo de handler, presupuesto, cancelación del host, descarte del future, cierre y recuperación explícita. Para efectos, una escritura incierta nunca se convierte en éxito por pasar por un fallback. Para HTTP se usan fixtures locales, no destinos reales.

Ejecutar formato, Clippy y pruebas afectadas. Como cambian contratos compartidos y el workspace, una pasada funcional del workspace verifica consumidores; después repetir únicamente lo afectado por fallos o correcciones. No repetir benchmarks ni matrices extensas de capacidad para cerrar este reajuste.

## Después del engine

Las integraciones reales (P-01), capacidad de sus cargas (P-07), UI y otros módulos se atienden cuando el usuario los elija. No son condiciones implícitas de esta entrega. La librería no incorpora soporte remoto, cron, workers distribuidos ni un servidor autónomo.
