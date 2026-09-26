# Workflow Forge — hoja de ruta de refactorización

Fecha: 2026-09-26. Secuencia de implementación autorizada, sin estimaciones de esfuerzo. El estado y evidencia de cada fase están en [PROJECT](PROJECT.md); cada fase completa se conserva en un commit. [PRD](PRD.md), [ARCHITECTURE](ARCHITECTURE.md), [TDD](TDD.md).

## Reglas de ejecución

Cada fase usa las decisiones adoptadas del PRD y concreta los detalles aún abiertos de su alcance antes de programar. Registrar resultados de checks en PROJECT. No sustituir aceptación por cantidad de código ni por compilar traits vacíos. El código existente aporta comportamiento y pruebas; está permitido cambiarlo de forma incompatible, documentando los cambios deliberados. CONTRACTS define F-1 y ACCEPTANCE sus casos/cargas; no es necesario resolver HTTP o SQL para implementar una operación pura.

## Fases y criterios de salida

| ID | Entrega y dependencia | Preparación necesaria | Criterio de salida |
|---|---|---|---|
| F-0 | Especificación y baseline. | Revisar decisiones adoptadas, C-01/C-02 y alcance de la primera entrega. | Trazabilidad y evidencia del prototipo con fallos/limitaciones registrados; casos de referencia concretos. |
| F-1 | Primera librería usable: secuencias en memoria. Depende de F-0. | Implementar CONTRACTS bajo P-03/P-06/P-08; TDD-01 a TDD-04 y partes secuenciales de TDD-05/06. | C-01A/C-03; V-01 a V-05, V-15/V-16/V-17/V-19 aplicables; primera medición de V-14. |
| F-2 | Coordinación completa y efectos. Depende de F-1. | Concretar instrucciones de P-09 y cuotas avanzadas de P-07; TDD-05/06. | C-01B/C-02 en memoria; V-06/V-07/V-18; cancelación, respuestas tardías y resolución explícitas. |
| F-3 | Persistencia, recuperación y esperas. Depende de F-2. | Codec/DDL/migraciones de SQLite y retención P-04/P-07; TDD-07/08/09. | C-04 y caídas de C-01B/C-02; V-08/V-09/V-12/V-15/V-18 durables; backend conforme. |
| F-4 | Servicio y módulos oficiales de integración. Depende de F-3 para perfil durable. | Rutas/DTOs/autenticación de P-05 bajo confianza ya definida P-08; TDD-10/11/12. | V-10/V-13/V-16; mismos C-01/C-02 por librería/HTTP; defaults publicados. |
| F-5 | Adopción, autoría y objetivos de rendimiento. Depende de F-4. | Contrastar dos casos reales P-01 y cerrar metas del despliegue P-07. | V-11/V-14, dos casos reales y extensión externa; manuales del producto entregado. |

P-02 separa la UI completa en otra entrega. F-1 define sus contratos y F-5 prueba un consumidor de autoría, sin obligar a construir un editor. Las mediciones y controles de acceso comienzan en F-1; no se difieren hasta F-5/F-4 respectivamente.

## Paquetes de trabajo y recorridos de referencia

Los fragmentos E-* y secuencias de [ARCHITECTURE](ARCHITECTURE.md#recorridos) muestran cómo comunicar los componentes. Son orientación de diseño; cada paquete debe concretar su contrato en TDD antes de escribir la implementación correspondiente.

### F-0 — cerrar la base de diseño

- Usar los fixtures C-01/C-02; incorporar datos reales cuando estén disponibles sin atribuirlos a las referencias.
- Revisar [comunicación y propiedad](ARCHITECTURE.md#comunicacion) con el escenario real: localizar host, operación, destino y cada punto de confirmación.
- Acordar qué comportamiento actual se conserva y qué cambia, especialmente joins, catálogo mutable y recuperación.
- Registrar baseline y matriz de features/servicios externos. Tratar fallos previos como trabajo identificado, no como resultados del diseño futuro.

Entrega revisable: trazabilidad del caso real a contratos y fases; decisiones necesarias para iniciar F-1 resueltas. No hace falta cerrar el framework visual ni optimizaciones futuras para describir el recorrido principal.

### F-1 — contrato público y primer recorrido completo

- Convertir identidades, descriptor, invocación y frontera async de CONTRACTS/E-01 en tipos y schemas ejecutables.
- Separar catálogo público de registro mutable de construcción; fijar lo resuelto en `PreparedWorkflow`.
- Implementar configuración integrada/sustituible de E-06 y validar recursos/capacidades.
- Separar `build`/`boot`/handle/`shutdown` desde el perfil en memoria; definir propietario y estado del lifecycle según E-09/E-10 y V-16.
- Construir C-01A: entrada → normalización → lectura simulada → salida; comprobar CT-01, bindings y diagnóstico por campo sin UI.
- Crear una extensión mínima fuera de los internos del engine para demostrar que los contratos bastan.
- Aplicar PAT-01/03/04/10 y V-17: descriptor de módulo, contribuciones explícitas, registro completo o rechazado y restricciones de protocolo/capacidades. Usar [PATTERNS](PATTERNS.md) como guía de revisión.
- Aplicar V-19: scopes del host, schema refs sin red, literal frente a selección, null frente a ausente y rechazo de exceso/capacidades no soportadas. Registrar primeras mediciones de ACCEPTANCE.

Entrega revisable: un recorrido usable desde Rust y una prueba de sustitución. No cerrar la fase únicamente porque existan nuevas carpetas y traits.

### F-2 — coordinación y efectos

- Representar trabajo listo y transiciones separadamente de la llamada asíncrona a una operación.
- Implementar ramas, joins, iteraciones y subworkflows con ámbito e identidad definidos.
- Conectar un adaptador de efecto controlado como E-05 y comprobar timeout, respuesta perdida y retorno tardío.
- Aplicar clasificación de retry antes de la política de demora E-07; probar cancelación y límites por intento/run.
- Aplicar PAT-02/05/06/08/09 y verificar que wrappers, políticas y subworkflows preserven identidad, clasificación de efectos y propiedad de las transiciones.
- Implementar `inspect_effect`/`reconcile` de TDD-06 y V-18 en memoria, con actor, evidencia, revisión esperada e idempotencia del comando. Medir C-02 con lotes antes de integrar persistencia.

Entrega revisable: recorridos que produzcan el resultado previsto bajo órdenes distintos de finalización y fallos. Las pruebas de efecto usan un destino simulado con registro verificable de solicitudes.

### F-3 — recuperación comprobable

- Concretar el puerto ilustrado por E-03, codec de checkpoint y garantías de transición atómica.
- Implementar backend seleccionado y recuperar el mismo recorrido de F-2 desde revisiones fijadas.
- Conectar recuperación y carga de definiciones al boot antes de habilitar admisión; comprobar codec/propiedad y limpieza de un inicio fallido.
- Inyectar reinicios antes/después de intención, efecto, resultado y habilitación del sucesor.
- Implementar señales/timers y resolver explícitamente el callback que llega antes de registrar una espera.
- Verificar artefactos retenidos, limpieza de huérfanos, incompatibilidad de revisiones y acuse de commit perdido.
- Repetir V-18 ante reinicio: auditoría y decisión atómicas; incertidumbre visible incluso si el operador cierra seguimiento. El servicio durable no cae a memoria por error de SQLite.

Entrega revisable: secuencias de recuperación y espera de ARCHITECTURE reproducidas por pruebas, con evidencia de que no se repiten pasos confirmados y de cómo se resuelven efectos inciertos.

### F-4 — servicio y composición operable

- Adaptar HTTP/JSON, archivos/CSV y proveedores de C-01/C-02 al protocolo probado por una extensión externa; agregar el destino real cuando P-01 lo identifique.
- Implementar handler delgado E-04 y traducciones de entrada/salida; fijar acceso y deduplicación de recepción.
- Implementar la raíz de composición y bootstrap E-09/E-10: cargar configuración, registrar módulos, cargar definiciones, iniciar una instancia y compartir el handle con los handlers.
- Verificar V-16 en el servicio: readiness después de boot/enlace, fallo de transporte con cierre del engine, supervisión y drenado; desacoplar vida del run de la conexión cliente.
- Conectar observación E-08 y consultas de estado; probar observador lento/fallido sin alterar resultados.
- Probar PAT-07 y conformidad V-17 de los módulos seleccionados: concurrencia entre runs y decoradores sin cambio de contrato.

Entrega revisable: la misma integración invocada por librería y servicio, con errores equivalentes y garantías de aceptación publicadas.

### F-5 — adopción y validación de eficiencia

- Contrastar las dos referencias con integraciones reales y demostrar una nueva extensión usando recetas públicas, sin cambios de negocio en el engine.
- Documentar versión/retiro de extensiones y comprobar recuperación con revisiones ausentes o conservadas, según V-15/V-17.
- Usar un consumidor de catálogo/validación para verificar autoría y round-trip; la UI completa es otra entrega según P-02.
- Comparar con las mediciones F-1/F-2/F-3 y verificar metas de producción acordadas, separando overhead de red.
- Reemplazar referencias del prototipo por ejemplos del producto entregado y documentar límites realmente comprobados.

Entrega revisable: casos reales acordados y guía breve que permita extender el motor siguiendo los contratos, con mediciones reproducibles y límites conocidos.

## Orden de refactorización dentro de una fase

1. Identificar contratos, comportamiento actual útil y cambios intencionales.
2. Concretar la firma/formato y ejemplos de aceptación en TDD.
3. Implementar un recorrido completo con pruebas observables.
4. Adaptar consumidores y módulos afectados; eliminar duplicación solo tras completar la sustitución.
5. Ejecutar checks apropiados, documentar evidencia y actualizar PROJECT.

No se exige conservar dos engines en producción ni crear capas paralelas indefinidamente. La transición puede usar adaptadores temporales con una condición clara de retiro.

## Verificación prevista

Baseline inicial: `cargo fmt --all --check`, `cargo test --workspace` y `cargo clippy --workspace --all-targets -- -D warnings`, registrando fallos existentes y requisitos del entorno. PROJECT conserva resultados del 2026-09-26. La CI existente también usa `--all-features` para tests/clippy y ejecuta por separado la prueba ignorada SFTP contra un servidor; un test local con features predeterminadas no acredita esas variantes.

Durante implementación, seleccionar pruebas de contratos y comportamiento afectado. Antes del cierre global, ejecutar la matriz acordada, paridad API/librería, recuperación con fallos inyectados y benchmarks representativos. Las verificaciones V-* están definidas en el TDD.

## Condición de cierre documental

La primera versión documental queda organizada cuando tiene autoridad, alcance, requisitos, arquitectura, mecanismos, pendientes y trazabilidad consistentes. Eso no equivale a cerrar todas las decisiones de implementación. Una fase no empieza a programarse con un P-* que contradiga o deje indeterminado su comportamiento.
