# Workflow Forge — módulos de integración F-4

Anexo normativo de TDD-02/10 para los módulos oficiales. Las garantías de ejecución siguen en [CONTRACTS](CONTRACTS.md); el transporte de entrada en [HTTP](HTTP.md). [PROJECT](PROJECT.md) distingue contrato de implementación verificada.

## 1. Composición, identidad y patrones

Factory Functions producen `OperationBundle` a partir de perfiles confiables del host. El builder los registra atómicamente como cualquier extensión. Cada operación adapta su cliente/parser al trait `Operation`; el engine no conoce reqwest, CSV ni rutas de archivos. Un Decorator conserva descriptor, contexto, InvocationId, AttemptId y effect key, y solo observa alrededor de `execute`. Las instancias comparten configuración/clientes; los datos de cada invocación son locales.

Los perfiles HTTP y de archivos fijan `name` y política. Exportan `forge.http.<name>` y `forge.files.<name>.read`, contrato `1`. La revisión de implementación se deriva de la versión del adaptador y SHA-256 de su perfil serializado. Cambiar URL, método, raíz, límites o referencia de secreto cambia la revisión; rotar el valor secreto no la cambia. Se obtiene la revisión exacta del catálogo/descriptores al crear la definición. El editor debe conservarla: no sustituirla por `latest`. Mantener revisiones antiguas exige conservar sus perfiles junto a los nuevos. El paquete del run continúa usando su revisión fijada.

Los perfiles no realizan requests, lecturas de archivos ni trabajo en segundo plano al registrarse. La construcción del cliente HTTP configura un pool compartido; la apertura del directorio de archivos ocurre al invocar la operación. Las rutas locales son de un filesystem administrado por el host. Instalar un módulo concede al proceso ese acceso; no convierte plugins compilados en código aislado.

## 2. HTTP/JSON

`HttpJsonProfile` fija nombre, URL absoluta HTTP(S), método, referencia opcional de bearer, plazo y presupuestos. No admite userinfo, query ni fragmentos en la URL configurada. El input tiene `query` como mapa de strings y, en escrituras, `body` JSON; no contiene URL, método, proxy ni cabeceras. GET omite body; POST/PUT/PATCH/DELETE admiten body opcional. El JSON enviado está acotado antes de despachar. La query se codifica con el cliente, con máximo 64 entradas y límites por clave/valor.

Defaults: 10 s, 1 MiB de request JSON, 4 MiB de respuesta. El cliente desactiva redirects, proxies implícitos y retries internos. El presupuesto de respuesta cuenta bytes realmente recibidos. Solo acepta respuestas 2xx con media type JSON; 204 produce body `null`. La salida es `{status, body}`. No devuelve las cabeceras de respuesta ni incluye el body remoto, URL o secreto en los mensajes de error.

GET declara `Read/Safe`; las escrituras, `Write/Unsafe`. Un fallo previo a despachar tiene certeza `NotApplied`. Después de despachar una escritura, timeout, desconexión, status no exitoso o respuesta que no se puede validar producen `Unknown`; el motor decide bloqueo/reconciliación conforme al contrato. El módulo genérico no asume garantías de idempotencia por la presencia de una cabecera ni declara un inspector de negocio. Un adaptador específico puede implementar `Keyed` e `EffectInspector` cuando su destino los respalde.

El bearer se obtiene de `OperationContext::secret` y se declara como `secret:<nombre>` en recursos. El host configura el destino permitido. Las operaciones sin secreto usan los permisos generales de preparación/ejecución de ese ámbito; no existe una ACL adicional por nombre de endpoint en este módulo.

## 3. Archivos y CSV

`FileReadProfile` fija raíz absoluta, nombre, media type y límite, 4 MiB por defecto. Input `{path}` relativo, máximo 1 024 bytes, sin componentes `..` ni rutas absolutas. La apertura usa un handle de directorio de `cap-std` para resolver dentro de la raíz, también frente a enlaces simbólicos. Solo se aceptan archivos regulares. La lectura se transmite en fragmentos al puerto de artefactos del contexto, bajo límite real de bytes; la salida es `ArtifactRef`. No publica rutas locales ni contenido como diagnóstico. Declara `Read/Safe` y recurso `artifacts`.

El filesystem es una fuente mutable: reintentar una lectura aún no confirmada puede observar contenido nuevo. Una referencia de artefacto publicada y confirmada conserva los bytes del paso. No se implementan escrituras arbitrarias al filesystem ni se promete cancelar una syscall del SO; los hosts deben usar almacenamiento local y archivos regulares bajo su control.

`csv_operations(CsvOptions)` exporta `forge.csv.read_batch`. Input `{source, offset?, size?}`, con `ArtifactRef` del mismo ámbito; la salida contiene `headers`, filas `{index, line, fields, valid_columns}` y `next_offset` nullable. El offset cuenta registros de datos, desde cero; `line` es la línea física reportada por el parser y respeta campos entrecomillados/multilínea. No interpreta SKU, cantidades, fechas ni reglas de negocio. Diferente número de columnas queda señalado en la fila; UTF-8, cabeceras o presupuestos inválidos rechazan la lectura.

Defaults: 4 MiB de fuente, 10 000 registros, 128 columnas, 16 KiB por campo; tamaño de lote 100, máximo 1 000. El módulo usa el parser `csv`, valida la fuente acotada y devuelve únicamente el lote solicitado. Esta primera implementación vuelve a leer/analizar la fuente acotada en cada lote; no se presenta como consumo constante para un archivo ilimitado. La concurrencia la limita el engine. Contenido de archivos/CSV se obtiene mediante `OperationContext`, sin acceso directo al store ni a tablas SQL. Un run durable requiere el proveedor coordinado de artefactos.

## 4. Verificación

Probar clientes reutilizados entre runs, configuración congelada, Decorator transparente, ausencia de redirects/retries, límite sin Content-Length, fallos de escritura con incertidumbre y mensajes sin credenciales. Para archivos: lectura dentro de raíz, rechazo de escape/symlink externo, límite y cancelación sin publicación parcial. Para CSV: comillas, multilinea, cabecera sola, UTF-8, columnas, límites y avance por lotes. Mantener reglas de C-01/C-02 en sus definiciones o extensiones de negocio.

Fuentes primarias: [reqwest ClientBuilder](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html), [política de retry desactivado](https://docs.rs/reqwest/latest/reqwest/retry/fn.never.html), [csv ReaderBuilder](https://docs.rs/csv/latest/csv/struct.ReaderBuilder.html), [cap-std Dir](https://docs.rs/cap-std/latest/cap_std/fs/struct.Dir.html). Estas APIs implementan mecanismos; las garantías del módulo son las definidas arriba y requieren las pruebas correspondientes.
