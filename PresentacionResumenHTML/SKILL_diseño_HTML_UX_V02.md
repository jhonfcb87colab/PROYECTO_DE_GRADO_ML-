# SKILL: Generador de Portales de Documentación y Conocimiento Minimalista (HTML/CSS/JS)

## 1. Propósito y Filosofía del Skill
El objetivo principal de este skill es **estructurar, documentar y divulgar conocimiento general, técnico o académico** de forma clara, directa e imparcial. 
* **Enfoque:** Divulgativo, educativo y documental (sin propósitos de venta, marketing ni oferta de servicios).
* **Experiencia de Usuario (UX):** Organización modular mediante pestañas y paneles dinámicos. Toda la información debe ser accesible sin saturar la pantalla principal ni obligar al usuario a hacer desplazamientos verticales infinitos.
* **Estética (UI):** Minimalismo puro, sobrio y elegante, basado en tonalidades azuladas suaves y frías con bordes estructurados de alto contraste visual pero sin estridencias.

---

## 2. Sistema de Diseño (Design System)

### Tipografía
* **Familia principal:** `Arial, Helvetica, sans-serif` (Limpia, neutra, legible y profesional, sin adornos innecesarios).
* **Estilos:** Títulos en peso semi-bold (`600`), cuerpo de texto en peso normal (`400`). Línea de altura (`line-height: 1.6`) para maximizar la legibilidad.

### Paleta de Colores Oficial (Gama Azules Pantone)
| Uso en la Interfaz | Código HEX | Descripción |
| :--- | :--- | :--- |
| **Fondos Principales** | `#FFFFFF`<br>`#F4F8FA` | Blanco puro / Azul Nieve ultra claro (para el lienzo general y contenedores principales). |
| **Pestañas y Ventanas (Superficies)** | `#E5EDF1`<br>`#F0F5F8` | Gris azulado claro / Bruma azulada (para diferenciar contenedores y pestañas inactivas). |
| **Marcos, Bordes y Acentos** | `#96C2DB`<br>`#5B9EC2` | Gris azulado medio / Azul acero (para límites de módulos, botones, separadores y estados activos). |
| **Texto y Lectura** | `#1C2833`<br>`#2C3E50` | Azul oscuro deeply contrastado (garantiza máxima legibilidad superando el contraste estándar). |

---

## 3. Reglas de Estructura e Interfaz (UX Rules)
1. **Navegación por Pestañas (Tabbed UI):** El contenido debe dividirse en módulos lógicos mediante pestañas horizontales superiores. Solo se muestra la pestaña activa.
2. **Ventanas Modales o Paneles Desplegables:** Para profundizar en detalles o referencias sin abandonar la vista actual ni recargar la página.
3. **Botones de Control Limpios:** Los botones de interacción deben ser sobrios, con bordes definidos utilizando las tonalidades de la paleta (`#96C2DB`, `#E5EDF1`) y estados *hover* sutiles.
4. **Sin Saturación Visual:** Distribución con espacio en blanco adecuado (*padding* generoso de 16px a 24px) y tarjetas con bordes suaves de 1px a 2px.

---

## 4. Plantilla Base de Código (Template HTML5 / CSS / JS)

Cada vez que este skill genere un archivo HTML, utilizará la siguiente arquitectura:

```html
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Documentación y Conocimiento</title>
    <style>
        /* Base y Tipografía Sobria */
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: Arial, Helvetica, sans-serif;
            background-color: #F4F8FA;
            color: #1C2833;
            line-height: 1.6;
            padding: 20px;
            display: flex;
            justify-content: center;
        }

        /* Contenedor Principal Minimalista */
        .main-container {
            width: 100%;
            max-width: 1000px;
            background-color: #FFFFFF;
            border: 2px solid #96C2DB;
            border-radius: 6px;
            padding: 24px;
            box-shadow: 0 4px 12px rgba(150, 194, 219, 0.15);
        }

        header {
            border-bottom: 2px solid #E5EDF1;
            padding-bottom: 16px;
            margin-bottom: 20px;
        }

        header h1 {
            font-size: 24px;
            font-weight: 600;
            color: #1C2833;
            margin-bottom: 6px;
        }

        header p {
            font-size: 14px;
            color: #4A607A;
        }

        /* Navegación por Pestañas */
        .tabs-nav {
            display: flex;
            gap: 8px;
            border-bottom: 2px solid #96C2DB;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }

        .tab-button {
            font-family: Arial, Helvetica, sans-serif;
            font-size: 14px;
            background-color: #E5EDF1;
            color: #1C2833;
            border: 1px solid #96C2DB;
            border-bottom: none;
            padding: 10px 18px;
            cursor: pointer;
            border-radius: 4px 4px 0 0;
            transition: background-color 0.2s ease, border-color 0.2s ease;
        }

        .tab-button:hover {
            background-color: #FFFFFF;
            border-color: #5B9EC2;
        }

        .tab-button.active {
            background-color: #FFFFFF;
            font-weight: bold;
            color: #1C2833;
            border: 2px solid #96C2DB;
            border-bottom: 2px solid #FFFFFF;
            margin-bottom: -2px;
        }

        /* Contenido de Paneles */
        .tab-content {
            display: none;
            background-color: #FFFFFF;
            border: 1px solid #E5EDF1;
            padding: 20px;
            border-radius: 4px;
        }

        .tab-content.active {
            display: block;
        }

        .tab-content h2 {
            font-size: 18px;
            color: #1C2833;
            margin-bottom: 12px;
            border-bottom: 2px solid #E5EDF1;
            padding-bottom: 4px;
        }

        /* Botones de Acción Internos */
        .btn-action {
            font-family: Arial, Helvetica, sans-serif;
            background-color: #E5EDF1;
            border: 1px solid #96C2DB;
            color: #1C2833;
            padding: 8px 14px;
            font-size: 13px;
            cursor: pointer;
            border-radius: 3px;
            margin-top: 10px;
            display: inline-block;
            transition: background-color 0.2s ease, color 0.2s ease;
        }

        .btn-action:hover {
            background-color: #96C2DB;
            color: #FFFFFF;
        }

        /* Panel Desplegable / Ventana Secundaria */
        .info-box {
            margin-top: 15px;
            background-color: #F0F5F8;
            border: 1px dashed #96C2DB;
            padding: 12px;
            font-size: 13px;
            display: none;
            border-radius: 4px;
        }
    </style>
</head>
<body>

<div class="main-container">
    <header>
        <h1>Compendio de Conocimiento</h1>
        <p>Documentación técnica y registro de información general.</p>
    </header>

    <!-- Barra de Pestañas -->
    <nav class="tabs-nav">
        <button class="tab-button active" onclick="switchTab(event, 'tab1')">1. Introducción</button>
        <button class="tab-button" onclick="switchTab(event, 'tab2')">2. Metodología</button>
        <button class="tab-button" onclick="switchTab(event, 'tab3')">3. Referencias</button>
    </nav>

    <!-- Contenido Pestaña 1 -->
    <section id="tab1" class="tab-content active">
        <h2>Introducción al Tema</h2>
        <p>Este espacio está destinado a la presentación objetiva de conceptos, fundamentos e información estructurada sin fines comerciales.</p>
        <button class="btn-action" onclick="toggleInfo('details-1')">Ver detalles complementarios</button>
        <div id="details-1" class="info-box">
            <p><strong>Nota técnica:</strong> Aquí se incluye información secundaria o notas aclaratorias que no recargan la vista principal.</p>
        </div>
    </section>

    <!-- Contenido Pestaña 2 -->
    <section id="tab2" class="tab-content">
        <h2>Metodología y Procesos</h2>
        <p>Descripción detallada de la estructura de datos, procedimientos y análisis aplicados.</p>
    </section>

    <!-- Contenido Pestaña 3 -->
    <section id="tab3" class="tab-content">
        <h2>Referencias y Fuentes</h2>
        <p>Recopilación de documentación, bibliografía y recursos para consulta pública.</p>
    </section>
</div>

<script>
    function switchTab(event, tabId) {
        const contents = document.querySelectorAll('.tab-content');
        contents.forEach(content => content.classList.remove('active'));

        const buttons = document.querySelectorAll('.tab-button');
        buttons.forEach(button => button.classList.remove('active'));

        document.getElementById(tabId).classList.add('active');
        event.currentTarget.classList.add('active');
    }

    function toggleInfo(infoId) {
        const box = document.getElementById(infoId);
        if (box.style.display === "block") {
            box.style.display = "none";
        } else {
            box.style.display = "block";
        }
    }
</script>

</body>
</html>
```