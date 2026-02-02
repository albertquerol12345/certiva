# Veri*Factu — Requisitos resumidos

## Concepto

Veri*Factu es el sistema voluntario de certificación de software de facturación que garantiza la integridad, conservación y trazabilidad de las facturas conforme a la Ley Crea y Crece.

## Exigencias principales

1. **Inalterabilidad**: cada factura se firma o sella digitalmente y se genera un código QR/CSV.  
2. **Trazabilidad**: debe guardarse el registro de eventos (emisión, modificación, anulación) sin posibilidad de borrado.  
3. **Remisión a AEAT**: posibilidad de envío automático (opcional, pero recomendable para PYMEs con bajo volumen).  
4. **Información mínima**: fecha/hora exacta, hash previo, dispositivo, responsable, motivo.

## Impacto para CERTIVA

- El pipeline debe conservar `doc_id`, `sha256` y metadatos de origen.  
- `audit` actúa como registro inmutable de acciones HITL.  
- Para facturas emitidas (AR), la reconciliación bancaria y los asientos deben enlazar con el código Veri*Factu.  
- Cualquier modificación posterior debe generar una rectificativa, nunca sobrescribir la original.
