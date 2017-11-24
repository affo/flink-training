### Traccia d'Esame

__NOTA:__

 * Puoi riusare il codice sviluppato ieri;
 * Usa la classe template `Exam` per la tua soluzione;
 * Devi usare il "tempo applicativo" per la tua soluzione;
 * Non preoccuparti della fault-tolerance.

Dei sensori posti nelle stanze di un edificio riportano continuamente valori per temperatura e
pressione nella seguente forma:

```
(
    timestamp,              # integer
    stanza,                 # string
    temperatura/pressione   # double
)
```

I record in uscita devono contenere, per ogni stanza, la pressione media negli ultimi 3 millisecondi e la massima
temperatura negli ultimi 5 millisecondi ogni millisecondo.

_BONUS:_

 * Come cambieresti la tua soluzione se ci fossero delle rilevazioni non valide (e.g. < 0)?
 * Puoi commentare riguardo alla fault-tolerance dell'applicazione?