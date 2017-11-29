## Exercise on Sensors

__NOTE:__

 * You can reuse the code already developed yesterday (or the one in `transformations`);
 * You must use `EventTime` to solve the exercise;
 * Do not worry about fault-tolerance.

Some sensors placed in a building report values for temperature and pressure in the form:

```
(
    timestamp,              # integer
    room,                   # string
    temperature/pressure    # double
)
```

The output records must track, for every room, the average pressure in the last 3 milliseconds and the maximum
temperature in the last 5 milliseconds every millisecond.

_BONUS:_

 * How do you change your solution if some measurements are invalid (e.g. below 0)?
 * Can you comment on the fault-tolerance of the application?

## Flink Official Training

We will focus on the official Flink training for streaming available at http://training.data-artisans.com/.

The installation instruction are available in the main [README file](/README.md).