/**
 * This file implements densification in JavaScript to compare with the output from the $densify
 * stage.
 */
load("jstests/aggregation/extras/utils.js");  // arrayEq

/**
 * The code is made a lot shorter by relying on accessing properties on Date objects with
 * the object lookup syntax.
 * @param {String} unitName
 * @param {Number} factor
 * @returns functions to immutably add/subtract a specific duration with a date.
 */
const makeArithmeticHelpers = (unitName, factor) => {
    const getter = date => {
        const newDate = new Date(date);
        // Calling the proper function on the passed in date object. If the unitName was "Seconds"
        // would be equivalent to `newDate.getSeconds()`.
        return newDate["get" + unitName]();
    };

    // Return a new date with the proper unit adjusted with the second parameter.
    // Dates and the setter helpers are generally mutable, but this function will make sure
    // the arithmetic helpers won't mutate their inputs.
    const setter = (date, newComponent) => {
        const newDate = new Date(date);
        newDate["set" + unitName](newComponent);
        return newDate;
    };

    const add = (val, step) => setter(val, getter(val) + (step * factor));
    const sub = (val, step) => setter(val, getter(val) - (step * factor));

    // Explicit ranges always generate on-step relative to the lower-bound of the range,
    // this function encapsulates the logic to do that for dates (requires a loop since steps aren't
    // always constant sized).
    const getNextStepFromBase = (val, base, step) => {
        let nextStep = base;
        while (nextStep <= val) {
            nextStep = add(nextStep, step);
        }
        return nextStep;
    };

    return {add: add, sub: sub, getNextStepFromBase: getNextStepFromBase};
};

/**
 * This function specifies the functions for performing arithmetic on densify values. A
 * null/undefined unitName will return functions for numbers rather than dates.
 * @param {String | null} unitName
 */
const getArithmeticFunctionsForUnit = (unitName) => {
    switch (unitName) {
        case "millisecond":
            return makeArithmeticHelpers("Milliseconds", 1);
        case "second":
            return makeArithmeticHelpers("Milliseconds", 1000);
        case "minute":
            return makeArithmeticHelpers("Minutes", 1);
        case "hour":
            return makeArithmeticHelpers("Hours", 1);
        case "day":
            return makeArithmeticHelpers("Date", 1);
        case "week":
            return makeArithmeticHelpers("Date", 7);
        case "month":
            return makeArithmeticHelpers("Month", 1);
        case "quarter":
            return makeArithmeticHelpers("Month", 3);
        case "year":
            return makeArithmeticHelpers("FullYear", 1);
        case null:  // missing unit means that we're dealing with numbers rather than dates.
        case undefined:
            return {
                add: (val, step) => val + step,
                sub: (val, step) => val - step,
                getNextStepFromBase: (val, base, step) => {
                    let nextStep = base;
                    while (nextStep <= val) {
                        nextStep = nextStep + step;
                    }
                    return nextStep;
                }
            };
    }
};

function densifyInJS(stage, docs) {
    const field = stage.field;
    const {step, bounds, unit} = stage.range;
    const stream = [];

    docs.sort((a, b) => a[field] - b[field]);

    const {add, sub, getNextStepFromBase} = getArithmeticFunctionsForUnit(unit);

    function generateDocuments(min, max, pred) {
        const docs = [];
        while (min < max) {
            if (!pred || pred(min)) {
                docs.push({[field]: min});
            }
            min = add(min, step);
        }
        return docs;
    }

    if (bounds === "full") {
        if (docs.length == 0) {
            return stream;
        }
        return densifyInJS({
            field: stage.field,
            range: {step, unit, bounds: [docs[0][field], docs[docs.length - 1][field]]}
        },
                           docs);

    } else if (bounds === "partition") {
        throw new Error("Partitioning not supported by JS densify.");
    } else if (bounds.length == 2) {
        const [lower, upper] = bounds;
        let currentVal =
            docs.length > 0 ? Math.min(docs[0][field], sub(lower, step)) : sub(lower, step);
        for (let i = 0; i < docs.length; i++) {
            const nextVal = docs[i][field];
            stream.push(...generateDocuments(getNextStepFromBase(currentVal, lower, step),
                                             nextVal,
                                             (val) => val >= lower && val < upper));
            stream.push(docs[i]);
            currentVal = nextVal;
        }
        const lastVal = docs.length > 0 ? docs[docs.length - 1][field] : sub(lower, step);
        if (lastVal < upper) {
            stream.push(...generateDocuments(getNextStepFromBase(currentVal, lower, step), upper));
        }
    }
    return stream;
}

const genRange = (min, max) => {
    const result = [];
    for (let i = min; i < max; i++) {
        result.push(i);
    }
    return result;
};

const insertDocumentsFromOffsets = ({base, offsets, addFunc, coll, field}) =>
    offsets.forEach(num => coll.insert({[field || "val"]: addFunc(base, num)}));

const insertDocumentsOnPredicate = ({base, min, max, pred, addFunc, coll, field}) =>
    insertDocumentsFromOffsets(
        {base, offsets: genRange(min, max).filter(pred), addFunc, coll, field});

const insertDocumentsOnStep = ({base, min, max, step, addFunc, coll, field}) =>
    insertDocumentsOnPredicate(
        {base, min, max, pred: i => ((i - min) % step) === 0, addFunc, coll, field});

const densifyUnits = [null, "millisecond", "second", "day", "month", "quarter", "year"];

const interestingSteps = [1, 2, 3, 4, 5, 7, 11, 13];

function testDensifyStage(stage, coll, msg) {
    if (stage.range.unit === null) {
        delete stage.range.unit;
    }
    const result = coll.aggregate([{"$densify": stage}]).toArray();
    const expected = densifyInJS(stage, coll.find({}).toArray());
    const newMsg = (msg || "") + " | stage: " + tojson(stage);
    assert.eq(expected, result, newMsg);
}
