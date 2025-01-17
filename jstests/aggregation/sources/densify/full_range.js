/**
 * $densify tests with full range without partitions.
 * @tags: [
 *   # Needed as $densify is a 51 feature.
 *   requires_fcv_51,
 * ]
 */

load("jstests/aggregation/sources/densify/libs/densify_in_js.js");

(function() {
"use strict";
const featureEnabled =
    assert.commandWorked(db.adminCommand({getParameter: 1, featureFlagDensify: 1}))
        .featureFlagDensify.value;
if (!featureEnabled) {
    jsTestLog("Skipping test because the densify feature flag is disabled");
    return;
}
const collName = jsTestName();
const coll = db.getCollection(collName);
coll.drop();

// Run all tests for each date unit and on numeric values.
for (let i = 0; i < densifyUnits.length; i++) {
    const unit = densifyUnits[i];
    coll.drop();
    const base = unit ? new Date(2021, 0, 1) : 0;
    const {add} = getArithmeticFunctionsForUnit(unit);

    // Run all tests for different step values.
    for (let i = 0; i < interestingSteps.length; i++) {
        const step = interestingSteps[i];
        const runDensifyFullTest = (msg) =>
            testDensifyStage({field: "val", range: {step, bounds: "full", unit: unit}}, coll, msg);

        // Fill in docs between 1 and 99.
        coll.drop();
        coll.insert({val: base});
        coll.insert({val: add(base, 99)});
        runDensifyFullTest();

        // Negative numbers and dates before the epoch.
        coll.drop();
        insertDocumentsOnStep({base, min: -100, max: -1, step: 2, addFunc: add, coll: coll});
        runDensifyFullTest();

        // Lots of off-step documents.
        coll.drop();
        insertDocumentsOnPredicate(
            {base, min: 0, max: 50, pred: i => i % 3 == 0 || i % 7 == 0, addFunc: add, coll: coll});
        runDensifyFullTest();
    }
}
})();
