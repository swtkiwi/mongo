'use strict';

var $config = (function() {
    const collSize = 10 * 1024 * 1024;
    const docSize = 8000;
    const numDocs = collSize / docSize;

    const states = {
        update: function update(db, collName) {
            let docId = Random.randInt(numDocs);
            assert.commandWorked(this.coll.update(
                {docNum: docId}, {$set: {time: ObjectId().getTimestamp()}, $inc: {numUpdates: 1}}));
        },

        changeSession: function changeSession(db, collName) {
            this.coll = db.getMongo()
                            .startSession({retryWrites: true})
                            .getDatabase(db.getName())
                            .getCollection(collName);
        },
    };

    function setup(db, collName, cluster) {
        let population = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        const coll = db.getCollection(collName);
        let val = [];
        jsTestLog({"DBG. Populating. NumDocs": numDocs});
        for (var stringNum = 0; stringNum < 8; ++stringNum) {
            let str = "";
            for (var stringLen = 0; stringLen < 1000; ++stringLen) {
                str += population[Random.randInt(population.length)];
            }
            val.push(str);
        }

        let bulk = coll.initializeUnorderedBulkOp();
        for (var docNum = 0; docNum < numDocs; ++docNum) {
            bulk.insert({time: ObjectId().getTimestamp(), val: val, docNum: docNum});
        }
        assert.commandWorked(bulk.execute());

        db.getCollection(collName).createIndexes([{docNum: 1}, {time: 1}, {numUpdates: 1}]);
    }

    function teardown(db, collName, cluster) {
        jsTestLog({"DBG. Validate": db.getCollection(collName).validate()});
        jsTestLog({"DBG. Stats": db.getCollection(collName).stats()});
    }

    const transitions = {
        update: {update: 1},
        changeSession: {update: 1},
    };

    return {
        threadCount: 10,
        iterations: 10090,
        startState: 'changeSession',
        states: states,
        transitions: transitions,
        setup: setup,
        teardown: teardown,
    };
})();